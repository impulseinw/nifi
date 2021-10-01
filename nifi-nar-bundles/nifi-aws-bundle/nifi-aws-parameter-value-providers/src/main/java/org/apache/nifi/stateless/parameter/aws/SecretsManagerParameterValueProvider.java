/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.stateless.parameter.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stateless.parameter.AbstractParameterValueProvider;
import org.apache.nifi.stateless.parameter.ParameterValueProvider;
import org.apache.nifi.stateless.parameter.ParameterValueProviderInitializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Reads secrets from AWS Secrets Manager to provide parameter values.  Secrets must be created similar to the following AWS cli command: <br/><br/>
 * <code>aws secretsmanager create-secret --name "[Context]" --secret-string '{ "[Param]": "[secretValue]", "[Param2]": "[secretValue2]" }'</code> <br/><br/>
 *
 * A standard configuration for this provider would be: <br/><br/>
 *
 * <code>
 *      nifi.stateless.parameter.provider.AWSSecretsManager.name=AWS Secrets Manager Value Provider
 *      nifi.stateless.parameter.provider.AWSSecretsManager.type=org.apache.nifi.stateless.parameter.aws.SecretsManagerParameterValueProvider
 *      nifi.stateless.parameter.provider.AWSSecretsManager.properties.aws-credentials-file=./conf/bootstrap-aws.conf
 * </code>
 */
public class SecretsManagerParameterValueProvider extends AbstractParameterValueProvider implements ParameterValueProvider {
    private static final Logger logger = LoggerFactory.getLogger(SecretsManagerParameterValueProvider.class);

    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";
    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.access.key";
    private static final String REGION_KEY_PROPS_NAME = "aws.region";

    public static final PropertyDescriptor AWS_CREDENTIALS_FILE = new PropertyDescriptor.Builder()
            .displayName("AWS Credentials File")
            .name("aws-credentials-file")
            .required(false)
            .defaultValue("./conf/bootstrap-aws.conf")
            .description("Location of the bootstrap-aws.conf file that configures the AWS credentials.  If not provided, the default AWS credentials will be used.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor DEFAULT_SECRET_NAME = new PropertyDescriptor.Builder()
            .displayName("Default Secret Name")
            .name("default-secret-name")
            .required(true)
            .defaultValue("Default")
            .description("The default AWS secret name to use.  This secret represents a default Parameter Context if there is not a matching key within the mapped Parameter Context secret")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    private List<PropertyDescriptor> descriptors;

    private AWSSecretsManager secretsManager;

    private String defaultSecretName;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected void init(final ParameterValueProviderInitializationContext context) {
        super.init(context);

        this.descriptors = Collections.unmodifiableList(Arrays.asList(
                AWS_CREDENTIALS_FILE,
                DEFAULT_SECRET_NAME
        ));

        defaultSecretName = context.getProperty(DEFAULT_SECRET_NAME).getValue();
        final String awsCredentialsFilename = context.getProperty(AWS_CREDENTIALS_FILE).getValue();
        try {
            this.secretsManager = this.configureClient(awsCredentialsFilename);
        } catch (final IOException e) {
            throw new IllegalStateException("Could not configure AWS Secrets Manager Client", e);
        }
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        return getParameterValue(contextName, parameterName) != null;
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        final String contextBasedValue = getSecretValue(contextName, parameterName);
        return contextBasedValue != null ? contextBasedValue : getSecretValue(defaultSecretName, parameterName);
    }

    private String getSecretValue(final String secretName, final String keyName) {
        final GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        try {
            final GetSecretValueResult getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest);

            if (getSecretValueResult.getSecretString() == null) {
                logger.debug("Secret [{}] not configured", secretName);
                return null;
            }

            return parseParameterValue(getSecretValueResult.getSecretString(), keyName);
        } catch (final ResourceNotFoundException e) {
            logger.debug("Secret [{}] not found", secretName);
            return null;
        }
    }

    private String parseParameterValue(final String secretString, final String parameterName) {
        try {
            final JsonNode root = objectMapper.readTree(secretString);
            final JsonNode parameter = root.get(parameterName);
            if (parameter == null) {
                logger.debug("Parameter [{}] not found", parameterName);
                return null;
            }

            return parameter.textValue();
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Secret String for [%s] could not be parsed", parameterName), e);
        }
    }

    private Properties loadProperties(final String propertiesFilename) throws IOException {
        final Properties properties = new Properties();

        try (final InputStream in = new FileInputStream(Paths.get(propertiesFilename).toFile())) {
            properties.load(in);
            return properties;
        }
    }

    AWSSecretsManager configureClient(final String awsCredentialsFilename) throws IOException {
        if (awsCredentialsFilename == null) {
            return getDefaultClient();
        }
        final Properties properties = loadProperties(awsCredentialsFilename);
        final String accessKey = properties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKey = properties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = properties.getProperty(REGION_KEY_PROPS_NAME);

        if (isNotBlank(accessKey) && isNotBlank(secretKey) && isNotBlank(region)) {
            return AWSSecretsManagerClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .build();
        } else {
            return getDefaultClient();
        }
    }

    private AWSSecretsManager getDefaultClient() {
        return AWSSecretsManagerClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().equals("");
    }
}
