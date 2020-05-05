/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.azure.cosmos.document;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDBConnectionService;

public abstract class AbstractAzureCosmosDBProcessor extends AbstractProcessor {
    static final String CONSISTENCY_STRONG = "STRONG";
    static final String CONSISTENCY_BOUNDED_STALENESS= "BOUNDED_STALENESS";
    static final String CONSISTENCY_SESSION = "SESSION";
    static final String CONSISTENCY_CONSISTENT_PREFIX = "CONSISTENT_PREFIX";
    static final String CONSISTENCY_EVENTUAL = "EVENTUAL";

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
    .description("All FlowFiles that are written to Azure Cosmos DB are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
    .description("All FlowFiles that cannot be written to Azure Cosmos DB are routed to this relationship").build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("All input FlowFiles that are part of a successful query execution go here.")
            .build();

    static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-connection-service")
        .displayName("Azure Cosmos DB Connection Service")
        .description("If configured, this property will use the assigned for retrieving connection string info.")
        .required(false)
        .identifiesControllerService(AzureCosmosDBConnectionService.class)
        .build();


    static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-uri")
        .displayName("Azure Cosmos DB URI")
        .description("Azure Cosmos DB URI, typically of the form: https://{databaseaccount}.documents.azure.com:443/. Note this host URL is for Azure Cosmos DB with CORE SQL API")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor DB_ACCESS_KEY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-key")
        .displayName("Azure Cosmos DB Access Key")
        .description("Azure Cosmos DB Access Key from Azure Portal (Settings->Keys)")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-name")
        .displayName("Azure Cosmos DB Name")
        .description("The name of the database to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor CONTAINER_ID = new PropertyDescriptor.Builder()
        .name("azure-cosmos-container-id")
        .displayName("Azure Cosmos Container ID")
        .description("Azure Cosmos DB Container Id to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor CONSISTENCY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-consistency-level")
        .displayName("Azure Cosmos DB Consistency Level")
        .description("Azure Cosmos DB Consistency Level to use")
        .required(false)
        .defaultValue(CONSISTENCY_SESSION)
        .allowableValues(CONSISTENCY_STRONG, CONSISTENCY_BOUNDED_STALENESS, CONSISTENCY_SESSION,
                CONSISTENCY_CONSISTENT_PREFIX, CONSISTENCY_EVENTUAL)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor PARTITION_KEY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-partition-key")
        .displayName("Azure Cosmos Partition Key")
        .description("Partition Key Field Name defined during Azure Cosmos Container Creation Time")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("charactor-set")
        .displayName("Charactor Set")
        .description("The Character Set in which the data is encoded")
        .required(false)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    static final List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(CONNECTION_SERVICE);
        _temp.add(URI);
        _temp.add(DB_ACCESS_KEY);
        _temp.add(CONSISTENCY);
        _temp.add(DATABASE_NAME);
        _temp.add(CONTAINER_ID);
        _temp.add(PARTITION_KEY);
        descriptors = Collections.unmodifiableList(_temp);
    }

    protected CosmosClient cosmosClient;
    protected CosmosContainer container;
    protected AzureCosmosDBConnectionService connectionService = null;

    @OnScheduled
    public void createClient(ProcessContext context) throws NullPointerException, CosmosClientException {
        final ComponentLog logger = getLogger();

        if (context.getProperty(CONNECTION_SERVICE).isSet()) {
            connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(AzureCosmosDBConnectionService.class);
            this.cosmosClient = this.connectionService.getCosmosClient();
        } else {
            final String uri, accessKey;
            uri =  context.getProperty(URI).getValue();
            accessKey = context.getProperty(DB_ACCESS_KEY).getValue();
            final ConsistencyLevel clevel;
            final String selectedConsistency = context.getProperty(CONSISTENCY).getValue();

            switch(selectedConsistency) {
                case CONSISTENCY_STRONG:
                    clevel =  ConsistencyLevel.STRONG;
                    break;
                case CONSISTENCY_CONSISTENT_PREFIX:
                    clevel = ConsistencyLevel.CONSISTENT_PREFIX;
                    break;
                case CONSISTENCY_SESSION:
                    clevel = ConsistencyLevel.SESSION;
                    break;
                case CONSISTENCY_BOUNDED_STALENESS:
                    clevel = ConsistencyLevel.BOUNDED_STALENESS;
                    break;
                case CONSISTENCY_EVENTUAL:
                    clevel = ConsistencyLevel.EVENTUAL;
                    break;
                default:
                    clevel = ConsistencyLevel.SESSION;
            }
            if (cosmosClient != null) {
                closeClient();
            }
            if(logger.isDebugEnabled()) {
                logger.debug("Creating CosmosClient");
            }

            createCosmosClient(uri, accessKey, clevel);
        }
        getCosmosDocumentContainer(context);
    }

    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel clevel) throws NullPointerException {
        try {
            ConnectionPolicy connectionPolicy = ConnectionPolicy.getDefaultPolicy();
            this.cosmosClient = new CosmosClientBuilder()
                .endpoint(uri)
                .key(accessKey)
                .connectionPolicy(connectionPolicy)
                .consistencyLevel(clevel)
                .buildClient();
        } catch (NullPointerException e) {
            getLogger().error("Failed to build cosmosClient due to {}",  e);
            throw e;
        }
    }

    protected abstract void doPostActionOnSchedule(final ProcessContext context);

    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosClientException {
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        final String containerID = context.getProperty(CONTAINER_ID).getValue();
        final String partitionKey = context.getProperty(PARTITION_KEY).getValue();

        CosmosDatabase database = this.cosmosClient.createDatabaseIfNotExists(databaseName).getDatabase();

        CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerID, "/"+partitionKey);

        //  Create container with 1000 RU/s by default if Not exists
        this.container = database.createContainerIfNotExists(containerProperties, 1000).getContainer();
        doPostActionOnSchedule(context);
    }

    @OnStopped
    public final void closeClient() {
        final ComponentLog logger = getLogger();
        if (connectionService == null && cosmosClient != null) {
            // close client only when cosmoclient is created in Processor.
            if(logger.isDebugEnabled()) {
                logger.debug("Closing CosmosClient");
            }
            try{
                this.container = null;
                this.cosmosClient.close();
            }catch(Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                this.cosmosClient = null;

            }
        }
    }

    protected String getURI(final ProcessContext context) {
        if (connectionService != null) {
            return connectionService.getURI();
        } else {
            return context.getProperty(URI).getValue();
        }
    }

    protected String getAccessKey(final ProcessContext context) {
        if (connectionService != null) {
            return connectionService.getAccessKey();
        } else {
            return context.getProperty(DB_ACCESS_KEY).getValue();
        }
    }
    protected String getConsistencyLevel(final ProcessContext context) {
        final String consistencyProperty = context.getProperty(CONSISTENCY).getValue();
        return consistencyProperty;
    }

    protected void writeBatch(String payload, FlowFile parent, ProcessContext context, ProcessSession session,
                              Map<String, String> extraAttributes, Relationship rel) {

        try {
            String charset = context.getProperty(CHARACTER_SET).getValue();

            FlowFile flowFile = parent != null ? session.create(parent) : session.create();
            flowFile = session.importFrom(new ByteArrayInputStream(payload.getBytes(charset)), flowFile);
            if(extraAttributes != null) {
                flowFile = session.putAllAttributes(flowFile, extraAttributes);
            }
            session.getProvenanceReporter().receive(flowFile, getURI(context));
            session.transfer(flowFile, rel);

        }catch(Exception e) {
            getLogger().error("Exception in writeBatch: "+ e.getMessage(), e);
        }
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> retVal = new ArrayList<>();

        boolean connectionServiceIsSet = context.getProperty(CONNECTION_SERVICE).isSet();
        boolean uriIsSet    = context.getProperty(URI).isSet();
        boolean accessKeyIsSet    = context.getProperty(DB_ACCESS_KEY).isSet();
        boolean databaseIsSet = context.getProperty(DATABASE_NAME).isSet();
        boolean collectionIsSet = context.getProperty(CONTAINER_ID).isSet();
        boolean partitionIsSet = context.getProperty(PARTITION_KEY).isSet();

        if (connectionServiceIsSet && (uriIsSet || accessKeyIsSet) ) {
            final String msg = String.format(
                "%s and %s with %s fields cannot be set at the same time.",
                AbstractAzureCosmosDBProcessor.CONNECTION_SERVICE.getDisplayName(),
                AbstractAzureCosmosDBProcessor.URI.getDisplayName(),
                AbstractAzureCosmosDBProcessor.DB_ACCESS_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        } else if (!connectionServiceIsSet && (!uriIsSet || !accessKeyIsSet)) {
            final String msg = String.format(
                "Either %s or %s with %s must be set",
                AbstractAzureCosmosDBProcessor.CONNECTION_SERVICE.getDisplayName(),
                AbstractAzureCosmosDBProcessor.URI.getDisplayName(),
                AbstractAzureCosmosDBProcessor.DB_ACCESS_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if(!databaseIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.DATABASE_NAME.getDisplayName() + " must be set.";
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if(!collectionIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.CONTAINER_ID.getDisplayName() + " must be set.";
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if(!partitionIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.PARTITION_KEY.getDisplayName() + " must be set.";
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        return retVal;
    }
}
