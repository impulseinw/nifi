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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.detect.EncodingDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


/**
 * <p>
 * Attempts to detect the MIME Type of a FlowFile by examining its contents. If the MIME Type is determined, it is added
 * to an attribute with the name mime.type. In addition, mime.extension is set if a common file extension is known.
 * </p>
 *
 * <p>
 * MIME Type detection is performed by Apache Tika; more information about detection is available at http://tika.apache.org.
 *
 * <ul>
 * <li>application/flowfile-v3</li>
 * <li>application/flowfile-v1</li>
 * </ul>
 * </p>
 */
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"compression", "gzip", "bzip2", "zip", "MIME", "mime.type", "file", "identify"})
@CapabilityDescription("Attempts to identify the MIME Type used for a FlowFile. If the MIME Type can be identified, "
        + "an attribute with the name 'mime.type' is added with the value being the MIME Type. If the MIME Type cannot be determined, "
        + "the value will be set to 'application/octet-stream'. In addition, the attribute 'mime.extension' will be set if a common file "
        + "extension for the MIME Type is known. If the MIME Type detected is of type text/*, attempts to identify the charset used " +
        "and an attribute with the name 'mime.charset' is added with the value being the charset.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "This Processor sets the FlowFile's mime.type attribute to the detected MIME Type. "
                + "If unable to detect the MIME Type, the attribute's value will be set to application/octet-stream"),
        @WritesAttribute(attribute = "mime.extension", description = "This Processor sets the FlowFile's mime.extension attribute to the file "
                + "extension associated with the detected MIME Type. "
                + "If there is no correlated extension, the attribute's value will be empty"),
        @WritesAttribute(attribute = "mime.charset", description = "This Processor sets the FlowFile's mime.charset attribute to the detected charset. "
                + "If unable to detect the charset or the detected MIME type is not of type text/*, the attribute's value will be empty")
}
)
public class IdentifyMimeType extends AbstractProcessor {
    static final AllowableValue PRESET = new AllowableValue("Preset", "Preset", "Use default NiFi MIME Types.");
    static final AllowableValue REPLACE = new AllowableValue("Replace", "Replace", "Use config MIME Types only.");
    static final AllowableValue MERGE = new AllowableValue("Merge", "Merge", "Use config together with default NiFi MIME Types.");

    public static final PropertyDescriptor USE_FILENAME_IN_DETECTION = new PropertyDescriptor.Builder()
            .displayName("Use Filename In Detection")
            .name("use-filename-in-detection")
            .description("If true will pass the filename to Tika to aid in detection.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor CONFIG_STRATEGY = new PropertyDescriptor.Builder()
            .displayName("Config Strategy")
            .name("config-strategy")
            .description("Select the loading strategy for MIME Type configuration to be used.")
            .required(true)
            .allowableValues(PRESET, REPLACE, MERGE)
            .defaultValue(PRESET.getValue())
            .build();

    public static final PropertyDescriptor MIME_CONFIG_FILE = new PropertyDescriptor.Builder()
            .displayName("Config File")
            .name("config-file")
            .required(false)
            .description("Path to MIME type config file. Only one of Config File or Config Body may be used.")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONFIG_STRATEGY, REPLACE, MERGE)
            .build();

    public static final PropertyDescriptor MIME_CONFIG_BODY = new PropertyDescriptor.Builder()
            .displayName("Config Body")
            .name("config-body")
            .required(false)
            .description("Body of MIME type config file. Only one of Config File or Config Body may be used.")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CONFIG_STRATEGY, REPLACE, MERGE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final TikaConfig config;
    private Detector detector;
    private EncodingDetector encodingDetector;
    private MimeTypes mimeTypes;

    public IdentifyMimeType() {
        this.config = TikaConfig.getDefaultConfig();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(USE_FILENAME_IN_DETECTION);
        properties.add(CONFIG_STRATEGY);
        properties.add(MIME_CONFIG_BODY);
        properties.add(MIME_CONFIG_FILE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        if (!config.hasProperty(CONFIG_STRATEGY)) {
            if (config.isPropertySet(MIME_CONFIG_FILE) || config.isPropertySet(MIME_CONFIG_BODY)) {
                config.setProperty(CONFIG_STRATEGY, REPLACE.getValue());
            }
        }
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        String configStrategy = context.getProperty(CONFIG_STRATEGY).getValue();

        if (configStrategy.equals(PRESET.getValue())) {
            this.detector = config.getDetector();
            this.mimeTypes = config.getMimeRepository();
        } else {
            setCustomMimeTypes(configStrategy, context);
        }

        this.encodingDetector = config.getEncodingDetector();
    }

    private void setCustomMimeTypes(String configStrategy, ProcessContext context) throws IOException {
        String configBody = context.getProperty(MIME_CONFIG_BODY).getValue();
        String configFile = context.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().getValue();

        try (final InputStream customInputStream = configBody != null ? new ByteArrayInputStream(configBody.getBytes()) : new FileInputStream(configFile)) {
            if (configStrategy.equals(REPLACE.getValue())) {
                this.detector = MimeTypesFactory.create(customInputStream);
            } else {
                try (final InputStream nifiInputStream = getClass().getClassLoader().getResourceAsStream("org/apache/tika/mime/custom-mimetypes.xml");
                     final InputStream tikaInputStream = MimeTypes.class.getClassLoader().getResourceAsStream("org/apache/tika/mime/tika-mimetypes.xml")) {
                    this.detector = MimeTypesFactory.create(customInputStream, nifiInputStream, tikaInputStream);
                }
            }
            this.mimeTypes = (MimeTypes) this.detector;
        } catch (Exception e) {
            context.yield();
            String configSource = configBody != null ? "body" : "file";
            throw new ProcessException("Failed to load config " + configSource, e);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String mimeType = identifyMimeType(context, session, flowFile);
        final String extension = lookupExtension(mimeType, logger);

        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            flowFile = session.putAttribute(flowFile, "mime.extension", "");
            flowFile = session.putAttribute(flowFile, "mime.charset", "");
            logger.info("Unable to identify MIME Type for {}; setting to application/octet-stream", flowFile);
        } else {
            final Charset charset = identifyCharset(context, session, flowFile, mimeType);

            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
            flowFile = session.putAttribute(flowFile, "mime.extension", extension);
            flowFile = session.putAttribute(flowFile, "mime.charset", charset == null ? "" : charset.name());
            logger.info("Identified {} as having MIME Type {}", flowFile, mimeType);
        }

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private String identifyMimeType(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        session.read(flowFile, stream -> {
            try (final InputStream in = new BufferedInputStream(stream);
                 final TikaInputStream tikaStream = TikaInputStream.get(in)) {
                Metadata metadata = new Metadata();

                if (filename != null && context.getProperty(USE_FILENAME_IN_DETECTION).asBoolean()) {
                    metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, filename);
                }
                // Get mime type
                MediaType mediatype = detector.detect(tikaStream, metadata);

                mimeTypeRef.set(mediatype.toString());
            }
        });

        return mimeTypeRef.get();
    }

    private String lookupExtension(String mimeType, ComponentLog logger) {
        String extension = "";
        try {
            MimeType mimetype;
            mimetype = mimeTypes.forName(mimeType);
            extension = mimetype.getExtension();
        } catch (MimeTypeException e) {
            logger.warn("MIME type extension lookup failed", e);
        }

        // Workaround for bug in Tika - https://issues.apache.org/jira/browse/TIKA-1563
        if (mimeType != null && mimeType.equals("application/gzip") && extension.equals(".tgz")) {
            extension = ".gz";
        }
        return extension;
    }

    private Charset identifyCharset(ProcessContext context, ProcessSession session, FlowFile flowFile, String mimeType) {
        if (!mimeType.startsWith("text/")) {
            // only mime-types text/* have a charset parameter
            return null;
        }

        final AtomicReference<Charset> charsetRef = new AtomicReference<>(null);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        session.read(flowFile, stream -> {
            try (final InputStream in = new BufferedInputStream(stream);
                 final TikaInputStream tikaStream = TikaInputStream.get(in)) {
                Metadata metadata = new Metadata();
                metadata.add(HttpHeaders.CONTENT_TYPE, mimeType);
                if (filename != null && context.getProperty(USE_FILENAME_IN_DETECTION).asBoolean()) {
                    metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, filename);
                }

                // Get charset
                Charset charset = encodingDetector.detect(tikaStream, metadata);

                charsetRef.set(charset);
            }
        });

        return charsetRef.get();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        String body = validationContext.getProperty(MIME_CONFIG_BODY).getValue();
        String file = validationContext.getProperty(MIME_CONFIG_FILE).getValue();
        if (!validationContext.getProperty(CONFIG_STRATEGY).getValue().equals(PRESET.getValue())) {
            if (body != null && file != null) {
                results.add(new ValidationResult.Builder()
                        .subject(MIME_CONFIG_FILE.getDisplayName())
                        .input(file)
                        .valid(false)
                        .explanation("Either [Config Body] or [Config File] can be specified, but not both properties.")
                        .build());
            } else if (body == null && file == null) {
                results.add(new ValidationResult.Builder()
                        .subject(MIME_CONFIG_FILE.getDisplayName())
                        .valid(false)
                        .explanation("Either [Config Body] or [Config File] must be specified")
                        .build());
            }
        }
        return results;
    }

}
