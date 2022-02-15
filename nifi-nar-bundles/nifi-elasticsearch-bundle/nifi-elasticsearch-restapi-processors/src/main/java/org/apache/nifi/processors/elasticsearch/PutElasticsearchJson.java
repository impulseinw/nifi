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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "put", "index"})
@CapabilityDescription("An Elasticsearch put processor that uses the official Elastic REST client libraries.")
@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.put.error", description = "The error message provided by Elasticsearch if there is an error indexing the document.")
})
@DynamicProperty(
        name = "The name of a URL query parameter to add",
        value = "The value of the URL query parameter",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                "These parameters will override any matching parameters in the _bulk request body. " +
                "If FlowFiles are batched, only the first FlowFile in the batch is used to evaluate property values.")
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "The Batch of FlowFiles will be stored in memory until the bulk operation is performed.")
public class PutElasticsearchJson extends AbstractPutElasticsearch {
    static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("put-es-json-id-attr")
            .displayName("Identifier Attribute")
            .description("The name of the FlowFile attribute containing the identifier for the document. If the Index Operation is \"index\", "
                    + "this property may be left empty or evaluate to an empty value, in which case the document's identifier will be "
                    + "auto-generated by Elasticsearch. For all other Index Operations, the attribute must evaluate to a non-empty value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("put-es-json-charset")
        .displayName("Character Set")
        .description("Specifies the character set of the document data.")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(StandardCharsets.UTF_8.name())
        .required(true)
        .build();

    static final PropertyDescriptor OUTPUT_ERROR_DOCUMENTS = new PropertyDescriptor.Builder()
        .name("put-es-json-error-documents")
        .displayName("Output Error Documents")
        .description("If this configuration property is true, the response from Elasticsearch will be examined for failed documents " +
                "and the failed documents will be sent to the \"errors\" relationship.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    static final Relationship REL_FAILED_DOCUMENTS = new Relationship.Builder()
            .name("errors").description("If \"" + OUTPUT_ERROR_DOCUMENTS.getDisplayName() + "\" is set, " +
                    "any FlowFile that failed to process the way it was configured will be sent to this relationship " +
                    "as part of a failed document set.")
            .autoTerminateDefault(true).build();

    static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        ID_ATTRIBUTE, INDEX_OP, INDEX, TYPE, BATCH_SIZE, CHARSET, CLIENT_SERVICE, LOG_ERROR_RESPONSES, OUTPUT_ERROR_DOCUMENTS
    ));
    static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_SUCCESS, REL_FAILURE, REL_RETRY, REL_FAILED_DOCUMENTS
    )));

    private boolean outputErrors;
    private final ObjectMapper inputMapper = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        this.outputErrors = context.getProperty(OUTPUT_ERROR_DOCUMENTS).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final String idAttribute = context.getProperty(ID_ATTRIBUTE).getValue();

        final List<FlowFile> originals = new ArrayList<>(flowFiles.size());
        final List<IndexOperationRequest> operations = new ArrayList<>(flowFiles.size());

        for (FlowFile input : flowFiles) {
            final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(input).getValue();
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
            final String type = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();
            final String id = StringUtils.isNotBlank(idAttribute) ? input.getAttribute(idAttribute) : null;

            final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(input).getValue();

            try (final InputStream inStream = session.read(input)) {
                final byte[] result = IOUtils.toByteArray(inStream);
                @SuppressWarnings("unchecked")
                final Map<String, Object> contentMap = inputMapper.readValue(new String(result, charset), Map.class);

                final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(indexOp);
                operations.add(new IndexOperationRequest(index, type, id, contentMap, o));

                originals.add(input);
            } catch (final IOException ioe) {
                getLogger().error("Could not read FlowFile content valid JSON.", ioe);
                input = session.putAttribute(input, "elasticsearch.put.error", ioe.getMessage());
                session.penalize(input);
                session.transfer(input, REL_FAILURE);
            } catch (final Exception ex) {
                getLogger().error("Could not index documents.", ex);
                input = session.putAttribute(input, "elasticsearch.put.error", ex.getMessage());
                session.penalize(input);
                session.transfer(input, REL_FAILURE);
            }
        }

        if (!originals.isEmpty()) {
            try {
                final List<FlowFile> errorDocuments = indexDocuments(operations, originals, context);
                session.transfer(errorDocuments, REL_FAILED_DOCUMENTS);

                session.transfer(originals.stream().filter(f -> !errorDocuments.contains(f)).collect(Collectors.toList()), REL_SUCCESS);
            } catch (final ElasticsearchException ese) {
                final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                        ese.isElastic() ? "Routing to retry." : "Routing to failure");
                getLogger().error(msg, ese);
                final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
                transferFlowFilesOnException(ese, rel, session, true, originals.toArray(new FlowFile[0]));
            } catch (final JsonProcessingException jpe) {
                getLogger().warn("Could not log Elasticsearch operation errors nor determine which documents errored.", jpe);
                final Relationship rel = outputErrors ? REL_FAILED_DOCUMENTS : REL_FAILURE;
                transferFlowFilesOnException(jpe, rel, session, true, originals.toArray(new FlowFile[0]));
            } catch (final Exception ex) {
                getLogger().error("Could not index documents.", ex);
                transferFlowFilesOnException(ex, REL_FAILURE, session, false, originals.toArray(new FlowFile[0]));
                context.yield();
            }
        } else {
            getLogger().warn("No FlowFiles successfully parsed for sending to Elasticsearch");
        }
    }

    private List<FlowFile> indexDocuments(final List<IndexOperationRequest> operations, final List<FlowFile> originals, final ProcessContext context) throws JsonProcessingException {
        final IndexOperationResponse response = clientService.bulk(operations, getUrlQueryParameters(context, originals.get(0)));
        final List<FlowFile> errorDocuments = new ArrayList<>(response.getItems() == null ? 0 : response.getItems().size());
        if (response.hasErrors()) {
            logElasticsearchDocumentErrors(response);

            if (outputErrors) {
                findElasticsearchErrorIndices(response).forEach(index -> errorDocuments.add(originals.get(index)));
            }
        }
        return errorDocuments;
    }
}
