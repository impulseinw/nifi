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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.OperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.util.Map;

@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.delete.took", description = "The amount of time that it took to complete the delete operation in ms."),
        @WritesAttribute(attribute = "elasticsearch.delete.error", description = "The error message provided by Elasticsearch if there is an error running the delete.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({ "elastic", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "delete", "query"})
@CapabilityDescription("Delete from an Elasticsearch index using a query. The query can be loaded from a flowfile body " +
        "or from the Query parameter.")
@DynamicProperty(
        name = "The name of a URL query parameter to add",
        value = "The value of the URL query parameter",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                "These parameters will override any matching parameters in the query request body")
public class DeleteByQueryElasticsearch extends AbstractByQueryElasticsearch {
    static final String TOOK_ATTRIBUTE = "elasticsearch.delete.took";
    static final String ERROR_ATTRIBUTE = "elasticsearch.delete.error";

    @Override
    String getTookAttribute() {
        return TOOK_ATTRIBUTE;
    }

    @Override
    String getErrorAttribute() {
        return ERROR_ATTRIBUTE;
    }

    @Override
    OperationResponse performOperation(final ElasticSearchClientService clientService, final String query,
                                       final String index, final String type, final Map<String, String> requestParameters) {
        return clientService.deleteByQuery(query, index, type, requestParameters);
    }
}
