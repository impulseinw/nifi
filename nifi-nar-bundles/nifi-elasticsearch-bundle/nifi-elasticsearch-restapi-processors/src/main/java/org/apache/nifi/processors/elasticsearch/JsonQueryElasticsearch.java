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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.elasticsearch.api.JsonQueryParameters;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "application/json"),
    @WritesAttribute(attribute = "aggregation.name", description = "The name of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "aggregation.number", description = "The number of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "hit.count", description = "The number of hits that are in the output flowfile")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@EventDriven
@Tags({"elasticsearch", "elasticsearch 5", "query", "read", "get", "json"})
@CapabilityDescription("A processor that allows the user to run a query (with aggregations) written with the " +
        "Elasticsearch JSON DSL. It does not automatically paginate queries for the user. If an incoming relationship is added to this " +
        "processor, it will use the flowfile's content for the query. Care should be taken on the size of the query because the entire response " +
        "from Elasticsearch will be loaded into memory all at once and converted into the resulting flowfiles.")
public class JsonQueryElasticsearch extends AbstractJsonQueryElasticsearch<JsonQueryParameters> {
    @Override
    JsonQueryParameters buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session)
            throws IOException {

        final JsonQueryParameters jsonQueryParameters = new JsonQueryParameters();
        populateCommonJsonQueryParameters(jsonQueryParameters, input, context, session);
        return jsonQueryParameters;
    }

    @Override
    SearchResponse doQuery(final JsonQueryParameters queryJsonParameters, final List<FlowFile> hitsFlowFiles,
                           final ProcessSession session, final FlowFile input, final StopWatch stopWatch) throws IOException {
        final SearchResponse response = clientService.get().search(
                queryJsonParameters.getQuery(),
                queryJsonParameters.getIndex(),
                queryJsonParameters.getType(),
                null
        );
        if (input != null) {
            session.getProvenanceReporter().send(
                    input,
                    clientService.get().getTransitUrl(queryJsonParameters.getIndex(), queryJsonParameters.getType()),
                    stopWatch.getElapsed(TimeUnit.MILLISECONDS)
            );
        }

        handleResponse(response, true, queryJsonParameters, hitsFlowFiles, session, input, stopWatch);

        return response;
    }

    @Override
    void finishQuery(final FlowFile input, final JsonQueryParameters jsonQueryParameters, final ProcessSession session, final SearchResponse response) {
        if (input != null) {
            session.transfer(input, REL_ORIGINAL);
        }
    }
}
