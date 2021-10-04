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
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.elasticsearch.api.PaginatedJsonQueryParameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "application/json"),
    @WritesAttribute(attribute = "aggregation.name", description = "The name of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "aggregation.number", description = "The number of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "page.number", description = "The number of the page (request) in which the results were returned that are in the output flowfile"),
    @WritesAttribute(attribute = "hit.count", description = "The number of hits that are in the output flowfile")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "elasticsearch 5", "query", "scroll", "page", "read", "json"})
@CapabilityDescription("A processor that allows the user to run a paginated query (with aggregations) written with the Elasticsearch JSON DSL. " +
        "It will use the flowfile's content for the query unless the QUERY attribute is populated. " +
        "Search After/Point in Time queries must include a valid \"sort\" field.")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Care should be taken on the size of each page because each response " +
        "from Elasticsearch will be loaded into memory all at once and converted into the resulting flowfiles.")
public class PaginatedJsonQueryElasticsearch extends AbstractPaginatedJsonQueryElasticsearch implements ElasticsearchRestProcessor {
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(QUERY);
        descriptors.addAll(paginatedPropertyDescriptors);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    void finishQuery(final FlowFile input, final PaginatedJsonQueryParameters paginatedQueryJsonParameters,
                     final ProcessSession session, final SearchResponse response) {
        session.transfer(input, REL_ORIGINAL);
    }

    @Override
    boolean isExpired(final PaginatedJsonQueryParameters paginatedQueryJsonParameters,
                      final ProcessSession session, final SearchResponse response) {
        // queries using input FlowFiles don't expire, they run until completion
        return false;
    }

    @Override
    String getScrollId(final ProcessSession session, final SearchResponse response) {
        return response != null ? response.getScrollId() : null;
    }

    @Override
    String getPitId(final ProcessSession session, final SearchResponse response) {
        return response != null ? response.getPitId() : null;
    }
}
