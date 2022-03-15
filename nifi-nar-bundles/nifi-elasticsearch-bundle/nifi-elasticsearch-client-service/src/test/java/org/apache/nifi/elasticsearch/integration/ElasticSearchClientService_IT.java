/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.elasticsearch.MapBuilder;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.elasticsearch.UpdateOperationResponse;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.MockVariableRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ElasticSearchClientService_IT extends AbstractElasticsearch_IT {
    @AfterEach
    void after() throws Exception {
        service.onDisabled();
    }

    private String prettyJson(final Object o) throws JsonProcessingException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(o);
    }

    private Map<PropertyDescriptor, String> getClientServiceProperties() {
        return ((MockControllerServiceLookup) runner.getProcessContext().getControllerServiceLookup())
                .getControllerServices().get(CLIENT_SERVICE_NAME).getProperties();
    }

    @Test
    void testVerifySuccess() {
        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), new MockVariableRegistry()),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(3, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
    }

    @Test
    void testVerifyFailedURL() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "invalid");

        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), new MockVariableRegistry()),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(2, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CLIENT_SETUP)
                        && Objects.equals(result.getExplanation(), "Incorrect/invalid " + ElasticSearchClientService.HTTP_HOSTS.getDisplayName())
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyFailedSSL() throws Exception {
        runner.disableControllerService(service);
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService("SSL Context", sslContextService);
        runner.setProperty(service, ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE, "SSL Context");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "not/a/file");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "ignored");
        try {
            runner.enableControllerService(sslContextService);
        } catch (final Exception ignored) {
            // expected, ignore
        }

        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), new MockVariableRegistry()),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(2, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CLIENT_SETUP)
                        && Objects.equals(result.getExplanation(), "Incorrect/invalid " + ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE.getDisplayName())
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyFailedAuth() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "invalid");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, "not-real");

        final List<ConfigVerificationResult> results = ((VerifiableControllerService) service).verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), new MockVariableRegistry()),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(3, results.size());
        assertEquals(1, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        assertEquals(1, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CONNECTION)
                        && Objects.equals(result.getExplanation(), "Unable to retrieve system summary from Elasticsearch root endpoint")
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testBasicSearch() throws Exception {
        final Map<String, Object> temp = new MapBuilder()
            .of("size", 10, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                    "aggs", new MapBuilder()
                            .of("term_counts", new MapBuilder()
                                    .of("terms", new MapBuilder()
                                            .of("field", "msg", "size", 5)
                                            .build())
                                    .build())
                            .build())
                .build();
        final String query = prettyJson(temp);

        final SearchResponse response = service.search(query, INDEX, type, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(10, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNull(response.getPitId(), "Unexpected pitId");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertNotNull(buckets, "Buckets branch was empty");
        final Map<String, Object> expected = new MapBuilder()
                .of("one", 1, "two", 2, "three", 3,
                        "four", 4, "five", 5)
                .build();

        buckets.forEach( aggRes -> {
            final String key = (String) aggRes.get("key");
            final Integer docCount = (Integer) aggRes.get("doc_count");
            assertEquals(expected.get(key), docCount, "${key} did not match.");
        });
    }

    @Test
    void testBasicSearchRequestParameters() throws Exception {
        final Map<String, Object> temp = new MapBuilder()
                .of("size", 10, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                                .of("terms", new MapBuilder()
                                        .of("field", "msg", "size", 5)
                                        .build())
                                .build())
                        .build())
                .build();
        final String query = prettyJson(temp);


        final SearchResponse response = service.search(query, "messages", type, createParameters("preference", "_local"));
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(10, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertNotNull(buckets, "Buckets branch was empty");
        final Map<String, Object> expected = new MapBuilder()
                .of("one", 1, "two", 2, "three", 3,
                        "four", 4, "five", 5)
                .build();

        buckets.forEach( (aggRes) -> {
            final String key = (String) aggRes.get("key");
            final Integer docCount = (Integer) aggRes.get("doc_count");
            assertEquals(expected.get(key), docCount, String.format("%s did not match.", key));
        });
    }

    @Test
    void testV6SearchWarnings() throws JsonProcessingException {
        assumeTrue(getElasticMajorVersion() == 6, "Requires Elasticsearch 6");
        final String query = prettyJson(new MapBuilder()
                .of("size", 1,
                        "query", new MapBuilder().of("query_string",
                                new MapBuilder().of("query", 1, "all_fields", true).build()
                        ).build())
                .build());
        final String type = "a-type";
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertFalse(response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Test
    void testV7SearchWarnings() throws JsonProcessingException {
        assumeTrue(getElasticMajorVersion() == 7, "Requires Elasticsearch 7");
        final String query = prettyJson(new MapBuilder()
                .of("size", 1, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .build());
        final String type = "a-type";
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertFalse(response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Disabled("Skip until Elasticsearch 8.x has a _search API deprecation")
    @Test
    void testV8SearchWarnings() {
        assumeTrue(getElasticMajorVersion() == 8, "Requires Elasticsearch 8");
        fail("Elasticsearch 8 search API deprecations not currently available for test");
    }

    @Test
    void testScroll() throws JsonProcessingException {
        final String query = prettyJson(new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                                .of("terms", new MapBuilder()
                                        .of("field", "msg", "size", 5)
                                        .build())
                                .build())
                        .build())
                .build());

        // initiate the scroll
        final SearchResponse response = service.search(query, INDEX, type, Collections.singletonMap("scroll", "10s"));
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(response.getScrollId(), "ScrollId missing");
        assertNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNull(response.getPitId(), "Unexpected pitId");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // scroll the next page
        final Map<String, String> parameters = createParameters("scroll_id", response.getScrollId(), "scroll", "10s");
        final SearchResponse scrollResponse = service.scroll(prettyJson(parameters));
        assertNotNull(scrollResponse, "Scroll Response was null");

        assertEquals(15, scrollResponse.getNumberOfHits(), "Wrong count");
        assertFalse(scrollResponse.isTimedOut(), "Timed out");
        assertNotNull(scrollResponse.getHits(), "Hits was null");
        assertEquals(2, scrollResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(scrollResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, scrollResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(scrollResponse.getScrollId(), "ScrollId missing");
        assertNull(scrollResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(scrollResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(scrollResponse.getHits(), response.getHits(), "Same results");

        // delete the scroll
        DeleteOperationResponse deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");
        assertTrue(deleteResponse.getTook() > 0);

        // delete scroll again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");
        assertEquals(0L, deleteResponse.getTook());
    }

    @Test
    void testSearchAfter() throws JsonProcessingException {
        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder()
                        .of("match_all", new HashMap<>()).build(), "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                            .of("terms", new MapBuilder()
                                    .of("field", "msg", "size", 5)
                                    .build())
                            .build()).build())
                .of("sort", Collections.singletonList(
                        new MapBuilder().of("msg", "desc").build()
                ))
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Search_After missing");
        assertNull(response.getPitId(), "Unexpected pitId");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // search the next page
        final Map<String, Object> page2QueryMap = new HashMap<>(queryMap);
        page2QueryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        page2QueryMap.remove("aggs");
        final String secondPage = prettyJson(page2QueryMap);
        final SearchResponse secondResponse = service.search(secondPage, INDEX, type, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(secondResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");
    }

    @Test
    void testPointInTime() throws JsonProcessingException {
        // Point in Time only available in 7.10+ with XPack enabled
        final double majorVersion = getElasticMajorVersion();
        final double minorVersion = getElasticMinorVersion();
        assumeTrue(majorVersion >= 8 || (majorVersion == 7 && minorVersion >= 10), "Requires version 7.10+");

        // initialise
        final String pitId = service.initialisePointInTime(INDEX, "10s");

        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .of("aggs", new MapBuilder().of("term_counts", new MapBuilder()
                        .of("terms", new MapBuilder()
                                .of("field", "msg", "size", 5)
                                .build())
                        .build()).build())
                .of("sort", Collections.singletonList(
                        new MapBuilder().of("msg", "desc").build()
                ))
                .of("pit", new MapBuilder()
                        .of("id", pitId, "keep_alive", "10s")
                        .build())
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, null, type, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(response.getPitId(), "pitId missing");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // search the next page
        final Map<String, Object> page2QueryMap = new HashMap<>(queryMap);
        page2QueryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        page2QueryMap.remove("aggs");
        final String secondPage = prettyJson(page2QueryMap);
        final SearchResponse secondResponse = service.search(secondPage, null, type, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(secondResponse.getPitId(), "pitId missing");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");

        // delete pitId
        DeleteOperationResponse deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");
        assertTrue(deleteResponse.getTook() > 0);

        // delete pitId again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");
        assertEquals(0L, deleteResponse.getTook());
    }

    @Test
    void testDeleteByQuery() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "five").build())
                                .build()).build());
        final DeleteOperationResponse response = service.deleteByQuery(query, INDEX, type, null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testDeleteByQueryRequestParameters() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "six").build())
                        .build()).build());
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        final DeleteOperationResponse response = service.deleteByQuery(query, INDEX, type, parameters);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testUpdateByQuery() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        final UpdateOperationResponse response = service.updateByQuery(query, INDEX, type, null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testUpdateByQueryRequestParameters() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        parameters.put("slices", "1");
        final UpdateOperationResponse response = service.updateByQuery(query, INDEX, type, parameters);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1";
        final Map<String, Object> originalDoc = service.get(INDEX, type, ID, null);
        try {
            final DeleteOperationResponse response = service.deleteById(INDEX, type, ID, null);
            assertNotNull(response);
            assertTrue(response.getTook() > 0);
            final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () ->
                service.get(INDEX, type, ID, null));
            assertTrue(ee.isNotFound());
            final Map<String, Object> doc = service.get(INDEX, type, "2", null);
            assertNotNull(doc);
        } finally {
            // replace the deleted doc
            service.add(new IndexOperationRequest(INDEX, type, "1", originalDoc, IndexOperationRequest.Operation.Index), null);
            waitForIndexRefresh(); // (affects later tests using _search or _bulk)
        }
    }

    @Test
    void testGet() {
        for (int index = 1; index <= 15; index++) {
            final String id = String.valueOf(index);
            final Map<String, Object> doc = service.get(INDEX, type, id, null);
            assertNotNull(doc, "Doc was null");
            assertNotNull(doc.get("msg"), "${doc.toString()}\t${doc.keySet().toString()}");
        }
    }

    @Test
    void testGetNotFound() {
        final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, type, "not_found", null));
        assertTrue(ee.isNotFound());
    }

    @Test
    void testExists() {
        assertTrue(service.exists(INDEX, null), "index does not exist");
        assertFalse(service.exists("index-does-not-exist", null), "index exists");
    }

    @Test
    void testNullSuppression() throws InterruptedException {
        final Map<String, Object> doc = new HashMap<>();
        doc.put("msg", "test");
        doc.put("is_null", null);
        doc.put("is_empty", "");
        doc.put("is_blank", " ");
        doc.put("empty_nested", Collections.emptyMap());
        doc.put("empty_array", Collections.emptyList());

        // index with nulls
        suppressNulls(false);
        IndexOperationResponse response = service.bulk(Collections.singletonList(new IndexOperationRequest("nulls", type, "1", doc, IndexOperationRequest.Operation.Index)), null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        Map<String, Object> result = service.get("nulls", type, "1", null);
        assertEquals(doc, result);

        // suppress nulls
        suppressNulls(true);
        response = service.bulk(Collections.singletonList(new IndexOperationRequest("nulls", type, "2", doc, IndexOperationRequest.Operation.Index)), null);
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        result = service.get("nulls", type, "2", null);
        assertTrue(result.keySet().containsAll(Arrays.asList("msg", "is_blank")), "Non-nulls (present): " + result);
        assertFalse(result.containsKey("is_null"), "is_null (should be omitted): " + result);
        assertFalse(result.containsKey("is_empty"), "is_empty (should be omitted): " + result);
        assertFalse(result.containsKey("empty_nested"), "empty_nested (should be omitted): " + result);
        assertFalse(result.containsKey("empty_array"), "empty_array (should be omitted): " + result);
    }

    private void suppressNulls(final boolean suppressNulls) {
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "Client Service");
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, suppressNulls
                ? ElasticSearchClientService.ALWAYS_SUPPRESS.getValue()
                : ElasticSearchClientService.NEVER_SUPPRESS.getValue());
        runner.enableControllerService(service);
        runner.assertValid();
    }

    @Test
    void testBulkAddTwoIndexes() throws Exception {
        final List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            final String index = x % 2 == 0 ? "bulk_a": "bulk_b";
            payload.add(new IndexOperationRequest(index, type, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", type, String.valueOf(x), new HashMap<String, Object>(){{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index));
        }
        final IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);
        assertTrue(response.getTook() > 0);
        waitForIndexRefresh();

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        final String query = "{ \"query\": { \"match_all\": {}}}";
        final Long indexA = service.count(query, "bulk_a", type, null);
        final Long indexB = service.count(query, "bulk_b", type, null);
        final Long indexC = service.count(query, "bulk_c", type, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        final Long total = service.count(query, "bulk_*", type, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testBulkRequestParameters() {
        final List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            final String index = x % 2 == 0 ? "bulk_a": "bulk_b";
            payload.add(new IndexOperationRequest(index, type, String.valueOf(x), new MapBuilder().of("msg", "test").build(), IndexOperationRequest.Operation.Index));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", type, String.valueOf(x), new MapBuilder().of("msg", "test").build(), IndexOperationRequest.Operation.Index));
        }
        final IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);
        assertTrue(response.getTook() > 0);

        /*
         * Now, check to ensure that both indexes got populated and refreshed appropriately.
         */
        final String query = "{ \"query\": { \"match_all\": {}}}";
        final Long indexA = service.count(query, "bulk_a", type, null);
        final Long indexB = service.count(query, "bulk_b", type, null);
        final Long indexC = service.count(query, "bulk_c", type, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        final Long total = service.count(query, "bulk_*", type, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testUpdateAndUpsert() throws InterruptedException {
        final String TEST_ID = "update-test";
        final Map<String, Object> doc = new HashMap<>();
        doc.put("msg", "Buongiorno, mondo");
        service.add(new IndexOperationRequest(INDEX, type, TEST_ID, doc, IndexOperationRequest.Operation.Index), createParameters("refresh", "true"));
        Map<String, Object> result = service.get(INDEX, type, TEST_ID, null);
        assertEquals(doc, result, "Not the same");

        final Map<String, Object> updates = new HashMap<>();
        updates.put("from", "john.smith");
        final Map<String, Object> merged = new HashMap<>();
        merged.putAll(updates);
        merged.putAll(doc);
        IndexOperationRequest request = new IndexOperationRequest(INDEX, type, TEST_ID, updates, IndexOperationRequest.Operation.Update);
        service.add(request, createParameters("refresh", "true"));
        result = service.get(INDEX, type, TEST_ID, null);
        assertTrue(result.containsKey("from"));
        assertTrue(result.containsKey("msg"));
        assertEquals(merged, result, "Not the same after update.");

        final String UPSERTED_ID = "upsert-ftw";
        final Map<String, Object> upsertItems = new HashMap<>();
        upsertItems.put("upsert_1", "hello");
        upsertItems.put("upsert_2", 1);
        upsertItems.put("upsert_3", true);
        request = new IndexOperationRequest(INDEX, type, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert);
        service.add(request, createParameters("refresh", "true"));
        result = service.get(INDEX, type, UPSERTED_ID, null);
        assertEquals(upsertItems, result);

        final List<IndexOperationRequest> deletes = new ArrayList<>();
        deletes.add(new IndexOperationRequest(INDEX, type, TEST_ID, null, IndexOperationRequest.Operation.Delete));
        deletes.add(new IndexOperationRequest(INDEX, type, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete));
        assertFalse(service.bulk(deletes, createParameters("refresh", "true")).hasErrors());
        waitForIndexRefresh(); // wait 1s for index refresh (doesn't prevent GET but affects later tests using _search or _bulk)
        ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, type, TEST_ID, null) );
        assertTrue(ee.isNotFound());
        ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, type, UPSERTED_ID, null));
        assertTrue(ee.isNotFound());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetBulkResponsesWithErrors() {
        final List<IndexOperationRequest> ops = Arrays.asList(
                new IndexOperationRequest(INDEX, type, "1", new MapBuilder().of("msg", "one", "intField", 1).build(), IndexOperationRequest.Operation.Index), // OK
                new IndexOperationRequest(INDEX, type, "2", new MapBuilder().of("msg", "two", "intField", 1).build(), IndexOperationRequest.Operation.Create), // already exists
                new IndexOperationRequest(INDEX, type, "1", new MapBuilder().of("msg", "one", "intField", "notaninteger").build(), IndexOperationRequest.Operation.Index) // can't parse int field
        );
        final IndexOperationResponse response = service.bulk(ops, createParameters("refresh", "true"));
        assertTrue(response.hasErrors());
        assertEquals(2, response.getItems().stream().filter(it -> {
            final Optional<String> first = it.keySet().stream().findFirst();
            if (first.isPresent()) {
                final String key = first.get();
                return ((Map<String, Object>) it.get(key)).containsKey("error");
            } else {
                throw new IllegalStateException("Cannot find index response operation items");
            }
        }).count());
    }

    private Map<String, String> createParameters(final String... extra) {
        if (extra.length % 2 == 1) { //Putting this here to help maintainers catch stupid bugs before they happen
            throw new RuntimeException("createParameters must have an even number of String parameters.");
        }

        final Map<String, String> parameters = new HashMap<>();
        for (int index = 0; index < extra.length; index += 2) {
            parameters.put(extra[index], extra[index + 1]);
        }

        return parameters;
    }

    private static void waitForIndexRefresh() throws InterruptedException {
        Thread.sleep(1000);
    }
}