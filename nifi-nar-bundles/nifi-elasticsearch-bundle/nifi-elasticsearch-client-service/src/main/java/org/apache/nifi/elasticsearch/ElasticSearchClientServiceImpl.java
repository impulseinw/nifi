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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticSearchClientServiceImpl extends AbstractControllerService implements ElasticSearchClientService {
    private ObjectMapper mapper;

    private static final List<PropertyDescriptor> properties;

    private RestClient client;

    private String url;
    private Charset responseCharset;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ElasticSearchClientService.HTTP_HOSTS);
        props.add(ElasticSearchClientService.USERNAME);
        props.add(ElasticSearchClientService.PASSWORD);
        props.add(ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE);
        props.add(ElasticSearchClientService.CONNECT_TIMEOUT);
        props.add(ElasticSearchClientService.SOCKET_TIMEOUT);
        props.add(ElasticSearchClientService.RETRY_TIMEOUT);
        props.add(ElasticSearchClientService.CHARSET);
        props.add(ElasticSearchClientService.SUPPRESS_NULLS);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            setupClient(context);
            responseCharset = Charset.forName(context.getProperty(CHARSET).getValue());

            // re-create the ObjectMapper in case the SUPPRESS_NULLS property has changed - the JsonInclude settings aren't dynamic
            mapper = new ObjectMapper();
            if (ALWAYS_SUPPRESS.getValue().equals(context.getProperty(SUPPRESS_NULLS).getValue())) {
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            }
        } catch (Exception ex) {
            getLogger().error("Could not initialize ElasticSearch client.", ex);
            throw new InitializationException(ex);
        }
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        this.client.close();
        this.url = null;
    }

    private void setupClient(ConfigurationContext context) throws MalformedURLException, InitializationException {
        final String hosts = context.getProperty(HTTP_HOSTS).evaluateAttributeExpressions().getValue();
        String[] hostsSplit = hosts.split(",[\\s]*");
        this.url = hostsSplit[0];
        final SSLContextService sslService =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        final Integer connectTimeout = context.getProperty(CONNECT_TIMEOUT).asInteger();
        final Integer readTimeout    = context.getProperty(SOCKET_TIMEOUT).asInteger();
        final Integer retryTimeout   = context.getProperty(RETRY_TIMEOUT).asInteger();

        HttpHost[] hh = new HttpHost[hostsSplit.length];
        for (int x = 0; x < hh.length; x++) {
            URL u = new URL(hostsSplit[x]);
            hh[x] = new HttpHost(u.getHost(), u.getPort(), u.getProtocol());
        }

        final SSLContext sslContext;
        try {
            sslContext = (sslService != null && (sslService.isKeyStoreConfigured() || sslService.isTrustStoreConfigured()))
                    ? sslService.createContext() : null;
        } catch (Exception e) {
            getLogger().error("Error building up SSL Context from the supplied configuration.", e);
            throw new InitializationException(e);
        }

        RestClientBuilder builder = RestClient.builder(hh)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    if (sslContext != null) {
                        httpClientBuilder.setSSLContext(sslContext);
                    }

                    if (username != null && password != null) {
                        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials(username, password));
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    return httpClientBuilder;
                })
                .setRequestConfigCallback(requestConfigBuilder -> {
                    requestConfigBuilder.setConnectTimeout(connectTimeout);
                    requestConfigBuilder.setSocketTimeout(readTimeout);
                    return requestConfigBuilder;
                })
                .setMaxRetryTimeoutMillis(retryTimeout);

        this.client = builder.build();
    }

    private Response runQuery(String endpoint, String query, String index, String type, Map<String, String> requestParameters) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(index)) {
            sb.append("/").append(index);
        }
        if (StringUtils.isNotBlank(type)) {
            sb.append("/").append(type);
        }

        sb.append(String.format("/%s", endpoint));

        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);

        try {
            return client.performRequest("POST", sb.toString(), requestParameters != null ? requestParameters : Collections.emptyMap(), queryEntity);
        } catch (Exception e) {
            throw new ElasticsearchError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseResponse(Response response) {
        final int code = response.getStatusLine().getStatusCode();

        try {
            if (code >= 200 && code < 300) {
                InputStream inputStream = response.getEntity().getContent();
                byte[] result = IOUtils.toByteArray(inputStream);
                inputStream.close();
                return (Map<String, Object>) mapper.readValue(new String(result, responseCharset), Map.class);
            } else {
                String errorMessage = String.format("ElasticSearch reported an error while trying to run the query: %s",
                        response.getStatusLine().getReasonPhrase());
                throw new IOException(errorMessage);
            }
        } catch (Exception ex) {
            throw new ElasticsearchError(ex);
        }
    }

    @Override
    public IndexOperationResponse add(IndexOperationRequest operation) {
        return bulk(Collections.singletonList(operation));
    }

    private String flatten(String str) {
        return str.replaceAll("[\\n\\r]", "\\\\n");
    }

    private String buildBulkHeader(IndexOperationRequest request) throws JsonProcessingException {
        String operation = request.getOperation().equals(IndexOperationRequest.Operation.Upsert)
                ? "update"
                : request.getOperation().getValue();
        return buildBulkHeader(operation, request.getIndex(), request.getType(), request.getId());
    }

    private String buildBulkHeader(String operation, String index, String type, String id) throws JsonProcessingException {
        Map<String, Object> header = new HashMap<String, Object>() {{
            put(operation, new HashMap<String, Object>() {{
                put("_index", index);
                if (StringUtils.isNotBlank(id)) {
                    put("_id", id);
                }
                if (StringUtils.isNotBlank(type)) {
                    put("_type", type);
                }
            }});
        }};

        return flatten(mapper.writeValueAsString(header));
    }

    protected void buildRequest(IndexOperationRequest request, StringBuilder builder) throws JsonProcessingException {
        String header = buildBulkHeader(request);
        builder.append(header).append("\n");
        switch (request.getOperation()) {
            case Index:
            case Create:
                String indexDocument = mapper.writeValueAsString(request.getFields());
                builder.append(indexDocument).append("\n");
                break;
            case Update:
            case Upsert:
                Map<String, Object> doc = new HashMap<String, Object>() {{
                    put("doc", request.getFields());
                    if (request.getOperation().equals(IndexOperationRequest.Operation.Upsert)) {
                        put("doc_as_upsert", true);
                    }
                }};
                String update = flatten(mapper.writeValueAsString(doc)).trim();
                builder.append(update).append("\n");
                break;
            case Delete:
                // nothing to do for Delete operations, it just needs the header
                break;
            default:
                throw new IllegalArgumentException(String.format("Unhandled Index Operation type: %s", request.getOperation().name()));
        }
    }

    @Override
    public IndexOperationResponse bulk(List<IndexOperationRequest> operations) {
        try {
            StringBuilder payload = new StringBuilder();
            for (final IndexOperationRequest or : operations) {
                buildRequest(or, payload);
            }

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(payload.toString());
            }
            HttpEntity entity = new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON);
            StopWatch watch = new StopWatch();
            watch.start();
            Response response = client.performRequest("POST", "/_bulk", Collections.emptyMap(), entity);
            watch.stop();

            String rawResponse = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response was: %s", rawResponse));
            }

            return IndexOperationResponse.fromJsonResponse(rawResponse);
        } catch (Exception ex) {
            throw new ElasticsearchError(ex);
        }
    }

    @Override
    public Long count(String query, String index, String type) {
        Response response = runQuery("_count", query, index, type, null);
        Map<String, Object> parsed = parseResponse(response);

        return ((Integer)parsed.get("count")).longValue();
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, String id) {
        return deleteById(index, type, Collections.singletonList(id));
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, List<String> ids) {
        try {
            StringBuilder sb = new StringBuilder();
            for (final String id : ids) {
                String header = buildBulkHeader("delete", index, type, id);
                sb.append(header).append("\n");
            }
            HttpEntity entity = new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON);
            StopWatch watch = new StopWatch();
            watch.start();
            Response response = client.performRequest("POST", "/_bulk", Collections.emptyMap(), entity);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for bulk delete: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8)));
            }

            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deleteByQuery(String query, String index, String type) {
        final StopWatch watch = new StopWatch();
        watch.start();
        Response response = runQuery("_delete_by_query", query, index, type, null);
        watch.stop();

        // check for errors in response
        parseResponse(response);

        return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> get(String index, String type, String id) {
        try {
            StringBuilder endpoint = new StringBuilder();
            endpoint.append(index);
            if (StringUtils.isNotBlank(type)) {
                endpoint.append("/").append(type);
            } else {
                endpoint.append("/_doc");
            }
            endpoint.append("/").append(id);
            Response response = client.performRequest("GET", endpoint.toString(), new BasicHeader("Content-Type", "application/json"));

            String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

            return (Map<String, Object>) mapper.readValue(body, Map.class).get("_source");
        } catch (Exception ex) {
            getLogger().error("", ex);
            return null;
        }
    }

    /*
     * In pre-7.X ElasticSearch, it should return just a number. 7.X and after they are returning a map.
     */
    @SuppressWarnings("unchecked")
    private int handleSearchCount(Object raw) {
        if (raw instanceof Number) {
            return Integer.parseInt(raw.toString());
        } else if (raw instanceof Map) {
            return (Integer)((Map<String, Object>)raw).get("value");
        } else {
            throw new ProcessException("Unknown type for hit count.");
        }
    }

    @Override
    public SearchResponse search(String query, String index, String type, Map<String, String> requestParameters) {
        try {
            final Response response = runQuery("_search", query, index, type, requestParameters);
            return buildSearchResponse(response);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public SearchResponse scroll(String scroll) {
        try {
            final HttpEntity scrollEntity = new NStringEntity(scroll, ContentType.APPLICATION_JSON);
            final Response response = client.performRequest("POST", "/_search/scroll", Collections.emptyMap(), scrollEntity);

            return buildSearchResponse(response);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String initialisePointInTime(String index, String keepAlive) {
        try {
            final Map<String, String> params = new HashMap<String, String>() {{
                if (StringUtils.isNotBlank(keepAlive)) {
                    put("keep_alive", keepAlive);
                }
            }};
            final Response response = client.performRequest("POST", index + "/_pit", params);
            final String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for initialising Point in Time: %s", body));
            }

            return (String) mapper.readValue(body, Map.class).get("id");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deletePointInTime(String pitId) {
        try {
            final HttpEntity pitEntity = new NStringEntity(String.format("{\"id\": \"%s\"}", pitId), ContentType.APPLICATION_JSON);

            final StopWatch watch = new StopWatch();
            watch.start();
            final Response response = client.performRequest("DELETE", "/_pit", Collections.emptyMap(), pitEntity);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for deleting Point in Time: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8))
                );
            }

            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deleteScroll(String scrollId) {
        try {
            final HttpEntity scrollBody = new NStringEntity(String.format("{\"scroll_id\": \"%s\"}", scrollId), ContentType.APPLICATION_JSON);

            final StopWatch watch = new StopWatch();
            watch.start();
            final Response response = client.performRequest("DELETE", "/_search/scroll", Collections.emptyMap(), scrollBody);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for deleting Scroll: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8))
                );
            }

            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private SearchResponse buildSearchResponse(final Response response) throws JsonProcessingException {
        final Map<String, Object> parsed = parseResponse(response);

        final int took = (Integer)parsed.get("took");
        final boolean timedOut = (Boolean)parsed.get("timed_out");
        final String pitId = parsed.get("pit_id") != null ? (String)parsed.get("pit_id") : null;
        final String scrollId = parsed.get("_scroll_id") != null ? (String)parsed.get("_scroll_id") : null;
        final Map<String, Object> aggregations = parsed.get("aggregations") != null
                ? (Map<String, Object>)parsed.get("aggregations") : new HashMap<>();
        final Map<String, Object> hitsParent = (Map<String, Object>)parsed.get("hits");
        final int count = handleSearchCount(hitsParent.get("total"));
        final List<Map<String, Object>> hits = (List<Map<String, Object>>)hitsParent.get("hits");
        final String searchAfter = getSearchAfter(hits);

        final SearchResponse esr = new SearchResponse(hits, aggregations, pitId, scrollId, searchAfter, count, took, timedOut);

        if (getLogger().isDebugEnabled()) {
            final String searchSummary = "******************" +
                    String.format("Took: %d", took) +
                    String.format("Timed out: %s", timedOut) +
                    String.format("Aggregation count: %d", aggregations.size()) +
                    String.format("Hit count: %d", hits.size()) +
                    String.format("PIT Id: %s", pitId) +
                    String.format("Scroll Id: %s", scrollId) +
                    String.format("Search After: %s", searchAfter) +
                    String.format("Total found: %d", count) +
                    "******************";
            getLogger().debug(searchSummary);
        }

        return esr;
    }

    private String getSearchAfter(final List<Map<String, Object>> hits) throws JsonProcessingException {
        String searchAfter = null;
        if (!hits.isEmpty()) {
            final Object lastHitSort = hits.get(hits.size() - 1).get("sort");
            if (lastHitSort != null && !"null".equalsIgnoreCase(lastHitSort.toString())) {
                searchAfter = mapper.writeValueAsString(lastHitSort);
            }
        }
        return searchAfter;
    }

    @Override
    public String getTransitUrl(String index, String type) {
        return this.url +
                (StringUtils.isNotBlank(index) ? "/" : "") +
                (StringUtils.isNotBlank(index) ? index : "") +
                (StringUtils.isNotBlank(type) ? "/" : "") +
                (StringUtils.isNotBlank(type) ? type : "");
    }
}
