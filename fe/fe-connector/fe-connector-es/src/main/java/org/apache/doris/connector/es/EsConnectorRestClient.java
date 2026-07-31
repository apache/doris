// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.api.DorisConnectorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * HTTP REST client for Elasticsearch.
 * Adapted from fe-core's EsRestClient — no fe-core dependencies.
 * Uses Jackson directly instead of JsonUtil wrapper.
 */
public class EsConnectorRestClient {

    private static final Logger LOG = LogManager.getLogger(EsConnectorRestClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OkHttpClient PLAIN_CLIENT = new OkHttpClient.Builder()
            .readTimeout(10, TimeUnit.SECONDS).build();
    private static volatile OkHttpClient sslClient;

    private final String authHeader;
    private final String[] nodes;
    private final boolean httpSslEnable;
    private final ConnectorHttpSecurityHook securityHook;
    private final AtomicInteger nodeIndex = new AtomicInteger(0);

    public EsConnectorRestClient(String[] nodes, String user, String password,
            boolean httpSslEnable, ConnectorHttpSecurityHook securityHook) {
        this.nodes = nodes;
        this.httpSslEnable = httpSslEnable;
        this.securityHook = securityHook != null ? securityHook : ConnectorHttpSecurityHook.NOOP;
        this.authHeader = (user != null && !user.isEmpty()
                && password != null && !password.isEmpty())
                ? Credentials.basic(user, password) : null;
    }

    public EsConnectorRestClient(String[] nodes, String user, String password,
            boolean httpSslEnable) {
        this(nodes, user, password, httpSslEnable, ConnectorHttpSecurityHook.NOOP);
    }

    /** Health check — returns true if ES cluster is reachable. */
    public boolean health() {
        return execute("") != null;
    }

    /** List all indices (+ aliases). */
    public List<String> listTable(boolean includeHiddenIndex) {
        List<String> indices = getIndices(includeHiddenIndex).stream()
                .distinct().collect(Collectors.toList());
        getAliases().entrySet().stream()
                .filter(e -> indices.contains(e.getKey()))
                .flatMap(e -> e.getValue().stream())
                .distinct()
                .forEach(indices::add);
        return indices;
    }

    /** Check whether an index exists. */
    public boolean existIndex(String indexName) {
        String path = indexName + "/_mapping";
        OkHttpClient client = httpSslEnable ? getOrCreateSslClient() : PLAIN_CLIENT;
        String node = currentNode();
        try (Response response = executeResponse(client, node, path, null)) {
            return response.isSuccessful();
        } catch (IOException e) {
            LOG.warn("existIndex error", e);
            return false;
        }
    }

    /** Get shard routing for an index via _search_shards API. */
    public EsShardPartitions searchShards(String indexName) {
        String path = indexName + "/_search_shards";
        String searchShards = execute(path);
        if (searchShards == null) {
            throw new DorisConnectorException(
                    "request index [" + indexName + "] search_shards failure");
        }
        return EsShardPartitions.findShardPartitions(indexName, searchShards);
    }

    /** Discover HTTP-enabled nodes via _nodes/http API. */
    @SuppressWarnings("unchecked")
    public Map<String, EsNodeInfo> getHttpNodes() {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        if (nodesData == null) {
            return Collections.emptyMap();
        }
        Map<String, EsNodeInfo> nodesMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue(), httpSslEnable);
            if (node.hasHttp()) {
                nodesMap.put(node.getId(), node);
            }
        }
        return nodesMap;
    }

    /** Execute a GET request and extract a typed value by key from the JSON response. */
    @SuppressWarnings("unchecked")
    public <T> T get(String path, String key) {
        String response = execute(path);
        if (response == null) {
            return null;
        }
        try {
            Map<String, Object> map = MAPPER.readValue(response, Map.class);
            return (T) map.get(key);
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to parse ES response for [" + path + "]: " + e.getMessage());
        }
    }

    /** Get index mapping JSON string. */
    public String getMapping(String indexName) {
        String path = indexName + "/_mapping";
        String result = execute(path);
        if (result == null) {
            throw new DorisConnectorException("index[" + indexName + "] not found");
        }
        return result;
    }

    /** Execute a search query on the given index. */
    public String searchIndex(String indexName, String body) {
        String path = indexName + "/_search";
        RequestBody requestBody = null;
        if (body != null && !body.isEmpty()) {
            requestBody = RequestBody.create(body, MediaType.get("application/json"));
        }
        return executeWithBody(path, requestBody);
    }

    /** Generic REST passthrough for arbitrary paths. */
    public String executePassthrough(String path, String body) {
        RequestBody requestBody = null;
        if (body != null && !body.isEmpty()) {
            requestBody = RequestBody.create(body, MediaType.get("application/json"));
        }
        return executeWithBody(path, requestBody);
    }

    // ------------------------------------------------------------------
    // Internal HTTP methods
    // ------------------------------------------------------------------

    private List<String> getIndices(boolean includeHiddenIndex) {
        String indexes = execute("_cat/indices?h=index&format=json&s=index:asc");
        if (indexes == null) {
            throw new DorisConnectorException("get es indexes error");
        }
        List<String> ret = new ArrayList<>();
        try {
            ArrayNode jsonNodes = (ArrayNode) MAPPER.readTree(indexes);
            jsonNodes.forEach(json -> {
                String index = json.get("index").asText();
                if (includeHiddenIndex || !index.startsWith(".")) {
                    ret.add(index);
                }
            });
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to parse ES indices: " + e.getMessage());
        }
        return ret;
    }

    private Map<String, List<String>> getAliases() {
        String res = execute("_aliases");
        Map<String, List<String>> ret = new HashMap<>();
        if (res == null) {
            return ret;
        }
        try {
            JsonNode root = MAPPER.readTree(res);
            Iterator<Map.Entry<String, JsonNode>> elements = root.fields();
            while (elements.hasNext()) {
                Map.Entry<String, JsonNode> element = elements.next();
                JsonNode aliases = element.getValue().get("aliases");
                List<String> aliasList = new ArrayList<>();
                aliases.fieldNames().forEachRemaining(aliasList::add);
                if (!aliasList.isEmpty()) {
                    ret.put(element.getKey(), Collections.unmodifiableList(aliasList));
                }
            }
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to parse ES aliases: " + e.getMessage());
        }
        return ret;
    }

    private String execute(String path) {
        return executeWithBody(path, null);
    }

    private String executeWithBody(String path, RequestBody requestBody) {
        int retrySize = nodes.length * 3;
        DorisConnectorException lastException = null;
        OkHttpClient client = httpSslEnable ? getOrCreateSslClient() : PLAIN_CLIENT;
        for (int i = 0; i < retrySize; i++) {
            String node = currentNode();
            try (Response response = executeResponse(client, node, path, requestBody)) {
                if (response.isSuccessful()) {
                    return response.body().string();
                } else {
                    LOG.warn("ES request response code: {}, body: {}",
                            response.code(), response.message());
                    lastException = new DorisConnectorException(response.message());
                }
            } catch (IOException e) {
                LOG.warn("ES request node [{}] [{}] failure: {}", node, path, e);
                lastException = new DorisConnectorException(e.getMessage());
            }
            advanceNode();
        }
        if (lastException != null) {
            throw lastException;
        }
        return null;
    }

    private Response executeResponse(OkHttpClient client, String nodeAddress, String path,
            RequestBody requestBody) throws IOException {
        String currentUrl = nodeAddress.trim();
        if (!currentUrl.startsWith("http://") && !currentUrl.startsWith("https://")) {
            currentUrl = "http://" + currentUrl;
        }
        if (!currentUrl.endsWith("/")) {
            currentUrl = currentUrl + "/";
        }
        String url = currentUrl + path;
        try {
            securityHook.beforeRequest(url);
            Request.Builder reqBuilder = new Request.Builder();
            if (authHeader != null) {
                reqBuilder.addHeader("Authorization", authHeader);
            }
            Request request;
            if (requestBody != null) {
                request = reqBuilder.post(requestBody).url(url).build();
            } else {
                request = reqBuilder.get().url(url).build();
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("ES rest client request URL: {}", request.url().toString());
            }
            return client.newCall(request).execute();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            securityHook.afterRequest();
        }
    }

    private String currentNode() {
        return nodes[Math.floorMod(nodeIndex.get(), nodes.length)];
    }

    private void advanceNode() {
        nodeIndex.incrementAndGet();
    }

    private static synchronized OkHttpClient getOrCreateSslClient() {
        if (sslClient == null) {
            sslClient = new OkHttpClient.Builder()
                    .readTimeout(10, TimeUnit.SECONDS)
                    .sslSocketFactory(createSslSocketFactory(), new TrustAllCerts())
                    .hostnameVerifier((hostname, session) -> true)
                    .build();
        }
        return sslClient;
    }

    private static SSLSocketFactory createSslSocketFactory() {
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[]{new TrustAllCerts()}, new SecureRandom());
            return sc.getSocketFactory();
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to create SSL socket factory");
        }
    }

    private static class TrustAllCerts implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
