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

package org.apache.doris.datasource.es;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * For get es metadata by http/https.
 **/
public class EsRestClient {

    private static final Logger LOG = LogManager.getLogger(EsRestClient.class);
    private static final OkHttpClient networkClient = new OkHttpClient
            .Builder().readTimeout(10, TimeUnit.SECONDS).build();

    private static OkHttpClient sslNetworkClient;
    private final Request.Builder builder;
    private final String[] nodes;
    private String currentNode;
    private int currentNodeIndex = 0;
    private final boolean httpSslEnable;

    /**
     * For EsTable.
     **/
    public EsRestClient(String[] nodes, String authUser, String authPassword, boolean httpSslEnable) {
        this.nodes = nodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION, Credentials.basic(authUser, authPassword));
        }
        this.currentNode = nodes[currentNodeIndex];
        this.httpSslEnable = httpSslEnable;
    }

    public OkHttpClient getClient() {
        if (httpSslEnable) {
            return getOrCreateSslNetworkClient();
        }
        return networkClient;
    }

    private void selectNextNode() {
        currentNodeIndex++;
        // reroute, because the previously failed node may have already been restored
        if (currentNodeIndex >= nodes.length) {
            currentNodeIndex = 0;
        }
        currentNode = nodes[currentNodeIndex];
    }

    /**
     * Get http nodes.
     **/
    public Map<String, EsNodeInfo> getHttpNodes() throws DorisEsException {
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

    /**
     * Get mapping for indexName.
     */
    public String getMapping(String indexName) throws DorisEsException {
        String path = indexName + "/_mapping";
        String indexMapping = execute(path);
        if (indexMapping == null) {
            throw new DorisEsException("index[" + indexName + "] not found");
        }
        return indexMapping;
    }

    /**
     * Check whether index exist.
     **/
    public boolean existIndex(OkHttpClient httpClient, String indexName) {
        String path = indexName + "/_mapping";
        try (Response response = executeResponse(httpClient, path)) {
            if (response.isSuccessful()) {
                return true;
            }
        } catch (IOException e) {
            LOG.warn("existIndex error", e);
            return false;
        }
        return false;
    }

    /**
     * Get all index.
     **/
    public List<String> getIndices(boolean includeHiddenIndex) {
        String indexes = execute("_cat/indices?h=index&format=json&s=index:asc");
        if (indexes == null) {
            throw new DorisEsException("get es indexes error");
        }
        List<String> ret = new ArrayList<>();
        ArrayNode jsonNodes = JsonUtil.parseArray(indexes);
        jsonNodes.forEach(json -> {
            // es 7.17 has .geoip_databases, but _mapping response 400.
            String index = json.get("index").asText();
            if (includeHiddenIndex) {
                ret.add(index);
            } else {
                if (!index.startsWith(".")) {
                    ret.add(index);
                }
            }

        });
        return ret;
    }

    /**
     * Get all alias.
     **/
    public Map<String, List<String>> getAliases() {
        String res = execute("_aliases");
        Map<String, List<String>> ret = new HashMap<>();
        JsonNode root = JsonUtil.readTree(res);
        if (root == null) {
            return ret;
        }
        Iterator<Map.Entry<String, JsonNode>> elements = root.fields();
        while (elements.hasNext()) {
            Map.Entry<String, JsonNode> element = elements.next();
            JsonNode aliases = element.getValue().get("aliases");
            Iterator<String> aliasNames = aliases.fieldNames();
            if (aliasNames.hasNext()) {
                ret.put(element.getKey(), ImmutableList.copyOf(aliasNames));
            }
        }
        return ret;
    }

    /**
     * Returns the merge of index and alias
     **/
    public List<String> listTable(boolean includeHiddenIndex) {
        List<String> indices = getIndices(includeHiddenIndex).stream().distinct().collect(Collectors.toList());
        getAliases().entrySet().stream().filter(e -> indices.contains(e.getKey())).flatMap(e -> e.getValue().stream())
                .distinct().forEach(indices::add);
        return indices;
    }

    /**
     * Get Shard location.
     **/
    public EsShardPartitions searchShards(String indexName) throws DorisEsException {
        String path = indexName + "/_search_shards";
        String searchShards = execute(path);
        if (searchShards == null) {
            throw new DorisEsException("request index [" + indexName + "] search_shards failure");
        }
        return EsShardPartitions.findShardPartitions(indexName, searchShards);
    }

    public boolean health() {
        String res = execute("");
        return res != null;
    }

    /**
     * init ssl networkClient use lazy way
     **/
    private synchronized OkHttpClient getOrCreateSslNetworkClient() {
        if (sslNetworkClient == null) {
            sslNetworkClient = new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS)
                    .sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts())
                    .hostnameVerifier(new TrustAllHostnameVerifier()).build();
        }
        return sslNetworkClient;
    }

    private Response executeResponse(OkHttpClient httpClient, String path) throws IOException {
        currentNode = currentNode.trim();
        if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
            currentNode = "http://" + currentNode;
        }
        if (!currentNode.endsWith("/")) {
            currentNode = currentNode + "/";
        }
        String url = currentNode + path;
        try {
            SecurityChecker.getInstance().startSSRFChecking(url);
            Request request = builder.get().url(currentNode + path).build();
            if (LOG.isInfoEnabled()) {
                LOG.info("es rest client request URL: {}", request.url().toString());
            }
            return httpClient.newCall(request).execute();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    /**
     * execute request for specific path，it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    private String execute(String path) throws DorisEsException {
        // try 3 times for every node
        int retrySize = nodes.length * 3;
        DorisEsException scratchExceptionForThrow = null;
        OkHttpClient httpClient;
        if (httpSslEnable) {
            httpClient = getOrCreateSslNetworkClient();
        } else {
            httpClient = networkClient;
        }
        for (int i = 0; i < retrySize; i++) {
            // maybe should add HTTP schema to the address
            // actually, at this time we can only process http protocol
            // NOTE. currentNode may have some spaces.
            // User may set a config like described below:
            // hosts: "http://192.168.0.1:8200, http://192.168.0.2:8200"
            // then currentNode will be "http://192.168.0.1:8200", " http://192.168.0.2:8200"
            if (LOG.isTraceEnabled()) {
                LOG.trace("es rest client request URL: {}", currentNode + "/" + path);
            }
            try (Response response = executeResponse(httpClient, path)) {
                if (response.isSuccessful()) {
                    return response.body().string();
                } else {
                    LOG.warn("request response code: {}, body: {}", response.code(), response.message());
                    scratchExceptionForThrow = new DorisEsException(response.message());
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = new DorisEsException(e.getMessage());
            }
            selectNextNode();
        }
        LOG.warn("try all nodes [{}], no other nodes left", nodes);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
        return null;
    }

    public <T> T get(String q, String key) throws DorisEsException {
        return parseContent(execute(q), key);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(String response, String key) {
        Map<String, Object> map;
        try {
            map = JsonUtil.readValue(response, Map.class);
        } catch (IOException ex) {
            LOG.error("parse es response failure: [{}]", response);
            throw new DorisEsException(ex.getMessage());
        }
        return (T) (key != null ? map.get(key) : map);
    }

    /**
     * support https
     **/
    private static class TrustAllCerts implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private static class TrustAllHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    private static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory ssfFactory;
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] {new TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new DorisEsException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }
}
