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

package org.apache.doris.external.elasticsearch;

import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class EsRestClient {

    private static final Logger LOG = LogManager.getLogger(EsRestClient.class);
    private ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    private static OkHttpClient networkClient = new OkHttpClient.Builder()
            .readTimeout(10, TimeUnit.SECONDS)
            .build();

    private static OkHttpClient sslNetworkClient;

    private Request.Builder builder;
    private String[] nodes;
    private String currentNode;
    private int currentNodeIndex = 0;
    private boolean httpSslEnable;

    public EsRestClient(String[] nodes, String authUser, String authPassword, boolean httpSslEnable) {
        this.nodes = nodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION,
                    Credentials.basic(authUser, authPassword));
        }
        this.currentNode = nodes[currentNodeIndex];
        this.httpSslEnable = httpSslEnable;
    }

    private void selectNextNode() {
        currentNodeIndex++;
        // reroute, because the previously failed node may have already been restored
        if (currentNodeIndex >= nodes.length) {
            currentNodeIndex = 0;
        }
        currentNode = nodes[currentNodeIndex];
    }

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
     * Get remote ES Cluster version
     *
     * @return
     * @throws Exception
     */
    public EsMajorVersion version() throws DorisEsException {
        Map<String, Object> result = get("/", null);
        if (result == null) {
            throw new DorisEsException("Unable to retrieve ES main cluster info.");
        }
        Map<String, String> versionBody = (Map<String, String>) result.get("version");
        return EsMajorVersion.parse(versionBody.get("number"));
    }

    /**
     * Get mapping for indexName
     *
     * @param indexName
     * @return
     * @throws Exception
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
     * Get Shard location
     *
     * @param indexName
     * @return
     * @throws DorisEsException
     */
    public EsShardPartitions searchShards(String indexName) throws DorisEsException {
        String path = indexName + "/_search_shards";
        String searchShards = execute(path);
        if (searchShards == null) {
            throw new DorisEsException("request index [" + indexName + "] search_shards failure");
        }
        return EsShardPartitions.findShardPartitions(indexName, searchShards);
    }

    /**
     * init ssl networkClient use lazy way
     **/
    private synchronized OkHttpClient getOrCreateSslNetworkClient() {
        if (sslNetworkClient == null) {
            sslNetworkClient = new OkHttpClient.Builder()
                    .readTimeout(10, TimeUnit.SECONDS)
                    .sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts())
                    .hostnameVerifier(new TrustAllHostnameVerifier())
                    .build();
        }
        return sslNetworkClient;
    }

    /**
     * execute request for specific path，it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    private String execute(String path) throws DorisEsException {
        int retrySize = nodes.length;
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
            currentNode = currentNode.trim();
            if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
                currentNode = "http://" + currentNode;
            }
            Request request = builder.get()
                    .url(currentNode + "/" + path)
                    .build();
            Response response = null;
            if (LOG.isTraceEnabled()) {
                LOG.trace("es rest client request URL: {}", currentNode + "/" + path);
            }
            try {
                response = httpClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body().string();
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = new DorisEsException(e.getMessage());
            } finally {
                if (response != null) {
                    response.close();
                }
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
        Map<String, Object> map = Collections.emptyMap();
        try {
            JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(response);
            map = mapper.readValue(jsonParser, Map.class);
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
            sc.init(null, new TrustManager[]{new TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new DorisEsException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }
}
