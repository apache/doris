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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Column;

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
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public abstract class DorisRestClient {
    private static final Logger LOG = LogManager.getLogger(DorisRestClient.class);

    private static final OkHttpClient networkClient = new OkHttpClient
            .Builder().readTimeout(10, TimeUnit.SECONDS).build();

    private static OkHttpClient sslNetworkClient;
    private final Request.Builder builder;
    private final List<String> feNodes;
    private String currentNode;
    private int currentNodeIndex = 0;
    private final boolean httpSslEnable;

    /**
     * For DorisTable.
     **/
    public DorisRestClient(List<String> feNodes, String authUser, String authPassword, boolean httpSslEnable) {
        this.feNodes = feNodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION,
                    Credentials.basic(authUser, Strings.isEmpty(authPassword) ? "" : authPassword));
        }
        this.currentNode = feNodes.get(currentNodeIndex);
        this.httpSslEnable = httpSslEnable;
    }

    public abstract List<String> getDatabaseNameList();

    public abstract List<String> getTablesNameList(String dbName);

    public abstract boolean isTableExist(String dbName, String tblName);

    public abstract boolean health();

    public abstract List<Column> getColumns(String dbName, String tableName);

    private void selectNextNode() {
        currentNodeIndex++;
        currentNodeIndex = currentNodeIndex % feNodes.size();
        currentNode = feNodes.get(currentNodeIndex);
    }

    public OkHttpClient getClient() {
        if (httpSslEnable) {
            return getOrCreateSslNetworkClient();
        }
        return networkClient;
    }

    /**
     * init ssl networkClient use lazy way
     **/
    private synchronized OkHttpClient getOrCreateSslNetworkClient() {
        if (sslNetworkClient == null) {
            sslNetworkClient = new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS)
                .sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts())
                .hostnameVerifier(new DorisRestClient.TrustAllHostnameVerifier()).build();
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
        Request request = builder.get().url(currentNode + path).build();
        if (LOG.isInfoEnabled()) {
            LOG.info("doris rest client request URL: {}", request.url().toString());
        }
        return httpClient.newCall(request).execute();
    }

    /**
     * execute request for specific path，it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    protected String execute(String path) {
        // try 3 times for every node
        int retrySize = feNodes.size() * 3;
        RuntimeException scratchExceptionForThrow = null;
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
                LOG.trace("doris rest client request URL: {}", currentNode + "/" + path);
            }
            try (Response response = executeResponse(httpClient, path)) {
                if (response.isSuccessful()) {
                    return response.body().string();
                } else {
                    LOG.warn("request response code: {}, body: {}", response.code(), response.message());
                    scratchExceptionForThrow = new RuntimeException(response.message());
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = new RuntimeException(e.getMessage());
            }
            selectNextNode();
        }
        LOG.warn("try all nodes [{}], no other nodes left", feNodes);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
        return null;
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
            sc.init(null, new TrustManager[] {new DorisRestClient.TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }
}
