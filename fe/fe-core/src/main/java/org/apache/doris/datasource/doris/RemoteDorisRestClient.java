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
import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.HealthAction;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.httpv2.rest.response.GsonSchemaResponse;
import org.apache.doris.persist.gson.GsonUtils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.reflect.TypeToken;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.lang.reflect.Type;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Use this restClient when the remote Doris cluster is the same version as the current cluster,
 * ensuring complete tableSchema compatibility.
 */
public class RemoteDorisRestClient {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisRestClient.class);

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
    public RemoteDorisRestClient(List<String> feNodes, String authUser, String authPassword, boolean httpSslEnable) {
        this.feNodes = feNodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION,
                    Credentials.basic(authUser, Strings.isEmpty(authPassword) ? "" : authPassword));
        }
        this.currentNode = feNodes.get(currentNodeIndex);
        this.httpSslEnable = httpSslEnable;
    }

    public List<String> getDatabaseNameList() {
        return parseStringLists(execute("api/meta/namespaces/default_cluster/databases"));
    }

    public List<String> getTablesNameList(String dbName) {
        return parseStringLists(execute("api/meta/namespaces/default_cluster/databases/" + dbName + "/tables"));
    }

    public boolean isTableExist(String dbName, String tableName) {
        return parseSuccessResponse(execute("api/" + dbName + "/" + tableName + "/_schema"));
    }

    public boolean health() {
        int aliveBeNum = parseOnlineBeNum(execute("api/health"));
        return aliveBeNum > 0;
    }

    public List<Column> getColumns(String dbName, String tableName) {
        return parseColumns(execute("api/" + dbName + "/" + tableName + "/_gson_schema"));
    }

    public long getRowCount(String dbName, String tableName) {
        return parseRowCount(execute("api/rowcount?db=" + dbName + "&table=" + tableName));
    }

    public static List<String> parseStringLists(String executeResult) {
        ResponseBody<ArrayList<String>> databasesResponse = parseResponse(
            new TypeToken<ArrayList<String>>() {},
                executeResult);
        if (successResponse(databasesResponse)) {
            return databasesResponse.getData();
        }
        return new ArrayList<>();
    }

    public static boolean parseSuccessResponse(String executeResult) {
        ObjectNode objectNode = JsonUtil.parseObject(executeResult);
        Integer code = JsonUtil.safeGetAsInt(objectNode, "code");
        return code != null && code == RestApiStatusCode.OK.code;
    }

    public static int parseOnlineBeNum(String executeResult) {
        ResponseBody<HashMap<String, Integer>> healthResponse = parseResponse(
            new TypeToken<HashMap<String, Integer>>() {},
                executeResult);
        if (successResponse(healthResponse)) {
            return healthResponse.getData().get(HealthAction.ONLINE_BACKEND_NUM);
        }
        throw new RuntimeException("get doris table schema error, msg: " + healthResponse.getMsg());
    }

    public static List<Column> parseColumns(String executeResult) {
        ResponseBody<GsonSchemaResponse> getColumnsResponse = parseResponse(
            new TypeToken<GsonSchemaResponse>(){},
                executeResult);
        if (successResponse(getColumnsResponse)) {
            return getColumnsResponse.getData().getJsonColumns().stream()
                .map(json -> GsonUtils.GSON.fromJson(json, Column.class))
                .collect(Collectors.toList());
        }
        throw new RuntimeException("get doris table schema error, msg: " + getColumnsResponse.getMsg());
    }

    public static long parseRowCount(String executeResult) {
        ResponseBody<HashMap<String, Long>> rowCountResponse = parseResponse(
            new TypeToken<HashMap<String, Long>>() {},
                executeResult);
        if (successResponse(rowCountResponse)) {
            return rowCountResponse.getData().values().iterator().next();
        }
        throw new RuntimeException("get doris table row count error, msg: " + rowCountResponse.getMsg());
    }

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
                .hostnameVerifier(new RemoteDorisRestClient.TrustAllHostnameVerifier()).build();
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
            sc.init(null, new TrustManager[] {new RemoteDorisRestClient.TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }

    private static <T> ResponseBody<T> parseResponse(TypeToken<T> typeToken, String responseBody) {
        ResponseBody errorResponseBody = new ResponseBody();
        if (responseBody == null) {
            return errorResponseBody.code(RestApiStatusCode.COMMON_ERROR).msg("responseBody is null");
        }

        try {
            Type type = TypeToken.getParameterized(ResponseBody.class, typeToken.getType()).getType();
            return GsonUtils.GSON.fromJson(responseBody, type);
        } catch (Exception e) {
            return errorResponseBody.code(RestApiStatusCode.COMMON_ERROR).msg(e.getMessage());
        }
    }

    private static boolean successResponse(ResponseBody<?> responseBody) {
        return responseBody != null && responseBody.getCode() == RestApiStatusCode.OK.code;
    }
}
