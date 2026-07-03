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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.httpv2.client.InternalHttpClientProvider;
import org.apache.doris.httpv2.client.InternalHttpClientProviderFactory;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/*
 * used to forward http requests from manager to be.
 */
public class HttpUtils {
    private static final Logger LOG = LogManager.getLogger(HttpUtils.class);

    public static final int REQUEST_SUCCESS_CODE = 0;
    static final int DEFAULT_TIME_OUT_MS = 2000;

    public static List<Pair<String, Integer>> getFeList() {
        return Env.getCurrentEnv().getFrontends(null)
                .stream().filter(Frontend::isAlive).map(fe -> Pair.of(fe.getHost(), Config.http_port))
                .collect(Collectors.toList());
    }

    static boolean isCurrentFe(String ip, int port) {
        HostInfo hostInfo = Env.getCurrentEnv().getSelfNode();
        return hostInfo.isSame(new HostInfo(ip, port));
    }

    public static String concatUrl(Pair<String, Integer> ipPort, String path, Map<String, String> arguments) {
        StringBuilder url = new StringBuilder("http://")
                .append(ipPort.first).append(":").append(ipPort.second).append(path);
        boolean isFirst = true;
        for (Map.Entry<String, String> entry : arguments.entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                if (isFirst) {
                    url.append("?");
                } else {
                    url.append("&");
                }
                isFirst = false;
                url.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return url.toString();
    }

    public static String concatInternalUrl(Pair<String, Integer> ipPort, String path, Map<String, String> arguments,
            InternalHttpClientProvider.Target target) {
        return InternalHttpClientProviderFactory.getProvider().normalizeInternalUrl(concatUrl(ipPort, path, arguments),
                target);
    }

    public static String doGet(String url, Map<String, String> headers, int timeoutMs) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        setRequestConfig(httpGet, headers, timeoutMs);
        return executeRequest(httpGet);
    }

    public static String doInternalGet(String url, Map<String, String> headers, int timeoutMs,
            InternalHttpClientProvider.Target target) throws IOException {
        HttpGet httpGet = new HttpGet(normalizeInternalUrl(url, target));
        setRequestConfig(httpGet, headers, timeoutMs);
        return executeInternalRequest(httpGet, target);
    }

    public static String doGet(String url, Map<String, String> headers) throws IOException {
        return doGet(url, headers, DEFAULT_TIME_OUT_MS);
    }

    public static String doInternalGet(String url, Map<String, String> headers,
            InternalHttpClientProvider.Target target) throws IOException {
        return doInternalGet(url, headers, DEFAULT_TIME_OUT_MS, target);
    }

    public static String doPost(String url, Map<String, String> headers, Object body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        if (Objects.nonNull(body)) {
            String jsonString = GsonUtils.GSON.toJson(body);
            StringEntity stringEntity = new StringEntity(jsonString, "UTF-8");
            httpPost.setEntity(stringEntity);
        }

        setRequestConfig(httpPost, headers, DEFAULT_TIME_OUT_MS);
        return executeRequest(httpPost);
    }

    public static String doInternalPost(String url, Map<String, String> headers, Object body,
            InternalHttpClientProvider.Target target) throws IOException {
        HttpPost httpPost = new HttpPost(normalizeInternalUrl(url, target));
        if (Objects.nonNull(body)) {
            String jsonString = GsonUtils.GSON.toJson(body);
            StringEntity stringEntity = new StringEntity(jsonString, "UTF-8");
            httpPost.setEntity(stringEntity);
        }

        setRequestConfig(httpPost, headers, DEFAULT_TIME_OUT_MS);
        return executeInternalRequest(httpPost, target);
    }

    private static void setRequestConfig(HttpRequestBase request, Map<String, String> headers, int timeoutMs) {
        if (null != headers) {
            for (String key : headers.keySet()) {
                request.setHeader(key, headers.get(key));
            }
        }

        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeoutMs)
                .setConnectionRequestTimeout(timeoutMs)
                .setSocketTimeout(timeoutMs)
                .build();
        request.setConfig(config);
    }

    public static CloseableHttpClient getHttpClient() {
        return HttpClientBuilder.create().build();
    }

    public static CloseableHttpClient getInternalHttpClient(InternalHttpClientProvider.Target target) {
        return InternalHttpClientProviderFactory.getProvider().getHttpClient(target);
    }

    private static String executeRequest(HttpRequestBase request) throws IOException {
        CloseableHttpClient client = getHttpClient();
        return client.execute(request, httpResponse -> EntityUtils.toString(httpResponse.getEntity()));
    }

    private static String executeInternalRequest(HttpRequestBase request,
            InternalHttpClientProvider.Target target) throws IOException {
        CloseableHttpClient client = getInternalHttpClient(target);
        return client.execute(request, httpResponse -> EntityUtils.toString(httpResponse.getEntity()));
    }

    public static String normalizeInternalUrl(String url, InternalHttpClientProvider.Target target) {
        return InternalHttpClientProviderFactory.getProvider().normalizeInternalUrl(url, target);
    }

    static String parseResponse(String response) {
        ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
        if (responseEntity.getCode() != REQUEST_SUCCESS_CODE) {
            throw new RuntimeException(responseEntity.getMsg());
        }
        return GsonUtils.GSON.toJson(responseEntity.getData());
    }

    public static String getBody(HttpServletRequest request) throws IOException {
        return IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
    }

    /**
     * Get the file size of the HTTP resource by sending a HEAD request.
     * This method uses HTTP HEAD request to get the Content-Length header
     * without downloading the entire file content.
     * @param uri the HTTP URI to get file size for
     * @return the file size in bytes, or -1 if the size cannot be determined
     * @throws IOException if there's an error connecting to the HTTP resource
     * @throws IllegalArgumentException if the URI is null or invalid
     */
    public static long getHttpFileSize(String uri, Map<String, String> headers) throws IOException {
        if (uri == null || uri.trim().isEmpty()) {
            throw new IllegalArgumentException("HTTP URI is null or empty");
        }

        HttpURLConnection connection = null;
        try {
            URL url = new URL(uri);
            connection = (HttpURLConnection) url.openConnection();

            // Use HEAD request to get headers without downloading content
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(10000); // 10 seconds connection timeout
            connection.setReadTimeout(30000);    // 30 seconds read timeout

            // Set common headers
            connection.setRequestProperty("User-Agent", "Doris-HttpUtils/1.0");
            connection.setRequestProperty("Accept", "*/*");
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                connection.setRequestProperty(entry.getKey(), entry.getValue());
            }

            // Connect and get response
            connection.connect();
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Try to get Content-Length header
                String contentLengthStr = connection.getHeaderField("Content-Length");
                if (contentLengthStr != null && !contentLengthStr.trim().isEmpty()) {
                    try {
                        return Long.parseLong(contentLengthStr.trim());
                    } catch (NumberFormatException e) {
                        throw new IOException("Invalid Content-Length header: " + contentLengthStr, e);
                    }
                } else {
                    // Content-Length header not available
                    return -1;
                }
            } else {
                throw new IOException("HTTP request failed with response code: " + responseCode
                        + ", message: " + connection.getResponseMessage());
            }
        } catch (IOException e) {
            LOG.warn("Failed to get file size for URI: {}", uri, e);
            throw new IOException("Failed to get file size for URI: " + uri + ". " + Util.getRootCauseMessage(e), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
