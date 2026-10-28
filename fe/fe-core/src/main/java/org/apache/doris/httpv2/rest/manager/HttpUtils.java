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
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.common.util.InternalHttpsUtils;
import org.apache.doris.common.util.Util;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/*
 * Used for internal HTTP(S) communication between FE nodes and from manager to BE.
 */
public class HttpUtils {
    private static final Logger LOG = LogManager.getLogger(HttpUtils.class);

    public static final int REQUEST_SUCCESS_CODE = 0;
    static final int DEFAULT_TIME_OUT_MS = 2000;
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final Pattern SATISFIED_CONTENT_RANGE =
            Pattern.compile("^bytes\\s+(\\d+)-(\\d+)/(\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UNSATISFIED_CONTENT_RANGE =
            Pattern.compile("^bytes\\s+\\*/(\\d+)$", Pattern.CASE_INSENSITIVE);

    public static List<Pair<String, Integer>> getFeList() {
        int port = HttpURLUtil.getHttpPort();
        return Env.getCurrentEnv().getFrontends(null)
                .stream().filter(Frontend::isAlive).map(fe -> Pair.of(fe.getHost(), port))
                .collect(Collectors.toList());
    }

    static boolean isCurrentFe(String ip, int port) {
        HostInfo hostInfo = Env.getCurrentEnv().getSelfNode();
        // Compare against the actual HTTP/HTTPS port, not the edit_log_port held by selfNode.
        int selfPort = HttpURLUtil.getHttpPort();
        return hostInfo.getHost().equals(ip) && selfPort == port;
    }

    public static String concatUrl(Pair<String, Integer> ipPort, String path, Map<String, String> arguments) {
        StringBuilder url = new StringBuilder(Config.enable_https ? "https://" : "http://")
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

    public static String doGet(String url, Map<String, String> headers, int timeoutMs) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        setRequestConfig(httpGet, headers, timeoutMs);
        return executeRequest(httpGet);
    }

    public static String doGet(String url, Map<String, String> headers) throws IOException {
        return doGet(url, headers, DEFAULT_TIME_OUT_MS);
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

    private static String executeRequest(HttpRequestBase request) throws IOException {
        // Pick client by this request's own scheme, since this method also serves plain http BE calls.
        boolean useHttpsClient = "https".equalsIgnoreCase(request.getURI().getScheme());
        try (CloseableHttpClient client = useHttpsClient
                ? InternalHttpsUtils.createValidatedHttpClient()
                : HttpClientBuilder.create().build()) {
            return client.execute(request, httpResponse -> EntityUtils.toString(httpResponse.getEntity()));
        }
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
     * Get the file size of the HTTP resource.
     *
     * <p>
     * This first tries an HTTP HEAD request to read the Content-Length header without
     * downloading the file body. Some resources reject HEAD requests, most notably presigned
     * object-storage URLs whose signature covers the HTTP method: a URL signed for GET is
     * rejected with 403 when accessed via HEAD, even though the same URL works fine with GET.
     * In that case we fall back to a GET request with {@code Range: bytes=0-0}, which mirrors
     * the actual read path used later and lets us recover the size from the {@code Content-Range}
     * (206) or {@code Content-Length} (200) response header.
     *
     * @param uri the HTTP URI to get file size for
     * @return the file size in bytes
     * @throws IOException              if there's an error connecting to the HTTP resource
     * @throws IllegalArgumentException if the URI is null or invalid
     */
    public static long getHttpFileSize(String uri, Map<String, String> headers) throws IOException {
        if (uri == null || uri.trim().isEmpty()) {
            throw new IllegalArgumentException("HTTP URI is null or empty");
        }
        Map<String, String> safeHeaders = headers != null
                ? headers
                : Collections.emptyMap();

        try {
            Long size = tryGetFileSizeWithHead(uri, safeHeaders);
            if (size != null) {
                return size;
            }
            LOG.warn("HEAD response has no usable Content-Length for URI: {}, falling back to GET with Range.", uri);
        } catch (IOException e) {
            LOG.warn("HEAD request failed for URI: {}, falling back to GET with Range. {}", uri, e.getMessage());
        }

        try {
            Long size = tryGetFileSizeWithGetRange(uri, safeHeaders);
            if (size == null) {
                throw new IOException("GET-based HTTP file size probe did not return a usable size for URI: " + uri);
            }
            return size;
        } catch (IOException e) {
            LOG.warn("Failed to get file size for URI: {}", uri, e);
            throw new IOException("Failed to get file size for URI: " + uri + ". " + Util.getRootCauseMessage(e), e);
        }
    }

    /**
     * Try to get the file size via a HEAD request.
     *
     * @return the file size, or null if the response was OK but had no Content-Length header
     * @throws IOException if the connection fails or the response code is not 2xx
     */
    private static Long tryGetFileSizeWithHead(String uri, Map<String, String> headers) throws IOException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(uri);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            configureFileSizeRequest(connection, headers);

            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HEAD request failed with response code: " + responseCode + ", message: "
                        + connection.getResponseMessage());
            }
            return parseContentLength(connection.getHeaderField("Content-Length"));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Try to get the file size via a GET request with {@code Range: bytes=0-0}, used as a
     * fallback when the HEAD request is rejected by the server.
     *
     * @return the file size
     * @throws IOException if the connection fails or the response does not provide a valid size
     */
    private static Long tryGetFileSizeWithGetRange(String uri, Map<String, String> headers) throws IOException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(uri);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Range", "bytes=0-0");
            configureFileSizeRequest(connection, headers);

            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_PARTIAL
                    && responseCode != HttpURLConnection.HTTP_OK
                    && responseCode != HTTP_RANGE_NOT_SATISFIABLE) {
                throw new IOException("GET request with Range failed with response code: " + responseCode
                        + ", message: " + connection.getResponseMessage());
            }

            if (responseCode == HttpURLConnection.HTTP_PARTIAL) {
                return parseProbeContentRange(connection.getHeaderField("Content-Range"), false);
            }
            if (responseCode == HTTP_RANGE_NOT_SATISFIABLE) {
                long total = parseProbeContentRange(connection.getHeaderField("Content-Range"), true);
                if (total != 0) {
                    throw new IOException("Range probe returned HTTP 416 for a non-empty resource of "
                            + total + " bytes");
                }
                return 0L;
            }
            // HTTP 200: server ignored Range and returned the full content; Content-Length
            // (if present) is the full file size.
            return parseContentLength(connection.getHeaderField("Content-Length"));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static void configureFileSizeRequest(HttpURLConnection connection, Map<String, String> headers) {
        connection.setConnectTimeout(10000); // 10 seconds connection timeout
        connection.setReadTimeout(30000); // 30 seconds read timeout
        connection.setRequestProperty("User-Agent", "Doris-HttpUtils/1.0");
        connection.setRequestProperty("Accept", "*/*");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            connection.setRequestProperty(entry.getKey(), entry.getValue());
        }
    }

    private static long parseProbeContentRange(String contentRange, boolean expectUnsatisfied)
            throws IOException {
        if (contentRange == null) {
            throw new IOException("Missing Content-Range header");
        }

        Matcher matcher = (expectUnsatisfied ? UNSATISFIED_CONTENT_RANGE : SATISFIED_CONTENT_RANGE)
                .matcher(contentRange.trim());
        if (!matcher.matches()) {
            throw new IOException("Invalid Content-Range header: " + contentRange);
        }

        try {
            if (expectUnsatisfied) {
                return Long.parseLong(matcher.group(1));
            }

            long start = Long.parseLong(matcher.group(1));
            long end = Long.parseLong(matcher.group(2));
            long total = Long.parseLong(matcher.group(3));
            if (start != 0 || end != 0 || total == 0) {
                throw new IOException("Unexpected Content-Range for bytes=0-0 probe: " + contentRange);
            }
            return total;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid Content-Range header: " + contentRange, e);
        }
    }

    private static Long parseContentLength(String contentLengthStr) throws IOException {
        if (contentLengthStr == null || contentLengthStr.trim().isEmpty()) {
            return null;
        }
        try {
            long contentLength = Long.parseLong(contentLengthStr.trim());
            if (contentLength < 0) {
                throw new IOException("Invalid Content-Length header: " + contentLengthStr);
            }
            return contentLength;
        } catch (NumberFormatException e) {
            throw new IOException("Invalid Content-Length header: " + contentLengthStr, e);
        }
    }
}
