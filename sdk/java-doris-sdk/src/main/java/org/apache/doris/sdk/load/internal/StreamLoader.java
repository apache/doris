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

package org.apache.doris.sdk.load.internal;

import org.apache.doris.sdk.load.exception.StreamLoadException;
import org.apache.doris.sdk.load.model.LoadResponse;
import org.apache.doris.sdk.load.model.RespContent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Uses RedirectStrategy so that 307 redirects on PUT requests are
 * followed automatically (Doris FE redirects stream load to BE).
 */
public class StreamLoader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLoader.class);
    private static final int SOCKET_TIMEOUT_MS = 9 * 60 * 1000;
    private static final int CONNECT_TIMEOUT_MS = 60_000;

    private final HttpClientBuilder httpClientBuilder;
    private final ObjectMapper objectMapper;

    public StreamLoader() {
        this.httpClientBuilder = buildHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    /** Package-private constructor for testing with a mock HTTP client. */
    StreamLoader(HttpClientBuilder httpClientBuilder) {
        this.httpClientBuilder = httpClientBuilder;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Executes the HTTP PUT request and returns a LoadResponse.
     *
     * @throws StreamLoadException for retryable HTTP-level errors (non-200 status, connection failure)
     * @throws IOException         for unrecoverable I/O errors
     */
    public LoadResponse execute(HttpPut request) throws IOException {
        log.debug("Sending HTTP PUT to {}", request.getURI());
        long start = System.currentTimeMillis();

        try (CloseableHttpClient httpClient = httpClientBuilder.build();
                CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            log.debug("HTTP response status: {}", statusCode);
            log.debug("HTTP request completed in {} ms", System.currentTimeMillis() - start);

            if (statusCode == 200) {
                return parseResponse(response);
            } else {
                // Non-200 is retryable (e.g. 503, 429)
                throw new StreamLoadException("stream load error: " + response.getStatusLine().toString());
            }
        } catch (StreamLoadException e) {
            throw e;
        } catch (IOException e) {
            throw new StreamLoadException("stream load request failed: " + e.getMessage(), e);
        }
    }

    private LoadResponse parseResponse(CloseableHttpResponse response) throws IOException {
        byte[] bodyBytes = EntityUtils.toByteArray(response.getEntity());
        String body = new String(bodyBytes, StandardCharsets.UTF_8);
        log.info("Stream Load Response: {}", body);

        RespContent resp = objectMapper.readValue(body, RespContent.class);

        if (isSuccess(resp.getStatus())) {
            log.info("Load operation completed successfully");
            return LoadResponse.success(resp);
        } else {
            log.error("Load operation failed with status: {}", resp.getStatus());
            String errorMsg;
            if (resp.getMessage() != null && !resp.getMessage().isEmpty()) {
                errorMsg = "load failed. cause by: " + resp.getMessage()
                        + ", please check more detail from url: " + resp.getErrorUrl();
            } else {
                errorMsg = body;
            }
            return LoadResponse.failure(resp, errorMsg);
        }
    }

    private static boolean isSuccess(String status) {
        return "success".equalsIgnoreCase(status);
    }

    private static HttpClientBuilder buildHttpClient() {
        try {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECT_TIMEOUT_MS)
                    .setSocketTimeout(SOCKET_TIMEOUT_MS)
                    .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                    .build();

            return HttpClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }
                    })
                    .setSSLSocketFactory(
                            new SSLConnectionSocketFactory(
                                    SSLContextBuilder.create()
                                            .loadTrustMaterial(null, (chain, authType) -> true)
                                            .build(),
                                    NoopHostnameVerifier.INSTANCE));
        } catch (Exception e) {
            throw new RuntimeException("Failed to build HTTP client", e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}
