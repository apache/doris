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

package org.apache.doris.httpv2.client;

import org.apache.doris.common.Config;
import org.apache.doris.tls.server.TlsProtocolSet;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class OssInternalHttpClientProvider implements InternalHttpClientProvider {
    private static final int MAX_CONN_TOTAL = 256;
    private static final int MAX_CONN_PER_ROUTE = 64;

    private final CloseableHttpClient httpClient;
    private final RestTemplate restTemplate;

    public OssInternalHttpClientProvider() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(MAX_CONN_TOTAL);
        connectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
        httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .evictExpiredConnections()
                .evictIdleConnections(30, java.util.concurrent.TimeUnit.SECONDS)
                .build();
        restTemplate = new RestTemplate();
    }

    @Override
    public String normalizeInternalUrl(String url, Target target) {
        if (TlsProtocolSet.isHttpTlsActive()) {
            throw new UnsupportedOperationException("FE HTTP TLS requires TLS module");
        }
        if (!Config.enable_https || target != Target.FE || isHttps(url)) {
            return url;
        }
        return rewriteSchemeAndPort(url, "https", Config.https_port);
    }

    @Override
    public HttpURLConnection openConnection(String url, Target target) throws IOException {
        return (HttpURLConnection) new URL(normalizeInternalUrl(url, target)).openConnection();
    }

    @Override
    public CloseableHttpClient getHttpClient(Target target) {
        return httpClient;
    }

    @Override
    public RestTemplate getRestTemplate(Target target) {
        return restTemplate;
    }

    private static boolean isHttps(String url) {
        return url != null && url.regionMatches(true, 0, "https://", 0, "https://".length());
    }

    private static String rewriteSchemeAndPort(String url, String scheme, int port) {
        try {
            URI uri = new URI(url);
            URI rewritten = new URI(scheme, uri.getUserInfo(), uri.getHost(), port,
                    uri.getPath(), uri.getQuery(), uri.getFragment());
            return rewritten.toString();
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid internal HTTP URL: " + url, e);
        }
    }
}
