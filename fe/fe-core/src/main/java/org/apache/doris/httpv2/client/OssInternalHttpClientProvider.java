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
import org.apache.doris.common.util.InternalHttpsUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.tls.server.TlsProtocolSet;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

public class OssInternalHttpClientProvider implements InternalHttpClientProvider {
    private final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    private final RestTemplate restTemplate = new RestTemplate();
    private volatile HttpsClients httpsClients;

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
        HttpURLConnection connection = (HttpURLConnection) new URL(normalizeInternalUrl(url, target)).openConnection();
        if (connection instanceof HttpsURLConnection && Config.enable_https && target == Target.FE) {
            HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
            httpsConnection.setSSLSocketFactory(InternalHttpsUtils.getSslContext().getSocketFactory());
            httpsConnection.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
        return connection;
    }

    @Override
    public CloseableHttpClient getHttpClient(Target target) {
        if (Config.enable_https && target == Target.FE) {
            return getHttpsClients().httpClient;
        }
        return httpClient;
    }

    @Override
    public RestTemplate getRestTemplate(Target target) {
        if (!Config.enable_https || target != Target.FE) {
            return restTemplate;
        }
        return getHttpsClients().restTemplate;
    }

    private HttpsClients getHttpsClients() {
        HttpsClients clients = httpsClients;
        if (clients != null) {
            return clients;
        }
        synchronized (this) {
            clients = httpsClients;
            if (clients == null) {
                clients = new HttpsClients(
                        InternalHttpsUtils.createValidatedHttpClient(),
                        new RestTemplate(new InternalHttpsClientHttpRequestFactory()));
                httpsClients = clients;
            }
        }
        return clients;
    }

    private static boolean isHttps(String url) {
        return url != null && url.regionMatches(true, 0, "https://", 0, "https://".length());
    }

    private static String rewriteSchemeAndPort(String url, String scheme, int port) {
        try {
            URI uri = new URI(url);
            if (uri.getHost() == null) {
                throw new IllegalArgumentException("Internal HTTP URL has no host: " + url);
            }

            StringBuilder rewritten = new StringBuilder(scheme).append("://");
            if (uri.getRawUserInfo() != null) {
                rewritten.append(uri.getRawUserInfo()).append('@');
            }
            rewritten.append(NetUtils.getHostPortInAccessibleFormat(uri.getHost(), port));
            if (uri.getRawPath() != null) {
                rewritten.append(uri.getRawPath());
            }
            if (uri.getRawQuery() != null) {
                rewritten.append('?').append(uri.getRawQuery());
            }
            if (uri.getRawFragment() != null) {
                rewritten.append('#').append(uri.getRawFragment());
            }
            return rewritten.toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid internal HTTP URL: " + url, e);
        }
    }

    private static class InternalHttpsClientHttpRequestFactory extends SimpleClientHttpRequestFactory {
        @Override
        protected void prepareConnection(HttpURLConnection connection, String httpMethod) throws IOException {
            super.prepareConnection(connection, httpMethod);
            if (connection instanceof HttpsURLConnection) {
                HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
                httpsConnection.setSSLSocketFactory(InternalHttpsUtils.getSslContext().getSocketFactory());
                httpsConnection.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
            }
        }
    }

    private static class HttpsClients {
        private final CloseableHttpClient httpClient;
        private final RestTemplate restTemplate;

        private HttpsClients(CloseableHttpClient httpClient, RestTemplate restTemplate) {
            this.httpClient = httpClient;
            this.restTemplate = restTemplate;
        }
    }
}
