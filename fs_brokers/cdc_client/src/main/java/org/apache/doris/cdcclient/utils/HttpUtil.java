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

package org.apache.doris.cdcclient.utils;

import org.apache.doris.cdcclient.common.Env;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

public class HttpUtil {
    private static int connectTimeout = 30 * 1000;
    private static int waitForContinueTimeout = 60 * 1000;
    private static int socketTimeout = 10 * 60 * 1000; // stream load timeout 10 min

    public static CloseableHttpClient getHttpClient() {
        return getHttpClient(socketTimeout);
    }

    public static CloseableHttpClient getHttpClient(int socketTimeoutMs) {
        HttpClientBuilder builder =
                HttpClients.custom()
                        // default timeout 3s, maybe report 307 error when fe busy
                        .setRequestExecutor(new HttpRequestExecutor(waitForContinueTimeout))
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                })
                        .setRetryHandler((exception, executionCount, context) -> false)
                        .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(connectTimeout)
                                        .setConnectionRequestTimeout(connectTimeout)
                                        .setSocketTimeout(socketTimeoutMs)
                                        .build())
                        .addInterceptorLast(new RequestContent(true));

        if (Env.getCurrentEnv().isBackendHttpTlsEnabled()) {
            builder.setSSLSocketFactory(
                    new SSLConnectionSocketFactory(
                            createSslContext(),
                            SSLConnectionSocketFactory.getDefaultHostnameVerifier()));
        }

        return builder.build();
    }

    static SSLContext createSslContext() {
        String caCertificatePath = Env.getCurrentEnv().getBackendHttpTlsCaCertificatePath();
        if (caCertificatePath == null || caCertificatePath.trim().isEmpty()) {
            return SSLContexts.createSystemDefault();
        }

        try {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);

            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            try (InputStream inputStream = Files.newInputStream(Paths.get(caCertificatePath))) {
                Collection<? extends Certificate> certificates =
                        certificateFactory.generateCertificates(inputStream);
                int index = 0;
                for (Certificate certificate : certificates) {
                    trustStore.setCertificateEntry("ca-" + index, certificate);
                    index++;
                }
                if (index == 0) {
                    throw new GeneralSecurityException(
                            "No certificates found in " + caCertificatePath);
                }
            }

            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException(
                    "Failed to create CDC client HTTP TLS context from " + caCertificatePath, e);
        }
    }

    public static String getAuthHeader() {
        return "Basic YWRtaW46";
    }
}
