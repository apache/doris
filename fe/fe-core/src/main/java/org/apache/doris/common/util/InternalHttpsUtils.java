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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * SSL-aware HTTP clients for internal FE communication using MySQL SSL truststore.
 */
public class InternalHttpsUtils {
    private static final Logger LOG = LogManager.getLogger(InternalHttpsUtils.class);

    public static CloseableHttpClient createValidatedHttpClient() {
        try {
            KeyStore trustStore = KeyStore.getInstance(Config.ssl_trust_store_type);
            try (InputStream stream = Files.newInputStream(
                    Paths.get(Config.mysql_ssl_default_ca_certificate))) {
                trustStore.load(stream, Config.mysql_ssl_default_ca_certificate_password.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(sslContext);

            return HttpClients.custom()
                    .setSSLSocketFactory(sslFactory)
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to create SSL-aware HTTP client using truststore: {}",
                    Config.mysql_ssl_default_ca_certificate, e);
            throw new RuntimeException("Failed to create SSL-aware HTTP client", e);
        }
    }

    public static void installTrustManagerForUrlConnection() {
        try {
            KeyStore trustStore = KeyStore.getInstance(Config.ssl_trust_store_type);
            try (InputStream stream = Files.newInputStream(
                    Paths.get(Config.mysql_ssl_default_ca_certificate))) {
                trustStore.load(stream, Config.mysql_ssl_default_ca_certificate_password.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        } catch (Exception e) {
            LOG.error("Failed to install trust manager for URLConnection using truststore: {}",
                    Config.mysql_ssl_default_ca_certificate, e);
            throw new RuntimeException("Failed to install trust manager for URLConnection", e);
        }
    }
}
