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

import org.apache.http.conn.ssl.NoopHostnameVerifier;
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
 *
 * Security Model:
 * - Validates certificates against configured CA truststore (mysql_ssl_default_ca_certificate)
 * - Hostname verification is DISABLED to support IP-based FE communication
 * - This is safe for internal cluster communication because:
 *   1. All endpoints enforce checkFromValidFe() - only registered FE nodes can connect
 *   2. FE cluster is assumed to be on trusted network
 *   3. Traffic is encrypted and authenticated via certificate validation
 *
 * This approach is similar to other distributed systems (Kafka, Elasticsearch, Cassandra)
 * where inter-node SSL communication disables hostname verification for operational flexibility.
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

            SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(
                    sslContext,
                    NoopHostnameVerifier.INSTANCE);

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
            javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
        } catch (Exception e) {
            LOG.error("Failed to install trust manager for URLConnection using truststore: {}",
                    Config.mysql_ssl_default_ca_certificate, e);
            throw new RuntimeException("Failed to install trust manager for URLConnection", e);
        }
    }
}
