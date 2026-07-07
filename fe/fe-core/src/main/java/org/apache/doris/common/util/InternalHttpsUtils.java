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
 * Utility for creating SSL-aware HTTP clients for internal FE communication, including
 * FE-to-FE calls.
 *
 * <p>Builds an {@link SSLContext} that trusts this FE's own HTTPS keystore
 * ({@code Config.key_store_path}) once and caches it. That keystore is the config surface
 * that actually defines what certificate the FE HTTPS listener presents, so it is the correct
 * trust anchor here — unlike {@code Config.mysql_ssl_default_ca_certificate}, which is a
 * separate, independently-configured trust store scoped to MySQL wire-protocol SSL and is not
 * guaranteed to share a CA with FE HTTPS.
 *
 * <p>This is correct for loopback clients (e.g. the audit log stream loader, which always
 * targets 127.0.0.1) and for genuine FE-to-FE calls, as long as every FE node in the cluster is
 * provisioned with the same {@code key_store_path} certificate — the expected Doris internal-
 * HTTPS deployment model (one shared cert for the cluster, mirroring how MySQL SSL already
 * assumes one shared CA/cert pair rather than per-node identities). A cluster that instead issues
 * a distinct certificate per FE node would need a real CA-based trust store instead of this
 * pinned-leaf model.
 *
 * <p>Hostname verification is disabled for IP-based intra-cluster connections.
 * Certificate rotation requires a FE restart.
 */
public class InternalHttpsUtils {
    private static volatile SSLContext cachedSslContext = null;

    private static final Logger LOG = LogManager.getLogger(InternalHttpsUtils.class);

    /**
     * Returns the cached SSLContext, building it from the FE's own HTTPS keystore on first call.
     */
    public static SSLContext getSslContext() {
        if (cachedSslContext == null) {
            synchronized (InternalHttpsUtils.class) {
                if (cachedSslContext == null) {
                    cachedSslContext = buildSslContext();
                }
            }
        }
        return cachedSslContext;
    }

    private static SSLContext buildSslContext() {
        try {
            KeyStore trustStore = KeyStore.getInstance(Config.key_store_type);
            try (InputStream stream = Files.newInputStream(Paths.get(Config.key_store_path))) {
                trustStore.load(stream, Config.key_store_password.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            LOG.error("Failed to build SSLContext from FE HTTPS keystore: {}",
                    Config.key_store_path, e);
            throw new RuntimeException(
                    "Failed to build SSLContext from FE HTTPS keystore: "
                            + Config.key_store_path, e);
        }
    }

    /**
     * Returns an HTTP client configured with the FE CA truststore and hostname verification disabled.
     */
    public static CloseableHttpClient createValidatedHttpClient() {
        SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(
                getSslContext(),
                NoopHostnameVerifier.INSTANCE);

        return HttpClients.custom()
                .setSSLSocketFactory(sslFactory)
                .build();
    }
}
