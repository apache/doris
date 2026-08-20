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
import java.security.cert.Certificate;
import java.util.Collections;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * SSL-aware HTTP client for internal FE communication (loopback and FE-to-FE).
 *
 * <p>Trusts this FE's own HTTPS keystore ({@code Config.key_store_path}), not
 * {@code Config.mysql_ssl_default_ca_certificate} (a separate store for MySQL SSL). Trusts every
 * cert in the keystore's chains, so a bundled CA also validates other nodes' certs it signed.
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
            KeyStore keyStore = KeyStore.getInstance(Config.key_store_type);
            try (InputStream stream = Files.newInputStream(Paths.get(Config.key_store_path))) {
                keyStore.load(stream, Config.key_store_password.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(buildTrustStore(keyStore));

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

    // Extracts every cert in every chain, since KeyStore.getCertificate() only returns the leaf.
    private static KeyStore buildTrustStore(KeyStore keyStore) throws Exception {
        KeyStore trustStore = KeyStore.getInstance(Config.key_store_type);
        trustStore.load(null, null);
        int certIndex = 0;
        for (String alias : Collections.list(keyStore.aliases())) {
            Certificate[] chain = keyStore.getCertificateChain(alias);
            if (chain == null) {
                Certificate cert = keyStore.getCertificate(alias);
                if (cert != null) {
                    trustStore.setCertificateEntry("cert-" + certIndex++, cert);
                }
                continue;
            }
            for (Certificate cert : chain) {
                trustStore.setCertificateEntry("cert-" + certIndex++, cert);
            }
        }
        return trustStore;
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
