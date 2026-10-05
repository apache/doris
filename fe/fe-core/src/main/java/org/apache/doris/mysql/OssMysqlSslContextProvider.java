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

package org.apache.doris.mysql;

import org.apache.doris.common.Config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

public class OssMysqlSslContextProvider implements MysqlSslContextProvider {
    private static final String KEY_STORE_FILE = Config.mysql_ssl_default_server_certificate;
    private static final String TRUST_STORE_FILE = Config.mysql_ssl_default_ca_certificate;
    private static final String CA_CERTIFICATE_PASSWORD = Config.mysql_ssl_default_ca_certificate_password;
    private static final String SERVER_CERTIFICATE_PASSWORD = Config.mysql_ssl_default_server_certificate_password;
    private static final String TRUST_STORE_TYPE = Config.ssl_trust_store_type;

    @Override
    public SSLContext createSslContext(String protocol) throws Exception {
        KeyStore ks = KeyStore.getInstance(TRUST_STORE_TYPE);
        KeyStore ts = KeyStore.getInstance(TRUST_STORE_TYPE);

        char[] serverPassword = SERVER_CERTIFICATE_PASSWORD.toCharArray();
        char[] caPassword = CA_CERTIFICATE_PASSWORD.toCharArray();

        try (InputStream stream = Files.newInputStream(Paths.get(KEY_STORE_FILE))) {
            ks.load(stream, serverPassword);
        }
        try (InputStream stream = Files.newInputStream(Paths.get(TRUST_STORE_FILE))) {
            ts.load(stream, caPassword);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, serverPassword);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext sslContext = SSLContext.getInstance(protocol);
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return sslContext;
    }

    @Override
    public void configureEngine(SSLEngine sslEngine) {
        sslEngine.setUseClientMode(false);
        sslEngine.setEnabledCipherSuites(sslEngine.getSupportedCipherSuites());
        sslEngine.setWantClientAuth(true);
        if (Config.ssl_force_client_auth) {
            sslEngine.setNeedClientAuth(true);
        }
    }
}
