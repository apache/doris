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

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

public class MysqlSslExchange {
    private static final String[] CIPHER_SUITES = {"TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"};

    public static boolean sslExchange() throws NoSuchAlgorithmException, KeyManagementException {
        // TODO: TLS protocol
        SSLContext sslCtx = initSSLCtx();
        SSLEngine sslEngine = sslCtx.createSSLEngine();
        try {
            sslEngine.beginHandshake();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private static SSLContext initSSLCtx() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslCtx = SSLContext.getInstance("TLSv1.2");
        sslCtx.init(null, null, null);
        return sslCtx;
    }

    private static SSLEngine initSSLEngine(SSLContext sslCtx) {
        SSLEngine sslEngine = sslCtx.createSSLEngine();
        // set to server mode
        sslEngine.setUseClientMode(false);
        sslEngine.setEnabledCipherSuites(CIPHER_SUITES);
        return sslEngine;
    }
}
