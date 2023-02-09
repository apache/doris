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

public class MysqlSslConnectionContext {
    private SSLEngine sslEngine;
    private SSLContext sslContext;
    private String protocol;
    private String[] ciphersuites;

    public MysqlSslConnectionContext(String protocol, String[] ciphersuites)
            throws NoSuchAlgorithmException, KeyManagementException {
        protocol = protocol;
        ciphersuites = ciphersuites;
        initSslContext();
        initSslEngine();
    }

    private void initSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        sslContext = SSLContext.getInstance(protocol);
        sslContext.init(null, null, null);
    }

    private void initSslEngine() {
        sslEngine = sslContext.createSSLEngine();
        // set to server mode
        sslEngine.setUseClientMode(false);
        sslEngine.setEnabledCipherSuites(ciphersuites);
    }

    public SSLEngine getSslEngine() {
        return sslEngine;
    }

    public String getProtocol() {
        return protocol;
    }

    public String[] getCiphersuites() {
        return ciphersuites;
    }

    public boolean sslExchange(MysqlChannel channel) {

        return true;
    }

}
