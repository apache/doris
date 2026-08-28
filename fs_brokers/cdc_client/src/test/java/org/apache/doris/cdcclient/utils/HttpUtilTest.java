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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.doris.cdcclient.common.Env;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;

class HttpUtilTest {

    @AfterEach
    void resetTlsConfiguration() {
        Env env = Env.getCurrentEnv();
        env.setBackendHttpTlsEnabled(false);
        env.setBackendHttpTlsCaCertificatePath(null);
    }

    @Test
    void createsTlsContextFromConfiguredCaCertificate() throws URISyntaxException {
        Path caCertificate =
                Path.of(
                        getClass()
                                .getClassLoader()
                                .getResource("tls/ca.pem")
                                .toURI());
        Env env = Env.getCurrentEnv();
        env.setBackendHttpTlsEnabled(true);
        env.setBackendHttpTlsCaCertificatePath(caCertificate.toString());

        SSLContext sslContext = HttpUtil.createSslContext();

        assertNotNull(sslContext);
        assertNotNull(sslContext.getSocketFactory());
    }
}
