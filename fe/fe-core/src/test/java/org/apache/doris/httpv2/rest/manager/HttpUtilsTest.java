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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.InternalHttpsUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

public class HttpUtilsTest {

    // Port 1 refuses connections on loopback almost immediately, so these tests fail fast
    // without needing a real HTTP(S) server.
    private static final String REFUSED_PORT_URL_HTTP = "http://127.0.0.1:1/ping";
    private static final String REFUSED_PORT_URL_HTTPS = "https://127.0.0.1:1/ping";

    private boolean originalEnableHttps;
    private String originalKeyStorePath;

    @Before
    public void setUp() throws Exception {
        originalEnableHttps = Config.enable_https;
        originalKeyStorePath = Config.key_store_path;
        resetCachedSslContext();
    }

    @After
    public void tearDown() throws Exception {
        Config.enable_https = originalEnableHttps;
        Config.key_store_path = originalKeyStorePath;
        resetCachedSslContext();
    }

    private void resetCachedSslContext() throws Exception {
        Field field = InternalHttpsUtils.class.getDeclaredField("cachedSslContext");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    public void testExecuteRequestUsesPlainClientForHttpUrlEvenWhenHttpsEnabled() throws Exception {
        Config.enable_https = true;
        Config.key_store_path = "/non/existent/path/doris_ssl_certificate.keystore";
        resetCachedSslContext();

        try {
            HttpUtils.doGet(REFUSED_PORT_URL_HTTP, null);
            Assert.fail("Expected a connection failure against the refused port");
        } catch (RuntimeException e) {
            Assert.fail("Should not have attempted to build the HTTPS client for an http:// URL: "
                    + e.getMessage());
        } catch (IOException expected) {
            // Plain client hit the network and failed there, never touching the broken keystore.
        }
    }

    @Test
    public void testExecuteRequestUsesHttpsClientForHttpsUrl() throws Exception {
        Config.enable_https = true;
        Config.key_store_path = "/non/existent/path/doris_ssl_certificate.keystore";
        resetCachedSslContext();

        try {
            HttpUtils.doGet(REFUSED_PORT_URL_HTTPS, null);
            Assert.fail("Expected SSLContext build failure before any connection attempt");
        } catch (RuntimeException e) {
            Assert.assertTrue("Failure should come from the missing keystore, not an unrelated error",
                    e.getMessage() != null && e.getMessage().contains("doris_ssl_certificate.keystore"));
        } catch (IOException e) {
            Assert.fail("Expected the HTTPS client's keystore failure, not a network-level error: "
                    + e.getMessage());
        }
    }
}
