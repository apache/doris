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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

public class InternalHttpsUtilsTest {

    private String originalCertPath;

    @Before
    public void setUp() throws Exception {
        originalCertPath = Config.mysql_ssl_default_ca_certificate;
        // Reset the cached SSLContext before each test so tests are independent.
        resetCachedSslContext();
    }

    @After
    public void tearDown() throws Exception {
        Config.mysql_ssl_default_ca_certificate = originalCertPath;
        resetCachedSslContext();
    }

    private void resetCachedSslContext() throws Exception {
        Field field = InternalHttpsUtils.class.getDeclaredField("cachedSslContext");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Test
    public void testGetSslContextThrowsWhenCertMissing() {
        Config.mysql_ssl_default_ca_certificate = "/non/existent/path/ca.p12";
        try {
            InternalHttpsUtils.getSslContext();
            Assert.fail("Expected RuntimeException when cert file does not exist");
        } catch (RuntimeException e) {
            // Error message must mention the cert path so operators know what to fix.
            Assert.assertTrue("Error message should contain cert path",
                    e.getMessage() != null && e.getMessage().contains("/non/existent/path/ca.p12"));
        }
    }
}
