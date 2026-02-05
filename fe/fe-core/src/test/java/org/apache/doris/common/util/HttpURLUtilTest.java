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
import org.junit.Test;

public class HttpURLUtilTest {

    @After
    public void tearDown() {
        Config.enable_https = false;
        Config.http_port = 8030;
        Config.https_port = 8050;
    }

    @Test
    public void testBuildInternalFeUrlHttp() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/put", "version=123&port=8030");
        Assert.assertEquals("http://192.168.1.10:8030/put?version=123&port=8030", url);
    }

    @Test
    public void testBuildInternalFeUrlHttps() {
        Config.enable_https = true;
        Config.https_port = 8050;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/put", "version=123&port=8050");
        Assert.assertEquals("https://192.168.1.10:8050/put?version=123&port=8050", url);
    }

    @Test
    public void testBuildInternalFeUrlNoQueryParams() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/journal_id", null);
        Assert.assertEquals("http://192.168.1.10:8030/journal_id", url);
    }

    @Test
    public void testBuildInternalFeUrlEmptyQueryParams() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/version", "");
        Assert.assertEquals("http://192.168.1.10:8030/version", url);
    }

    @Test
    public void testBuildInternalFeUrlHttpsWithIPv6() {
        Config.enable_https = true;
        Config.https_port = 8050;

        String url = HttpURLUtil.buildInternalFeUrl("fe80::1", "/role", "host=fe80::2&port=9010");
        Assert.assertTrue(url.startsWith("https://"));
        Assert.assertTrue(url.contains("/role?host=fe80::2&port=9010"));
    }
}
