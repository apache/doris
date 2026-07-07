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

package org.apache.doris.httpv2.client;

import org.apache.doris.common.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OssInternalHttpClientProviderTest {
    private boolean oldEnableHttps;
    private int oldHttpsPort;
    private boolean oldEnableTls;
    private String oldTlsExcludedProtocols;

    @Before
    public void setUp() {
        oldEnableHttps = Config.enable_https;
        oldHttpsPort = Config.https_port;
        oldEnableTls = Config.enable_tls;
        oldTlsExcludedProtocols = Config.tls_excluded_protocols;
        Config.enable_https = false;
        Config.https_port = 8443;
        Config.enable_tls = false;
        Config.tls_excluded_protocols = "";
    }

    @After
    public void tearDown() {
        Config.enable_https = oldEnableHttps;
        Config.https_port = oldHttpsPort;
        Config.enable_tls = oldEnableTls;
        Config.tls_excluded_protocols = oldTlsExcludedProtocols;
    }

    @Test
    public void testDefaultConfigurationDoesNotRewriteUrl() {
        OssInternalHttpClientProvider provider = new OssInternalHttpClientProvider();

        Assert.assertEquals("http://fe-host:8080/rest/v1/session",
                provider.normalizeInternalUrl("http://fe-host:8080/rest/v1/session",
                        InternalHttpClientProvider.Target.FE));
    }

    @Test
    public void testFeUrlRewrittenWhenOpenSourceHttpsEnabled() {
        Config.enable_https = true;
        OssInternalHttpClientProvider provider = new OssInternalHttpClientProvider();

        Assert.assertEquals("https://fe-host:8443/rest/v1/session",
                provider.normalizeInternalUrl("http://fe-host:8080/rest/v1/session",
                        InternalHttpClientProvider.Target.FE));
    }

    @Test
    public void testHttpsInputIsIdempotent() {
        Config.enable_https = true;
        OssInternalHttpClientProvider provider = new OssInternalHttpClientProvider();

        Assert.assertEquals("https://fe-host:9443/rest/v1/session",
                provider.normalizeInternalUrl("https://fe-host:9443/rest/v1/session",
                        InternalHttpClientProvider.Target.FE));
    }

    @Test
    public void testBeUrlIsNotRewrittenByOpenSourceHttps() {
        Config.enable_https = true;
        OssInternalHttpClientProvider provider = new OssInternalHttpClientProvider();

        Assert.assertEquals("http://be-host:8040/api/show_config",
                provider.normalizeInternalUrl("http://be-host:8040/api/show_config",
                        InternalHttpClientProvider.Target.BE));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testHttpTlsRequiresTlsModuleProvider() {
        Config.enable_tls = true;
        Config.tls_excluded_protocols = "";
        OssInternalHttpClientProvider provider = new OssInternalHttpClientProvider();

        provider.normalizeInternalUrl("http://fe-host:8080/check", InternalHttpClientProvider.Target.FE);
    }
}
