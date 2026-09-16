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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.client.InternalHttpClientProvider;
import org.apache.doris.httpv2.client.InternalHttpClientProviderFactory;
import org.apache.doris.httpv2.meta.MetaBaseAction;
import org.apache.doris.system.SystemInfoService.HostInfo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.util.Map;

public class HttpURLUtilTest {

    @AfterEach
    public void tearDown() {
        Config.enable_https = false;
        Config.http_port = 8030;
        Config.https_port = 8050;
        Config.fe_meta_auth_token = "";
    }

    @Test
    public void testNodeIdentHeadersIncludeClusterToken() throws Exception {
        Config.fe_meta_auth_token = "cluster-token";
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getServingEnv).thenReturn(env);

            Map<String, String> headers = HttpURLUtil.getNodeIdentHeaders();

            Assertions.assertEquals("127.0.0.1", headers.get(Env.CLIENT_NODE_HOST_KEY));
            Assertions.assertEquals("9010", headers.get(Env.CLIENT_NODE_PORT_KEY));
            Assertions.assertEquals("cluster-token", headers.get(MetaBaseAction.TOKEN));
        }
    }

    @Test
    public void testNodeIdentHeadersOmitTokenWhenNotConfigured() throws Exception {
        Config.fe_meta_auth_token = "";
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getServingEnv).thenReturn(env);

            Map<String, String> headers = HttpURLUtil.getNodeIdentHeaders();

            Assertions.assertEquals("127.0.0.1", headers.get(Env.CLIENT_NODE_HOST_KEY));
            Assertions.assertFalse(headers.containsKey(MetaBaseAction.TOKEN));
        }
    }

    @Test
    public void testNodeIdentConnectionIncludesClusterToken() throws Exception {
        Config.fe_meta_auth_token = "cluster-token";
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getServingEnv).thenReturn(env);

            HttpURLConnection connection = HttpURLUtil.getConnectionWithNodeIdent("http://127.0.0.1:8030/info");

            Assertions.assertEquals("127.0.0.1", connection.getRequestProperty(Env.CLIENT_NODE_HOST_KEY));
            Assertions.assertEquals("9010", connection.getRequestProperty(Env.CLIENT_NODE_PORT_KEY));
            Assertions.assertEquals("cluster-token", connection.getRequestProperty(MetaBaseAction.TOKEN));
        }
    }

    @Test
    public void testNodeIdentConnectionChecksAndOpensNormalizedUrl() throws Exception {
        String request = "http://fe-host:8030/info";
        String normalizedRequest = "https://fe-host:8050/info";
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        InternalHttpClientProvider provider = Mockito.mock(InternalHttpClientProvider.class);
        HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
        Mockito.when(provider.normalizeInternalUrl(request, InternalHttpClientProvider.Target.FE))
                .thenReturn(normalizedRequest);
        Mockito.when(provider.openConnection(normalizedRequest, InternalHttpClientProvider.Target.FE))
                .thenReturn(connection);
        SecurityChecker securityChecker = Mockito.mock(SecurityChecker.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<InternalHttpClientProviderFactory> providerFactory =
                        Mockito.mockStatic(InternalHttpClientProviderFactory.class);
                MockedStatic<SecurityChecker> securityCheckerFactory = Mockito.mockStatic(SecurityChecker.class)) {
            envStatic.when(Env::getServingEnv).thenReturn(env);
            providerFactory.when(InternalHttpClientProviderFactory::getProvider).thenReturn(provider);
            securityCheckerFactory.when(SecurityChecker::getInstance).thenReturn(securityChecker);

            Assertions.assertSame(connection, HttpURLUtil.getConnectionWithNodeIdent(request));

            Mockito.verify(securityChecker).startSSRFChecking(normalizedRequest);
            Mockito.verify(provider).openConnection(normalizedRequest, InternalHttpClientProvider.Target.FE);
            Mockito.verify(securityChecker).stopSSRFChecking();
        }
    }

    @Test
    public void testBuildInternalFeUrlHttp() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/put", "version=123&port=8030");
        Assertions.assertEquals("http://192.168.1.10:8030/put?version=123&port=8030", url);
    }

    @Test
    public void testBuildInternalFeUrlHttps() {
        Config.enable_https = true;
        Config.https_port = 8050;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/put", "version=123&port=8050");
        Assertions.assertEquals("https://192.168.1.10:8050/put?version=123&port=8050", url);
    }

    @Test
    public void testBuildInternalFeUrlNoQueryParams() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/journal_id", null);
        Assertions.assertEquals("http://192.168.1.10:8030/journal_id", url);
    }

    @Test
    public void testBuildInternalFeUrlEmptyQueryParams() {
        Config.enable_https = false;
        Config.http_port = 8030;

        String url = HttpURLUtil.buildInternalFeUrl("192.168.1.10", "/version", "");
        Assertions.assertEquals("http://192.168.1.10:8030/version", url);
    }

    @Test
    public void testBuildInternalFeUrlHttpsWithIPv6() {
        Config.enable_https = true;
        Config.https_port = 8050;

        String url = HttpURLUtil.buildInternalFeUrl("fe80::1", "/role", "host=fe80::2&port=9010");
        Assertions.assertTrue(url.startsWith("https://"));
        Assertions.assertTrue(url.contains("/role?host=fe80::2&port=9010"));
    }
}
