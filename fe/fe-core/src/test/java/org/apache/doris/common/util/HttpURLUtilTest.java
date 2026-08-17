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
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.meta.MetaBaseAction;
import org.apache.doris.system.SystemInfoService.HostInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.util.Map;

public class HttpURLUtilTest {

    @After
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

            Assert.assertEquals("127.0.0.1", headers.get(Env.CLIENT_NODE_HOST_KEY));
            Assert.assertEquals("9010", headers.get(Env.CLIENT_NODE_PORT_KEY));
            Assert.assertEquals("cluster-token", headers.get(MetaBaseAction.TOKEN));
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

            Assert.assertEquals("127.0.0.1", headers.get(Env.CLIENT_NODE_HOST_KEY));
            Assert.assertFalse(headers.containsKey(MetaBaseAction.TOKEN));
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

            Assert.assertEquals("127.0.0.1", connection.getRequestProperty(Env.CLIENT_NODE_HOST_KEY));
            Assert.assertEquals("9010", connection.getRequestProperty(Env.CLIENT_NODE_PORT_KEY));
            Assert.assertEquals("cluster-token", connection.getRequestProperty(MetaBaseAction.TOKEN));
        }
    }
}
