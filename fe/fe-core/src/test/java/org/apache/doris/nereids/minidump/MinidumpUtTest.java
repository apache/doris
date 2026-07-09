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

package org.apache.doris.nereids.minidump;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.system.Frontend;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Used for add unit test of minidump
 * Input: minidump filter which describe table information and sessionVariables which different with default
 *        example: MinidumpUtTestData.json. We use this json file as input because it can be modified easily
 * Output: ResultPlan in json format, which we can get information from it directly
 */
class MinidumpUtTest {

    @Disabled
    @Test
    public void testMinidumpUt() {
        Minidump minidump = null;
        String filePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        String directory = filePath.substring(0, filePath.indexOf("/target/test-classes"));
        String currentMinidumpPath = "/src/test/java/org/apache/doris/nereids/minidump/MinidumpUtTestData.json";
        try {
            minidump = MinidumpUtils.jsonMinidumpLoad(directory + currentMinidumpPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MinidumpUtils.setConnectContext(minidump);
        JSONObject resultPlan = MinidumpUtils.executeSql("select * from t1 where l1 = 1");
        assert (minidump != null);
        assert (resultPlan != null);
    }

    @Test
    public void testSaveMinidumpStringUsesHttpsWhenEnabled() {
        Frontend fe = Mockito.mock(Frontend.class);
        Mockito.when(fe.getHost()).thenReturn("192.168.1.1");
        Env mockEnv = Mockito.mock(Env.class);

        boolean originalEnableHttps = Config.enable_https;
        int originalHttpPort = Config.http_port;
        int originalHttpsPort = Config.https_port;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<FrontendsProcNode> procStatic = Mockito.mockStatic(FrontendsProcNode.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
            procStatic.when(() -> FrontendsProcNode.getCurrentFrontendVersion(mockEnv)).thenReturn(fe);

            // enable_https=true: URL must use https scheme and https_port
            Config.enable_https = true;
            Config.https_port = 8050;
            Config.http_port = 0;
            MinidumpUtils.saveMinidumpString(new JSONObject(), "query-001");
            String url = MinidumpUtils.getHttpGetString();
            Assertions.assertTrue(url.startsWith("https://"), "Expected https scheme but got: " + url);
            Assertions.assertTrue(url.contains(":8050/"), "Expected https_port 8050 but got: " + url);

            // enable_https=false: URL must use http scheme and http_port
            Config.enable_https = false;
            Config.http_port = 8030;
            MinidumpUtils.saveMinidumpString(new JSONObject(), "query-002");
            url = MinidumpUtils.getHttpGetString();
            Assertions.assertTrue(url.startsWith("http://"), "Expected http scheme but got: " + url);
            Assertions.assertTrue(url.contains(":8030/"), "Expected http_port 8030 but got: " + url);
        } finally {
            Config.enable_https = originalEnableHttps;
            Config.http_port = originalHttpPort;
            Config.https_port = originalHttpsPort;
        }
    }
}
