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
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.http.DorisHttpTestCase;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DebugPointUtilTest extends DorisHttpTestCase {

    @Test
    public void testDebugPoint() throws Exception {
        Config.enable_debug_points = true;

        Assertions.assertFalse(DebugPointUtil.isEnable("dbug1"));
        sendRequest("/api/debug_point/add/dbug1");
        Assertions.assertTrue(DebugPointUtil.isEnable("dbug1"));
        sendRequest("/api/debug_point/remove/dbug1");
        Assertions.assertFalse(DebugPointUtil.isEnable("dbug1"));

        sendRequest("/api/debug_point/add/dbug2");
        Assertions.assertTrue(DebugPointUtil.isEnable("dbug2"));
        sendRequest("/api/debug_point/clear");
        Assertions.assertFalse(DebugPointUtil.isEnable("dbug2"));

        sendRequest("/api/debug_point/add/dbug3?execute=3");
        for (int i = 0; i < 3; i++) {
            Assertions.assertTrue(DebugPointUtil.isEnable("dbug3"));
        }
        Assertions.assertFalse(DebugPointUtil.isEnable("dbug3"));

        sendRequest("/api/debug_point/add/dbug4?timeout=1");
        Thread.sleep(200);
        Assertions.assertTrue(DebugPointUtil.isEnable("dbug4"));
        Thread.sleep(1000);
        Assertions.assertFalse(DebugPointUtil.isEnable("dbug4"));

        sendRequest("/api/debug_point/add/dbug5?v1=1&v2=a&v3=1.2&v4=true&v5=false");
        Assertions.assertTrue(DebugPointUtil.isEnable("dbug5"));
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint("dbug5");
        Assertions.assertNotNull(debugPoint);
        Assertions.assertEquals(1, (int) debugPoint.param("v1", 0));
        Assertions.assertEquals("a", debugPoint.param("v2", ""));
        Assertions.assertEquals(1.2, debugPoint.param("v3", 0.0), 1e-6);
        Assertions.assertTrue(debugPoint.param("v4", false));
        Assertions.assertFalse(debugPoint.param("v5", false));
        Assertions.assertEquals(123L, (long) debugPoint.param("v_no_exist", 123L));

        Assertions.assertEquals(1, (int) DebugPointUtil.getDebugParamOrDefault("dbug5", "v1", 0));
        Assertions.assertEquals(100, (int) DebugPointUtil.getDebugParamOrDefault("point_not_exists", "v1", 100));

        sendRequest("/api/debug_point/add/dbug6?value=100");
        Assertions.assertEquals(100, (int) DebugPointUtil.getDebugParamOrDefault("dbug6", 0));
    }

    private void sendRequest(String uri) throws Exception {
        Request request = new Request.Builder()
                .post(RequestBody.create(JSON, "{}"))
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + uri)
                .build();

        Response response = networkClient.newCall(request).execute();
        Assertions.assertNotNull(response.body());
        Assertions.assertEquals(200, response.code());
    }
}
