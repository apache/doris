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
import org.junit.Assert;
import org.junit.Test;

public class DebugPointUtilTest extends DorisHttpTestCase {

    @Test
    public void testDebugPoint() throws Exception {
        Config.enable_debug_points = true;

        Assert.assertFalse(DebugPointUtil.isEnable("dbug1"));
        sendRequest("/api/debug_point/add/dbug1");
        Assert.assertTrue(DebugPointUtil.isEnable("dbug1"));
        sendRequest("/api/debug_point/remove/dbug1");
        Assert.assertFalse(DebugPointUtil.isEnable("dbug1"));

        sendRequest("/api/debug_point/add/dbug2");
        Assert.assertTrue(DebugPointUtil.isEnable("dbug2"));
        sendRequest("/api/debug_point/clear");
        Assert.assertFalse(DebugPointUtil.isEnable("dbug2"));

        sendRequest("/api/debug_point/add/dbug3?execute=3");
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(DebugPointUtil.isEnable("dbug3"));
        }
        Assert.assertFalse(DebugPointUtil.isEnable("dbug3"));

        sendRequest("/api/debug_point/add/dbug4?timeout=1");
        Thread.sleep(200);
        Assert.assertTrue(DebugPointUtil.isEnable("dbug4"));
        Thread.sleep(1000);
        Assert.assertFalse(DebugPointUtil.isEnable("dbug4"));

        sendRequest("/api/debug_point/add/dbug5?v1=1&v2=a&v3=1.2&v4=true&v5=false");
        Assert.assertTrue(DebugPointUtil.isEnable("dbug5"));
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint("dbug5");
        Assert.assertNotNull(debugPoint);
        Assert.assertEquals(1, (int) debugPoint.param("v1", 0));
        Assert.assertEquals("a", debugPoint.param("v2", ""));
        Assert.assertEquals(1.2, debugPoint.param("v3", 0.0), 1e-6);
        Assert.assertTrue(debugPoint.param("v4", false));
        Assert.assertFalse(debugPoint.param("v5", false));
        Assert.assertEquals(123L, (long) debugPoint.param("v_no_exist", 123L));

        Assert.assertEquals(1, (int) DebugPointUtil.getDebugParamOrDefault("dbug5", "v1", 0));
        Assert.assertEquals(100, (int) DebugPointUtil.getDebugParamOrDefault("point_not_exists", "v1", 100));

        sendRequest("/api/debug_point/add/dbug6?value=100");
        Assert.assertEquals(100, (int) DebugPointUtil.getDebugParamOrDefault("dbug6", 0));
    }

    private void sendRequest(String uri) throws Exception {
        Request request = new Request.Builder()
                .post(RequestBody.create(JSON, "{}"))
                .addHeader("Authorization", rootAuth)
                .url("http://localhost:" + HTTP_PORT + uri)
                .build();

        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        Assert.assertEquals(200, response.code());
    }
}
