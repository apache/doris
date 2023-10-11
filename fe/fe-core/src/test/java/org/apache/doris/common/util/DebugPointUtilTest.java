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
