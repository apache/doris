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

package org.apache.doris.http;

import org.apache.doris.common.FeConstants;

import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Test;

public class ForwardToMasterTest extends DorisHttpTestCase {
    @Test
    public void testAddBeDropBe() throws Exception {
        FeConstants.runningUnitTest = true;
        String port = "48013";
            {
                //query backends
                String url = "http://localhost:" + HTTP_PORT + "/rest/v2/manager/node/backends";
                Request request = new Request.Builder()
                        .get()
                        .addHeader("Authorization", rootAuth)
                        .addHeader("forward_master_ut_test", "true")
                        .url(url)
                        .build();
                Response response = networkClient.newCall(request).execute();
                Assert.assertTrue(response.isSuccessful());
                Assert.assertNotNull(response.body());
                String respStr = response.body().string();
                JSONObject object = (JSONObject) JSONValue.parse(respStr);

                JSONObject data = (JSONObject) object.get("data");
                JSONArray columnNames = (JSONArray) ((JSONObject) data.get("columnNames")).get("columnNames");
                JSONArray rows = (JSONArray) ((JSONObject) data.get("rows")).get("rows");
                int sz = columnNames.size();
                int index = columnNames.indexOf("HeartbeatPort");
                int existsbe = 0;
                for (int i = 0; i < rows.size(); i += sz) {
                    if (port.equals(rows.get(i + index).toString())) {
                        existsbe++;
                    }
                }
                Assert.assertEquals(0, existsbe);
            }

            {
                String jsonBody = "{"
                        + "  \"hostPorts\": [\"localhost:" + port + "\"],"
                        + "  \"properties\": {"
                        + "    \"key1\": \"value1\","
                        + "    \"key2\": \"value2\""
                        + "  }"
                        + "}";

                String url = "http://localhost:" + HTTP_PORT + "/rest/v2/manager/node/ADD/be";
                Request request = new Request.Builder()
                        .url(url)
                        .addHeader("Content-Type", "application/json")
                        .addHeader("Authorization", rootAuth)
                        .addHeader("forward_master_ut_test", "true")
                        .post(RequestBody.create(jsonBody, MediaType.parse("application/json")))
                        .build();
                Response response = networkClient.newCall(request).execute();
                Assert.assertTrue(response.isSuccessful());
            }

            {
                //query be
                String url = "http://localhost:" + HTTP_PORT + "/rest/v2/manager/node/backends";
                Request request = new Request.Builder()
                        .get()
                        .addHeader("Authorization", rootAuth)
                        .addHeader("forward_master_ut_test", "true")
                        .url(url)
                        .build();
                Response response = networkClient.newCall(request).execute();
                Assert.assertTrue(response.isSuccessful());
                Assert.assertNotNull(response.body());
                String respStr = response.body().string();

                JSONObject object = (JSONObject) JSONValue.parse(respStr);
                JSONObject data = (JSONObject) object.get("data");
                JSONArray columnNames = (JSONArray) ((JSONObject) data.get("columnNames")).get("columnNames");
                JSONArray rows = (JSONArray) ((JSONObject) data.get("rows")).get("rows");
                int sz = columnNames.size();
                int index = columnNames.indexOf("HeartbeatPort");
                int existsbe = 0;
                for (int i = 0; i < rows.size(); i += sz) {
                    if (port.equals(rows.get(i + index).toString())) {
                        existsbe++;
                    }
                }
                Assert.assertEquals(1, existsbe);
            }

            {
                // DROP be
                String jsonBody = "{"
                        + "  \"hostPorts\": [\"localhost:" + port + "\"],"
                        + "  \"properties\": {"
                        + "    \"key1\": \"value1\","
                        + "    \"key2\": \"value2\""
                        + "  }"
                        + "}";

                String url = "http://localhost:" + HTTP_PORT + "/rest/v2/manager/node/DROP/be";
                Request request = new Request.Builder()
                        .url(url)
                        .addHeader("Content-Type", "application/json")
                        .addHeader("Authorization", rootAuth)
                        .addHeader("forward_master_ut_test", "true")
                        .post(RequestBody.create(jsonBody, MediaType.parse("application/json")))
                        .build();
                Response response = networkClient.newCall(request).execute();
                Assert.assertTrue(response.isSuccessful());
            }

            {
                //query be
                String url = "http://localhost:" + HTTP_PORT + "/rest/v2/manager/node/backends";
                Request request = new Request.Builder()
                        .get()
                        .addHeader("forward_master_ut_test", "true")
                        .addHeader("Authorization", rootAuth)
                        .url(url)
                        .build();
                Response response = networkClient.newCall(request).execute();
                Assert.assertTrue(response.isSuccessful());
                Assert.assertNotNull(response.body());
                String respStr = response.body().string();
                JSONObject object = (JSONObject) JSONValue.parse(respStr);

                JSONObject data = (JSONObject) object.get("data");
                JSONArray columnNames = (JSONArray) ((JSONObject) data.get("columnNames")).get("columnNames");
                JSONArray rows = (JSONArray) ((JSONObject) data.get("rows")).get("rows");
                int sz = columnNames.size();
                int index = columnNames.indexOf("HeartbeatPort");
                int existsbe = 0;
                for (int i = 0; i < rows.size(); i += sz) {
                    if (port.equals(rows.get(i + index).toString())) {
                        existsbe++;
                    }
                }
                Assert.assertEquals(0, existsbe);
            }
    }

    @Test
    public void testPost1() throws Exception {
        FeConstants.runningUnitTest = true;
        RequestBody emptyBody = RequestBody.create(new byte[0], null);
        String url = "http://localhost:" + HTTP_PORT + "/api/dbforwardmaster/_cancel?label=1";
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", rootAuth)
                .addHeader("forward_master_ut_test", "true")
                .post(emptyBody)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject object = (JSONObject) JSONValue.parse(respStr);

        String data = (String) object.get("data");
        Assert.assertTrue(data.contains("does not exist"));
    }

    @Test
    public void testPost2() throws Exception {
        FeConstants.runningUnitTest = true;
        String url = "http://localhost:" + HTTP_PORT + "/api/colocate/group_stable";
        RequestBody requestBody = new okhttp3.FormBody.Builder()
                .add("group_id", "18888")
                .add("db_id", "99999999")
                .build();

        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .addHeader("forward_master_ut_test", "true")
                .addHeader("Authorization", rootAuth)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject object = (JSONObject) JSONValue.parse(respStr);

        String data = (String) object.get("data");
        Assert.assertTrue(data.contains("the group 99999999.18888 isn't exist"));
    }

    @Test
    public void testGet1() throws Exception {
        FeConstants.runningUnitTest = true;
        String url = "http://localhost:" + HTTP_PORT + "/rest/v2/api/cluster_overview";
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .addHeader("forward_master_ut_test", "true")
                .url(url)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject object = (JSONObject) JSONValue.parse(respStr);

        JSONObject data = (JSONObject) object.get("data");
        Assert.assertTrue(data.toString().contains("diskOccupancy"));
    }
}
