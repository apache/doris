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

import org.apache.doris.thrift.TQueryPlanInfo;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class TableQueryPlanActionTest extends DorisHttpTestCase {

    private static  String PATH_URI = "/_query_plan";
    protected static String ES_TABLE_URL;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        ES_TABLE_URL = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/es_table";
    }
    @Test
    public void testQueryPlanAction() throws IOException, TException {
        RequestBody body = RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        JSONObject jsonObject = new JSONObject(respStr);
        System.out.println(respStr);
        Assert.assertEquals(200, jsonObject.getInt("status"));

        JSONObject partitionsObject = jsonObject.getJSONObject("partitions");
        Assert.assertNotNull(partitionsObject);
        for (String tabletKey : partitionsObject.keySet()) {
            JSONObject tabletObject = partitionsObject.getJSONObject(tabletKey);
            Assert.assertNotNull(tabletObject.getJSONArray("routings"));
            Assert.assertEquals(3, tabletObject.getJSONArray("routings").length());
            Assert.assertEquals(testStartVersion, tabletObject.getLong("version"));
            Assert.assertEquals(testStartVersionHash, tabletObject.getLong("versionHash"));
            Assert.assertEquals(testSchemaHash, tabletObject.getLong("schemaHash"));

        }
        String queryPlan = jsonObject.getString("opaqued_query_plan");
        Assert.assertNotNull(queryPlan);
        byte[] binaryPlanInfo = Base64.getDecoder().decode(queryPlan);
        TDeserializer deserializer = new TDeserializer();
        TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();
        deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo);
        expectThrowsNoException(() -> deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo));
        System.out.println(tQueryPlanInfo);
    }

    @Test
    public void testInconsistentResource() throws IOException {
        RequestBody body = RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + 1 + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        System.out.println(respStr);
        Assert.assertNotNull(respStr);
        expectThrowsNoException(() -> new JSONObject(respStr));
        JSONObject jsonObject = new JSONObject(respStr);
        Assert.assertEquals(400, jsonObject.getInt("status"));
        String exception = jsonObject.getString("exception");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.startsWith("requested database and table must consistent with sql"));
    }

    @Test
    public void testMalformedJson() throws IOException {
        RequestBody body = RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \"");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        expectThrowsNoException(() -> new JSONObject(respStr));
        JSONObject jsonObject = new JSONObject(respStr);
        Assert.assertEquals(400, jsonObject.getInt("status"));
        String exception = jsonObject.getString("exception");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.startsWith("malformed json"));
    }

    @Test
    public void testNotOlapTableFailure() throws IOException {
        RequestBody body = RequestBody.create(JSON, "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + ".es_table" + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        expectThrowsNoException(() -> new JSONObject(respStr));
        JSONObject jsonObject = new JSONObject(respStr);
        Assert.assertEquals(403, jsonObject.getInt("status"));
        String exception = jsonObject.getString("exception");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.startsWith("only support OlapTable currently"));
    }
}
