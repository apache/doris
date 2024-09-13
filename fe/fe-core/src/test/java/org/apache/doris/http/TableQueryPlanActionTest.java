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

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;

public class TableQueryPlanActionTest extends DorisHttpTestCase {

    private static String PATH_URI = "/_query_plan";
    protected static String ES_TABLE_URL;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        ES_TABLE_URL = "http://localhost:" + HTTP_PORT + "/api/" + DB_NAME + "/es_table";
    }

    @Test
    public void testQueryPlanAction() throws IOException, TException {
        RequestBody body = RequestBody.create(
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \" }", JSON);
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(200, (long) ((JSONObject) jsonObject.get("data")).get("status"));

        JSONObject partitionsObject = (JSONObject) ((JSONObject) jsonObject.get("data")).get("partitions");
        Assert.assertNotNull(partitionsObject);
        for (Object tabletKey : partitionsObject.keySet()) {
            JSONObject tabletObject = (JSONObject) partitionsObject.get(tabletKey);
            Assert.assertNotNull(tabletObject.get("routings"));
            Assert.assertEquals(3, ((JSONArray) tabletObject.get("routings")).size());
            Assert.assertEquals(testStartVersion, (long) tabletObject.get("version"));
        }
        String queryPlan = (String) ((JSONObject) jsonObject.get("data")).get("opaqued_query_plan");
        Assert.assertNotNull(queryPlan);
        byte[] binaryPlanInfo = Base64.getDecoder().decode(queryPlan);
        TDeserializer deserializer = new TDeserializer();
        TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();
        deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo);
        expectThrowsNoException(() -> deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo));
        System.out.println(tQueryPlanInfo);
    }

    @Test
    public void testNoSqlFailure() throws IOException {
        RequestBody body = RequestBody.create(JSON, "{}");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertNotNull(exception);
        Assert.assertEquals("POST body must contains [sql] root object", exception);
    }

    @Test
    public void testEmptySqlFailure() throws IOException {
        RequestBody body = RequestBody.create(JSON, "{ \"sql\" :  \"\" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertNotNull(exception);
        Assert.assertEquals("POST body must contains [sql] root object", exception);
    }

    @Test
    public void testInconsistentResource() throws IOException {
        RequestBody body = RequestBody.create(JSON,
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + 1 + " \" }");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(400, (long) ((JSONObject) jsonObject.get("data")).get("status"));
        String exception = (String) ((JSONObject) jsonObject.get("data")).get("exception");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.startsWith("requested database and table must consistent with sql"));
    }

    @Test
    public void testMalformedJson() throws IOException {
        RequestBody body = RequestBody.create(JSON,
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \"");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.startsWith("malformed json"));
    }

    @Test
    public void testNotOlapTableFailure() throws IOException {
        RequestBody body = RequestBody.create(
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + ".es_table" + " \" }", JSON);
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(ES_TABLE_URL + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(1, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertTrue(exception.contains("table type is not OLAP"));
    }

    @Test
    public void testHasAggFailure() throws IOException {
        RequestBody body = RequestBody.create(
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " group by k1, k2 \" }", JSON);
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        Assert.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        String exception = jsonObject.get("data").toString();
        Assert.assertTrue(exception.contains("only support single table filter-prune-scan"));
    }
}
