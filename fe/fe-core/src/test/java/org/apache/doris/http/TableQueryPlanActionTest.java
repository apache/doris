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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;

public class TableQueryPlanActionTest extends DorisHttpTestCase {

    private static String PATH_URI = "/_query_plan";

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
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(200, (long) ((JSONObject) jsonObject.get("data")).get("status"));

        JSONObject partitionsObject = (JSONObject) ((JSONObject) jsonObject.get("data")).get("partitions");
        Assertions.assertNotNull(partitionsObject);
        for (Object tabletKey : partitionsObject.keySet()) {
            JSONObject tabletObject = (JSONObject) partitionsObject.get(tabletKey);
            Assertions.assertNotNull(tabletObject.get("routings"));
            Assertions.assertEquals(3, ((JSONArray) tabletObject.get("routings")).size());
            Assertions.assertEquals(testStartVersion, (long) tabletObject.get("version"));
        }
        String queryPlan = (String) ((JSONObject) jsonObject.get("data")).get("opaqued_query_plan");
        Assertions.assertNotNull(queryPlan);
        byte[] binaryPlanInfo = Base64.getDecoder().decode(queryPlan);
        TDeserializer deserializer = new TDeserializer();
        TQueryPlanInfo tQueryPlanInfo = new TQueryPlanInfo();
        deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo);
        expectThrowsNoException(() -> deserializer.deserialize(tQueryPlanInfo, binaryPlanInfo));
        System.out.println(tQueryPlanInfo);
    }

    @Test
    public void testQueryPlanActionEmptyRelation() throws IOException, TException {
        RequestBody body = RequestBody.create(
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " where false \" }", JSON);
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(200, (long) ((JSONObject) jsonObject.get("data")).get("status"));

        JSONObject partitionsObject = (JSONObject) ((JSONObject) jsonObject.get("data")).get("partitions");
        Assertions.assertNotNull(partitionsObject);
        for (Object tabletKey : partitionsObject.keySet()) {
            JSONObject tabletObject = (JSONObject) partitionsObject.get(tabletKey);
            Assertions.assertNotNull(tabletObject.get("routings"));
            Assertions.assertEquals(3, ((JSONArray) tabletObject.get("routings")).size());
            Assertions.assertEquals(testStartVersion, (long) tabletObject.get("version"));
        }
        String queryPlan = (String) ((JSONObject) jsonObject.get("data")).get("opaqued_query_plan");
        Assertions.assertNotNull(queryPlan);
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
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assertions.assertNotNull(exception);
        Assertions.assertEquals("POST body must contains [sql] root object", exception);
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
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assertions.assertNotNull(exception);
        Assertions.assertEquals("POST body must contains [sql] root object", exception);
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
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(400, (long) ((JSONObject) jsonObject.get("data")).get("status"));
        String exception = (String) ((JSONObject) jsonObject.get("data")).get("exception");
        Assertions.assertNotNull(exception);
        Assertions.assertTrue(exception.startsWith("requested database and table must consistent with sql"));
    }

    @Test
    public void testMalformedJson() throws IOException {
        RequestBody body = RequestBody.create(JSON,
                "{ \"sql\" :  \" select k1,k2 from " + DB_NAME + "." + TABLE_NAME + " \"");
        Request request = new Request.Builder()
                .post(body)
                .addHeader("Authorization", rootAuth)
                .url(URI + PATH_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assertions.assertNotNull(exception);
        Assertions.assertTrue(exception.startsWith("malformed json"));
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
        Assertions.assertNotNull(response.body());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        String exception = jsonObject.get("data").toString();
        Assertions.assertTrue(exception.contains("only support single table filter-prune-scan"));
    }
}
