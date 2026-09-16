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

import okhttp3.Request;
import okhttp3.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TableSchemaActionTest extends DorisHttpTestCase {

    private static final String QUERY_PLAN_URI = "/_schema";

    @Test
    public void testGetTableSchema() throws IOException {
        Request request = new Request.Builder()
                .get()
                .addHeader("Authorization", rootAuth)
                .url(URI + QUERY_PLAN_URI)
                .build();
        Response response = networkClient.newCall(request).execute();
        Assertions.assertTrue(response.isSuccessful());
        String respStr = response.body().string();
        Assertions.assertNotNull(respStr);
        JSONObject object = (JSONObject) JSONValue.parse(respStr);
        Assertions.assertEquals(200, (long) ((JSONObject) object.get("data")).get("status"));
        JSONArray propArray = (JSONArray) ((JSONObject) object.get("data")).get("properties");
        // k1, k2
        Assertions.assertEquals(2, propArray.size());

        JSONObject column = (JSONObject) propArray.get(0);
        Assertions.assertEquals("BIGINT", column.get("type"));
        Assertions.assertEquals("bigint", column.get("type_sql"));
        Assertions.assertEquals("BIGINT", ((JSONObject) column.get("type_desc")).get("kind"));

        JSONObject materializedIndexes =
                (JSONObject) ((JSONObject) object.get("data")).get("materialized_indexes");
        JSONObject baseIndex = (JSONObject) materializedIndexes.get("testIndex");
        JSONArray indexColumns = (JSONArray) baseIndex.get("columns");
        Assertions.assertEquals("BIGINT",
                ((JSONObject) ((JSONObject) indexColumns.get(0)).get("type_desc")).get("kind"));
    }
}
