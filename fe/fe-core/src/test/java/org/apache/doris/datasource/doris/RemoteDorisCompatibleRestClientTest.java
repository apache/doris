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

package org.apache.doris.datasource.doris;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.http.DorisHttpTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteDorisCompatibleRestClientTest extends DorisHttpTestCase {

    @Test
    public void testGetColumns() throws Exception {
        String executeRes = execute("api/" + DB_NAME + "/" + TABLE_NAME + "/_schema");
        RemoteDorisCompatibleRestClient.DorisApiResponse tableSchemaResponse = RemoteDorisCompatibleRestClient.parseResponse(
            executeRes,
            "get doris table schema error");
        ObjectNode objectNode = JsonUtil.parseObject(tableSchemaResponse.getData());
        JsonNode properties = objectNode.path("properties");
        List<Column> res = new ArrayList<>();
        for (JsonNode columnJson : properties) {
            if (columnJson.isObject()) {
                res.add(RemoteDorisCompatibleRestClient.parseColumn((ObjectNode) columnJson));
            }
        }

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        Column k2 = new Column("k2", PrimitiveType.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);

        Assert.assertArrayEquals(columns.toArray(), res.toArray());
    }

    private String execute(String url) throws IOException {
        Request request = new Request.Builder()
            .get()
            .addHeader("Authorization", rootAuth)
            .addHeader("forward_master_ut_test", "true")
            .url("http://localhost:" + HTTP_PORT + "/" + url)
            .build();
        Response response = networkClient.newCall(request).execute();
        return response.body().string();
    }
}
