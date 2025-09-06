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

import okhttp3.Request;
import okhttp3.Response;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.http.DorisHttpTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteDorisRestClientTest extends DorisHttpTestCase {
    @Test
    public void testGetDatabaseNameList() throws Exception {
        List<String> res = RemoteDorisRestClient.parseStringLists(
            execute("api/meta/namespaces/default_cluster/databases"));
        Assert.assertArrayEquals(new String[]{DB_NAME}, res.toArray());
    }

    @Test
    public void testGetTablesNameList() throws Exception {
        List<String> res = RemoteDorisRestClient.parseStringLists(
            execute("api/meta/namespaces/default_cluster/databases/" + DB_NAME + "/tables"));
        Assert.assertArrayEquals(new String[]{"es_table","testTbl1"}, res.toArray());
    }

    @Test
    public void testGetTablesNameListByErrorDb() throws Exception {
        List<String> res = RemoteDorisRestClient.parseStringLists(
            execute("api/meta/namespaces/default_cluster/databases/not_" + DB_NAME + "/tables"));
        Assert.assertEquals(0, res.size());
    }

    @Test
    public void testTableExist() throws Exception {
        boolean res = RemoteDorisRestClient.parseSuccessResponse(
            execute("api/" + DB_NAME + "/" + TABLE_NAME + "/_schema"));
        Assert.assertTrue(res);
    }

    @Test
    public void testTableNotExist() throws Exception {
        boolean res = RemoteDorisRestClient.parseSuccessResponse(
            execute("api/" + DB_NAME + "/not_" + TABLE_NAME + "/_schema"));
        Assert.assertFalse(res);
    }

    @Test
    public void testHealth() throws Exception {
        int res = RemoteDorisRestClient.parseOnlineBeNum(
            execute("api/health"));
        Assert.assertEquals(3, res);
    }

    @Test
    public void testGetColumns() throws Exception {
        List<Column> res = RemoteDorisRestClient.parseColumns(
            execute("api/" + DB_NAME + "/" + TABLE_NAME + "/_gson_schema"));

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        Column k2 = new Column("k2", PrimitiveType.DOUBLE);
        List<Column> columns = new ArrayList<>();
        columns.add(k1);
        columns.add(k2);

        Assert.assertArrayEquals(columns.toArray(), res.toArray());
    }

    @Test
    public void testGetRowCount() throws Exception {
        long res = RemoteDorisRestClient.parseRowCount(
            execute("api/rowcount?db=" + DB_NAME + "&table=" + TABLE_NAME));

        Assert.assertEquals(2000L, res);
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
