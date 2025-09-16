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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.http.DorisHttpTestCase;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter.StmtContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.utframe.MockedMetaServerFactory;
import org.apache.doris.utframe.UtFrameUtils;

import mockit.Expectations;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class CopyIntoTest extends DorisHttpTestCase {
    protected static final String runningDir = "fe/mocked/" + CopyIntoTest.class.getSimpleName() + "/" + UUID.randomUUID() + "/";
    protected static int port;
    private static final String UPDATE_URI = "/copy/upload";
    private static final String QUERY_URI = "/copy/query";

    protected String rootAuth = Credentials.basic("root", "");

    @BeforeClass
    public static void beforeClass() throws Exception {
        MetricRepo.init();
        port = UtFrameUtils.createMetaServer(MockedMetaServerFactory.METASERVER_DEFAULT_IP);
    }

    @Ignore
    @Test
    public void testUpload() throws IOException {
        FeConstants.runningUnitTest = true;
        Request request = new Request.Builder()
                .put(RequestBody.create("12345".getBytes()))
                .addHeader("Authorization", rootAuth)
                .addHeader("Content-Type", "text/plain; charset=UTF-8").url(CloudURI + UPDATE_URI).build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertTrue(exception.contains("http header must have filename entry"));

        // case 1
        request = new Request.Builder()
            .put(RequestBody.create("12345".getBytes()))
            .addHeader("Authorization", rootAuth)
            .addHeader("fileName", "test.csv")
            .addHeader("Content-Type", "text/plain; charset=UTF-8").url(CloudURI + UPDATE_URI).build();

        Config.cloud_unique_id = "Internal-MetaServiceCode.OK";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + port;
        response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.request().url().toString().contains("http://bucketbucket.cos.ap-beijing.myqcloud.internal.com/ut-test/test.csv"));

        // case 2 add header endpointHeader, __USE_ENDPOINT__
        request = new Request.Builder()
            .put(RequestBody.create("12345".getBytes()))
            .addHeader("Authorization", rootAuth)
            .addHeader("fileName", "test.csv")
            .addHeader("__USE_ENDPOINT__", "internal")
            .addHeader("Content-Type", "text/plain; charset=UTF-8").url(CloudURI + UPDATE_URI).build();

        Config.cloud_unique_id = "Internal-MetaServiceCode.OK";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + port;
        response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.request().url().toString().contains("http://bucketbucket.cos.ap-beijing.myqcloud.internal.com/ut-test/test.csv"));

        // case 3 add header endpointHeader, __USE_ENDPOINT__
        request = new Request.Builder()
            .put(RequestBody.create("12345".getBytes()))
            .addHeader("Authorization", rootAuth)
            .addHeader("fileName", "test.csv")
            .addHeader("__USE_ENDPOINT__", "external")
            .addHeader("Content-Type", "text/plain; charset=UTF-8").url(CloudURI + UPDATE_URI).build();

        Config.cloud_unique_id = "Internal-MetaServiceCode.OK";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + port;
        response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.request().url().toString().contains("http://bucketbucket.cos.ap-beijing.myqcloud.com/ut-test/test.csv"));

        // case 4 add header endpointHeader, host
        request = new Request.Builder()
            .put(RequestBody.create("12345".getBytes()))
            .addHeader("Authorization", rootAuth)
            .addHeader("fileName", "test.csv")
            .addHeader("host", "192.168.0.1:7788")
            .addHeader("Content-Type", "text/plain; charset=UTF-8").url(CloudURI + UPDATE_URI).build();

        Config.cloud_unique_id = "Internal-MetaServiceCode.OK";
        Config.meta_service_endpoint = MockedMetaServerFactory.METASERVER_DEFAULT_IP + ":" + port;
        response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.request().url().toString().contains("http://bucketbucket.cos.ap-beijing.myqcloud.com/ut-test/test.csv"));
    }

    @Test
    public void testQuery() throws IOException, ExecutionException, InterruptedException {
        String emptySql = JSONObject.toJSONString(new HashMap(){});
        Request request = new Request.Builder().post(RequestBody.create(emptySql.getBytes())).addHeader("Authorization", rootAuth)
                .addHeader("Content-Type", "application/json").url(CloudURI + QUERY_URI).build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertNotNull(response.body());
        String respStr = response.body().string();
        JSONObject jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(403, (long) jsonObject.get("code"));
        String exception = (String) jsonObject.get("data");
        Assert.assertTrue(exception.contains("POST body must contain [sql] root object"));

        java.util.concurrent.FutureTask<ExecutionResultSet> ret = new FutureTask<>(() -> new ExecutionResultSet(new HashMap<>()));

        HashMap<String, Object> om = new HashMap<>();
        HashMap<String, Object> im = new HashMap<>();
        im.put("copyId", "copy_1296997def6d4887_9e7ff31a7f3842cc");
        im.put("msg", "");
        im.put("loadedRows", "");
        im.put("state", "CANCELLED");
        im.put("type", "LOAD_RUN_FAIL");
        im.put("filterRows", "");
        im.put("unselectRows", "");
        im.put("url", null);
        om.put("result", im);
        ExecutionResultSet e = new ExecutionResultSet(om);

        new Expectations(CopyIntoAction.getStmtSubmitter(), ret) {
            {
                CopyIntoAction.getStmtSubmitter().submitBlock((StmtContext) any);
                minTimes = 0;
                result = ret;

                ret.get();
                minTimes = 0;
                result = e;
            }
        };

        Map<String, String> m = new HashMap<>();
        m.put("sql", "copy into db1.t2 from @~(\"{t3.dat}\")");
        String copyIntoSql = JSONObject.toJSONString(m);
        request = new Request.Builder().post(RequestBody.create(copyIntoSql.getBytes())).addHeader("Authorization", rootAuth)
                .addHeader("Content-Type", "application/json").url(CloudURI + QUERY_URI).build();
        response = networkClient.newCall(request).execute();
        respStr = response.body().string();
        // {"msg":"success","code":0,"data":{"result":{"copyId":"copy_1296997def6d4887_9e7ff31a7f3842cc","msg":"","loadedRows":"","state":"CANCELLED","type":"LOAD_RUN_FAIL","filterRows":"","unselectRows":"","url":null}},"count":0}
        System.out.println(respStr);
        jsonObject = (JSONObject) JSONValue.parse(respStr);
        Assert.assertEquals(0, (long) jsonObject.get("code"));
        JSONObject data = (JSONObject) jsonObject.get("data");
        JSONObject result = (JSONObject) data.get("result");
        String copyId = (String) result.get("copyId");
        Assert.assertEquals(copyId, "copy_1296997def6d4887_9e7ff31a7f3842cc");
    }
}
