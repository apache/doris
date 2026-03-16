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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.system.SystemInfoService.HostInfo;

import mockit.Mock;
import mockit.MockUp;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LogQueryActionTest extends DorisHttpTestCase {
    private String originalSysLogDir;

    @Override
    public void doSetUp() {
        originalSysLogDir = Config.sys_log_dir;
        new MockUp<Env>() {
            @Mock
            HostInfo getSelfNode() {
                return new HostInfo("localhost", HTTP_PORT);
            }
        };
    }

    @Test
    public void testGroupedLogQuery() throws Exception {
        Path logDir = Paths.get(getDorisHome(), "log");
        Files.createDirectories(logDir);
        Files.write(logDir.resolve("fe.warn.log"), String.join("\n",
                "2026-03-16 11:06:22,507 WARN publish version bad tablet_id=20001730 host=11.1.1.1:8240",
                "2026-03-16 11:06:23,507 WARN publish version bad tablet_id=20001731 host=11.1.1.2:8240",
                "2026-03-16 11:06:24,507 WARN other warning").getBytes(StandardCharsets.UTF_8));
        Config.sys_log_dir = logDir.toString();

        String jsonBody = "{"
                + "\"frontendNodes\":[\"localhost:" + HTTP_PORT + "\"],"
                + "\"logTypes\":[\"fe.warn\"],"
                + "\"keyword\":\"publish version\","
                + "\"reductionMode\":\"grouped\","
                + "\"maxEntries\":10"
                + "}";

        Request request = new Request.Builder()
                .url(CloudURI + "/rest/v2/manager/logs/query")
                .addHeader("Authorization", rootAuth)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(jsonBody, MediaType.parse("application/json")))
                .build();
        Response response = networkClient.newCall(request).execute();
        Assert.assertTrue(response.isSuccessful());
        Assert.assertNotNull(response.body());
        JSONObject root = (JSONObject) JSONValue.parse(response.body().string());
        Assert.assertEquals(0L, root.get("code"));

        JSONObject data = (JSONObject) root.get("data");
        JSONArray results = (JSONArray) data.get("results");
        Assert.assertEquals(1, results.size());
        JSONObject result = (JSONObject) results.get(0);
        Assert.assertEquals("fe.warn", result.get("logType"));
        Assert.assertEquals(2L, result.get("matchedEventCount"));
        JSONArray groups = (JSONArray) result.get("groups");
        Assert.assertEquals(1, groups.size());
        JSONObject group = (JSONObject) groups.get(0);
        Assert.assertEquals(2L, group.get("count"));
    }

    private String getDorisHome() throws NoSuchFieldException, IllegalAccessException {
        Field field = DorisHttpTestCase.class.getDeclaredField("DORIS_HOME");
        field.setAccessible(true);
        return (String) field.get(null);
    }

    @Override
    public void tearDown() {
        Config.sys_log_dir = originalSysLogDir;
    }
}
