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

package org.apache.doris.load.loadv2;

import org.apache.doris.thrift.TLoadJob;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LoadManagerLoadsRowTest {
    @Test
    public void testLegacyShowInfoIsGroupedIntoLoadsDetails() {
        List<Comparable> showInfo = Arrays.asList(
                10001L,
                "label_1",
                "FINISHED",
                "100%",
                "BROKER",
                "dpp.norm.ALL=10; dpp.abnorm.ALL=2; unselected.rows=1",
                "cluster:default_cluster; timeout(s):3600; max_filter_ratio:0.1; priority:NORMAL",
                "type:ETL_RUN_FAIL; msg:bad row",
                "2026-07-10 10:00:00",
                "2026-07-10 10:00:01",
                "2026-07-10 10:00:02",
                "2026-07-10 10:00:02",
                "2026-07-10 10:00:03",
                "http://error-log",
                "{\"ScannedRows\":13,\"LoadBytes\":128,\"FileNumber\":2,"
                        + "\"FileSize\":256,\"FilteredRows\":2,"
                        + "\"TaskNumber\":3,\"Unfinished backends\":[3],"
                        + "\"All backends\":[1,2,3]}",
                20001L,
                "{\"10002\":\"tablet failed\"}",
                "user_1",
                "comment_1",
                "first error");

        TLoadJob job = LoadManager.toTLoadJob(showInfo);

        JsonObject etlInfo = JsonParser.parseString(job.getEtlInfo()).getAsJsonObject();
        Assert.assertEquals("10", etlInfo.get("dpp.norm.ALL").getAsString());
        Assert.assertEquals("2", etlInfo.get("dpp.abnorm.ALL").getAsString());
        Assert.assertEquals("1", etlInfo.get("unselected.rows").getAsString());
        Assert.assertEquals("2026-07-10 10:00:01",
                etlInfo.get("ETL_START_TIME").getAsString());
        Assert.assertEquals("2026-07-10 10:00:02",
                etlInfo.get("ETL_FINISH_TIME").getAsString());

        JsonObject taskInfo = JsonParser.parseString(job.getTaskInfo()).getAsJsonObject();
        Assert.assertEquals("default_cluster", taskInfo.get("cluster").getAsString());
        Assert.assertEquals("3600", taskInfo.get("timeout(s)").getAsString());
        Assert.assertEquals("0.1", taskInfo.get("max_filter_ratio").getAsString());
        Assert.assertEquals("NORMAL", taskInfo.get("priority").getAsString());
        Assert.assertEquals(13, taskInfo.get("ScannedRows").getAsLong());
        Assert.assertEquals(128, taskInfo.get("LoadBytes").getAsLong());
        Assert.assertEquals(2, taskInfo.get("FileNumber").getAsLong());
        Assert.assertEquals(256, taskInfo.get("FileSize").getAsLong());
        Assert.assertEquals(2, taskInfo.get("FilteredRows").getAsLong());
        Assert.assertEquals(1, taskInfo.getAsJsonArray("Unfinished backends").size());
        Assert.assertEquals(3, taskInfo.getAsJsonArray("All backends").size());
        Assert.assertFalse(taskInfo.has("TaskNumber"));

        JsonObject errorDetail = JsonParser.parseString(job.getErrorDetail()).getAsJsonObject();
        Assert.assertEquals("http://error-log", errorDetail.get("URL").getAsString());
        Assert.assertEquals("{\"10002\":\"tablet failed\"}",
                errorDetail.get("ERROR_TABLETS").getAsString());
        Assert.assertEquals("type:ETL_RUN_FAIL; msg:bad row",
                errorDetail.get("ERROR_MSG").getAsString());
    }
}
