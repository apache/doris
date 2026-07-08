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

package org.apache.doris.load;

import org.apache.doris.thrift.TLoadJob;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

public class StreamLoadRecordMgrTest {

    @Test
    public void testStreamLoadRecordToLoadJob() {
        // "Success" should be mapped to the unified JobState vocabulary "FINISHED".
        StreamLoadRecord record = new StreamLoadRecord("label_1", "db_1", "tbl_1", "127.0.0.1",
                "Success", "OK", "N/A", "10", "9", "1", "0", "128", "3", "7",
                "1000", "2000", "user_1", "comment_1", "first_error");

        TLoadJob job = StreamLoadRecordMgr.streamLoadRecordToLoadJob(record);

        Assert.assertEquals("", job.getJobId());
        Assert.assertEquals("label_1", job.getLabel());
        // STATE must use the unified LoadManager vocabulary, not the raw Stream Load status.
        Assert.assertEquals("FINISHED", job.getState());
        Assert.assertEquals("100%", job.getProgress());
        Assert.assertEquals("STREAM_LOAD", job.getType());
        Assert.assertEquals("", job.getEtlInfo());
        Assert.assertEquals("", job.getCreateTime());
        Assert.assertEquals("", job.getEtlStartTime());
        Assert.assertEquals("", job.getEtlFinishTime());
        Assert.assertEquals("1000", job.getLoadStartTime());
        Assert.assertEquals("2000", job.getLoadFinishTime());
        Assert.assertEquals("", job.getTransactionId());
        Assert.assertEquals("", job.getErrorTablets());
        Assert.assertEquals("user_1", job.getUser());
        Assert.assertEquals("comment_1", job.getComment());
        Assert.assertEquals("first_error", job.getFirstErrorMsg());

        JsonObject taskInfo = JsonParser.parseString(job.getTaskInfo()).getAsJsonObject();
        Assert.assertEquals("db_1", taskInfo.get("Db").getAsString());
        Assert.assertEquals("tbl_1", taskInfo.get("Table").getAsString());
        Assert.assertEquals("127.0.0.1", taskInfo.get("ClientIp").getAsString());

        JsonObject jobDetails = JsonParser.parseString(job.getJobDetails()).getAsJsonObject();
        Assert.assertEquals("10", jobDetails.get("TotalRows").getAsString());
        Assert.assertEquals("9", jobDetails.get("LoadedRows").getAsString());
        Assert.assertEquals("1", jobDetails.get("FilteredRows").getAsString());
        Assert.assertEquals("0", jobDetails.get("UnselectedRows").getAsString());
        Assert.assertEquals("128", jobDetails.get("LoadBytes").getAsString());
        Assert.assertEquals("3", jobDetails.get("BeginTxnTimeMs").getAsString());
        Assert.assertEquals("7", jobDetails.get("StreamLoadPutTimeMs").getAsString());
    }

    /**
     * Full STATE mapping coverage — all four Stream Load statuses to the unified vocabulary.
     */
    @Test
    public void testUnifyStreamLoadState() {
        Assert.assertEquals("FINISHED", StreamLoadRecordMgr.unifyStreamLoadState("Success"));
        Assert.assertEquals("FINISHED", StreamLoadRecordMgr.unifyStreamLoadState("Publish Timeout"));
        Assert.assertEquals("CANCELLED", StreamLoadRecordMgr.unifyStreamLoadState("Fail"));
        Assert.assertEquals("CANCELLED", StreamLoadRecordMgr.unifyStreamLoadState("Label Already Exists"));
        // Unknown status is preserved as-is (forward-compatible with future BE additions).
        Assert.assertEquals("SomeFuture", StreamLoadRecordMgr.unifyStreamLoadState("SomeFuture"));
        // Null is handled gracefully.
        Assert.assertEquals("", StreamLoadRecordMgr.unifyStreamLoadState(null));
    }
}
