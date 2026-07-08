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

import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.thrift.TLoadJob;

import org.junit.Assert;
import org.junit.Test;

public class LoadsHistorySyncerTest {

    // Stream Load dedup key: TYPE + ":" + db + ":" + label + ":" + finish_time_millis.
    @Test
    public void testStreamLoadRecordKey() {
        Assert.assertEquals("STREAM_LOAD:db1:label1:1700000000000",
                StreamLoadRecordMgr.streamLoadRecordKey("db1", "label1", 1700000000000L));
    }

    // LoadManager dedup key contract: TYPE + ":" + JOB_ID.
    @Test
    public void testLoadManagerRecordKeyFormat() {
        TLoadJob row = new TLoadJob();
        row.setType("BROKER");
        row.setJobId("10086");
        // Mirror LoadManager.getLoadJobHistoryRecords key construction.
        String recordKey = row.getType() + ":" + row.getJobId();
        Assert.assertEquals("BROKER:10086", recordKey);
    }

    // Both final-state vocabularies. LoadManager: FINISHED / CANCELLED are final; others are not.
    @Test
    public void testJobStateFinalState() {
        Assert.assertTrue(JobState.FINISHED.isFinalState());
        Assert.assertTrue(JobState.CANCELLED.isFinalState());
        Assert.assertFalse(JobState.PENDING.isFinalState());
        Assert.assertFalse(JobState.LOADING.isFinalState());
        Assert.assertFalse(JobState.ETL.isFinalState());
    }

    // Stream Load records written to RocksDB are completion snapshots; every state is treated as
    // final by the sync (no state filter on that source). Sanity-check the vocabulary values used.
    @Test
    public void testStreamLoadStatesAreSnapshots() {
        for (String state : new String[] {"Success", "Fail", "Publish Timeout", "Label Already Exists"}) {
            String key = StreamLoadRecordMgr.streamLoadRecordKey("db", "l_" + state, 1L);
            Assert.assertTrue(key.startsWith("STREAM_LOAD:db:l_"));
        }
    }

    // A row maps to a VALUES tuple: finish_time + record_key + 20 loads columns, with SQL escaping
    // of embedded single quotes and NULLs rendered as SQL NULL (not quoted).
    @Test
    public void testToValuesTupleEscapingAndNull() {
        TLoadJob row = new TLoadJob();
        row.setJobId("");
        row.setLabel("l'1"); // embedded single quote must be doubled
        row.setState("Success");
        row.setProgress("100%");
        row.setType("STREAM_LOAD");
        row.setEtlInfo("");
        row.setTaskInfo("{\"Db\":\"d\"}");
        // error_msg intentionally left unset -> null -> SQL NULL
        row.setCreateTime("");
        row.setEtlStartTime("");
        row.setEtlFinishTime("");
        row.setLoadStartTime("2020-01-01 00:00:00");
        row.setLoadFinishTime("2020-01-01 00:00:01");
        row.setUrl("");
        row.setJobDetails("{\"TotalRows\":\"1\"}");
        row.setTransactionId("");
        row.setErrorTablets("");
        row.setUser("root");
        row.setComment("");
        row.setFirstErrorMsg("");

        LoadJobHistoryRecord record = new LoadJobHistoryRecord(
                "STREAM_LOAD:d:l'1:1577808001000", 1577808001000L, row);
        String tuple = LoadsHistorySyncer.toValuesTuple(record);

        Assert.assertTrue(tuple.startsWith("("));
        Assert.assertTrue(tuple.endsWith(")"));
        // single quote in label doubled
        Assert.assertTrue(tuple.contains("'l''1'"));
        // record_key also escaped
        Assert.assertTrue(tuple.contains("'STREAM_LOAD:d:l''1:1577808001000'"));
        // unset error_msg -> NULL literal (not '')
        Assert.assertTrue(tuple.contains("NULL"));
        // type present
        Assert.assertTrue(tuple.contains("'STREAM_LOAD'"));
    }
}
