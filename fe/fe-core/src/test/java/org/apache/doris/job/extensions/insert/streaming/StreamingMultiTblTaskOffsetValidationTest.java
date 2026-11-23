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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamingMultiTblTaskOffsetValidationTest {
    @Test
    public void invalidBatchDoesNotAdvanceAnySplitOrMarkTaskSuccessful() {
        SnapshotSplit first = new SnapshotSplit();
        first.setSplitId("orders-0");
        SnapshotSplit second = new SnapshotSplit();
        second.setSplitId("orders-1");
        StreamingMultiTblTask task = new StreamingMultiTblTask(1L, 2L, null, null, null, "db", null,
                new StreamingJobProperties(new HashMap<>()), null, null);
        Deencapsulation.setField(task, "runningOffset", new JdbcOffset(List.of(first, second)));
        Deencapsulation.setField(task, "status", TaskStatus.RUNNING);
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setRemainingSplits(new ArrayList<>(List.of(first, second)));
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Deencapsulation.setField(job, "lock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(job, "runningStreamTask", task);
        Deencapsulation.setField(job, "offsetProvider", provider);
        Deencapsulation.setField(job, "targetProperties", new HashMap<>());
        CommitOffsetRequest request = new CommitOffsetRequest();
        request.setTaskId(2L);
        for (String offsets : List.of(
                "[{\"splitId\":\"orders-0\",\"lsn\":\"100\"}]",
                "[{\"splitId\":\"orders-0\",\"lsn\":\"100\"},"
                        + "{\"splitId\":\"wrong\",\"lsn\":\"100\"}]",
                "[{\"splitId\":\"orders-0\",\"lsn\":\"100\"},"
                        + "{\"splitId\":\"orders-0\",\"lsn\":\"100\"}]",
                "[{\"splitId\":\"orders-0\",\"lsn\":\"100\"},{\"splitId\":\"orders-1\"}]")) {
            request.setOffset(offsets);
            Assertions.assertThrows(IllegalStateException.class, () -> job.commitOffset(request));
            Assertions.assertEquals(2, provider.getRemainingSplits().size());
            Assertions.assertNull(provider.getCurrentOffset());
            Assertions.assertNull(first.getHighWatermark());
            Assertions.assertNull(second.getHighWatermark());
            Assertions.assertEquals(TaskStatus.RUNNING, task.getStatus());
        }
        request.setOffset("[{\"splitId\":\"orders-0\",\"lsn\":\"100\"},"
                + "{\"splitId\":\"orders-1\",\"lsn\":\"200\"}]");
        List<Map<String, String>> validated = task.validateOffsetMeta(request);
        Assertions.assertEquals(List.of(Map.of("lsn", "100"), Map.of("lsn", "200")), validated);
        Assertions.assertNull(first.getHighWatermark());
        Assertions.assertNull(second.getHighWatermark());
    }

    @Test
    public void binlogValidationRejectsTrailingOrInvalidOffsets() {
        StreamingMultiTblTask task = new StreamingMultiTblTask(1L, 2L, null, null, null, "db", null,
                new StreamingJobProperties(new HashMap<>()), null, null);
        Deencapsulation.setField(task, "runningOffset", new JdbcOffset(List.of(new BinlogSplit())));
        CommitOffsetRequest request = new CommitOffsetRequest();
        for (String offset : List.of(
                "[{\"splitId\":\"binlog-split\",\"lsn\":\"100\"},"
                        + "{\"splitId\":\"orders-0\",\"lsn\":\"200\"}]",
                "[{\"splitId\":\"binlog-split\"}]",
                "[{\"splitId\":\"orders-0\",\"lsn\":\"100\"}]")) {
            request.setOffset(offset);
            Assertions.assertThrows(IllegalStateException.class, () -> task.validateOffsetMeta(request));
        }
        request.setOffset("[{\"splitId\":\"binlog-split\",\"lsn\":\"100\"}]");
        Assertions.assertEquals(List.of(Map.of("lsn", "100")), task.validateOffsetMeta(request));
    }
}
