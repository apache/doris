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
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamingInsertJobLateCallbackTest {

    private static StreamingMultiTblTask newTask(long taskId, TaskStatus initialStatus) {
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        StreamingMultiTblTask task = new StreamingMultiTblTask(
                0L, taskId, null, null, null, null, null, jobProps, null, null);
        Deencapsulation.setField(task, "status", initialStatus);
        return task;
    }

    @Test
    public void testCancelMarksIsCanceledOnFailedTask() {
        StreamingMultiTblTask task = newTask(1001L, TaskStatus.FAILED);

        task.cancel(true);

        Assertions.assertTrue(task.getIsCanceled().get(), "isCanceled must flip even when task already FAILED");
        Assertions.assertEquals(TaskStatus.FAILED, task.getStatus(), "status preserved when already terminal");
    }

    @Test
    public void testCancelMarksIsCanceledOnSuccessTask() {
        StreamingMultiTblTask task = newTask(1002L, TaskStatus.SUCCESS);

        task.cancel(true);

        Assertions.assertTrue(task.getIsCanceled().get());
        Assertions.assertEquals(TaskStatus.SUCCESS, task.getStatus());
    }

    @Test
    public void testCancelTransitionsRunningToCanceled() {
        StreamingMultiTblTask task = newTask(1003L, TaskStatus.RUNNING);

        task.cancel(true);

        Assertions.assertTrue(task.getIsCanceled().get());
        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
    }

    @Test
    public void testCancelIdempotent() {
        StreamingMultiTblTask task = newTask(1004L, TaskStatus.RUNNING);

        task.cancel(true);
        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Assertions.assertTrue(task.getIsCanceled().get());

        Deencapsulation.setField(task, "errMsg", "first cancel");
        task.cancel(true);
        Assertions.assertEquals("first cancel", Deencapsulation.getField(task, "errMsg"), "second cancel must early-return and leave state untouched");
    }

    @Test
    public void testCommitOffsetSkipsCanceledTask() throws Exception {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Deencapsulation.setField(job, "lock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(job, "jobId", 9001L);
        Deencapsulation.setField(job, "jobName", "test_job");
        Deencapsulation.setField(job, "jobStatus", JobStatus.PAUSED);

        JdbcSourceOffsetProvider provider = Deencapsulation.newInstance(JdbcSourceOffsetProvider.class);
        Deencapsulation.setField(job, "offsetProvider", provider);

        StreamingMultiTblTask task = newTask(7777L, TaskStatus.FAILED);
        // simulate the bug timeline: task already FAILED via onFail, then cancel marks isCanceled.
        task.cancel(true);
        Assertions.assertTrue(task.getIsCanceled().get());

        Deencapsulation.setField(job, "runningStreamTask", task);

        CommitOffsetRequest req = new CommitOffsetRequest();
        req.setJobId(9001L);
        req.setTaskId(7777L);
        req.setOffset("[{\"splitId\":\"binlog-split\"}]");
        req.setScannedRows(123L);
        req.setLoadBytes(456L);

        // Should silently skip — no JobException, no Task.status flip back to SUCCESS,
        // no successCallback side-effects.
        job.commitOffset(req);

        Assertions.assertEquals(TaskStatus.FAILED, task.getStatus(), "task status must stay terminal — late callback ignored");
    }
}
