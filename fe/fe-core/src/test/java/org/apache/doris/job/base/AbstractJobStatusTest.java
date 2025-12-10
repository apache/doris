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

package org.apache.doris.job.base;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.thrift.TRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class AbstractJobStatusTest {

    private static class DummyTask extends AbstractTask {
        private final AtomicBoolean cancelledLogicCalled = new AtomicBoolean(false);

        DummyTask(long taskId) {
            setTaskId(taskId);
            setStatus(TaskStatus.PENDING);
        }

        @Override
        protected void closeOrReleaseResources() { }

        @Override
        protected void executeCancelLogic(boolean needWaitCancelComplete) {
            cancelledLogicCalled.set(true);
        }

        @Override
        public TRow getTvfInfo(String jobName) {
            return null;
        }

        @Override
        public void run() throws JobException {
            // dummy implementation
        }

        boolean cancelLogicCalled() {
            return cancelledLogicCalled.get();
        }
    }

    private static class DummyJob extends AbstractJob<DummyTask, Void> {
        private final List<DummyTask> history = new ArrayList<>();

        DummyJob(JobStatus initial) {
            super(1L, "job", initial, "db", "comment",
                    UserIdentity.ROOT, createJobConfig());
        }

        private static JobExecutionConfiguration createJobConfig() {
            JobExecutionConfiguration config = new JobExecutionConfiguration();
            config.setExecuteType(JobExecuteType.ONE_TIME);
            return config;
        }

        @Override
        protected void checkJobParamsInternal() { }

        @Override
        public List<DummyTask> createTasks(TaskType taskType, Void ctx) {
            DummyTask t = new DummyTask(100L);
            history.add(t);
            return Collections.singletonList(t);
        }

        @Override
        public boolean isReadyForScheduling(Void ctx) {
            return true;
        }

        @Override
        public org.apache.doris.qe.ShowResultSetMetaData getTaskMetaData() {
            return null;
        }

        @Override
        public org.apache.doris.job.common.JobType getJobType() {
            return null;
        }

        @Override
        public List<DummyTask> queryTasks() {
            return history;
        }

        @Override
        public String formatMsgWhenExecuteQueueFull(Long taskId) {
            return "";
        }

        @Override
        public void write(java.io.DataOutput out) throws java.io.IOException {
            // dummy implementation for Writable interface
        }
    }

    /**
     * Test pending status
     */
    @Test
    void testPendingFromPaused() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PAUSED);
        job.updateJobStatus(JobStatus.PENDING);
        Assertions.assertEquals(JobStatus.PENDING, job.getJobStatus());
    }

    @Test
    void testPendingFromRunning() throws Exception {
        DummyJob job = new DummyJob(JobStatus.RUNNING);
        job.updateJobStatus(JobStatus.PENDING);
        Assertions.assertEquals(JobStatus.PENDING, job.getJobStatus());
    }

    @Test
    void testPendingFromStoppedIsInvalid() {
        DummyJob job = new DummyJob(JobStatus.STOPPED);
        Assertions.assertThrows(IllegalArgumentException.class, () -> job.updateJobStatus(JobStatus.PENDING));
    }

    /**
     * Test running status
     */
    @Test
    void testRunningFromPending() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PENDING);
        job.updateJobStatus(JobStatus.RUNNING);
        Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
    }

    @Test
    void testRunningFromPaused() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PAUSED);
        job.updateJobStatus(JobStatus.RUNNING);
        Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
    }

    /**
     * Test pause status
     */
    @Test
    void testPauseFromRunning() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PENDING);
        job.updateJobStatus(JobStatus.RUNNING);
        job.updateJobStatus(JobStatus.PAUSED);
        Assertions.assertEquals(JobStatus.PAUSED, job.getJobStatus());
    }

    @Test
    void testPauseFromPending() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PENDING);
        job.updateJobStatus(JobStatus.PAUSED);
        Assertions.assertEquals(JobStatus.PAUSED, job.getJobStatus());
    }

    @Test
    void testPauseFromStoppedIsInvalid() {
        DummyJob job = new DummyJob(JobStatus.STOPPED);
        Assertions.assertThrows(IllegalArgumentException.class, () -> job.updateJobStatus(JobStatus.PAUSED));
    }

    /**
     * Test stop status
     */
    @Test
    void testStopFromPending() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PENDING);
        job.updateJobStatus(JobStatus.STOPPED);
        Assertions.assertEquals(JobStatus.STOPPED, job.getJobStatus());
    }

    @Test
    void testStopFromPaused() throws Exception {
        DummyJob job = new DummyJob(JobStatus.PAUSED);
        job.updateJobStatus(JobStatus.STOPPED);
        Assertions.assertEquals(JobStatus.STOPPED, job.getJobStatus());
    }

    @Test
    void testStopFromRunning() throws Exception {
        DummyJob job = new DummyJob(JobStatus.RUNNING);
        job.updateJobStatus(JobStatus.STOPPED);
        Assertions.assertEquals(JobStatus.STOPPED, job.getJobStatus());
    }

    @Test
    void testFinishSetsFinishTime() throws Exception {
        DummyJob job = new DummyJob(JobStatus.RUNNING);
        long before = System.currentTimeMillis();
        job.updateJobStatus(JobStatus.FINISHED);
        Assertions.assertEquals(JobStatus.FINISHED, job.getJobStatus());
        Assertions.assertTrue(job.getFinishTimeMs() >= before);
    }
}
