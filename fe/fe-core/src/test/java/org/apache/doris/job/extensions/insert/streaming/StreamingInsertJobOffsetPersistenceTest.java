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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.job.manager.StreamingTaskManager;
import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamingInsertJobOffsetPersistenceTest {

    @Test
    public void testFirstSnapshotCommitPersistsImmediately() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.getRemainingSplits().add(snapshotSplit("source_table:0"));
        TestStreamingInsertJob job = newJob(provider, 1001L);

        job.commitOffset(snapshotRequest(1001L, "source_table:0", null));

        Assert.assertEquals(1, job.journalCount);
        Assert.assertNotNull(job.getOffsetProviderPersist());
    }

    @Test
    public void testSnapshotCommitWithinIntervalDoesNotPersistAgain() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.getRemainingSplits().add(snapshotSplit("source_table:0"));
        TestStreamingInsertJob job = newJob(provider, 1003L);
        job.commitOffset(snapshotRequest(1003L, "source_table:0", null));

        provider.getRemainingSplits().add(snapshotSplit("source_table:1"));
        job.commitOffset(snapshotRequest(1003L, "source_table:1", null));

        Assert.assertEquals(1, job.journalCount);
        Assert.assertNotNull(job.getOffsetProviderPersist());
    }

    @Test
    public void testBinlogCommitPersistsImmediately() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        TestStreamingInsertJob job = newJob(provider, 1002L);

        job.commitOffset(binlogRequest(1002L, "100"));
        job.commitOffset(binlogRequest(1002L, "200"));

        Assert.assertEquals(2, job.journalCount);
        Assert.assertNotNull(job.getOffsetProviderPersist());
    }

    @Test
    public void testSnapshotToBinlogTransitionPersistsCompactedState() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.getRemainingSplits().add(snapshotSplit("source_table:0"));
        TestStreamingInsertJob job = newJob(provider, 1008L);
        job.commitOffset(snapshotRequest(1008L, "source_table:0", null));
        Assert.assertEquals(1, job.journalCount);

        job.commitOffset(binlogRequest(1008L, "200"));

        Assert.assertEquals(2, job.journalCount);
        Assert.assertFalse(job.getOffsetProviderPersist().contains("source_table:0"));
        Assert.assertTrue(provider.getFinishedSplits().isEmpty());
        Assert.assertTrue(provider.getChunkHighWatermarkMap().isEmpty());
    }

    @Test
    public void testSnapshotOffsetPersistsOnNextCommitAfterInterval() throws Exception {
        int oldInterval = Config.streaming_job_snapshot_offset_persist_interval_sec;
        Config.streaming_job_snapshot_offset_persist_interval_sec = 300;
        try {
            JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
            provider.getRemainingSplits().add(snapshotSplit("source_table:0"));
            TestStreamingInsertJob job = newJob(provider, 1011L);

            job.commitOffset(snapshotRequest(1011L, "source_table:0", null));
            Assert.assertEquals(1, job.journalCount);
            Deencapsulation.setField(job, "lastOffsetPersistTimeMs",
                    System.currentTimeMillis() - 300_000L);
            provider.getRemainingSplits().add(snapshotSplit("source_table:1"));
            job.commitOffset(snapshotRequest(1011L, "source_table:1", null));

            Assert.assertEquals(2, job.journalCount);
            Assert.assertTrue((long) Deencapsulation.getField(job, "lastOffsetPersistTimeMs") > 0L);
        } finally {
            Config.streaming_job_snapshot_offset_persist_interval_sec = oldInterval;
        }
    }

    @Test
    public void testAlterOffsetReplacesSnapshotState() throws Exception {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.getRemainingSplits().add(snapshotSplit("source_table:0"));
        TestStreamingInsertJob job = newJob(provider, 1009L);
        job.commitOffset(snapshotRequest(1009L, "source_table:0", null));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(StreamingJobProperties.OFFSET_PROPERTY, "{\"lsn\":\"300\"}");
        Deencapsulation.invoke(job, "modifyPropertiesInternal", properties);

        Assert.assertTrue(job.getOffsetProviderPersist().contains("300"));
        Assert.assertTrue(provider.getFinishedSplits().isEmpty());
        Assert.assertTrue(provider.getChunkHighWatermarkMap().isEmpty());
    }

    @Test
    public void testNaturalFinishPersistsFinalState() throws Exception {
        TestStreamingInsertJob job = newJob(new EndJdbcSourceOffsetProvider(), 1012L);
        NoopStreamingMultiTblTask task =
                (NoopStreamingMultiTblTask) Deencapsulation.getField(job, "runningStreamTask");

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            JobManager<?, ?> jobManager = Mockito.mock(JobManager.class);
            StreamingTaskManager streamingTaskManager = Mockito.mock(StreamingTaskManager.class);
            GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(env.getJobManager()).thenReturn(jobManager);
            Mockito.when(jobManager.getStreamingTaskManager()).thenReturn(streamingTaskManager);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);

            long beforeFinish = System.currentTimeMillis();
            job.onStreamTaskSuccess(task);

            Assert.assertEquals(JobStatus.FINISHED, job.getJobStatus());
            Assert.assertTrue(job.getFinishTimeMs() >= beforeFinish);
            Assert.assertEquals(1, job.journalCount);
            Mockito.verify(callbackFactory).removeCallback(9001L);
        }
    }

    @Test
    public void testReplayUpdatedRestoresFinalStateAndRemovesCallback() {
        TestStreamingInsertJob job = newJob(new JdbcSourceOffsetProvider(), 1013L);
        TestStreamingInsertJob replayJob = newJob(new JdbcSourceOffsetProvider(), 1014L);
        replayJob.setJobStatus(JobStatus.FINISHED);
        replayJob.setFinishTimeMs(1234L);

        try (MockedStatic<Env> envMockedStatic = Mockito.mockStatic(Env.class)) {
            GlobalTransactionMgrIface transactionMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            envMockedStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactionMgr);
            Mockito.when(transactionMgr.getCallbackFactory()).thenReturn(callbackFactory);

            job.replayOnUpdated(replayJob);

            Assert.assertEquals(JobStatus.FINISHED, job.getJobStatus());
            Assert.assertEquals(1234L, job.getFinishTimeMs());
            Mockito.verify(callbackFactory).removeCallback(9001L);
        }
    }

    @Test
    public void testReplayUpdatedRestoresStartTime() {
        TestStreamingInsertJob job = newJob(new JdbcSourceOffsetProvider(), 1015L);
        TestStreamingInsertJob replayJob = newJob(new JdbcSourceOffsetProvider(), 1016L);
        replayJob.setStartTimeMs(1234L);

        job.replayOnUpdated(replayJob);

        Assert.assertEquals(1234L, job.getStartTimeMs());
    }

    @Test
    public void testPauseCancelsTaskAfterReleasingJobWriteLock() throws Exception {
        TestStreamingInsertJob job = newJob(new JdbcSourceOffsetProvider(), 1017L);
        ReentrantReadWriteLock jobLock = Deencapsulation.getField(job, "lock");
        LockCheckingTask task = new LockCheckingTask(1017L, jobLock);
        Deencapsulation.setField(job, "runningStreamTask", task);

        job.updateJobStatus(JobStatus.PAUSED);

        Assert.assertTrue(task.cancelCalled);
        Assert.assertFalse(task.cancelObservedWriteLock);
        Assert.assertNull(Deencapsulation.getField(job, "runningStreamTask"));
    }

    private static TestStreamingInsertJob newJob(JdbcSourceOffsetProvider provider, long taskId) {
        TestStreamingInsertJob job = new TestStreamingInsertJob();
        Deencapsulation.setField(job, "lock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(job, "jobId", 9001L);
        Deencapsulation.setField(job, "jobName", "test_job");
        Deencapsulation.setField(job, "jobStatus", JobStatus.RUNNING);
        Deencapsulation.setField(job, "offsetProvider", provider);
        Deencapsulation.setField(job, "properties", new HashMap<String, String>());
        Deencapsulation.setField(job, "targetProperties", new HashMap<String, String>());
        Deencapsulation.setField(job, "runningStreamTask", new NoopStreamingMultiTblTask(taskId));
        return job;
    }

    private static SnapshotSplit snapshotSplit(String splitId) {
        return new SnapshotSplit(
                splitId,
                "source_db.source_table",
                Collections.singletonList("id"),
                new Object[]{1L},
                new Object[]{2L},
                null);
    }

    private static CommitOffsetRequest snapshotRequest(long taskId, String splitId, String tableSchemas) {
        CommitOffsetRequest request = new CommitOffsetRequest();
        request.setTaskId(taskId);
        request.setOffset("[{\"splitId\":\"" + splitId + "\",\"lsn\":\"100\"}]");
        request.setTableSchemas(tableSchemas);
        return request;
    }

    private static CommitOffsetRequest binlogRequest(long taskId, String lsn) {
        CommitOffsetRequest request = new CommitOffsetRequest();
        request.setTaskId(taskId);
        request.setOffset("[{\"splitId\":\"binlog-split\",\"lsn\":\"" + lsn + "\"}]");
        return request;
    }

    private static class TestStreamingInsertJob extends StreamingInsertJob {
        private int journalCount;

        @Override
        public void logUpdateOperation() {
            journalCount++;
        }
    }

    private static class EndJdbcSourceOffsetProvider extends JdbcSourceOffsetProvider {
        @Override
        public boolean hasReachedEnd() {
            return true;
        }
    }

    private static class NoopStreamingMultiTblTask extends StreamingMultiTblTask {
        NoopStreamingMultiTblTask(long taskId) {
            super(9001L, taskId, null, null, null, null, null,
                    new StreamingJobProperties(new HashMap<>()), null, null);
            Deencapsulation.setField(this, "status", TaskStatus.RUNNING);
        }

        @Override
        public void successCallback(CommitOffsetRequest offsetRequest) throws JobException {
        }
    }

    private static class LockCheckingTask extends NoopStreamingMultiTblTask {
        private final ReentrantReadWriteLock jobLock;
        private boolean cancelCalled;
        private boolean cancelObservedWriteLock;

        LockCheckingTask(long taskId, ReentrantReadWriteLock jobLock) {
            super(taskId);
            this.jobLock = jobLock;
        }

        @Override
        public void cancel(boolean needWaitCancelComplete) {
            cancelCalled = true;
            cancelObservedWriteLock = jobLock.isWriteLockedByCurrentThread();
            super.cancel(false);
        }
    }
}
