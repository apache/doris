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

package org.apache.doris.load.routineload.kinesis;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.kinesis.KinesisUtil;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.load.routineload.RoutineLoadTaskScheduler;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.KinesisLatestPositionOperation;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class KinesisLatestPositionTest {
    private static class Fixture implements AutoCloseable {
        final Env env = Mockito.mock(Env.class);
        final EditLog editLog = Mockito.mock(EditLog.class);
        final RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        final MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
        final MockedStatic<KinesisUtil> utilMock = Mockito.mockStatic(KinesisUtil.class);
        final CompletableFuture<InternalService.PProxyResult> scan = new CompletableFuture<>();
        final KinesisRoutineLoadJob job;
        final KinesisProgress progress;

        Fixture() {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            job = new KinesisRoutineLoadJob(1L, "job", 1L, 1L, "region", "stream", UserIdentity.ADMIN);
            progress = new KinesisProgress(Map.of("shard-0", "LATEST", "shard-1", "-1", "shard-2", "900"));
            Deencapsulation.setField(job, "progress", progress);
            Deencapsulation.setField(job, "state", JobState.NEED_SCHEDULE);
            Deencapsulation.setField(job, "openKinesisShards",
                    new ArrayList<>(List.of("shard-0", "shard-1", "shard-2")));
            utilMock.when(() -> KinesisUtil.getLatestSequenceNumbersAsync(Mockito.anyString(),
                    Mockito.anyString(), Mockito.isNull(), Mockito.anyMap(),
                    Mockito.eq(Set.of("shard-0", "shard-1")), Mockito.anyInt())).thenReturn(scan);
        }

        void verifyScanOnce() {
            utilMock.verify(() -> KinesisUtil.getLatestSequenceNumbersAsync(Mockito.anyString(),
                    Mockito.anyString(), Mockito.isNull(), Mockito.anyMap(),
                    Mockito.eq(Set.of("shard-0", "shard-1")), Mockito.anyInt()), Mockito.times(1));
        }

        @Override
        public void close() {
            utilMock.close();
            envMock.close();
        }
    }

    private static InternalService.PProxyResult response(Map<String, String> positions) {
        return InternalService.PProxyResult.newBuilder()
                .setStatus(Types.PStatus.newBuilder().setStatusCode(0))
                .setKinesisMetaResult(InternalService.PKinesisMetaProxyResult.newBuilder()
                        .putAllShardLatestSequences(positions)).build();
    }

    @Test
    public void testPersistBeforeCreatingTaskAndReuseOnReplacement() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.prepare();
            f.job.prepare();
            f.job.divideRoutineLoadJob(1);
            Assert.assertEquals(JobState.NEED_SCHEDULE, f.job.getState());
            Mockito.verifyNoInteractions(f.editLog, f.scheduler);
            f.verifyScanOnce();

            Mockito.doAnswer(invocation -> {
                Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("shard-0"));
                List<RoutineLoadTaskInfo> tasks = Deencapsulation.getField(f.job, "routineLoadTaskInfoList");
                Assert.assertTrue(tasks.isEmpty());
                return null;
            }).when(f.editLog).logKinesisLatestPosition(Mockito.any());
            f.scan.complete(response(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON")));
            f.job.prepare();
            f.job.divideRoutineLoadJob(1);
            Assert.assertEquals(JobState.RUNNING, f.job.getState());
            List<RoutineLoadTaskInfo> tasks = Deencapsulation.getField(f.job, "routineLoadTaskInfoList");
            Assert.assertEquals(1, tasks.size());
            KinesisTaskInfo task = (KinesisTaskInfo) tasks.get(0);
            Assert.assertEquals(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON", "shard-2", "900"),
                    task.getShardIdToSequenceNumber());
            Mockito.verify(f.scheduler).addTasksInQueue(tasks);
            KinesisTaskInfo retry = (KinesisTaskInfo) f.job.unprotectRenewTask(task, true);
            Assert.assertEquals(task.getShardIdToSequenceNumber(), retry.getShardIdToSequenceNumber());
            f.verifyScanOnce();
        }
    }

    @Test
    public void testCaseInsensitiveLatest() throws Exception {
        try (Fixture f = new Fixture()) {
            f.progress.addShardPosition(Pair.of("shard-0", "LaTeSt"));
            f.scan.complete(response(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON")));
            f.job.prepare();
            Assert.assertEquals("150", f.progress.getSequenceNumberByShard("shard-0"));
            f.verifyScanOnce();
        }
    }

    @Test
    public void testClosedAndUnassignedShardsAreNotScanned() throws Exception {
        try (Fixture f = new Fixture()) {
            f.progress.addShardPosition(Pair.of("closed-shard", "LATEST"));
            f.progress.addShardPosition(Pair.of("removed-shard", "LATEST"));
            Deencapsulation.setField(f.job, "closedKinesisShards",
                    new ArrayList<>(List.of("closed-shard")));
            f.scan.complete(response(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON")));
            f.job.prepare();
            f.job.divideRoutineLoadJob(1);
            f.verifyScanOnce();
            Assert.assertEquals(JobState.RUNNING, f.job.getState());
            Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("closed-shard"));
            Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("removed-shard"));
        }
    }

    @Test
    public void testPauseCancelsPendingScan() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.prepare();
            f.job.updateState(JobState.PAUSED, null, false);
            Assert.assertTrue(f.scan.isCancelled());
            Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("shard-0"));
            Mockito.verify(f.editLog, Mockito.never()).logKinesisLatestPosition(Mockito.any());
        }
    }

    @Test
    public void testIncompleteResponseCannotPublishPositions() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.prepare();
            f.scan.complete(response(Map.of("shard-0", "150")));
            Assert.assertThrows(LoadException.class, f.job::prepare);
            Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("shard-0"));
            Mockito.verifyNoInteractions(f.editLog, f.scheduler);
        }
    }

    @Test
    public void testTimeoutKeepsLatestUnresolved() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.prepare();
            Deencapsulation.setField(f.job, "latestSequenceDeadlineNs", System.nanoTime() - 1);
            Assert.assertThrows(LoadException.class, f.job::prepare);
            Assert.assertTrue(f.scan.isCancelled());
            Assert.assertEquals("LATEST", f.progress.getSequenceNumberByShard("shard-0"));
            Mockito.verifyNoInteractions(f.editLog, f.scheduler);
        }
    }

    @Test
    public void testReplayRestoresPositionsWithoutScanningAgain() throws Exception {
        try (Fixture f = new Fixture()) {
            f.scan.complete(response(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON")));
            f.job.prepare();
            ArgumentCaptor<KinesisLatestPositionOperation> captor =
                    ArgumentCaptor.forClass(KinesisLatestPositionOperation.class);
            Mockito.verify(f.editLog).logKinesisLatestPosition(captor.capture());
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            captor.getValue().write(new DataOutputStream(bytes));
            KinesisLatestPositionOperation restored = KinesisLatestPositionOperation.read(
                    new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            KinesisRoutineLoadJob replayed = new KinesisRoutineLoadJob(1L, "job", 1L, 1L,
                    "region", "stream", UserIdentity.ADMIN);
            replayed.replayLatestPosition(restored);
            replayed.prepare();
            KinesisProgress progress = Deencapsulation.getField(replayed, "progress");
            Assert.assertEquals("150", progress.getSequenceNumberByShard("shard-0"));
            Assert.assertEquals("TRIM_HORIZON", progress.getSequenceNumberByShard("shard-1"));
            f.verifyScanOnce();
        }
    }

    @Test
    public void testAlterDiscardsPendingScanForOldSource() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.prepare();
            KinesisDataSourceProperties properties = new KinesisDataSourceProperties(
                    Map.of(KinesisConfiguration.KINESIS_STREAM.getName(), "other-stream"));
            properties.setAlter(true);
            properties.setTimezone("UTC");
            properties.analyze();
            Deencapsulation.invoke(f.job, "modifyPropertiesInternal", Map.of(), properties);
            Assert.assertTrue(f.scan.isCancelled());
            Assert.assertFalse(f.scan.complete(response(Map.of("shard-0", "150", "shard-1", "TRIM_HORIZON"))));
            Mockito.verifyNoInteractions(f.editLog, f.scheduler);
        }
    }
}
