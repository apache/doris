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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.load.routineload.RoutineLoadTaskScheduler;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.KinesisShardTopologyOperation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService.PShardInfo;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KinesisIndependentShardSchedulingTest {
    private static PShardInfo shard(String id, boolean closed, String... parents) {
        PShardInfo.Builder builder = PShardInfo.newBuilder().setShardId(id).setClosed(closed);
        if (parents.length > 0) {
            builder.setParentShardId(parents[0]);
        }
        if (parents.length > 1) {
            builder.setAdjacentParentShardId(parents[1]);
        }
        return builder.build();
    }

    private static class Fixture implements AutoCloseable {
        final Env env = Mockito.mock(Env.class);
        final EditLog editLog = Mockito.mock(EditLog.class);
        final RoutineLoadManager manager = Mockito.mock(RoutineLoadManager.class);
        final RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        final MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
        final KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(
                1L, "independent-shards", 1L, 1L, "region", "stream", UserIdentity.ADMIN);

        Fixture() {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(manager);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            Mockito.when(manager.getJob(1L)).thenReturn(job);
            Deencapsulation.setField(job, "kinesisDefaultPosition", "LATEST");
        }

        KinesisShardTopology topology() {
            return Deencapsulation.getField(job, "shardTopology");
        }

        List<RoutineLoadTaskInfo> tasks() {
            return Deencapsulation.getField(job, "routineLoadTaskInfoList");
        }

        boolean discover(List<PShardInfo> infos) {
            Deencapsulation.setField(job, "newCurrentKinesisShardInfos", infos);
            return Deencapsulation.invoke(job, "isKinesisShardsChanged");
        }

        @Override
        public void close() {
            envMock.close();
        }
    }

    private static void assertAssignments(KinesisRoutineLoadJob job, Set<String> expected) {
        List<RoutineLoadTaskInfo> tasks = Deencapsulation.getField(job, "routineLoadTaskInfoList");
        Map<String, Integer> owners = new HashMap<>();
        for (RoutineLoadTaskInfo task : tasks) {
            List<String> ids = ((KinesisTaskInfo) task).getShards();
            Assertions.assertFalse(ids.isEmpty());
            ids.forEach(id -> owners.merge(id, 1, Integer::sum));
        }
        Assertions.assertEquals(expected, owners.keySet());
        owners.forEach((id, count) -> Assertions.assertEquals(1, count.intValue(), id));
    }

    private static TransactionState eofTransaction(String id, long txnId) {
        KinesisProgress progress = new KinesisProgress(Map.of(id, "200"));
        progress.getClosedShardIds().add(id);
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        Deencapsulation.setField(attachment, "progress", progress);
        Deencapsulation.setField(attachment, "taskId", new TUniqueId(1L, txnId));
        TransactionState txn = new TransactionState();
        txn.setTransactionId(txnId);
        txn.setTxnCommitAttachment(attachment);
        txn.setTransactionStatus(TransactionStatus.COMMITTED);
        return txn;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReshardChildrenScheduledWhileParentsDrain(boolean merge) throws Exception {
        try (Fixture f = new Fixture()) {
            List<PShardInfo> initial = merge
                    ? List.of(shard("P", false), shard("P2", false)) : List.of(shard("P", false));
            Map<String, String> positions = merge ? Map.of("P", "100", "P2", "100") : Map.of("P", "100");
            Deencapsulation.setField(f.job, "progress", new KinesisProgress(positions));
            f.discover(initial);
            f.job.divideRoutineLoadJob(initial.size());

            List<PShardInfo> reshard = merge
                    ? List.of(shard("P", true), shard("P2", true), shard("C", false, "P", "P2"))
                    : List.of(shard("P", true), shard("C", false, "P"), shard("C2", false, "P"));
            Assertions.assertTrue(f.discover(reshard));
            // Follow the normal scheduler's NEED_SCHEDULE -> divide path.
            f.job.updateState(JobState.NEED_SCHEDULE, null, true);
            f.job.divideRoutineLoadJob(3);
            Set<String> expected = merge ? Set.of("P", "P2", "C") : Set.of("P", "C", "C2");
            assertAssignments(f.job, expected);
            Assertions.assertEquals("TRIM_HORIZON", f.topology().getStartPosition("C"));
            Assertions.assertEquals(merge ? Set.of("P", "P2") : Set.of("P"),
                    f.topology().getNodes().get("C").getParentShardIds());

            // Renew a child task without stealing the shards owned by other live tasks.
            RoutineLoadTaskInfo child = f.tasks().stream()
                    .filter(t -> ((KinesisTaskInfo) t).getShards().contains("C")).findFirst().orElseThrow();
            Assertions.assertNotNull(f.job.unprotectRenewTask(child, false));
            assertAssignments(f.job, expected);

            RoutineLoadTaskInfo parent = f.tasks().stream()
                    .filter(t -> ((KinesisTaskInfo) t).getShards().contains("P")).findFirst().orElseThrow();
            Deencapsulation.setField(parent, "txnId", 11L);
            TransactionState txn = eofTransaction("P", 11L);
            f.job.beforeCommitted(txn);
            f.job.afterCommitted(txn, true);
            Assertions.assertEquals(JobState.RUNNING, f.job.getState());
            Assertions.assertEquals(TransactionStatus.COMMITTED, parent.getTxnStatus());
            Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                    f.topology().getNodes().get("P").getState());
            Assertions.assertFalse(f.topology().getReadyShardIds().contains("P"));
            Assertions.assertTrue(f.topology().getReadyShardIds().contains("C"));
            Assertions.assertTrue(f.tasks().contains(parent));
            Mockito.verify(f.scheduler, Mockito.never()).addTaskInQueue(Mockito.any());

            txn.setTransactionStatus(TransactionStatus.VISIBLE);
            f.job.afterVisible(txn, true);
            Assertions.assertEquals(KinesisShardTopology.ShardState.COMPLETED,
                    f.topology().getNodes().get("P").getState());
            assertAssignments(f.job, merge ? Set.of("P2", "C") : Set.of("C", "C2"));
            Assertions.assertEquals(JobState.RUNNING, f.job.getState());
            Mockito.verify(f.scheduler, Mockito.never()).addTaskInQueue(Mockito.any());
        }
    }

    @Test
    public void testSameShardRenewsOnlyAfterVisibleWithCommittedProgress() throws Exception {
        try (Fixture f = new Fixture()) {
            Deencapsulation.setField(f.job, "progress", new KinesisProgress(Map.of("P", "100")));
            f.discover(List.of(shard("P", false)));
            f.job.divideRoutineLoadJob(1);
            RoutineLoadTaskInfo oldTask = f.tasks().get(0);
            Deencapsulation.setField(oldTask, "txnId", 11L);
            TransactionState txn = eofTransaction("P", 11L);
            KinesisProgress reported = (KinesisProgress)
                    ((RLTaskTxnCommitAttachment) txn.getTxnCommitAttachment()).getProgress();
            reported.getClosedShardIds().clear();
            f.job.beforeCommitted(txn);
            f.job.afterCommitted(txn, true);
            Assertions.assertEquals(JobState.RUNNING, f.job.getState());
            Assertions.assertEquals(List.of(oldTask), f.tasks());
            Mockito.verify(f.scheduler, Mockito.never()).addTaskInQueue(Mockito.any());

            txn.setTransactionStatus(TransactionStatus.VISIBLE);
            f.job.afterVisible(txn, true);
            assertAssignments(f.job, Set.of("P"));
            Assertions.assertNotSame(oldTask, f.tasks().get(0));
            Assertions.assertEquals(Map.of("P", "200"),
                    ((KinesisTaskInfo) f.tasks().get(0)).getShardIdToSequenceNumber());
            Mockito.verify(f.scheduler).addTaskInQueue(f.tasks().get(0));
        }
    }

    @Test
    public void testMissingUnfinishedParentPausesWholeJobWithActiveChild() throws Exception {
        try (Fixture f = new Fixture()) {
            Deencapsulation.setField(f.job, "kinesisDefaultPosition", "TRIM_HORIZON");
            f.discover(List.of(shard("P", true), shard("C", false, "P")));
            f.job.divideRoutineLoadJob(2);
            assertAssignments(f.job, Set.of("P", "C"));

            Assertions.assertFalse(f.discover(List.of(shard("C", false, "P"))));
            Assertions.assertEquals(JobState.PAUSED, f.job.getState());
            Assertions.assertTrue(f.job.getPauseReason().getMsg().contains("P"));
            Assertions.assertTrue(f.tasks().isEmpty());
            Assertions.assertFalse(f.job.needAutoResume());
            f.job.divideRoutineLoadJob(2);
            Assertions.assertTrue(f.tasks().isEmpty());
        }
    }

    @Test
    public void testReplayAndImageKeepChildReadyWhileParentAwaitsVisible() throws Exception {
        try (Fixture f = new Fixture()) {
            f.job.replayShardTopology(new KinesisShardTopologyOperation(1L,
                    List.of(shard("P", true)), "LATEST", Map.of("P", "100")));
            f.job.replayShardTopology(new KinesisShardTopologyOperation(1L,
                    List.of(shard("P", true), shard("C", false, "P")), "LATEST", Map.of()));
            TransactionState txn = eofTransaction("P", 11L);
            f.job.replayOnCommitted(txn);
            KinesisShardTopology restored = GsonUtils.GSON.fromJson(f.topology().toJson(),
                    KinesisShardTopology.class);
            Deencapsulation.setField(f.job, "shardTopology", restored);
            Assertions.assertEquals(List.of("C"), restored.getReadyShardIds());
            Assertions.assertEquals("TRIM_HORIZON", restored.getStartPosition("C"));
            Assertions.assertEquals(Set.of("P"), restored.getNodes().get("C").getParentShardIds());
            f.job.divideRoutineLoadJob(1);
            assertAssignments(f.job, Set.of("C"));
            f.job.replayOnVisible(txn);
            f.job.replayOnVisible(txn);
            Assertions.assertEquals(KinesisShardTopology.ShardState.COMPLETED,
                    restored.getNodes().get("P").getState());
            assertAssignments(f.job, Set.of("C"));
        }
    }

    @Test
    public void testChildHintRequiresMetadataButNotParentVisibility() throws Exception {
        try (Fixture f = new Fixture()) {
            Deencapsulation.setField(f.job, "progress", new KinesisProgress(Map.of("P", "100")));
            f.discover(List.of(shard("P", false)));
            TransactionState txn = eofTransaction("P", 11L);
            KinesisProgress attachmentProgress = (KinesisProgress)
                    ((RLTaskTxnCommitAttachment) txn.getTxnCommitAttachment()).getProgress();
            attachmentProgress.getChildShardParentIds().put("C", Set.of("P"));
            f.job.replayOnCommitted(txn);
            Assertions.assertTrue(f.topology().getReadyShardIds().isEmpty());
            Assertions.assertFalse(f.discover(List.of(shard("P", true))));
            Assertions.assertNull(f.topology().getLineageError());
            Assertions.assertTrue(f.discover(List.of(shard("P", true), shard("C", false, "P"))));
            f.job.divideRoutineLoadJob(1);
            assertAssignments(f.job, Set.of("C"));
            Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                    f.topology().getNodes().get("P").getState());
        }
    }
}
