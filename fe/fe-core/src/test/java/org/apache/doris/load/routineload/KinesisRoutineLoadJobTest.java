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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.kinesis.KinesisUtil;
import org.apache.doris.load.routineload.kinesis.KinesisConfiguration;
import org.apache.doris.load.routineload.kinesis.KinesisDataSourceProperties;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisShardTopology;
import org.apache.doris.load.routineload.kinesis.KinesisTaskInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class KinesisRoutineLoadJobTest {

    @Test
    public void testRoutineLoadTaskConcurrentNum() {
        int oldMaxConcurrent = Config.max_routine_load_task_concurrent_num;
        try {
            Config.max_routine_load_task_concurrent_num = 6;
            KinesisRoutineLoadJob routineLoadJob =
                    new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                            1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
            Deencapsulation.setField(routineLoadJob, "openKinesisShards",
                    Lists.newArrayList("shard-0", "shard-1"));
            Deencapsulation.setField(routineLoadJob, "closedKinesisShards",
                    Lists.newArrayList("shard-2"));

            Assertions.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

            Deencapsulation.setField(routineLoadJob, "desireTaskConcurrentNum", 2);
            Assertions.assertEquals(2, routineLoadJob.calculateCurrentConcurrentTaskNum());

            Config.max_routine_load_task_concurrent_num = 1;
            Assertions.assertEquals(1, routineLoadJob.calculateCurrentConcurrentTaskNum());
        } finally {
            Config.max_routine_load_task_concurrent_num = oldMaxConcurrent;
        }
    }

    @Test
    public void testRoutineLoadTaskConcurrentNumUsesTopologyReadyShards() {
        int oldMaxConcurrent = Config.max_routine_load_task_concurrent_num;
        try {
            Config.max_routine_load_task_concurrent_num = 6;
            KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                    1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
            KinesisShardTopology topology = new KinesisShardTopology();
            topology.mergeShardInfos(List.of(
                    createShardInfo("parent"),
                    createChildShardInfo("child", "parent"),
                    InternalService.PShardInfo.newBuilder().setShardId("closed").setClosed(true).build()),
                    KinesisProgress.POSITION_TRIM_HORIZON, KinesisProgress.POSITION_TRIM_HORIZON);
            Deencapsulation.setField(job, "shardTopology", topology);
            Deencapsulation.setField(job, "desireTaskConcurrentNum", 6);

            // Child, parent and the initially closed shard can all consume independently.
            Assertions.assertEquals(3, topology.getReadyShardIds().size());
            Assertions.assertEquals(3, job.calculateCurrentConcurrentTaskNum());

            Deencapsulation.setField(job, "desireTaskConcurrentNum", 1);
            Assertions.assertEquals(1, job.calculateCurrentConcurrentTaskNum());
        } finally {
            Config.max_routine_load_task_concurrent_num = oldMaxConcurrent;
        }
    }

    @Test
    public void testGetStatisticContainsKinesisFields() {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Deencapsulation.setField(routineLoadJob, "openKinesisShards",
                Lists.newArrayList("shard-0", "shard-1"));
        Deencapsulation.setField(routineLoadJob, "closedKinesisShards",
                Lists.newArrayList("shard-2"));
        Map<String, String> shardToSeqNum = new HashMap<>();
        shardToSeqNum.put("shard-0", "100");
        shardToSeqNum.put("shard-1", "200");
        shardToSeqNum.put("shard-2", "300");
        shardToSeqNum.put("shard-3", "400");
        Deencapsulation.setField(routineLoadJob, "progress",
                new KinesisProgress(shardToSeqNum));
        Map<String, Long> shardToMillisBehindLatest = new HashMap<>();
        shardToMillisBehindLatest.put("shard-0", 100L);
        shardToMillisBehindLatest.put("shard-1", 0L);
        shardToMillisBehindLatest.put("shard-2", -1L);
        Deencapsulation.setField(routineLoadJob, "cachedShardWithMillsBehindLatest",
                shardToMillisBehindLatest);

        Gson gson = new Gson();
        Map<String, Object> statistic = gson.fromJson(routineLoadJob.getStatistic(), Map.class);

        Assertions.assertEquals(2L, ((Number) statistic.get("openShardNum")).longValue());
        Assertions.assertEquals(1L, ((Number) statistic.get("closedShardNum")).longValue());
        Assertions.assertEquals(4L, ((Number) statistic.get("trackedShardNum")).longValue());
        Assertions.assertEquals(3L, ((Number) statistic.get("cachedMillisBehindLatestShardNum")).longValue());
        Assertions.assertEquals(100L, ((Number) statistic.get("totalMillisBehindLatest")).longValue());
        Assertions.assertEquals(100L, ((Number) statistic.get("maxMillisBehindLatest")).longValue());
    }

    @Test
    public void testHasMoreDataToConsumeShouldKeepPollingWhenLagCacheIsZero() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Map<String, Long> shardToMillisBehindLatest = new HashMap<>();
        shardToMillisBehindLatest.put("shard-0", 0L);
        Deencapsulation.setField(routineLoadJob, "cachedShardWithMillsBehindLatest",
                shardToMillisBehindLatest);

        Map<String, String> shardToSeqNum = new HashMap<>();
        shardToSeqNum.put("shard-0", "100");
        Assertions.assertTrue(routineLoadJob.hasMoreDataToConsume(UUID.randomUUID(), shardToSeqNum));
    }

    @Test
    public void testLagCacheShouldUseLatestReportInsteadOfHistoricalMax() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Map<String, String> shardToSeqNum = new HashMap<>();
        shardToSeqNum.put("shard-0", "100");
        Deencapsulation.setField(routineLoadJob, "progress", new KinesisProgress(shardToSeqNum));

        Map<String, Long> cachedLag = new HashMap<>();
        cachedLag.put("shard-0", 60_000L);
        Deencapsulation.setField(routineLoadJob, "cachedShardWithMillsBehindLatest", cachedLag);

        Map<String, String> updatedSeqNum = new HashMap<>();
        updatedSeqNum.put("shard-0", "101");
        Map<String, Long> latestLag = new HashMap<>();
        latestLag.put("shard-0", 100L);
        RLTaskTxnCommitAttachment attachment =
                createCommitAttachment(createProgress(updatedSeqNum, latestLag));
        Deencapsulation.invoke(routineLoadJob, "updateProgressAndOffsetsCache", attachment);

        Map<String, Long> updatedLagCache = Deencapsulation.getField(routineLoadJob, "cachedShardWithMillsBehindLatest");
        Assertions.assertEquals(100L, updatedLagCache.get("shard-0").longValue());

        Gson gson = new Gson();
        Map<String, Object> statistic = gson.fromJson(routineLoadJob.getStatistic(), Map.class);
        Assertions.assertEquals(100L, ((Number) statistic.get("totalMillisBehindLatest")).longValue());
        Assertions.assertEquals(100L, ((Number) statistic.get("maxMillisBehindLatest")).longValue());

        Map<String, Object> lag = gson.fromJson(routineLoadJob.getLag(), Map.class);
        Assertions.assertEquals(100L, ((Number) lag.get("shard-0")).longValue());
    }

    @Test
    public void testModifyPropertiesShouldClearStaleCustomShardsWhenStreamChanges() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Deencapsulation.setField(routineLoadJob, "customKinesisShards",
                Lists.newArrayList("shard-old-0", "shard-old-1"));
        Deencapsulation.setField(routineLoadJob, "openKinesisShards",
                Lists.newArrayList("shard-old-0"));
        Deencapsulation.setField(routineLoadJob, "closedKinesisShards",
                Lists.newArrayList("shard-old-1"));
        Map<String, String> oldProgress = new HashMap<>();
        oldProgress.put("shard-old-0", "100");
        oldProgress.put("shard-old-1", "200");
        Deencapsulation.setField(routineLoadJob, "progress", new KinesisProgress(oldProgress));
        Map<String, Long> oldLag = new HashMap<>();
        oldLag.put("shard-old-0", 10L);
        Deencapsulation.setField(routineLoadJob, "cachedShardWithMillsBehindLatest", oldLag);

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put(KinesisConfiguration.KINESIS_STREAM.getName(), "stream-2");
        KinesisDataSourceProperties dataSourceProperties = new KinesisDataSourceProperties(alterProps);
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone("Asia/Shanghai");
        dataSourceProperties.analyze();

        Deencapsulation.invoke(routineLoadJob, "modifyPropertiesInternal",
                new HashMap<String, String>(), dataSourceProperties);

        Assertions.assertEquals("stream-2", Deencapsulation.getField(routineLoadJob, "stream"));

        List<String> customKinesisShards = Deencapsulation.getField(routineLoadJob, "customKinesisShards");
        Assertions.assertTrue(customKinesisShards.isEmpty());
        List<String> openKinesisShards = Deencapsulation.getField(routineLoadJob, "openKinesisShards");
        Assertions.assertTrue(openKinesisShards.isEmpty());
        List<String> closedKinesisShards = Deencapsulation.getField(routineLoadJob, "closedKinesisShards");
        Assertions.assertTrue(closedKinesisShards.isEmpty());

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assertions.assertFalse(progress.hasShards());
        Map<String, Long> cachedLag = Deencapsulation.getField(routineLoadJob, "cachedShardWithMillsBehindLatest");
        Assertions.assertTrue(cachedLag.isEmpty());
    }

    @Test
    public void testModifyPropertiesShouldReplaceCustomShardsWhenExplicitShardsProvided() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Deencapsulation.setField(routineLoadJob, "customKinesisShards",
                Lists.newArrayList("shard-0"));
        Map<String, String> oldProgress = new HashMap<>();
        oldProgress.put("shard-0", "10");
        oldProgress.put("shard-1", "20");
        oldProgress.put("shard-2", "30");
        Deencapsulation.setField(routineLoadJob, "progress", new KinesisProgress(oldProgress));

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put(KinesisConfiguration.KINESIS_SHARDS.getName(), "shard-1,shard-2");
        alterProps.put(KinesisConfiguration.KINESIS_POSITIONS.getName(), "101,202");
        KinesisDataSourceProperties dataSourceProperties = new KinesisDataSourceProperties(alterProps);
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone("Asia/Shanghai");
        dataSourceProperties.analyze();

        Deencapsulation.invoke(routineLoadJob, "modifyPropertiesInternal",
                new HashMap<String, String>(), dataSourceProperties);

        List<String> customKinesisShards = Deencapsulation.getField(routineLoadJob, "customKinesisShards");
        Assertions.assertEquals(Lists.newArrayList("shard-1", "shard-2"), customKinesisShards);

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assertions.assertEquals("101", progress.getSequenceNumberByShard("shard-1"));
        Assertions.assertEquals("202", progress.getSequenceNumberByShard("shard-2"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFailedAlterPreservesStateAndShardDiscovery(boolean explicitShards) throws Exception {
        KinesisRoutineLoadJob job = createPausedJobForAlter(explicitShards);
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-west-2");
        sourceProperties.put(KinesisConfiguration.KINESIS_ENDPOINT.getName(), "http://new-endpoint:4566");
        sourceProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(), "TRIM_HORIZON");
        sourceProperties.put(KinesisConfiguration.KINESIS_SHARDS.getName(), "shard-0,shard-unknown");
        AlterRoutineLoadCommand command = createAlterCommand(
                Map.of(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "123"), sourceProperties);

        assertAlterFailsWithoutChanges(job, command,
                "The specified shard shard-unknown is not in the consumed shards");

        job.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, true);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KinesisUtil> kinesisUtil = Mockito.mockStatic(KinesisUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            kinesisUtil.when(() -> KinesisUtil.getAllKinesisShardInfos(
                    job.getRegion(), job.getStream(), job.getEndpoint(), job.getConvertedCustomProperties()))
                    .thenReturn(List.of(
                            org.apache.doris.proto.InternalService.PShardInfo.newBuilder()
                                    .setShardId("shard-0").build(),
                            org.apache.doris.proto.InternalService.PShardInfo.newBuilder()
                                    .setShardId("shard-new").build()));
            Assertions.assertTrue((Boolean) Deencapsulation.invoke(job, "refreshKafkaPartitions", false));
            kinesisUtil.verify(() -> KinesisUtil.getAllKinesisShardInfos(
                    job.getRegion(), job.getStream(), job.getEndpoint(), job.getConvertedCustomProperties()));
            Assertions.assertTrue((Boolean) Deencapsulation.invoke(job, "isKinesisShardsChanged"));
            KinesisShardTopology topology = Deencapsulation.getField(job, "shardTopology");
            List<String> openShards = topology.getOpenShardIds();
            if (explicitShards) {
                Assertions.assertEquals(List.of("shard-0"), openShards);
            } else {
                Assertions.assertEquals(Set.of("shard-0", "shard-new"), new HashSet<>(openShards));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFailedCommonPropertyValidationPreservesKinesisState(boolean changeStream) throws Exception {
        KinesisRoutineLoadJob job = createPausedJobForAlter(true);
        Deencapsulation.setField(job, "isMultiTable", true);
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(), "TRIM_HORIZON");
        sourceProperties.put(KinesisConfiguration.KINESIS_SHARDS.getName(), "shard-0");
        if (changeStream) {
            sourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "stream-2");
        }
        AlterRoutineLoadCommand command = createAlterCommand(Map.of(
                CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "123",
                CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FLEXIBLE_COLUMNS"), sourceProperties);

        assertAlterFailsWithoutChanges(job, command,
                "Flexible partial update is not supported in multi-table load");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSuccessfulAlterMatchesJournalReplay(boolean changeStream) throws Exception {
        KinesisRoutineLoadJob job = createPausedJobForAlter(true);
        KinesisRoutineLoadJob replayedJob = createPausedJobForAlter(true);
        Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put(KinesisConfiguration.KINESIS_REGION.getName(), "us-west-2");
        sourceProperties.put(KinesisConfiguration.KINESIS_DEFAULT_POSITION.getName(), "TRIM_HORIZON");
        sourceProperties.put(KinesisConfiguration.KINESIS_SHARDS.getName(), changeStream ? "shard-new" : "shard-0");
        if (changeStream) {
            sourceProperties.put(KinesisConfiguration.KINESIS_STREAM.getName(), "stream-2");
        }
        AlterRoutineLoadCommand command = createAlterCommand(
                Map.of(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "123"), sourceProperties);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            job.modifyProperties(command);

            ArgumentCaptor<AlterRoutineLoadJobOperationLog> logCaptor =
                    ArgumentCaptor.forClass(AlterRoutineLoadJobOperationLog.class);
            Mockito.verify(editLog).logAlterRoutineLoadJob(logCaptor.capture());
            AlterRoutineLoadJobOperationLog log = GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(logCaptor.getValue()), AlterRoutineLoadJobOperationLog.class);
            replayedJob.replayModifyProperties(log);
        }

        Assertions.assertEquals(123L, (long) Deencapsulation.getField(job, "maxErrorNum"));
        Assertions.assertEquals("us-west-2", job.getRegion());
        Assertions.assertEquals(changeStream ? "stream-2" : "stream-1", job.getStream());
        KinesisProgress progress = Deencapsulation.getField(job, "progress");
        Assertions.assertEquals("TRIM_HORIZON",
                progress.getSequenceNumberByShard(changeStream ? "shard-new" : "shard-0"));
        Assertions.assertEquals(snapshotAlterState(job), snapshotAlterState(replayedJob));
    }

    private KinesisRoutineLoadJob createPausedJobForAlter(boolean explicitShards) throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(job, "createTimestamp", 1L);
        Deencapsulation.setField(job, "endpoint", "http://old-endpoint:4566");
        List<String> openShards = Lists.newArrayList("shard-0");
        Deencapsulation.setField(job, "openKinesisShards", openShards);
        if (explicitShards) {
            // The scheduler can make the open and explicit shard lists share the same instance.
            Deencapsulation.setField(job, "customKinesisShards", openShards);
        }
        Deencapsulation.setField(job, "closedKinesisShards", Lists.newArrayList("shard-closed"));
        Deencapsulation.setField(job, "progress", new KinesisProgress(Map.of("shard-0", "100", "shard-closed", "200")));
        Deencapsulation.setField(job, "cachedShardWithMillsBehindLatest",
                new HashMap<>(Map.of("shard-0", 10L)));
        Deencapsulation.setField(job, "customProperties", new HashMap<>(Map.of("kinesis_default_pos", "LATEST")));
        job.prepare();
        return job;
    }

    private AlterRoutineLoadCommand createAlterCommand(Map<String, String> jobProperties,
            Map<String, String> sourceProperties) throws Exception {
        KinesisDataSourceProperties dataSourceProperties = new KinesisDataSourceProperties(sourceProperties);
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone("Asia/Shanghai");
        dataSourceProperties.analyze();
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(jobProperties);
        Mockito.when(command.getDataSourceProperties()).thenReturn(dataSourceProperties);
        return command;
    }

    private Map<String, Object> snapshotAlterState(KinesisRoutineLoadJob job) {
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("persisted", GsonUtils.GSON.toJsonTree(job));
        // Include derived and transient fields that the persisted representation does not cover.
        for (String field : List.of("convertedCustomProperties", "kinesisDefaultPosition",
                "cachedShardWithMillsBehindLatest", "newCurrentKinesisShards", "maxFilterRatio",
                "uniqueKeyUpdateMode", "isPartialUpdate", "partialUpdateNewKeyPolicy")) {
            Object value = Deencapsulation.getField(job, field);
            snapshot.put(field, new Gson().toJsonTree(value));
        }
        return snapshot;
    }

    private void assertAlterFailsWithoutChanges(KinesisRoutineLoadJob job, AlterRoutineLoadCommand command,
            String expectedMessage) throws Exception {
        Map<String, Object> before = snapshotAlterState(job);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            DdlException exception = Assertions.assertThrows(DdlException.class, () -> job.modifyProperties(command));
            Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
            Mockito.verifyNoInteractions(editLog);
        }
        Assertions.assertEquals(before, snapshotAlterState(job));
    }

    @Test
    public void testShardRefreshShouldMoveRetiredParentToClosedUntilConsumed() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            KinesisRoutineLoadJob job = createJobBeforeSplit();
            Deencapsulation.setField(job, "newCurrentKinesisShardInfos", splitShardInfos());
            Assertions.assertTrue((Boolean) Deencapsulation.invoke(job, "isKinesisShardsChanged"));
            KinesisShardTopology topology = Deencapsulation.getField(job, "shardTopology");
            Assertions.assertEquals(List.of("shard-child-0", "shard-child-1"), topology.getOpenShardIds());
            Assertions.assertEquals(List.of("shard-parent"), topology.getClosedShardIds());
            KinesisProgress progress = Deencapsulation.getField(job, "progress");
            Assertions.assertEquals("100", progress.getSequenceNumberByShard("shard-parent"));
            Assertions.assertEquals("TRIM_HORIZON", progress.getSequenceNumberByShard("shard-child-0"));
            Assertions.assertEquals("TRIM_HORIZON", progress.getSequenceNumberByShard("shard-child-1"));
            job.divideRoutineLoadJob(2);
            Assertions.assertEquals(Set.of("shard-parent", "shard-child-0", "shard-child-1"),
                    collectAssignedShards(job));
        }
    }

    @Test
    public void testFullyConsumedClosedParentShouldNotReappearOnRefresh() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            KinesisRoutineLoadJob job = createJobBeforeSplit();
            Deencapsulation.setField(job, "newCurrentKinesisShardInfos", splitShardInfos());
            Assertions.assertTrue((Boolean) Deencapsulation.invoke(job, "isKinesisShardsChanged"));
            Map<String, String> childProgress = Map.of("shard-child-0", "200", "shard-child-1", "300");
            Map<String, Long> childLag = Map.of("shard-child-0", 0L, "shard-child-1", 100L);
            RLTaskTxnCommitAttachment attachment = createCommitAttachment(
                    createProgress(childProgress, childLag, "shard-parent"));
            TransactionState txn = new TransactionState();
            txn.setTransactionId(11L);
            txn.setTxnCommitAttachment(attachment);
            job.replayOnCommitted(txn);
            job.replayOnVisible(txn);
            KinesisProgress progress = Deencapsulation.getField(job, "progress");
            Assertions.assertEquals(childProgress, progress.getShardIdToSequenceNumber());
            KinesisShardTopology topology = Deencapsulation.getField(job, "shardTopology");
            Assertions.assertEquals(List.of("shard-child-0", "shard-child-1"), topology.getOpenShardIds());
            Assertions.assertTrue(topology.getClosedShardIds().isEmpty());
            Map<String, Long> cachedLag = Deencapsulation.getField(job, "cachedShardWithMillsBehindLatest");
            Assertions.assertEquals(childLag, cachedLag);
            job.divideRoutineLoadJob(2);

            // AWS still lists the completed parent; refreshing must not revive it.
            Deencapsulation.setField(job, "newCurrentKinesisShardInfos", splitShardInfos());
            Assertions.assertFalse((Boolean) Deencapsulation.invoke(job, "isKinesisShardsChanged"));
            Assertions.assertFalse(progress.containsShard("shard-parent"));
            Assertions.assertEquals(Set.of("shard-child-0", "shard-child-1"), collectAssignedShards(job));
        }
    }

    private KinesisRoutineLoadJob createJobBeforeSplit() {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("shard-parent")), "100", "TRIM_HORIZON");
        Deencapsulation.setField(job, "shardTopology", topology);
        Deencapsulation.setField(job, "progress", new KinesisProgress(Map.of("shard-parent", "100")));
        return job;
    }

    private List<InternalService.PShardInfo> splitShardInfos() {
        return List.of(InternalService.PShardInfo.newBuilder().setShardId("shard-parent").setClosed(true).build(),
                createChildShardInfo("shard-child-0", "shard-parent"),
                createChildShardInfo("shard-child-1", "shard-parent"));
    }

    @Test
    public void testReplayCommittedAllowsChildBeforeParentVisible() throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("parent"),
                createChildShardInfo("child", "parent")), KinesisProgress.POSITION_TRIM_HORIZON,
                KinesisProgress.POSITION_TRIM_HORIZON);
        Deencapsulation.setField(job, "shardTopology", topology);
        Deencapsulation.setField(job, "progress", new KinesisProgress(Map.of("parent", "100")));

        KinesisProgress taskProgress = createProgress(Map.of("parent", "200"), Map.of(), "parent");
        Deencapsulation.setField(taskProgress, "childShardParentIds",
                Map.of("child", Set.of("parent")));
        RLTaskTxnCommitAttachment attachment = createCommitAttachment(taskProgress);
        TransactionState txn = new TransactionState();
        txn.setTransactionId(11L);
        txn.setTxnCommitAttachment(attachment);

        job.replayOnCommitted(txn);
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("child").getState());
        Assertions.assertEquals(List.of("child"), topology.getReadyShardIds());
        Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                topology.getNodes().get("parent").getState());

        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler scheduler = Mockito.mock(RoutineLoadTaskScheduler.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(scheduler);
            job.divideRoutineLoadJob(1);
            Assertions.assertEquals(Set.of("child"), collectAssignedShards(job));
        }

        // Visibility completes only the parent; the child has already been scheduled.
        job.replayOnVisible(txn);
        Assertions.assertEquals(KinesisShardTopology.ShardState.COMPLETED,
                topology.getNodes().get("parent").getState());
        job.replayOnVisible(txn);
        Assertions.assertEquals(List.of("child"), topology.getReadyShardIds());
    }

    @Test
    public void testAbortedTransactionDoesNotPublishKinesisProgress() {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("parent")), KinesisProgress.POSITION_TRIM_HORIZON,
                KinesisProgress.POSITION_TRIM_HORIZON);
        Deencapsulation.setField(job, "shardTopology", topology);
        KinesisProgress progress = new KinesisProgress(Map.of("parent", "100"));
        Deencapsulation.setField(job, "progress", progress);

        KinesisProgress attachmentProgress = createProgress(Map.of("parent", "200"), Map.of(), "parent");
        RLTaskTxnCommitAttachment attachment = createCommitAttachment(attachmentProgress);
        TransactionState txn = new TransactionState();
        txn.setTransactionId(12L);
        txn.setTransactionStatus(TransactionStatus.ABORTED);
        txn.setTxnCommitAttachment(attachment);

        job.replayOnAborted(txn);
        Assertions.assertEquals("100", progress.getSequenceNumberByShard("parent"));
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("parent").getState());
    }

    @Test
    public void testImageRoundTripRepairsTopologyFromConcreteProgress() throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("shard-0")), KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        Deencapsulation.setField(job, "shardTopology", topology);
        Deencapsulation.setField(job, "progress", new KinesisProgress(Map.of("shard-0", "900")));

        // Base gsonPostProcess restores the load definition before reconciling Kinesis positions.
        // Supply its catalog/parser dependencies rather than restoring an invalid job with no SQL.
        String sql = "CREATE ROUTINE LOAD job ON tbl FROM KINESIS";
        job.setOrigStmt(new OriginStatement(sql, 0));
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getName()).thenReturn("test_db");
        Mockito.when(db.getId()).thenReturn(1L);
        InternalCatalog catalog = env.getInternalCatalog();
        Mockito.when(catalog.getDb(1L)).thenReturn(java.util.Optional.of(db));
        Mockito.when(catalog.getDb("test_db")).thenReturn(java.util.Optional.of(db));
        Mockito.when(env.getCatalogMgr().getCatalog(Mockito.anyString())).thenReturn(catalog);
        CreateRoutineLoadCommand command = Mockito.mock(CreateRoutineLoadCommand.class);
        CreateRoutineLoadInfo info = Mockito.mock(CreateRoutineLoadInfo.class);
        Mockito.when(command.getCreateRoutineLoadInfo()).thenReturn(info);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                        (parser, context) -> Mockito.when(parser.parseSingle(sql)).thenReturn(command))) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            KinesisRoutineLoadJob restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job),
                    KinesisRoutineLoadJob.class);
            Assertions.assertEquals(1, parsers.constructed().size());
            Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, restored.getState());
            KinesisShardTopology restoredTopology = Deencapsulation.getField(restored, "shardTopology");
            KinesisProgress restoredProgress = Deencapsulation.getField(restored, "progress");
            Assertions.assertEquals("900", restoredTopology.getStartPosition("shard-0"));
            Assertions.assertEquals("900", restoredProgress.getSequenceNumberByShard("shard-0"));
            Assertions.assertEquals(List.of("shard-0"), restoredTopology.getReadyShardIds());
        }
    }

    @Test
    public void testMissingExplicitShardPausesJob() throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        Deencapsulation.setField(job, "customKinesisShards", Lists.newArrayList("missing"));
        Deencapsulation.setField(job, "newCurrentKinesisShardInfos", Lists.newArrayList());
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Assertions.assertFalse((Boolean) Deencapsulation.invoke(job, "isKinesisShardsChanged"));
        }
        Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.getState());
        Assertions.assertTrue(job.getPauseReason().getMsg().contains("missing"));
    }

    @Test
    public void testLineageErrorDisablesAutoResume() throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("parent")), "900", "TRIM_HORIZON");
        topology.mergeChildShardInfos(Map.of("child", Set.of("parent")), KinesisProgress.TRIM_HORIZON_VAL);
        topology.mergeShardInfos(List.of(createChildShardInfo("child", "parent")), "900", "TRIM_HORIZON");
        Deencapsulation.setField(job, "shardTopology", topology);
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);

        Assertions.assertNotNull(topology.getLineageError());
        Assertions.assertFalse((Boolean) Deencapsulation.invoke(job, "needAutoResume"));
    }

    /**
     * Mirrors the resume scenario behind "an empty progress map is not first setup": the consumed
     * parent is already gone from progress when the children are discovered, so the start position
     * comes from the topology instead of the configured default position.
     */
    @Test
    public void testShardDiscoveredAfterInitialSnapshotUsesTrimHorizonInProgress() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "us-east-1", "stream-1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "kinesisDefaultPosition",
                KinesisProgress.POSITION_LATEST);
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(createShardInfo("shard-parent")),
                KinesisProgress.POSITION_LATEST, KinesisProgress.POSITION_TRIM_HORIZON);
        Deencapsulation.setField(routineLoadJob, "shardTopology", topology);
        Deencapsulation.invoke(routineLoadJob, "updateNewShardProgress");

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assertions.assertEquals(KinesisProgress.POSITION_LATEST,
                progress.getSequenceNumberByShard("shard-parent"));

        // The parent is fully consumed and removed from progress before its children are seen.
        progress.getShardIdToSequenceNumber().clear();
        topology.markCompleted("shard-parent");
        topology.mergeShardInfos(
                List.of(createShardInfo("shard-parent"),
                        createChildShardInfo("shard-child", "shard-parent")),
                KinesisProgress.POSITION_LATEST, KinesisProgress.POSITION_TRIM_HORIZON);
        Deencapsulation.invoke(routineLoadJob, "updateNewShardProgress");

        Assertions.assertEquals(KinesisProgress.POSITION_TRIM_HORIZON,
                progress.getSequenceNumberByShard("shard-child"));
    }

    private static InternalService.PShardInfo createShardInfo(String shardId) {
        return InternalService.PShardInfo.newBuilder().setShardId(shardId).build();
    }

    private static InternalService.PShardInfo createChildShardInfo(String shardId, String parentShardId) {
        return InternalService.PShardInfo.newBuilder().setShardId(shardId)
                .setParentShardId(parentShardId).build();
    }

    @Test
    public void testDisplayCustomPropertiesMasksKinesisSecrets() {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put(KinesisConfiguration.KINESIS_ACCESS_KEY.getName(), "aws_access_key");
        customProperties.put(KinesisConfiguration.KINESIS_SECRET_KEY.getName(), "aws_secret");
        customProperties.put(KinesisConfiguration.KINESIS_SESSION_TOKEN.getName(), "aws_session_secret");
        customProperties.put("aws.role_arn", "role_arn_value");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

        String customPropertiesJson = routineLoadJob.customPropertiesJsonToString();
        Map<String, String> showCreateCustomProperties = routineLoadJob.getCustomProperties();

        Assertions.assertFalse(customPropertiesJson.contains("aws_access_key"));
        Assertions.assertFalse(customPropertiesJson.contains("aws_secret"));
        Assertions.assertFalse(customPropertiesJson.contains("aws_session_secret"));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.access_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.secret_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.session_key\":\"******\""));
        Assertions.assertTrue(customPropertiesJson.contains("\"aws.role_arn\":\"role_arn_value\""));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.access_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.secret_key"));
        Assertions.assertEquals("******", showCreateCustomProperties.get("property.aws.session_key"));
        Assertions.assertEquals("role_arn_value", showCreateCustomProperties.get("property.aws.role_arn"));
    }

    private Set<String> collectAssignedShards(KinesisRoutineLoadJob routineLoadJob) {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Set<String> assignedShards = new HashSet<>();
        for (RoutineLoadTaskInfo taskInfo : routineLoadTaskInfoList) {
            assignedShards.addAll(((KinesisTaskInfo) taskInfo).getShards());
        }
        return assignedShards;
    }

    private RLTaskTxnCommitAttachment createCommitAttachment(KinesisProgress progress) {
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        Deencapsulation.setField(attachment, "progress", progress);
        return attachment;
    }

    private KinesisProgress createProgress(Map<String, String> shardToSeqNum, Map<String, Long> lagMap,
            String... closedShards) {
        KinesisProgress progress = new KinesisProgress(shardToSeqNum);
        Map<String, Long> shardIdToMillsBehindLatest = Maps.newConcurrentMap();
        shardIdToMillsBehindLatest.putAll(lagMap);
        Deencapsulation.setField(progress, "shardIdToMillsBehindLatest", shardIdToMillsBehindLatest);
        Deencapsulation.setField(progress, "closedShardIds", new HashSet<>(Lists.newArrayList(closedShards)));
        return progress;
    }
}
