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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.kinesis.KinesisConfiguration;
import org.apache.doris.load.routineload.kinesis.KinesisDataSourceProperties;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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
    public void testModifyTargetTableWithPropertiesAndReplay() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        101L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        KinesisProgress progress = new KinesisProgress(Maps.newHashMap());
        Deencapsulation.setField(routineLoadJob, "progress", progress);

        Map<String, String> sourceProperties = Maps.newHashMap();
        sourceProperties.put("property.client.timeout", "1000");
        KinesisDataSourceProperties dataSourceProperties = new KinesisDataSourceProperties(sourceProperties);
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone("UTC");
        dataSourceProperties.analyze();

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "10");
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.hasTargetTable()).thenReturn(true);
        Mockito.when(command.getTargetTableId()).thenReturn(202L);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(jobProperties);
        Mockito.when(command.getDataSourceProperties()).thenReturn(dataSourceProperties);

        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            routineLoadJob.modifyProperties(command);
        }

        Assert.assertEquals(202L, routineLoadJob.getTableId());
        Assert.assertSame(progress, routineLoadJob.getProgress());
        Assert.assertEquals(10L, ((Long) Deencapsulation.getField(routineLoadJob, "maxErrorNum")).longValue());
        Assert.assertEquals("1000", routineLoadJob.getCustomProperties().get("property.client.timeout"));
        ArgumentCaptor<AlterRoutineLoadJobOperationLog> logCaptor =
                ArgumentCaptor.forClass(AlterRoutineLoadJobOperationLog.class);
        Mockito.verify(editLog).logAlterRoutineLoadJob(logCaptor.capture());
        Assert.assertEquals(202L, logCaptor.getValue().getTargetTableId());

        KinesisRoutineLoadJob replayJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        303L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);
        Map<String, String> shardPositions = Maps.newHashMap();
        shardPositions.put("shard-0", "123");
        KinesisProgress replayProgress = new KinesisProgress(shardPositions);
        Deencapsulation.setField(replayJob, "progress", replayProgress);
        replayJob.replayModifyProperties(new AlterRoutineLoadJobOperationLog(
                1L, Maps.newHashMap(), null));
        Assert.assertEquals(303L, replayJob.getTableId());
        replayJob.replayModifyProperties(logCaptor.getValue());
        Assert.assertEquals(202L, replayJob.getTableId());
        Assert.assertSame(replayProgress, replayJob.getProgress());
        Assert.assertEquals("123", ((KinesisProgress) replayJob.getProgress())
                .getSequenceNumberByShard("shard-0"));
    }

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

            Assert.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

            Deencapsulation.setField(routineLoadJob, "desireTaskConcurrentNum", 2);
            Assert.assertEquals(2, routineLoadJob.calculateCurrentConcurrentTaskNum());

            Config.max_routine_load_task_concurrent_num = 1;
            Assert.assertEquals(1, routineLoadJob.calculateCurrentConcurrentTaskNum());
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

        Assert.assertEquals(2L, ((Number) statistic.get("openShardNum")).longValue());
        Assert.assertEquals(1L, ((Number) statistic.get("closedShardNum")).longValue());
        Assert.assertEquals(4L, ((Number) statistic.get("trackedShardNum")).longValue());
        Assert.assertEquals(3L, ((Number) statistic.get("cachedMillisBehindLatestShardNum")).longValue());
        Assert.assertEquals(100L, ((Number) statistic.get("totalMillisBehindLatest")).longValue());
        Assert.assertEquals(100L, ((Number) statistic.get("maxMillisBehindLatest")).longValue());
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
        Assert.assertTrue(routineLoadJob.hasMoreDataToConsume(UUID.randomUUID(), shardToSeqNum));
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
        Assert.assertEquals(100L, updatedLagCache.get("shard-0").longValue());

        Gson gson = new Gson();
        Map<String, Object> statistic = gson.fromJson(routineLoadJob.getStatistic(), Map.class);
        Assert.assertEquals(100L, ((Number) statistic.get("totalMillisBehindLatest")).longValue());
        Assert.assertEquals(100L, ((Number) statistic.get("maxMillisBehindLatest")).longValue());

        Map<String, Object> lag = gson.fromJson(routineLoadJob.getLag(), Map.class);
        Assert.assertEquals(100L, ((Number) lag.get("shard-0")).longValue());
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

        Assert.assertEquals("stream-2", Deencapsulation.getField(routineLoadJob, "stream"));

        List<String> customKinesisShards = Deencapsulation.getField(routineLoadJob, "customKinesisShards");
        Assert.assertTrue(customKinesisShards.isEmpty());
        List<String> openKinesisShards = Deencapsulation.getField(routineLoadJob, "openKinesisShards");
        Assert.assertTrue(openKinesisShards.isEmpty());
        List<String> closedKinesisShards = Deencapsulation.getField(routineLoadJob, "closedKinesisShards");
        Assert.assertTrue(closedKinesisShards.isEmpty());

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assert.assertFalse(progress.hasShards());
        Map<String, Long> cachedLag = Deencapsulation.getField(routineLoadJob, "cachedShardWithMillsBehindLatest");
        Assert.assertTrue(cachedLag.isEmpty());
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
        Assert.assertEquals(Lists.newArrayList("shard-1", "shard-2"), customKinesisShards);

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assert.assertEquals("101", progress.getSequenceNumberByShard("shard-1"));
        Assert.assertEquals("202", progress.getSequenceNumberByShard("shard-2"));
    }

    @Test
    public void testShardRefreshShouldMoveRetiredParentToClosedUntilConsumed() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Map<String, String> shardToSeqNum = new HashMap<>();
        shardToSeqNum.put("shard-parent", "100");
        Deencapsulation.setField(routineLoadJob, "progress", new KinesisProgress(shardToSeqNum));
        Deencapsulation.setField(routineLoadJob, "openKinesisShards", Lists.newArrayList("shard-parent"));
        Deencapsulation.setField(routineLoadJob, "closedKinesisShards", Lists.newArrayList());
        Deencapsulation.setField(routineLoadJob, "newCurrentKinesisShards",
                Lists.newArrayList("shard-child-0", "shard-child-1"));

        Assert.assertTrue((Boolean) Deencapsulation.invoke(routineLoadJob, "isKinesisShardsChanged"));
        List<String> openKinesisShards = Deencapsulation.getField(routineLoadJob, "openKinesisShards");
        List<String> closedKinesisShards = Deencapsulation.getField(routineLoadJob, "closedKinesisShards");
        Assert.assertEquals(new HashSet<>(Lists.newArrayList("shard-child-0", "shard-child-1")),
                new HashSet<>(openKinesisShards));
        Assert.assertEquals(new HashSet<>(Lists.newArrayList("shard-parent")),
                new HashSet<>(closedKinesisShards));

        Deencapsulation.invoke(routineLoadJob, "updateNewShardProgress");
        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assert.assertTrue(progress.containsShard("shard-parent"));
        Assert.assertTrue(progress.containsShard("shard-child-0"));
        Assert.assertTrue(progress.containsShard("shard-child-1"));

        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        routineLoadJob.divideRoutineLoadJob(2);
        Assert.assertEquals(new HashSet<>(Lists.newArrayList("shard-parent", "shard-child-0", "shard-child-1")),
                collectAssignedShards(routineLoadJob));
    }

    @Test
    public void testFullyConsumedClosedParentShouldNotReappearOnRefresh() throws Exception {
        KinesisRoutineLoadJob routineLoadJob =
                new KinesisRoutineLoadJob(1L, "kinesis_routine_load_job", 1L,
                        1L, "ap-southeast-1", "stream-1", UserIdentity.ADMIN);

        Map<String, String> parentProgress = new HashMap<>();
        parentProgress.put("shard-parent", "100");
        Deencapsulation.setField(routineLoadJob, "progress", new KinesisProgress(parentProgress));
        Deencapsulation.setField(routineLoadJob, "openKinesisShards", Lists.newArrayList("shard-parent"));
        Deencapsulation.setField(routineLoadJob, "closedKinesisShards", Lists.newArrayList());
        Deencapsulation.setField(routineLoadJob, "newCurrentKinesisShards",
                Lists.newArrayList("shard-child-0", "shard-child-1"));

        Assert.assertTrue((Boolean) Deencapsulation.invoke(routineLoadJob, "isKinesisShardsChanged"));
        Deencapsulation.invoke(routineLoadJob, "updateNewShardProgress");

        Map<String, String> childProgress = new HashMap<>();
        childProgress.put("shard-child-0", "200");
        childProgress.put("shard-child-1", "300");
        Map<String, Long> oldLag = new HashMap<>();
        oldLag.put("shard-parent", 60_000L);
        Deencapsulation.setField(routineLoadJob, "cachedShardWithMillsBehindLatest", oldLag);
        Map<String, Long> childLag = new HashMap<>();
        childLag.put("shard-child-0", 0L);
        childLag.put("shard-child-1", 100L);
        RLTaskTxnCommitAttachment attachment =
                createCommitAttachment(createProgress(childProgress, childLag, "shard-parent"));
        Deencapsulation.invoke(routineLoadJob, "updateProgressAndOffsetsCache", attachment);

        KinesisProgress progress = Deencapsulation.getField(routineLoadJob, "progress");
        Assert.assertFalse(progress.containsShard("shard-parent"));
        Assert.assertTrue(progress.containsShard("shard-child-0"));
        Assert.assertTrue(progress.containsShard("shard-child-1"));
        List<String> openKinesisShards = Deencapsulation.getField(routineLoadJob, "openKinesisShards");
        Assert.assertEquals(new HashSet<>(Lists.newArrayList("shard-child-0", "shard-child-1")),
                new HashSet<>(openKinesisShards));
        Assert.assertTrue(((List<String>) Deencapsulation.getField(routineLoadJob, "closedKinesisShards")).isEmpty());
        Map<String, Long> cachedLag = Deencapsulation.getField(routineLoadJob, "cachedShardWithMillsBehindLatest");
        Assert.assertFalse(cachedLag.containsKey("shard-parent"));
        Assert.assertEquals(0L, cachedLag.get("shard-child-0").longValue());
        Assert.assertEquals(100L, cachedLag.get("shard-child-1").longValue());

        Deencapsulation.setField(routineLoadJob, "newCurrentKinesisShards",
                Lists.newArrayList("shard-child-0", "shard-child-1"));
        Assert.assertFalse((Boolean) Deencapsulation.invoke(routineLoadJob, "isKinesisShardsChanged"));
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

        Assert.assertFalse(customPropertiesJson.contains("aws_access_key"));
        Assert.assertFalse(customPropertiesJson.contains("aws_secret"));
        Assert.assertFalse(customPropertiesJson.contains("aws_session_secret"));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.access_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.secret_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.session_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.role_arn\":\"role_arn_value\""));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.access_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.secret_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.session_key"));
        Assert.assertEquals("role_arn_value", showCreateCustomProperties.get("property.aws.role_arn"));
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
