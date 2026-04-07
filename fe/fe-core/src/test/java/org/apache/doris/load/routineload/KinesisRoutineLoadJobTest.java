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
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.kinesis.KinesisConfiguration;
import org.apache.doris.load.routineload.kinesis.KinesisDataSourceProperties;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
}
