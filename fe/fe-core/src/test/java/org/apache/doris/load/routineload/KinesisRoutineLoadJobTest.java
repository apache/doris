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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
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
}
