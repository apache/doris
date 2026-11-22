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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.load.routineload.kinesis.KinesisProgress;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisTaskInfo;
import org.apache.doris.thrift.TRoutineLoadTask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

class RoutineLoadTimeoutBoundaryTest {
    private final int savedMultiplier = Config.routine_load_task_timeout_multiplier;
    private final int savedMinimum = Config.routine_load_task_min_timeout_sec;
    private final int savedAdaptiveMinimum = Config.routine_load_adaptive_min_batch_interval_sec;
    private final RoutineLoadManager manager = Mockito.mock(RoutineLoadManager.class);
    private MockedStatic<Env> envStatic;

    @BeforeEach
    void setUp() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getRoutineLoadManager()).thenReturn(manager);
        Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(Mockito.mock(RoutineLoadTaskScheduler.class));
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        Config.routine_load_task_timeout_multiplier = 1;
        Config.routine_load_task_min_timeout_sec = 60;
        Config.routine_load_adaptive_min_batch_interval_sec = 1;
    }

    @AfterEach
    void tearDown() {
        envStatic.close();
        Config.routine_load_task_timeout_multiplier = savedMultiplier;
        Config.routine_load_task_min_timeout_sec = savedMinimum;
        Config.routine_load_adaptive_min_batch_interval_sec = savedAdaptiveMinimum;
    }

    @Test
    void jobAndPlanKeepLargeIntervals() throws Exception {
        // No source data is needed. Both callers must use the bounded seconds calculation.
        // 2^32 + 1 narrowed to 1 before the old multiplication and selected the 60-second minimum.
        for (RoutineLoadJob job : List.of(new KafkaRoutineLoadJob(), new KinesisRoutineLoadJob())) {
            Deencapsulation.setField(job, "maxBatchIntervalS", 4294967297L);
            Assertions.assertEquals(Integer.MAX_VALUE, job.getTimeout());
            Assertions.assertEquals(Integer.MAX_VALUE, job.toNereidsRoutineLoadTaskInfo().getTimeout());
        }
    }

    @Test
    void initialTasksKeepMillisecondTimeouts() throws Exception {
        // One partition or shard creates one task. 2147484 is the first whole-second
        // timeout whose millisecond value exceeds INT_MAX. No messages are consumed.
        KafkaRoutineLoadJob kafka = new KafkaRoutineLoadJob();
        Deencapsulation.setField(kafka, "currentKafkaPartitions", List.of(0));
        Deencapsulation.setField(kafka, "progress", new KafkaProgress(Map.of(0, 0L)));
        KinesisRoutineLoadJob kinesis = new KinesisRoutineLoadJob();
        Deencapsulation.setField(kinesis, "openKinesisShards", List.of("shard-0"));
        Deencapsulation.setField(kinesis, "progress", new KinesisProgress(Map.of("shard-0", "0")));
        for (RoutineLoadJob job : List.of(kafka, kinesis)) {
            Deencapsulation.setField(job, "maxBatchIntervalS", 2147484L);
            job.divideRoutineLoadJob(1);
            List<RoutineLoadTaskInfo> tasks = Deencapsulation.getField(job, "routineLoadTaskInfoList");
            Assertions.assertEquals(1, tasks.size());
            Assertions.assertEquals(2147484000L, tasks.get(0).getTimeoutMs());
        }
    }

    @Test
    void kafkaAdaptiveAndFixedTimeoutsKeepTheirBounds() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        KafkaTaskInfo task = new KafkaTaskInfo(new UUID(0, 1), 1, 0, Map.of(0, 0L), false, 0, false);
        // One partition with no cached lag uses the fixed timeout. A cached offset
        // above the batch row limit selects the adaptive path without a broker.
        Deencapsulation.setField(job, "maxBatchIntervalS", 2147484L);
        task.updateAdaptiveTimeout(job);
        Assertions.assertEquals(2147484000L, task.getTimeoutMs());
        Deencapsulation.setField(job, "cachedPartitionWithLatestOffsets", Map.of(0, Long.MAX_VALUE));
        Deencapsulation.setField(job, "maxBatchIntervalS", Long.MAX_VALUE);
        task.updateAdaptiveTimeout(job);
        Assertions.assertEquals(2147483647000L, task.getTimeoutMs());
        Deencapsulation.setField(job, "maxBatchIntervalS", 1L);
        task.updateAdaptiveTimeout(job);
        Assertions.assertEquals(60000L, task.getTimeoutMs());
    }

    @Test
    void kinesisAdaptiveAndFixedTimeoutsKeepTheirBounds() {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob();
        KinesisTaskInfo task = new KinesisTaskInfo(new UUID(0, 1), 1, 0, Map.of(), false, 0, true);
        TRoutineLoadTask request = new TRoutineLoadTask();
        // The EOF path uses the ordinary job timeout. Exercise the actual request builder.
        Deencapsulation.setField(job, "maxBatchIntervalS", 2147484L);
        Deencapsulation.invoke(task, "adaptiveBatchParam", request, job);
        Assertions.assertEquals(2147484000L, task.getTimeoutMs());
        // The adaptive path caps seconds before multiplication. Its existing minimum is zero.
        Deencapsulation.setField(task, "isEof", false);
        Deencapsulation.setField(job, "maxBatchIntervalS", Long.MAX_VALUE);
        Deencapsulation.invoke(task, "adaptiveBatchParam", request, job);
        Assertions.assertEquals(2147483647000L, task.getTimeoutMs());
        Assertions.assertEquals(Long.MAX_VALUE, request.getMaxIntervalS());
        Deencapsulation.setField(job, "maxBatchIntervalS", 1L);
        Deencapsulation.invoke(task, "adaptiveBatchParam", request, job);
        Assertions.assertEquals(1000L, task.getTimeoutMs());
    }

    @Test
    void emptyBatchStillReachesEofWithLargeInterval() {
        // Zero rows and bytes end before a positive interval. Long.MAX_VALUE * 1000
        // used to wrap to -1000 and incorrectly report that this empty batch reached its limit.
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "maxBatchIntervalS", Long.MAX_VALUE);
        Mockito.when(manager.getJob(1L)).thenReturn(job);
        KafkaTaskInfo task = new KafkaTaskInfo(new UUID(0, 1), 1, 0, Map.of(), false, 0, false);
        task.handleTaskByTxnCommitAttachment(Mockito.mock(RLTaskTxnCommitAttachment.class));
        Assertions.assertTrue(task.getIsEof());
    }

    @Test
    void delayedTaskRemainsQueuedWithLargeInterval() {
        // One queued task is sufficient. No clock wait is needed: even a last schedule
        // time of zero is inside the saturated interval. The old overflow dequeued it.
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob();
        Deencapsulation.setField(job, "maxBatchIntervalS", Long.MAX_VALUE);
        Mockito.when(manager.getJob(1L)).thenReturn(job);
        Mockito.when(manager.getClusterIdleSlotNum()).thenReturn(1);
        KafkaTaskInfo task = new KafkaTaskInfo(new UUID(0, 1), 1, 0, Map.of(), false, 0, true);
        task.setDelaySchedule(true);
        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler(manager);
        LinkedBlockingDeque<RoutineLoadTaskInfo> queue = Deencapsulation.getField(scheduler, "needScheduleTasksQueue");
        queue.add(task);
        scheduler.runAfterCatalogReady();
        Assertions.assertEquals(1, queue.size());
        Assertions.assertSame(task, queue.peek());
        Mockito.verify(manager, Mockito.never()).checkTaskInJob(Mockito.any());
    }
}
