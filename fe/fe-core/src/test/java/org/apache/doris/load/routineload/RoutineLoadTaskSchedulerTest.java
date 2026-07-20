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

import org.apache.doris.analysis.LoadColumnsInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RoutineLoadTaskSchedulerTest {

    @Mocked
    private RoutineLoadManager routineLoadManager;
    @Mocked
    private Env env;
    @Mocked
    private AgentTaskExecutor agentTaskExecutor;

    @Test
    public void testRunOneCycle(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob1,
                                @Injectable KafkaRoutineLoadJob routineLoadJob,
                                @Injectable RoutineLoadDesc routineLoadDesc,
                                @Injectable LoadColumnsInfo loadColumnsInfo,
                                @Mocked GlobalTransactionMgr globalTransactionMgr,
                                @Mocked BackendService.Client client,
                                @Mocked ClientPool clientPool) throws LoadException,
            MetaNotFoundException, AnalysisException, LabelAlreadyUsedException, BeginTransactionException {
        long beId = 100L;

        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

        LinkedBlockingDeque<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = new LinkedBlockingDeque<>();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfoQueue.addFirst(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
                env.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;

                routineLoadManager.getClusterIdleSlotNum();
                minTimes = 0;
                result = 1;
                routineLoadManager.checkTaskInJob((RoutineLoadTaskInfo) any);
                minTimes = 0;
                result = true;

                kafkaRoutineLoadJob1.getDbId();
                minTimes = 0;
                result = 1L;
                kafkaRoutineLoadJob1.getTableId();
                minTimes = 0;
                result = 1L;
                kafkaRoutineLoadJob1.getName();
                minTimes = 0;
                result = "";
                routineLoadManager.getMinTaskBeId(anyString);
                minTimes = 0;
                result = beId;
                routineLoadManager.getJob(anyLong);
                minTimes = 0;
                result = kafkaRoutineLoadJob1;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
        routineLoadTaskScheduler.runAfterCatalogReady();
    }

    @Test
    public void testSubmitTaskFailureRenewsTaskWithJobWriteLock() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        new Expectations() {
            {
                routineLoadManager.getJob(1L);
                result = routineLoadJob;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assert.assertTrue(routineLoadJob.isRenewCalledWithWriteLock());
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assert.assertEquals(1, routineLoadTaskInfoList.size());
        Assert.assertNotSame(routineLoadTaskInfo, routineLoadTaskInfoList.get(0));

        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertSame(routineLoadTaskInfoList.get(0), needScheduleTasksQueue.peek());
    }

    @Test
    public void testSubmitTaskFailureSkipsRenewWhenTaskRemoved() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", Lists.newArrayList());
        new Expectations() {
            {
                routineLoadManager.getJob(1L);
                result = routineLoadJob;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assert.assertFalse(routineLoadJob.isRenewCalled());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    @Test
    public void testSubmitTaskFailureSkipsRenewWhenJobPaused() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        LockCheckingKafkaRoutineLoadJob routineLoadJob = new LockCheckingKafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        new Expectations() {
            {
                routineLoadManager.getJob(1L);
                result = routineLoadJob;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assert.assertFalse(routineLoadJob.isRenewCalled());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    private static class LockCheckingKafkaRoutineLoadJob extends KafkaRoutineLoadJob {
        private boolean renewCalled;
        private boolean renewCalledWithWriteLock;

        @Override
        protected RoutineLoadTaskInfo unprotectRenewTask(RoutineLoadTaskInfo routineLoadTaskInfo,
                boolean delaySchedule) {
            renewCalled = true;
            renewCalledWithWriteLock = lock.isWriteLockedByCurrentThread();
            return super.unprotectRenewTask(routineLoadTaskInfo, delaySchedule);
        }

        private boolean isRenewCalled() {
            return renewCalled;
        }

        private boolean isRenewCalledWithWriteLock() {
            return renewCalledWithWriteLock;
        }
    }
}
