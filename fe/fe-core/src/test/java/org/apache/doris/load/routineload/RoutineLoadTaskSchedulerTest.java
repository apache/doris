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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RoutineLoadTaskSchedulerTest {

    private RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
    private Env env = Mockito.mock(Env.class);
    private AgentTaskExecutor agentTaskExecutor = Mockito.mock(AgentTaskExecutor.class);
    private EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> envStatic;

    @Before
    public void setUp() {
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
    }

    @After
    public void tearDown() {
        envStatic.close();
    }

    @Test
    public void testRunOneCycle() throws LoadException,
            MetaNotFoundException, AnalysisException, LabelAlreadyUsedException, BeginTransactionException {
        KafkaRoutineLoadJob kafkaRoutineLoadJob1 = Mockito.mock(KafkaRoutineLoadJob.class);
        KafkaRoutineLoadJob routineLoadJob = Mockito.mock(KafkaRoutineLoadJob.class);
        Mockito.mock(RoutineLoadDesc.class);
        Mockito.mock(GlobalTransactionMgr.class);
        Mockito.mock(BackendService.Client.class);

        try (MockedStatic<ClientPool> clientPoolStatic = Mockito.mockStatic(ClientPool.class)) {
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

            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(routineLoadManager.getClusterIdleSlotNum()).thenReturn(1);
            Mockito.when(routineLoadManager.checkTaskInJob(Mockito.any(RoutineLoadTaskInfo.class))).thenReturn(true);
            Mockito.when(kafkaRoutineLoadJob1.getDbId()).thenReturn(1L);
            Mockito.when(kafkaRoutineLoadJob1.getTableId()).thenReturn(1L);
            Mockito.when(kafkaRoutineLoadJob1.getName()).thenReturn("");
            Mockito.when(routineLoadManager.getMinTaskBeId(Mockito.anyString())).thenReturn(beId);
            Mockito.when(routineLoadManager.getJob(Mockito.anyLong())).thenReturn(kafkaRoutineLoadJob1);

            RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
            Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
            routineLoadTaskScheduler.runAfterCatalogReady();
        }
    }

    @Test
    public void testSubmitTaskFailurePausesJob() {
        assertSubmitTaskFailurePausesJob("network error");
        assertSubmitTaskFailurePausesJob("failed to submit task. error code: TOO_MANY_TASKS");
        assertSubmitTaskFailurePausesJob("MEM_LIMIT_EXCEEDED");
    }

    private void assertSubmitTaskFailurePausesJob(String errorMsg) {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, errorMsg);

        Assert.assertEquals(-1L, routineLoadTaskInfo.getBeId());
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        Assert.assertEquals(InternalErrorCode.CREATE_TASKS_ERR, routineLoadJob.getPauseReason().getCode());
        Assert.assertEquals("failed to submit task: " + errorMsg, routineLoadJob.getPauseReason().getMsg());
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assert.assertTrue(routineLoadTaskInfoList.isEmpty());

        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    @Test
    public void testSubmitTaskFailureSkipsPauseWhenTaskRemoved() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", Lists.newArrayList());
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertNull(routineLoadJob.getPauseReason());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertTrue(needScheduleTasksQueue.isEmpty());
    }

    @Test
    public void testSubmitTaskFailureSkipsPauseWhenJobPaused() {
        ConcurrentMap<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();
        partitionIdToOffset.put(1, 100L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        routineLoadTaskInfo.setBeId(100L);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList",
                Lists.newArrayList(routineLoadTaskInfo));
        Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.invoke(routineLoadTaskScheduler, "handleSubmitTaskFailure",
                routineLoadTaskInfo, "network error");

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        LinkedBlockingDeque<RoutineLoadTaskInfo> needScheduleTasksQueue =
                Deencapsulation.getField(routineLoadTaskScheduler, "needScheduleTasksQueue");
        Assert.assertTrue(needScheduleTasksQueue.isEmpty());
    }
}
