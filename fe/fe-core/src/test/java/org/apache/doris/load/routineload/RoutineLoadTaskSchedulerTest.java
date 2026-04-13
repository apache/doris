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
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RoutineLoadTaskSchedulerTest {

    private RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
    private Env env = Mockito.mock(Env.class);
    private AgentTaskExecutor agentTaskExecutor = Mockito.mock(AgentTaskExecutor.class);
    private MockedStatic<Env> envStatic;

    @Before
    public void setUp() {
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
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
}
