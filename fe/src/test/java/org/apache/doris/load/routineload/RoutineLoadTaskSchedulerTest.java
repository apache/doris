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

import mockit.Verifications;
import org.apache.doris.analysis.LoadColumnsInfo;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.transaction.BeginTransactionException;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.doris.transaction.GlobalTransactionMgr;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

import static mockit.Deencapsulation.invoke;

public class RoutineLoadTaskSchedulerTest {

    @Mocked
    private RoutineLoadManager routineLoadManager;
    @Mocked
    private Catalog catalog;
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

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

        Queue<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = Queues.newLinkedBlockingQueue();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), 1l, "default_cluster", partitionIdToOffset);
        routineLoadTaskInfoQueue.add(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        new Expectations() {
            {
                Catalog.getInstance();
                result = catalog;
                catalog.getRoutineLoadManager();
                result = routineLoadManager;

                routineLoadManager.getClusterIdleSlotNum();
                result = 1;
                routineLoadManager.checkTaskInJob((UUID) any);
                result = true;

                kafkaRoutineLoadJob1.getDbId();
                result = 1L;
                kafkaRoutineLoadJob1.getTableId();
                result = 1L;
                kafkaRoutineLoadJob1.getName();
                result = "";
                routineLoadManager.getMinTaskBeId(anyString);
                result = beId;
                routineLoadManager.getJob(anyLong);
                result = kafkaRoutineLoadJob1;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
        routineLoadTaskScheduler.runOneCycle();
    }
}
