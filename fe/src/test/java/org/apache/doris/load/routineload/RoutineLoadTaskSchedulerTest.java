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

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Queue;

public class RoutineLoadTaskSchedulerTest {

    @Mocked
    private RoutineLoadManager routineLoadManager;
    @Mocked
    private Catalog catalog;
    @Mocked
    private AgentTaskExecutor agentTaskExecutor;

    @Test
    public void testRunOneCycle(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob1) {
        long beId = 100L;

        Queue<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = Queues.newLinkedBlockingQueue();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(1L);
        routineLoadTaskInfo1.addKafkaPartition(1);
        routineLoadTaskInfo1.addKafkaPartition(2);
        routineLoadTaskInfoQueue.add(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        kafkaProgress.setPartitionIdToOffset(partitionIdToOffset);

        new Expectations() {
            {
                Catalog.getInstance();
                result = catalog;
                catalog.getRoutineLoadInstance();
                result = routineLoadManager;

                routineLoadManager.getClusterIdleSlotNum();
                result = 3;
                routineLoadManager.getNeedSchedulerRoutineLoadTasks();
                result = routineLoadTaskInfoQueue;
                routineLoadManager.getIdToRoutineLoadTask();
                result = idToRoutineLoadTask;

                kafkaRoutineLoadJob1.getDbId();
                result = 1L;
                kafkaRoutineLoadJob1.getTableId();
                result = 1L;
                kafkaRoutineLoadJob1.getColumns();
                result = "columns";
                kafkaRoutineLoadJob1.getColumnSeparator();
                result = "";
                kafkaRoutineLoadJob1.getProgress();
                result = kafkaProgress;


                routineLoadManager.getMinTaskBeId();
                result = beId;
                routineLoadManager.getJobByTaskId(1L);
                result = kafkaRoutineLoadJob1;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        routineLoadTaskScheduler.runOneCycle();

        new Verifications() {
            {
                AgentTask routineLoadTask =
                        AgentTaskQueue.getTask(beId, TTaskType.STREAM_LOAD, routineLoadTaskInfo1.getSignature());

                Assert.assertEquals(beId, routineLoadTask.getBackendId());
                Assert.assertEquals(100L,
                        (long) ((KafkaRoutineLoadTask) routineLoadTask).getPartitionIdToOffset().get(1));
                Assert.assertEquals(200L,
                        (long) ((KafkaRoutineLoadTask) routineLoadTask).getPartitionIdToOffset().get(2));

                routineLoadManager.addNumOfConcurrentTasksByBeId(beId);
                times = 1;
            }
        };
    }
}
