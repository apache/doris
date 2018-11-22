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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
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
    public void testRunOneCycle(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob1,
                                @Injectable KafkaRoutineLoadJob routineLoadJob) throws LoadException {
        long beId = 100L;

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo("1", "1");
        routineLoadTaskInfo1.addKafkaPartition(1);
        routineLoadTaskInfo1.addKafkaPartition(2);
        routineLoadTaskInfoList.add(routineLoadTaskInfo1);


        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        kafkaProgress.setPartitionIdToOffset(partitionIdToOffset);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        new Expectations() {
            {
                Catalog.getInstance();
                result = catalog;
                catalog.getRoutineLoadInstance();
                result = routineLoadManager;
                Catalog.getCurrentCatalog();
                result = catalog;
                catalog.getNextId();
                result = 2L;

                routineLoadManager.getClusterIdleSlotNum();
                result = 3;
                routineLoadManager.getNeedSchedulerRoutineLoadTasks();
                result = routineLoadTaskInfoList;

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
                routineLoadManager.getJob(anyString);
                result = kafkaRoutineLoadJob1;
            }
        };

        KafkaRoutineLoadTask kafkaRoutineLoadTask = new KafkaRoutineLoadTask(kafkaRoutineLoadJob1.getResourceInfo(),
                beId, kafkaRoutineLoadJob1.getDbId(), kafkaRoutineLoadJob1.getTableId(),
                0L, 0L, 0L, kafkaRoutineLoadJob1.getColumns(), kafkaRoutineLoadJob1.getWhere(),
                kafkaRoutineLoadJob1.getColumnSeparator(),
                (KafkaTaskInfo) routineLoadTaskInfo1,
                kafkaRoutineLoadJob1.getProgress());

        new Expectations() {
            {
                kafkaRoutineLoadJob1.createTask((RoutineLoadTaskInfo) any, anyLong);
                result = kafkaRoutineLoadTask;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        routineLoadTaskScheduler.runOneCycle();

        new Verifications() {
            {
                AgentTask routineLoadTask =
                        AgentTaskQueue.getTask(beId, TTaskType.STREAM_LOAD, 2L);

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
