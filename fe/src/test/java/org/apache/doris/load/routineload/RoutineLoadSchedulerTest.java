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
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RoutineLoadSchedulerTest {

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Test
    public void testNormalRunOneCycle(@Mocked KafkaConsumer consumer,
                                      @Mocked Catalog catalog,
                                      @Injectable RoutineLoadManager routineLoadManager,
                                      @Injectable SystemInfoService systemInfoService,
                                      @Injectable Database database,
                                      @Injectable RoutineLoadDesc routineLoadDesc)
            throws LoadException, MetaNotFoundException {
        String clusterName = "cluster1";
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        List<Integer> partitions = Lists.newArrayList();
        partitions.add(100);
        partitions.add(200);
        partitions.add(300);

        new Expectations(){
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc ,3, 0,
                                        "", "", new KafkaProgress());
        Deencapsulation.setField(routineLoadJob,"state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
        routineLoadJobList.add(routineLoadJob);

        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", partitions);
        Deencapsulation.setField(routineLoadJob, "desireTaskConcurrentNum", 3);
        Deencapsulation.setField(routineLoadJob, "consumer", consumer);

        new Expectations() {
            {
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
                routineLoadManager.getRoutineLoadJobByState(RoutineLoadJob.JobState.NEED_SCHEDULE);
                result = routineLoadJobList;
                catalog.getDb(anyLong);
                result = database;
                systemInfoService.getBackendIds( true);
                result = beIds;
                routineLoadManager.getSizeOfIdToRoutineLoadTask();
                result = 1;
                routineLoadManager.getTotalMaxConcurrentTaskNum();
                result = 10;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();
        Deencapsulation.setField(routineLoadScheduler, "routineLoadManager", routineLoadManager);
        routineLoadScheduler.runOneCycle();

        Assert.assertEquals(2, routineLoadJob.getNeedScheduleTaskInfoList().size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadJob.getNeedScheduleTaskInfoList()) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(100));
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(300));
            } else {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(200));
            }
        }
    }


    public void functionTest(@Mocked Catalog catalog,
                             @Mocked SystemInfoService systemInfoService,
                             @Injectable Database database) throws DdlException, InterruptedException {
        new Expectations(){
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob("test", 1L, 1L, "10.74.167.16:8092", "test");
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob);

        List<Long> backendIds = new ArrayList<>();
        backendIds.add(1L);

        new Expectations(){
            {
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
                catalog.getDb(anyLong);
                result = database;
                systemInfoService.getBackendIds(true);
                result = backendIds;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        routineLoadTaskScheduler.setInterval(5000);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(routineLoadScheduler);
        executorService.submit(routineLoadTaskScheduler);



        KafkaRoutineLoadJob kafkaRoutineLoadJob1 = new KafkaRoutineLoadJob("test_custom_partition", 1L, 1L, "10.74.167.16:8092", "test_1");
        List<Integer> customKafkaPartitions = new ArrayList<>();
        customKafkaPartitions.add(2);
        Deencapsulation.setField(kafkaRoutineLoadJob1, "customKafkaPartitions", customKafkaPartitions);
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob1);

        Thread.sleep(10000);
    }
}
