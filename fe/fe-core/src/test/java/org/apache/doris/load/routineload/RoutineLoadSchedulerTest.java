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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;


public class RoutineLoadSchedulerTest {

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Test
    public void testNormalRunOneCycle(@Mocked Env env, @Mocked InternalCatalog catalog,
            @Injectable RoutineLoadManager routineLoadManager, @Injectable SystemInfoService systemInfoService,
            @Injectable Database database, @Injectable RoutineLoadDesc routineLoadDesc,
            @Mocked StreamLoadPlanner planner, @Injectable OlapTable olapTable)
            throws LoadException, MetaNotFoundException {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        List<Integer> partitions = Lists.newArrayList();
        partitions.add(100);
        partitions.add(200);
        partitions.add(300);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.setField(env, "routineLoadTaskScheduler", routineLoadTaskScheduler);

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L,
                "xxx", "test", UserIdentity.ADMIN);
        Deencapsulation.setField(kafkaRoutineLoadJob, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
        routineLoadJobList.add(kafkaRoutineLoadJob);

        Deencapsulation.setField(kafkaRoutineLoadJob, "customKafkaPartitions", partitions);
        Deencapsulation.setField(kafkaRoutineLoadJob, "desireTaskConcurrentNum", 3);

        new Expectations() {
            {
                env.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
                routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
                minTimes = 0;
                result = routineLoadJobList;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(1L);
                minTimes = 0;
                result = olapTable;
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = beIds;
                routineLoadManager.getSizeOfIdToRoutineLoadTask();
                minTimes = 0;
                result = 1;
                routineLoadManager.getTotalMaxConcurrentTaskNum();
                minTimes = 0;
                result = 10;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();
        Deencapsulation.setField(routineLoadScheduler, "routineLoadManager", routineLoadManager);
        routineLoadScheduler.runAfterCatalogReady();

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(kafkaRoutineLoadJob, "routineLoadTaskInfoList");
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(100));
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(300));
            } else {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(200));
            }
        }
    }

    public void functionTest(@Mocked Env env, @Mocked InternalCatalog catalog,
            @Mocked SystemInfoService systemInfoService, @Injectable Database database)
            throws UserException, InterruptedException {
        new Expectations() {
            {
                minTimes = 0;
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L,
                "10.74.167.16:8092", "test", UserIdentity.ADMIN);
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db", "table");

        List<Long> backendIds = new ArrayList<>();
        backendIds.add(1L);

        new Expectations() {
            {
                env.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = backendIds;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        routineLoadTaskScheduler.setInterval(5000);

        ExecutorService executorService = ThreadPoolManager.newDaemonFixedThreadPool(2, 2, "routine-load-task-scheduler", false);
        executorService.submit(routineLoadScheduler);
        executorService.submit(routineLoadTaskScheduler);

        KafkaRoutineLoadJob kafkaRoutineLoadJob1 = new KafkaRoutineLoadJob(1L, "test_custom_partition",
                1L, 1L, "xxx", "test_1", UserIdentity.ADMIN);
        List<Integer> customKafkaPartitions = new ArrayList<>();
        customKafkaPartitions.add(2);
        Deencapsulation.setField(kafkaRoutineLoadJob1, "customKafkaPartitions", customKafkaPartitions);
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob1, "db", "table");

        Thread.sleep(10000);
    }
}
