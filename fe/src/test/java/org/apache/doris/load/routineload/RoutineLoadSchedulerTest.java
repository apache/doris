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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class RoutineLoadSchedulerTest {

    @Test
    public void testNormalRunOneCycle(@Mocked Catalog catalog,
                                      @Injectable RoutineLoadManager routineLoadManager,
                                      @Injectable SystemInfoService systemInfoService,
                                      @Injectable Database database)
            throws LoadException, MetaNotFoundException {

        String clusterName = "cluster1";
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        List<Integer> partitions = Lists.newArrayList();
        partitions.add(100);
        partitions.add(200);
        partitions.add(300);
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob("1", "kafka_routine_load_job", "miaoling", 1L,
                1L, "1L", "v1", "", "", 3,
                RoutineLoadJob.JobState.NEED_SCHEDULER, RoutineLoadJob.DataSourceType.KAFKA, 0, new TResourceInfo(),
                "", "");
        routineLoadJob.setState(RoutineLoadJob.JobState.NEED_SCHEDULER);
        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
        routineLoadJobList.add(routineLoadJob);

        Deencapsulation.setField(routineLoadJob, "kafkaPartitions", partitions);
        Deencapsulation.setField(routineLoadJob, "desireTaskConcurrentNum", 3);

        new Expectations() {
            {
                catalog.getRoutineLoadInstance();
                result = routineLoadManager;
                routineLoadManager.getRoutineLoadJobByState(RoutineLoadJob.JobState.NEED_SCHEDULER);
                result = routineLoadJobList;
                catalog.getDb(anyLong);
                result = database;
                database.getClusterName();
                result = clusterName;
                systemInfoService.getClusterBackendIds(clusterName, true);
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

        Assert.assertEquals(2, routineLoadJob.getNeedSchedulerTaskInfoList().size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadJob.getNeedSchedulerTaskInfoList()) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(100));
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(300));
            } else {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(200));
            }
        }
    }
}
