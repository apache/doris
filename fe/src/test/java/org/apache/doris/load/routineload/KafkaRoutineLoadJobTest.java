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
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.LabelAlreadyExistsException;
import org.apache.doris.transaction.TransactionState;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaRoutineLoadJobTest {

    private static final int DEFAULT_TASK_TIMEOUT_SECONDS = 10;

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Test
    public void testBeNumMin(@Mocked KafkaConsumer kafkaConsumer,
                             @Injectable PartitionInfo partitionInfo1,
                             @Injectable PartitionInfo partitionInfo2,
                             @Mocked Catalog catalog,
                             @Mocked SystemInfoService systemInfoService,
                             @Mocked Database database,
                             @Mocked RoutineLoadDesc routineLoadDesc) throws MetaNotFoundException {
        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        partitionInfoList.add(partitionInfo1);
        partitionInfoList.add(partitionInfo2);
        List<Long> beIds = Lists.newArrayList(1L);

        String clusterName = "clusterA";

        new Expectations() {
            {
                kafkaConsumer.partitionsFor(anyString, (Duration) any);
                result = partitionInfoList;
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
                Catalog.getCurrentCatalog();
                result = catalog;
                catalog.getDb(anyLong);
                result = database;
                database.getClusterName();
                result = clusterName;
                systemInfoService.getClusterBackendIds(clusterName, true);
                result = beIds;
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob =
                new KafkaRoutineLoadJob("1", "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc ,3, 0,
                                        "", "", null);
        Assert.assertEquals(1, kafkaRoutineLoadJob.calculateCurrentConcurrentTaskNum());
    }


    @Test
    public void testDivideRoutineLoadJob(@Injectable GlobalTransactionMgr globalTransactionMgr,
                                         @Mocked Catalog catalog,
                                         @Injectable RoutineLoadManager routineLoadManager,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws BeginTransactionException, LabelAlreadyExistsException, AnalysisException {

        new Expectations(){
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob =
                new KafkaRoutineLoadJob("1", "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc , 3, 0,
                                        "", "", null);

        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, anyString, anyString,
                                                      TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, (KafkaRoutineLoadJob) any);
                result = 0L;
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
            }
        };

        Deencapsulation.setField(kafkaRoutineLoadJob, "kafkaPartitions", Arrays.asList(1, 4, 6));

        kafkaRoutineLoadJob.divideRoutineLoadJob(2);

        List<RoutineLoadTaskInfo> result = kafkaRoutineLoadJob.getNeedSchedulerTaskInfoList();
        Assert.assertEquals(2, result.size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : result) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(1));
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(6));
            } else if (kafkaTaskInfo.getPartitions().size() == 1) {
                Assert.assertTrue(kafkaTaskInfo.getPartitions().contains(4));
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void testProcessTimeOutTasks(@Injectable GlobalTransactionMgr globalTransactionMgr,
                                        @Mocked Catalog catalog,
                                        @Injectable RoutineLoadManager routineLoadManager,
                                        @Mocked RoutineLoadDesc routineLoadDesc)
            throws AnalysisException, LabelAlreadyExistsException,
            BeginTransactionException {

        new Expectations(){
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob("1", "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc ,3, 0,
                                        "", "", null);
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, anyString, anyString,
                                                      TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, routineLoadJob);
                result = 0L;
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo("1", "1");
        kafkaTaskInfo.addKafkaPartition(100);
        kafkaTaskInfo.setLoadStartTimeMs(System.currentTimeMillis() - DEFAULT_TASK_TIMEOUT_SECONDS * 60 * 1000);
        routineLoadTaskInfoList.add(kafkaTaskInfo);

        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        new MockUp<SystemIdGenerator>() {
            @Mock
            public long getNextId() {
                return 2L;
            }
        };

        new Expectations() {
            {
                routineLoadManager.getJob("1");
                result = routineLoadJob;
            }
        };


        routineLoadJob.processTimeoutTasks();
        new Verifications() {
            {
                List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                        Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
                Assert.assertNotEquals("1", idToRoutineLoadTask.get(0).getId());
                Assert.assertEquals(1, idToRoutineLoadTask.size());
                List<RoutineLoadTaskInfo> needSchedulerTask =
                        Deencapsulation.getField(routineLoadJob, "needSchedulerTaskInfoList");
                Assert.assertEquals(1, needSchedulerTask.size());
                Assert.assertEquals(100, (int) ((KafkaTaskInfo) (needSchedulerTask.get(0)))
                        .getPartitions().get(0));
            }
        };
    }
}
