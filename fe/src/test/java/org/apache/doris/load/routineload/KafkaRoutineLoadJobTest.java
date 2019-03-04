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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.LoadException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaRoutineLoadJobTest {

    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);

    private static final int DEFAULT_TASK_TIMEOUT_SECONDS = 10;

    private String jobName = "job1";
    private String dbName = "db1";
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String serverAddress = "http://127.0.0.1:8080";
    private String kafkaPartitionString = "1,2,3";

    private PartitionNames partitionNames;

    private ColumnSeparator columnSeparator = new ColumnSeparator(",");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        partitionNames = new PartitionNames(partitionNameString);
    }

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
                systemInfoService.getBackendIds(true);
                result = beIds;
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc, 3, 0,
                                        "", "", new KafkaProgress());
        Deencapsulation.setField(kafkaRoutineLoadJob, "consumer", kafkaConsumer);
        Assert.assertEquals(1, kafkaRoutineLoadJob.calculateCurrentConcurrentTaskNum());
    }


    @Test
    public void testDivideRoutineLoadJob(@Mocked KafkaConsumer kafkaConsumer,
                                         @Injectable GlobalTransactionMgr globalTransactionMgr,
                                         @Mocked Catalog catalog,
                                         @Injectable RoutineLoadManager routineLoadManager,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws BeginTransactionException, LabelAlreadyUsedException, AnalysisException {

        new Expectations() {
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc, 3, 0,
                                        "", "", null);

        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, anyString, anyLong, anyString,
                                                      TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, (KafkaRoutineLoadJob) any);
                result = 0L;
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
            }
        };

        Deencapsulation.setField(kafkaRoutineLoadJob, "currentKafkaPartitions", Arrays.asList(1, 4, 6));
        Deencapsulation.setField(kafkaRoutineLoadJob, "consumer", kafkaConsumer);

        kafkaRoutineLoadJob.divideRoutineLoadJob(2);

        List<RoutineLoadTaskInfo> result = kafkaRoutineLoadJob.getNeedScheduleTaskInfoList();
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
    public void testProcessTimeOutTasks(@Mocked KafkaConsumer kafkaConsumer,
                                        @Injectable GlobalTransactionMgr globalTransactionMgr,
                                        @Mocked Catalog catalog,
                                        @Injectable RoutineLoadManager routineLoadManager,
                                        @Mocked RoutineLoadDesc routineLoadDesc)
            throws AnalysisException, LabelAlreadyUsedException,
            BeginTransactionException {

        new Expectations() {
            {
                connectContext.toResourceCtx();
                result = tResourceInfo;
            }
        };

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                                        1L, routineLoadDesc, 3, 0,
                                        "", "", null);
        new Expectations() {
            {
                globalTransactionMgr.beginTransaction(anyLong, anyString, anyLong, anyString,
                                                      TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, routineLoadJob);
                result = 0L;
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L);
        kafkaTaskInfo.addKafkaPartition(100);
        kafkaTaskInfo.setLoadStartTimeMs(System.currentTimeMillis() - DEFAULT_TASK_TIMEOUT_SECONDS * 60 * 1000);
        routineLoadTaskInfoList.add(kafkaTaskInfo);

        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "consumer", kafkaConsumer);

        new MockUp<SystemIdGenerator>() {
            @Mock
            public long getNextId() {
                return 2L;
            }
        };

        new Expectations() {
            {
                routineLoadManager.getJob(1L);
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
                List<RoutineLoadTaskInfo> needScheduleTask =
                        Deencapsulation.getField(routineLoadJob, "needScheduleTaskInfoList");
                Assert.assertEquals(1, needScheduleTask.size());
                Assert.assertEquals(100, (int) ((KafkaTaskInfo) (needScheduleTask.get(0)))
                        .getPartitions().get(0));
            }
        };
    }

    @Test
    public void testFromCreateStmtWithErrorPartition(@Mocked Catalog catalog,
                                                     @Injectable Database database,
                                                     @Injectable OlapTable table) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, partitionNames.getPartitionNames());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = database;
                database.getTable(tableNameString);
                result = table;
                table.getPartition("p1");
                result = null;
                table.getType();
                result = Table.TableType.OLAP;
            }
        };

        try {
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmtWithErrorTable(@Mocked Catalog catalog,
                                                 @Injectable Database database) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, partitionNames.getPartitionNames());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = database;
                database.getTable(tableNameString);
                result = null;
            }
        };

        try {
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmt(@Mocked Catalog catalog,
                                   @Mocked KafkaConsumer kafkaConsumer,
                                   @Injectable Database database,
                                   @Injectable OlapTable table) throws LoadException, AnalysisException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, partitionNames.getPartitionNames());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Integer> kafkaIntegerList = Lists.newArrayList();
        List<PartitionInfo> kafkaPartitionInfoList = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            kafkaIntegerList.add(Integer.valueOf(s));
            PartitionInfo partitionInfo = new PartitionInfo(topicName, Integer.valueOf(s), null, null, null);
            kafkaPartitionInfoList.add(partitionInfo);
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitions", kafkaIntegerList);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaEndpoint", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1l;
        long tableId = 2L;

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = database;
                database.getTable(tableNameString);
                result = table;
                database.getId();
                result = dbId;
                table.getId();
                result = tableId;
                table.getType();
                result = Table.TableType.OLAP;
                kafkaConsumer.partitionsFor(anyString, (Duration) any);
                result = kafkaPartitionInfoList;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assert.assertEquals(jobName, kafkaRoutineLoadJob.getName());
        Assert.assertEquals(dbId, kafkaRoutineLoadJob.getDbId());
        Assert.assertEquals(tableId, kafkaRoutineLoadJob.getTableId());
        Assert.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "serverAddress"));
        Assert.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
        List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
        Assert.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
        Assert.assertEquals(routineLoadDesc, kafkaRoutineLoadJob.getRoutineLoadDesc());
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        TableName tableName = new TableName(dbName, tableNameString);
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_ENDPOINT_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(jobName, tableName,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties);
        return createRoutineLoadStmt;
    }
}
