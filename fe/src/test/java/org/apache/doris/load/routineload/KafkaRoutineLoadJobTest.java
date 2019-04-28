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

import com.sleepycat.je.tree.IN;
import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.SystemIdGenerator;
import org.apache.doris.common.UserException;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
import java.util.Queue;
import java.util.UUID;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;

public class KafkaRoutineLoadJobTest {

    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);

    private static final int DEFAULT_TASK_TIMEOUT_SECONDS = 10;

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelName labelName = new LabelName(dbName, jobName);
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
    @Mocked
    KafkaConsumer kafkaConsumer;

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
        List<Integer> partitionList = new ArrayList<>();
        partitionList.add(1);
        partitionList.add(2);
        List<Long> beIds = Lists.newArrayList(1L);

        String clusterName = "default";

        new Expectations() {
            {
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getClusterBackendIds(clusterName, true);
                result = beIds;
            }
        };

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", clusterName, 1L,
                                        1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "consumer", kafkaConsumer);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList);
        Assert.assertEquals(1, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }


    @Test
    public void testDivideRoutineLoadJob(@Mocked KafkaConsumer kafkaConsumer,
                                         @Injectable GlobalTransactionMgr globalTransactionMgr,
                                         @Mocked Catalog catalog,
                                         @Injectable RoutineLoadManager routineLoadManager,
                                         @Injectable RoutineLoadTaskScheduler routineLoadTaskScheduler,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws BeginTransactionException, LabelAlreadyUsedException, AnalysisException {

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", "default", 1L,
                                        1L, "127.0.0.1:9020", "topic1");

        new Expectations() {
            {
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
                catalog.getRoutineLoadTaskScheduler();
                result = routineLoadTaskScheduler;
            }
        };

        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Arrays.asList(1, 4, 6));
        Deencapsulation.setField(routineLoadJob, "consumer", kafkaConsumer);

        routineLoadJob.divideRoutineLoadJob(2);

        // todo(ml): assert
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assert.assertEquals(2, routineLoadTaskInfoList.size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            Assert.assertEquals(false, kafkaTaskInfo.isRunning());
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
            throws AnalysisException, LabelAlreadyUsedException,
            BeginTransactionException {

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", "default", 1L,
                                        1L, "127.0.0.1:9020", "topic1");
        long maxBatchIntervalS = 10;
        Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", maxBatchIntervalS);
        new Expectations() {
            {
                catalog.getRoutineLoadManager();
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
        partitionIdsToOffset.put(100, 0L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, "default_cluster", partitionIdsToOffset);
        kafkaTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);
        routineLoadTaskInfoList.add(kafkaTaskInfo);

        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        routineLoadJob.processTimeoutTasks();
        new Verifications() {
            {
                List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                        Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
                Assert.assertNotEquals("1", idToRoutineLoadTask.get(0).getId());
                Assert.assertEquals(1, idToRoutineLoadTask.size());
            }
        };
    }

    @Test
    public void testFromCreateStmtWithErrorTable(@Mocked Catalog catalog,
                                                 @Injectable Database database) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, partitionNames.getPartitionNames());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                database.getTable(tableNameString);
                result = null;
            }
        };

        try {
            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmt(@Mocked Catalog catalog,
                                   @Injectable Database database,
            @Injectable OlapTable table) throws UserException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, partitionNames.getPartitionNames());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        List<PartitionInfo> kafkaPartitionInfoList = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0l));
            PartitionInfo partitionInfo = new PartitionInfo(topicName, Integer.valueOf(s), null, null, null);
            kafkaPartitionInfoList.add(partitionInfo);
        }
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaBrokerList", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "kafkaTopic", topicName);
        long dbId = 1l;
        long tableId = 2L;

        new Expectations() {
            {
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
        Assert.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "brokerList"));
        Assert.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
        List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
        Assert.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
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
        customProperties.put(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties);
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }
}
