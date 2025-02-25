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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.ImportSequenceStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelName labelName = new LabelName(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String serverAddress = "http://127.0.0.1:8080";
    private String kafkaPartitionString = "1,2,3";

    private PartitionNames partitionNames;

    private Separator columnSeparator = new Separator(",");

    private ImportSequenceStmt sequenceStmt = new ImportSequenceStmt("source_sequence");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        MockedAuth.mockedConnectContext(connectContext, "root", "192.168.1.1");

        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNames(false, partitionNameList);
    }

    @Test
    public void testRoutineLoadTaskConcurrentNum(@Injectable PartitionInfo partitionInfo1,
                             @Injectable PartitionInfo partitionInfo2,
                             @Mocked Env env,
                             @Mocked SystemInfoService systemInfoService,
                             @Mocked Database database,
                             @Mocked RoutineLoadDesc routineLoadDesc) throws MetaNotFoundException {
        List<Integer> partitionList1 = Lists.newArrayList(1, 2);
        List<Integer> partitionList2 = Lists.newArrayList(1, 2, 3);
        List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3, 4);
        List<Integer> partitionList4 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        List<Long> beIds1 = Lists.newArrayList(1L);
        List<Long> beIds2 = Lists.newArrayList(1L, 2L, 3L, 4L);

        new Expectations() {
            {
                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = beIds1;
                systemInfoService.getAllBackendIds(true);
                result = beIds2;
                minTimes = 0;
            }
        };
        Config.max_routine_load_task_concurrent_num = 6;
        // 2 partitions, 1 be
        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList1);
        Assert.assertEquals(2, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 3 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList2);
        Assert.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 4 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList3);
        Assert.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 7 partitions, 4 be
        routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", partitionList4);
        Assert.assertEquals(6, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }


    @Test
    public void testDivideRoutineLoadJob(@Injectable RoutineLoadManager routineLoadManager,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws UserException {

        Env env = Deencapsulation.newInstance(Env.class);

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);

        new Expectations(env) {
            {
                env.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.setField(env, "routineLoadTaskScheduler", routineLoadTaskScheduler);

        Deencapsulation.setField(routineLoadJob, "currentKafkaPartitions", Arrays.asList(1, 4, 6));

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
                                        @Injectable RoutineLoadManager routineLoadManager,
                                        @Mocked RoutineLoadDesc routineLoadDesc)
            throws AnalysisException, LabelAlreadyUsedException,
            BeginTransactionException {

        Env env = Deencapsulation.newInstance(Env.class);

        RoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                        1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        long maxBatchIntervalS = 10;
        Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", maxBatchIntervalS);
        new Expectations() {
            {
                env.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
        partitionIdsToOffset.put(100, 0L);
        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L,
                maxBatchIntervalS * 2 * 1000, partitionIdsToOffset, false, -1, false);
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
    public void testFromCreateStmt(@Mocked Env env,
                                   @Injectable Database database,
            @Injectable OlapTable table) throws UserException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, null, partitionNames, null,
                LoadTask.MergeType.APPEND, sequenceStmt.getSequenceColName());
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        List<PartitionInfo> kafkaPartitionInfoList = Lists.newArrayList();
        for (String s : kafkaPartitionString.split(",")) {
            partitionIdToOffset.add(Pair.of(Integer.valueOf(s), 0L));
            PartitionInfo partitionInfo = new PartitionInfo(topicName, Integer.valueOf(s), null, null, null);
            kafkaPartitionInfoList.add(partitionInfo);
        }
        KafkaDataSourceProperties dsProperties = new KafkaDataSourceProperties(null);
        dsProperties.setKafkaPartitionOffsets(partitionIdToOffset);
        Deencapsulation.setField(dsProperties, "brokerList", serverAddress);
        Deencapsulation.setField(dsProperties, "topic", topicName);
        Deencapsulation.setField(createRoutineLoadStmt, "dataSourceProperties", dsProperties);

        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                database.getTableNullable(tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.getType();
                minTimes = 0;
                result = Table.TableType.OLAP;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                    Map<String, String> convertedCustomProperties) throws UserException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Pair<Integer, Long>> getRealOffsets(String brokerList, String topic,
                                                             Map<String, String> convertedCustomProperties,
                                                             List<Pair<Integer, Long>> offsetFlags)
                                                             throws LoadException {
                List<Pair<Integer, Long>> pairList = new ArrayList<>();
                pairList.add(Pair.of(1, 0L));
                pairList.add(Pair.of(2, 0L));
                pairList.add(Pair.of(3, 0L));
                return pairList;
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
        Assert.assertEquals(sequenceStmt.getSequenceColName(), kafkaRoutineLoadJob.getSequenceCol());
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties,
                                                                                LoadTask.MergeType.APPEND, "");
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }
}
