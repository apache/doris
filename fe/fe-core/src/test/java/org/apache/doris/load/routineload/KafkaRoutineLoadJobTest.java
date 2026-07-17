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

import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaTaskInfo;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(KafkaRoutineLoadJobTest.class);

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String serverAddress = "http://127.0.0.1:8080";
    private String kafkaPartitionString = "1,2,3";

    private PartitionNamesInfo partitionNames;

    private Separator columnSeparator = new Separator(",");

    private String sequenceColumnName = "source_sequence";

    ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    TResourceInfo tResourceInfo = Mockito.mock(TResourceInfo.class);

    private MockedStatic<ConnectContext> connectContextStatic;

    @Before
    public void init() {
        connectContextStatic = MockedAuth.mockedConnectContext(connectContext, "root", "192.168.1.1");

        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNamesInfo(false, partitionNameList);
    }

    @After
    public void tearDown() {
        if (connectContextStatic != null) {
            connectContextStatic.close();
        }
    }

    @Test
    public void testRoutineLoadTaskConcurrentNum() throws MetaNotFoundException {
        Config.max_routine_load_task_concurrent_num = 6;

        List<Integer> partitionList1 = Lists.newArrayList(1, 2);
        List<Integer> partitionList2 = Lists.newArrayList(1, 2, 3);
        List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3, 4);
        List<Integer> partitionList4 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);

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
    public void testDivideRoutineLoadJob() throws UserException {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = Mockito.mock(RoutineLoadTaskScheduler.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(routineLoadTaskScheduler);

            RoutineLoadJob routineLoadJob =
                    new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                            1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);

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
    }

    @Test
    public void testUpdateLagRefreshesLatestOffsetCache() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 10L);
        partitionIdToOffset.put(2, 20L);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));

        try (MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
            kafkaUtilStatic.when(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                    Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"), Mockito.anyMap(), Mockito.anyList()))
                    .thenReturn(Lists.newArrayList(Pair.of(1, 15L), Pair.of(2, 30L)));

            routineLoadJob.updateLag();

            Assert.assertEquals(15L, routineLoadJob.totalLag().longValue());
        }
    }

    @Test
    public void testModifyPropertiesHonorsExplicitUniqueKeyUpdateModePrecedence() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);
        Deencapsulation.setField(routineLoadJob, "isPartialUpdate", false);

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPSERT");
        jobProperties.put(CreateRoutineLoadInfo.PARTIAL_COLUMNS, "true");

        Deencapsulation.invoke(routineLoadJob, "modifyPropertiesInternal", jobProperties,
                new KafkaDataSourceProperties(Maps.newHashMap()));

        Assert.assertEquals(TUniqueKeyUpdateMode.UPSERT, routineLoadJob.getUniqueKeyUpdateMode());
        Assert.assertFalse(routineLoadJob.isFixedPartialUpdate());
    }

    @Test
    public void testModifyPropertiesRevalidatesWhileHoldingWriteLock() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = Mockito.spy(new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN));
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Maps.newHashMap());

        Mockito.doAnswer(invocation -> {
            ReentrantReadWriteLock lock = Deencapsulation.getField(routineLoadJob, "lock");
            Assert.assertTrue(lock.isWriteLockedByCurrentThread());
            throw new DdlException("stop after validation");
        }).when(routineLoadJob).validateAlterJobPropertiesForMutation(command);

        try {
            routineLoadJob.modifyProperties(command);
            Assert.fail("Expected mutation-time validation to fail");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("stop after validation"));
        }
    }

    @Test
    public void testModifyTargetTableValidatesAndLogsUnderMetadataLocks() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = Mockito.spy(new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN));
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "uniqueKeyUpdateMode", TUniqueKeyUpdateMode.UPSERT);

        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.hasTargetTable()).thenReturn(true);
        Mockito.when(command.getTargetTableId()).thenReturn(2L);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Maps.newHashMap());

        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable targetTable = Mockito.mock(OlapTable.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(database);
        Mockito.when(database.getTableNullable(2L)).thenReturn(targetTable);

        AtomicBoolean databaseLocked = new AtomicBoolean(false);
        AtomicBoolean tableLocked = new AtomicBoolean(false);
        AtomicBoolean targetValidated = new AtomicBoolean(false);
        Mockito.doAnswer(invocation -> {
            databaseLocked.set(true);
            return null;
        }).when(database).readLock();
        Mockito.doAnswer(invocation -> {
            databaseLocked.set(false);
            return null;
        }).when(database).readUnlock();
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(databaseLocked.get());
            tableLocked.set(true);
            return null;
        }).when(targetTable).readLock();
        Mockito.doAnswer(invocation -> {
            tableLocked.set(false);
            return null;
        }).when(targetTable).readUnlock();
        Mockito.doAnswer(invocation -> {
            ReentrantReadWriteLock lock = Deencapsulation.getField(routineLoadJob, "lock");
            Assert.assertTrue(lock.isWriteLockedByCurrentThread());
            Assert.assertTrue(databaseLocked.get());
            Assert.assertTrue(tableLocked.get());
            targetValidated.set(true);
            return null;
        }).when(routineLoadJob).unprotectedValidateTargetTable(Mockito.eq(database), Mockito.eq(targetTable),
                Mockito.anyMap(), Mockito.eq(TUniqueKeyUpdateMode.UPSERT));
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(databaseLocked.get());
            Assert.assertTrue(tableLocked.get());
            Assert.assertEquals(2L, routineLoadJob.getTableId());
            return null;
        }).when(editLog).logAlterRoutineLoadJob(Mockito.any(AlterRoutineLoadJobOperationLog.class));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            routineLoadJob.modifyProperties(command);
        }

        Assert.assertTrue(targetValidated.get());
        Assert.assertFalse(databaseLocked.get());
        Assert.assertFalse(tableLocked.get());
        Assert.assertEquals(2L, routineLoadJob.getTableId());
        Mockito.verify(editLog).logAlterRoutineLoadJob(Mockito.any(AlterRoutineLoadJobOperationLog.class));
    }

    @Test
    public void testModifyPropertiesKeepsKafkaPropertiesWhenFileConversionFails() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Map<String, String> originalCustomProperties = Maps.newHashMap();
        originalCustomProperties.put("client.id", "original-client");
        Map<String, String> originalConvertedProperties = Maps.newHashMap(originalCustomProperties);
        Deencapsulation.setField(routineLoadJob, "customProperties", originalCustomProperties);
        Deencapsulation.setField(routineLoadJob, "convertedCustomProperties", originalConvertedProperties);

        Map<String, String> alteredSourceProperties = Maps.newHashMap();
        alteredSourceProperties.put("property.ssl.ca.location", "FILE:missing.pem");
        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(alteredSourceProperties);
        AlterRoutineLoadCommand command = mockAlterCommand(dataSourceProperties);
        Env env = Mockito.mock(Env.class);
        SmallFileMgr smallFileMgr = Mockito.mock(SmallFileMgr.class);
        Mockito.when(env.getSmallFileMgr()).thenReturn(smallFileMgr);
        Mockito.when(smallFileMgr.getSmallFile(1L, KafkaRoutineLoadJob.KAFKA_FILE_CATALOG,
                "missing.pem", true)).thenThrow(new DdlException("missing file"));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            try {
                routineLoadJob.modifyProperties(command);
                Assert.fail("Expected missing small file to reject ALTER");
            } catch (DdlException e) {
                Assert.assertTrue(e.getMessage().contains("missing file"));
            }
        }

        Assert.assertEquals(Maps.newHashMap(ImmutableMap.of("client.id", "original-client")),
                Deencapsulation.getField(routineLoadJob, "customProperties"));
        Assert.assertEquals(Maps.newHashMap(ImmutableMap.of("client.id", "original-client")),
                routineLoadJob.getConvertedCustomProperties());
    }

    @Test
    public void testModifyPropertiesKeepsKafkaPropertiesWhenPartitionValidationFails() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "progress",
                new KafkaProgress(Maps.newHashMap(ImmutableMap.of(1, 10L))));
        Deencapsulation.setField(routineLoadJob, "customProperties",
                Maps.newHashMap(ImmutableMap.of("client.id", "original-client")));
        Deencapsulation.setField(routineLoadJob, "convertedCustomProperties",
                Maps.newHashMap(ImmutableMap.of("client.id", "original-client")));

        Map<String, String> alteredSourceProperties = Maps.newHashMap();
        alteredSourceProperties.put("property.client.id", "new-client");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "2");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "100");
        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(alteredSourceProperties);

        try {
            routineLoadJob.modifyProperties(mockAlterCommand(dataSourceProperties));
            Assert.fail("Expected unknown partition to reject ALTER");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("2"));
        }

        Assert.assertEquals("original-client",
                Deencapsulation.<Map<String, String>>getField(routineLoadJob, "customProperties").get("client.id"));
        Assert.assertEquals("original-client", routineLoadJob.getConvertedCustomProperties().get("client.id"));
    }

    @Test
    public void testModifyTopicRejectsPartitionOutsidePinnedSet() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        KafkaProgress progress = new KafkaProgress(Maps.newHashMap(ImmutableMap.of(1, 10L)));
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1));

        Map<String, String> alteredSourceProperties = Maps.newHashMap();
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), "topic2");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "2");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "100");
        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(alteredSourceProperties);

        DdlException exception = Assert.assertThrows(DdlException.class,
                () -> routineLoadJob.modifyProperties(mockAlterCommand(dataSourceProperties)));
        Assert.assertTrue(exception.getMessage().contains("2"));
        Assert.assertEquals("topic1", routineLoadJob.getTopic());
        Assert.assertSame(progress, routineLoadJob.getProgress());
        Assert.assertEquals(Lists.newArrayList(1),
                Deencapsulation.getField(routineLoadJob, "customKafkaPartitions"));
    }

    @Test
    public void testModifyPropertiesResolvesOffsetsWithEffectiveKafkaConfiguration() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "old-broker:9092", "old-topic", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Map<String, String> alteredSourceProperties = Maps.newHashMap();
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), "new-broker:9092");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), "new-topic");
        alteredSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "7");
        alteredSourceProperties.put("property." + KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName(),
                "2026-01-01 00:00:00");
        alteredSourceProperties.put("property.client.id", "new-client");
        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(alteredSourceProperties);
        AlterRoutineLoadCommand command = mockAlterCommand(dataSourceProperties);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));

        String originalCloudUniqueId = Config.cloud_unique_id;
        String originalDeployMode = Config.deploy_mode;
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            kafkaUtilStatic.when(() -> KafkaUtil.getOffsetsForTimes(Mockito.eq("new-broker:9092"),
                    Mockito.eq("new-topic"), Mockito.anyMap(), Mockito.anyList())).thenAnswer(invocation -> {
                        Map<String, String> effectiveCustomProperties = invocation.getArgument(2);
                        Assert.assertEquals("new-client", effectiveCustomProperties.get("client.id"));
                        Assert.assertFalse(effectiveCustomProperties
                                .containsKey(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName()));
                        Assert.assertFalse(effectiveCustomProperties
                                .containsKey(KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName()));
                        return Lists.newArrayList(Pair.of(7, 700L));
                    });

            routineLoadJob.modifyProperties(command);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
            Config.deploy_mode = originalDeployMode;
        }

        Assert.assertEquals("new-broker:9092", routineLoadJob.getBrokerList());
        Assert.assertEquals("new-topic", routineLoadJob.getTopic());
        Assert.assertEquals(Long.valueOf(700L), ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(7));
        Assert.assertEquals("new-client", routineLoadJob.getCustomProperties().get("property.client.id"));
    }

    @Test
    public void testModifyCustomKafkaPropertiesDoesNotResetCloudProgress() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        KafkaProgress progress = new KafkaProgress(Maps.newHashMap(ImmutableMap.of(1, 123L)));
        Deencapsulation.setField(routineLoadJob, "progress", progress);
        Deencapsulation.setField(routineLoadJob, "customProperties", Maps.newHashMap(
                ImmutableMap.of(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName(), KafkaProgress.OFFSET_BEGINNING)));
        Deencapsulation.setField(routineLoadJob, "kafkaDefaultOffSet", KafkaProgress.OFFSET_BEGINNING);

        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(
                Maps.newHashMap(ImmutableMap.of("property.client.id", "new-client")));
        Assert.assertFalse(dataSourceProperties.getCustomKafkaProperties()
                .containsKey(KafkaConfiguration.KAFKA_DEFAULT_OFFSETS.getName()));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        MetaServiceProxy metaServiceProxy = Mockito.mock(MetaServiceProxy.class);

        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test-cloud";
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MetaServiceProxy> metaServiceProxyStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            metaServiceProxyStatic.when(MetaServiceProxy::getInstance).thenReturn(metaServiceProxy);

            routineLoadJob.modifyProperties(mockAlterCommand(dataSourceProperties));
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }

        Mockito.verifyNoInteractions(metaServiceProxy);
        Assert.assertSame(progress, routineLoadJob.getProgress());
        Assert.assertEquals(Long.valueOf(123L), ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        Assert.assertEquals(KafkaProgress.OFFSET_BEGINNING,
                routineLoadJob.getCustomProperties().get("property.kafka_default_offsets"));
    }

    @Test
    public void testAlterRejectsEmptyKafkaDefaultOffset() {
        Map<String, String> alteredSourceProperties = Maps.newHashMap();
        alteredSourceProperties.put("property.kafka_default_offsets", "");

        Assert.assertThrows(UserException.class,
                () -> analyzedAlterDataSourceProperties(alteredSourceProperties));
    }

    @Test
    public void testModifyKafkaTopicResetsCloudProgressWithoutOffsets() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        Deencapsulation.setField(routineLoadJob, "progress",
                new KafkaProgress(Maps.newHashMap(ImmutableMap.of(1, 123L))));

        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(
                Maps.newHashMap(ImmutableMap.of(KafkaConfiguration.KAFKA_TOPIC.getName(), "topic2")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        MetaServiceProxy metaServiceProxy = Mockito.mock(MetaServiceProxy.class);
        Cloud.ResetRLProgressResponse response = Cloud.ResetRLProgressResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                .build();
        Mockito.when(metaServiceProxy.resetRLProgress(Mockito.any())).thenReturn(response);

        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test-cloud";
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<MetaServiceProxy> metaServiceProxyStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            metaServiceProxyStatic.when(MetaServiceProxy::getInstance).thenReturn(metaServiceProxy);

            routineLoadJob.modifyProperties(mockAlterCommand(dataSourceProperties));
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }

        Mockito.verify(metaServiceProxy).resetRLProgress(Mockito.argThat(
                request -> request.getPartitionToOffsetMap().isEmpty()));
        Assert.assertEquals("topic2", routineLoadJob.getTopic());
        Assert.assertTrue(((KafkaProgress) routineLoadJob.getProgress()).getPartitionOffsetPairs(false).isEmpty());
    }

    @Test
    public void testUpdateLagRebuildsConvertedPropertiesAfterReplay() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Deencapsulation.setField(routineLoadJob, "customKafkaPartitions", Lists.newArrayList(1));

        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("security.protocol", "SASL_PLAINTEXT");
        customProperties.put("sasl.mechanism", "PLAIN");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);
        Deencapsulation.setField(routineLoadJob, "convertedCustomProperties", Maps.newHashMap());

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 10L);
        Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(partitionIdToOffset));

        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            kafkaUtilStatic.when(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                    Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"),
                    Mockito.<Map<String, String>>argThat(properties ->
                            "SASL_PLAINTEXT".equals(properties.get("security.protocol"))
                                    && "PLAIN".equals(properties.get("sasl.mechanism"))),
                    Mockito.argThat(partitions -> partitions.size() == 1 && partitions.contains(1))))
                    .thenReturn(Lists.newArrayList(Pair.of(1, 15L)));

            routineLoadJob.updateLag();

            Assert.assertEquals(5L, routineLoadJob.totalLag().longValue());
            kafkaUtilStatic.verify(() -> KafkaUtil.getLatestOffsets(Mockito.eq(1L), Mockito.any(UUID.class),
                    Mockito.eq("127.0.0.1:9020"), Mockito.eq("topic1"),
                    Mockito.<Map<String, String>>argThat(properties ->
                            "SASL_PLAINTEXT".equals(properties.get("security.protocol"))
                                    && "PLAIN".equals(properties.get("sasl.mechanism"))),
                    Mockito.argThat(partitions -> partitions.size() == 1 && partitions.contains(1))));
        }
    }

    @Test
    public void testUpdateProgressWarnsWhenReadCommittedTaskHasZeroRowsAndLag() throws UserException {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("isolation.level", "read_committed");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

        Map<Integer, Long> cachedPartitionWithLatestOffsets = Maps.newHashMap();
        cachedPartitionWithLatestOffsets.put(1, 20L);
        Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets",
                cachedPartitionWithLatestOffsets);

        Map<Integer, Long> taskProgress = Maps.newHashMap();
        taskProgress.put(1, 10L);
        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        Deencapsulation.setField(attachment, "progress", new KafkaProgress(taskProgress));

        Deencapsulation.invoke(routineLoadJob, "updateProgress", attachment);

        String otherMsg = Deencapsulation.getField(routineLoadJob, "otherMsg");
        Assert.assertTrue(otherMsg.contains("some records may be in uncommitted transactions"));
    }

    @Test
    public void testDisplayCustomPropertiesMasksKafkaSecrets() {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("security.protocol", "SASL_PLAINTEXT");
        customProperties.put("sasl.username", "doris");
        customProperties.put("sasl.password", "plain_secret");
        customProperties.put("sasl.jaas.config", "username=\"doris\" password=\"jaas_secret\"");
        customProperties.put("sasl.oauthbearer.client.secret", "oauth_client_secret");
        customProperties.put("sasl.oauthbearer.client.credentials.client.secret", "oauth_alias_secret");
        customProperties.put("sasl.oauthbearer.assertion.private.key.pem", "oauth_private_key_pem");
        customProperties.put("sasl.oauthbearer.assertion.private.key.passphrase", "oauth_private_key_passphrase");
        customProperties.put("ssl.keystore.password", "keystore_secret");
        customProperties.put("ssl.keystore.key", "keystore_key_secret");
        customProperties.put("ssl.key.pem", "key_pem_secret");
        customProperties.put(KafkaConfiguration.AWS_ACCESS_KEY, "aws_access_key");
        customProperties.put("aws.secret_key", "aws_secret");
        customProperties.put("aws.session_key", "aws_session_secret");
        customProperties.put("password", "bare_password_secret");
        customProperties.put("secret_key", "bare_secret_key");
        customProperties.put("session_token", "bare_session_token");
        Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

        String customPropertiesJson = routineLoadJob.customPropertiesJsonToString();
        Map<String, String> showCreateCustomProperties = routineLoadJob.getCustomProperties();

        Assert.assertFalse(customPropertiesJson.contains("plain_secret"));
        Assert.assertFalse(customPropertiesJson.contains("jaas_secret"));
        Assert.assertFalse(customPropertiesJson.contains("oauth_client_secret"));
        Assert.assertFalse(customPropertiesJson.contains("oauth_alias_secret"));
        Assert.assertFalse(customPropertiesJson.contains("oauth_private_key_pem"));
        Assert.assertFalse(customPropertiesJson.contains("oauth_private_key_passphrase"));
        Assert.assertFalse(customPropertiesJson.contains("keystore_secret"));
        Assert.assertFalse(customPropertiesJson.contains("keystore_key_secret"));
        Assert.assertFalse(customPropertiesJson.contains("key_pem_secret"));
        Assert.assertFalse(customPropertiesJson.contains("aws_access_key"));
        Assert.assertFalse(customPropertiesJson.contains("aws_secret"));
        Assert.assertFalse(customPropertiesJson.contains("aws_session_secret"));
        Assert.assertFalse(customPropertiesJson.contains("bare_password_secret"));
        Assert.assertFalse(customPropertiesJson.contains("bare_secret_key"));
        Assert.assertFalse(customPropertiesJson.contains("bare_session_token"));
        Assert.assertTrue(customPropertiesJson.contains("\"sasl.password\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"sasl.jaas.config\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"sasl.oauthbearer.client.secret\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains(
                "\"sasl.oauthbearer.client.credentials.client.secret\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"sasl.oauthbearer.assertion.private.key.pem\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains(
                "\"sasl.oauthbearer.assertion.private.key.passphrase\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"ssl.keystore.password\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"ssl.keystore.key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"ssl.key.pem\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.access_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.secret_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"aws.session_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"password\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"secret_key\":\"******\""));
        Assert.assertTrue(customPropertiesJson.contains("\"session_token\":\"******\""));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.sasl.password"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.sasl.jaas.config"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.sasl.oauthbearer.client.secret"));
        Assert.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.client.credentials.client.secret"));
        Assert.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.assertion.private.key.pem"));
        Assert.assertEquals("******",
                showCreateCustomProperties.get("property.sasl.oauthbearer.assertion.private.key.passphrase"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.ssl.keystore.password"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.ssl.keystore.key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.ssl.key.pem"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.access_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.secret_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.aws.session_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.password"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.secret_key"));
        Assert.assertEquals("******", showCreateCustomProperties.get("property.session_token"));
        Assert.assertEquals("doris", showCreateCustomProperties.get("property.sasl.username"));
        Assert.assertEquals("plain_secret", customProperties.get("sasl.password"));
    }

    @Test
    public void testReadCommittedZeroRowsWithLagDelaysNextTask() throws UserException {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);

            KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                    1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            Mockito.when(routineLoadManager.getJob(1L)).thenReturn(routineLoadJob);

            Map<String, String> customProperties = Maps.newHashMap();
            customProperties.put("isolation.level", "read_committed");
            Deencapsulation.setField(routineLoadJob, "customProperties", customProperties);

            Map<Integer, Long> cachedPartitionWithLatestOffsets = Maps.newHashMap();
            cachedPartitionWithLatestOffsets.put(1, 20L);
            Deencapsulation.setField(routineLoadJob, "cachedPartitionWithLatestOffsets",
                    cachedPartitionWithLatestOffsets);

            Map<Integer, Long> taskProgress = Maps.newHashMap();
            taskProgress.put(1, 10L);
            Deencapsulation.setField(routineLoadJob, "progress", new KafkaProgress(taskProgress));

            KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                    taskProgress, false, 1000, false);
            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
            routineLoadTaskInfoList.add(kafkaTaskInfo);
            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

            RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
            Deencapsulation.setField(attachment, "progress", new KafkaProgress(taskProgress));
            Deencapsulation.setField(attachment, "taskExecutionTimeMs",
                    routineLoadJob.getMaxBatchIntervalS() * 1000);

            kafkaTaskInfo.handleTaskByTxnCommitAttachment(attachment);

            Assert.assertFalse(kafkaTaskInfo.getIsEof());
            Assert.assertTrue(kafkaTaskInfo.needDedalySchedule());

            RoutineLoadTaskInfo newTask = Deencapsulation.invoke(routineLoadJob,
                    "unprotectRenewTask", kafkaTaskInfo, false);
            Assert.assertTrue(newTask.needDedalySchedule());
        }
    }

    @Test
    public void testProcessTimeOutTasks() throws Exception {
        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Env env = Mockito.mock(Env.class);
        RoutineLoadTaskScheduler routineLoadTaskScheduler = Mockito.mock(RoutineLoadTaskScheduler.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);
            Mockito.when(env.getRoutineLoadTaskScheduler()).thenReturn(routineLoadTaskScheduler);

            RoutineLoadJob routineLoadJob =
                    new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                            1L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
            long maxBatchIntervalS = 10;
            Deencapsulation.setField(routineLoadJob, "maxBatchIntervalS", maxBatchIntervalS);

            List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
            Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
            partitionIdsToOffset.put(100, 0L);
            KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo(new UUID(1, 1), 1L,
                    maxBatchIntervalS * 2 * 1000, partitionIdsToOffset, false, -1, false);
            kafkaTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);
            routineLoadTaskInfoList.add(kafkaTaskInfo);

            Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

            routineLoadJob.processTimeoutTasks();

            List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                    Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
            Assert.assertNotEquals("1", idToRoutineLoadTask.get(0).getId());
            Assert.assertEquals(1, idToRoutineLoadTask.size());
        }
    }

    @Test
    public void testFromCreateStmt() throws UserException {
        Env env = Mockito.mock(Env.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafkaUtilStatic = Mockito.mockStatic(KafkaUtil.class)) {

            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);

            long dbId = 1L;
            long tableId = 2L;

            Mockito.doReturn(database).when(internalCatalog).getDbOrDdlException(dbName);
            Mockito.doReturn(database).when(internalCatalog).getDbOrAnalysisException(dbName);
            Mockito.doReturn(table).when(database).getOlapTableOrDdlException(tableNameString);
            Mockito.doReturn(table).when(database).getTableOrAnalysisException(tableNameString);
            Mockito.when(database.getId()).thenReturn(dbId);
            Mockito.when(table.getId()).thenReturn(tableId);
            Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
            Mockito.doReturn(Mockito.mock(Partition.class)).when(table)
                    .getPartition(Mockito.anyString(), Mockito.anyBoolean());

            kafkaUtilStatic.when(() -> KafkaUtil.getAllKafkaPartitions(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                    .thenReturn(Lists.newArrayList(1, 2, 3));

            kafkaUtilStatic.when(() -> KafkaUtil.getRealOffsets(
                    Mockito.anyString(), Mockito.anyString(), Mockito.anyMap(), Mockito.anyList()))
                    .thenAnswer(invocation -> {
                        List<Pair<Integer, Long>> pairList = new ArrayList<>();
                        pairList.add(Pair.of(1, 0L));
                        pairList.add(Pair.of(2, 0L));
                        pairList.add(Pair.of(3, 0L));
                        return pairList;
                    });

            CreateRoutineLoadInfo createRoutineLoadInfo = initCreateRoutineLoadInfo();
            createRoutineLoadInfo.validate(connectContext);
            RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, null, partitionNames, null,
                    LoadTask.MergeType.APPEND, sequenceColumnName);
            Deencapsulation.setField(createRoutineLoadInfo, "routineLoadDesc", routineLoadDesc);
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
            Deencapsulation.setField(createRoutineLoadInfo, "dataSourceProperties", dsProperties);

            KafkaRoutineLoadJob kafkaRoutineLoadJob = KafkaRoutineLoadJob.fromCreateInfo(createRoutineLoadInfo, connectContext);
            Assert.assertEquals(jobName, kafkaRoutineLoadJob.getName());
            Assert.assertEquals(dbId, kafkaRoutineLoadJob.getDbId());
            Assert.assertEquals(tableId, kafkaRoutineLoadJob.getTableId());
            Assert.assertEquals(serverAddress, Deencapsulation.getField(kafkaRoutineLoadJob, "brokerList"));
            Assert.assertEquals(topicName, Deencapsulation.getField(kafkaRoutineLoadJob, "topic"));
            List<Integer> kafkaPartitionResult = Deencapsulation.getField(kafkaRoutineLoadJob, "customKafkaPartitions");
            Assert.assertEquals(kafkaPartitionString, Joiner.on(",").join(kafkaPartitionResult));
            Assert.assertEquals(sequenceColumnName, kafkaRoutineLoadJob.getSequenceCol());
        }
    }

    @Test
    public void testReplayModifyPropertiesWithDataSourceSwitchesTargetTableInCloudMode() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", 1L,
                101L, "127.0.0.1:9020", "topic1", UserIdentity.ADMIN);
        Map<Integer, Long> partitionToOffset = Maps.newHashMap();
        partitionToOffset.put(1, 123L);
        KafkaProgress progress = new KafkaProgress(partitionToOffset);
        Deencapsulation.setField(routineLoadJob, "progress", progress);

        KafkaDataSourceProperties dataSourceProperties = analyzedAlterDataSourceProperties(
                Maps.newHashMap(ImmutableMap.of("property.client.id", "replayed-client")));
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(1L,
                Maps.newHashMap(), dataSourceProperties, 202L);

        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "replay-cloud";
        try {
            routineLoadJob.replayModifyProperties(log);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }

        Assert.assertEquals(202L, routineLoadJob.getTableId());
        Assert.assertSame(progress, routineLoadJob.getProgress());
        Assert.assertEquals(Long.valueOf(123L), ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        Assert.assertEquals("replayed-client", routineLoadJob.getCustomProperties().get("property.client.id"));
    }

    private KafkaDataSourceProperties analyzedAlterDataSourceProperties(Map<String, String> properties)
            throws UserException {
        KafkaDataSourceProperties dataSourceProperties = new KafkaDataSourceProperties(properties);
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone("UTC");
        dataSourceProperties.analyze();
        return dataSourceProperties;
    }

    private AlterRoutineLoadCommand mockAlterCommand(KafkaDataSourceProperties dataSourceProperties) {
        AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
        Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Maps.newHashMap());
        Mockito.when(command.getDataSourceProperties()).thenReturn(dataSourceProperties);
        return command;
    }

    private CreateRoutineLoadInfo initCreateRoutineLoadInfo() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);

        LoadSeparator loadSeparator = new LoadSeparator(",");
        Map<String, LoadProperty> loadPropertyMap = new HashMap<>();
        loadPropertyMap.put(loadSeparator.getClass().getName(), loadSeparator);
        CreateRoutineLoadInfo createRoutineLoadInfo = new CreateRoutineLoadInfo(labelNameInfo, tableNameString,
                loadPropertyMap, properties, typeName, customProperties, LoadTask.MergeType.APPEND, "");
        return createRoutineLoadInfo;
    }
}
