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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.nereids.trees.plans.commands.load.PauseRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.ResumeRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.StopRoutineLoadCommand;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TxnStateCallbackFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RoutineLoadManagerTest {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadManagerTest.class);

    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);

    private void mockSessionVariable(ConnectContext connectContext) {
        Mockito.when(connectContext.getSessionVariable()).thenReturn(VariableMgr.newSessionVariable());
    }

    @Test
    public void testCreateJobAuthDeny() {
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);

        String jobName = "job1";
        String dbName = "db1";
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, jobName);
        String tableNameString = "table1";
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();
        String topicName = "topic1";
        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        String serverAddress = "http://127.0.0.1:8080";
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        LoadSeparator loadSeparator = new LoadSeparator(",");
        Map<String, LoadProperty> loadPropertyMap = new HashMap<>();
        loadPropertyMap.put(loadSeparator.getClass().getName(), loadSeparator);
        CreateRoutineLoadInfo createRoutineLoadInfo = new CreateRoutineLoadInfo(labelNameInfo, tableNameString,
                loadPropertyMap, properties, typeName, customProperties, LoadTask.MergeType.APPEND, "");

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            Mockito.when(connectContext.getState()).thenReturn(new org.apache.doris.qe.QueryState());

            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            try {
                createRoutineLoadInfo.checkJobProperties();
                routineLoadManager.createRoutineLoadJob(createRoutineLoadInfo, connectContext);
                Assert.fail();
            } catch (LoadException | DdlException e) {
                Assert.fail();
            } catch (AnalysisException e) {
                LOG.info("Access deny");
            } catch (UserException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCreateWithSameName() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);

            String jobName = "job1";
            String topicName = "topic1";
            String serverAddress = "http://127.0.0.1:8080";
            KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                    serverAddress, topicName, UserIdentity.ADMIN);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();

            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            KafkaRoutineLoadJob kafkaRoutineLoadJobWithSameName = new KafkaRoutineLoadJob(1L, jobName,
                    1L, 1L, serverAddress, topicName, UserIdentity.ADMIN);
            routineLoadJobList.add(kafkaRoutineLoadJobWithSameName);
            nameToRoutineLoadJob.put(jobName, routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);

            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
            try {
                routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db", "table");
                Assert.fail();
            } catch (UserException e) {
                LOG.info(e.getMessage());
            }
        }
    }

    @Test
    public void testCreateWithSameNameOfStoppedJob() throws UserException {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);

            String jobName = "job1";
            String topicName = "topic1";
            String serverAddress = "http://127.0.0.1:8080";
            KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                    serverAddress, topicName, UserIdentity.ADMIN);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();

            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            KafkaRoutineLoadJob kafkaRoutineLoadJobWithSameName = new KafkaRoutineLoadJob(1L, jobName,
                    1L, 1L, serverAddress, topicName, UserIdentity.ADMIN);
            Deencapsulation.setField(kafkaRoutineLoadJobWithSameName, "state", RoutineLoadJob.JobState.STOPPED);
            routineLoadJobList.add(kafkaRoutineLoadJobWithSameName);
            nameToRoutineLoadJob.put(jobName, routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
            idToRoutineLoadJob.put(UUID.randomUUID().toString(), kafkaRoutineLoadJobWithSameName);

            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
            routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db", "table");

            Map<Long, Map<String, List<RoutineLoadJob>>> result =
                    Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
            Map<String, RoutineLoadJob> result1 = Deencapsulation.getField(routineLoadManager, "idToRoutineLoadJob");
            Assert.assertEquals(1, result.size());
            Assert.assertEquals(Long.valueOf(1L), result.keySet().iterator().next());
            Map<String, List<RoutineLoadJob>> resultNameToRoutineLoadJob = result.get(1L);
            Assert.assertEquals(jobName, resultNameToRoutineLoadJob.keySet().iterator().next());
            Assert.assertEquals(2, resultNameToRoutineLoadJob.values().iterator().next().size());
            Assert.assertEquals(2, result1.values().size());
        }
    }

    @Test
    public void testGetMinTaskBeId() throws LoadException {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);

        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(beIds);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
            idToRoutineLoadJob.put(1L, routineLoadJob);
            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
            beIdToConcurrentTaskMap.put(1L, 1);

            Mockito.when(routineLoadJob.getBeCurrentTasksNumMap()).thenReturn(beIdToConcurrentTaskMap);
            Mockito.when(routineLoadJob.getState()).thenReturn(RoutineLoadJob.JobState.RUNNING);

            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

            Assert.assertEquals(2L, routineLoadManager.getMinTaskBeId("default"));
        }
    }

    @Test
    public void testGetMinTaskBeIdWhileClusterDeleted() {
        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(null);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            try {
                routineLoadManager.getMinTaskBeId("default");
                Assert.fail();
            } catch (LoadException e) {
                // do nothing
            }
        }

    }

    @Test
    public void testGetMinTaskBeIdWhileNoSlot() {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);

        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
        beIdToConcurrentTaskMap.put(1L, 11);

        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(beIds);
        Mockito.when(routineLoadJob.getBeCurrentTasksNumMap()).thenReturn(beIdToConcurrentTaskMap);
        Mockito.when(routineLoadJob.getState()).thenReturn(RoutineLoadJob.JobState.RUNNING);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Config.max_routine_load_task_num_per_be = 0;
            Map<Long, RoutineLoadJob> routineLoadJobMap = Maps.newHashMap();
            routineLoadJobMap.put(1L, routineLoadJob);
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", routineLoadJobMap);

            try {
                Assert.assertEquals(-1, routineLoadManager.getMinTaskBeId("default"));
            } catch (LoadException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    @Test
    public void testGetTotalIdleTaskNum() {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);

        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(beIds);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
            idToRoutineLoadJob.put(1L, routineLoadJob);
            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
            beIdToConcurrentTaskMap.put(1L, 1);

            Mockito.when(routineLoadJob.getBeCurrentTasksNumMap()).thenReturn(beIdToConcurrentTaskMap);
            Mockito.when(routineLoadJob.getState()).thenReturn(RoutineLoadJob.JobState.RUNNING);

            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
            routineLoadManager.updateBeIdToMaxConcurrentTasks();
            Assert.assertEquals(Config.max_routine_load_task_num_per_be * 2 - 1,
                    routineLoadManager.getClusterIdleSlotNum());
        }
    }

    @Test
    public void testUpdateBeIdTaskMaps() {
        List<Long> oldBeIds = Lists.newArrayList();
        oldBeIds.add(1L);
        oldBeIds.add(2L);

        List<Long> newBeIds = Lists.newArrayList();
        newBeIds.add(1L);
        newBeIds.add(3L);

        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(oldBeIds).thenReturn(newBeIds);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            routineLoadManager.updateBeIdToMaxConcurrentTasks();
        }
    }

    @Test
    public void testGetJobByName() {
        RoutineLoadJob routineLoadJob1 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob2 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob3 = Mockito.mock(RoutineLoadJob.class);

        String jobName = "ilovedoris";
        List<RoutineLoadJob> routineLoadJobList1 = Lists.newArrayList();
        routineLoadJobList1.add(routineLoadJob1);
        routineLoadJobList1.add(routineLoadJob2);
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadList1 = Maps.newHashMap();
        nameToRoutineLoadList1.put(jobName, routineLoadJobList1);

        List<RoutineLoadJob> routineLoadJobList2 = Lists.newArrayList();
        routineLoadJobList2.add(routineLoadJob3);
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadList2 = Maps.newHashMap();
        nameToRoutineLoadList2.put(jobName, routineLoadJobList2);

        Map<String, Map<String, List<RoutineLoadJob>>> dbToNameRoutineLoadList = Maps.newHashMap();
        dbToNameRoutineLoadList.put("db1", nameToRoutineLoadList1);
        dbToNameRoutineLoadList.put("db2", nameToRoutineLoadList2);

        Mockito.when(routineLoadJob1.isFinal()).thenReturn(true);
        Mockito.when(routineLoadJob2.isFinal()).thenReturn(false);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameRoutineLoadList);
        List<RoutineLoadJob> result = routineLoadManager.getJobByName(jobName);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));

    }

    @Test
    public void testGetJob() throws MetaNotFoundException,
            PatternMatcherException {
        RoutineLoadJob routineLoadJob1 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob2 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob3 = Mockito.mock(RoutineLoadJob.class);

        Mockito.when(routineLoadJob1.isFinal()).thenReturn(true);
        Mockito.when(routineLoadJob2.isFinal()).thenReturn(false);
        Mockito.when(routineLoadJob3.isFinal()).thenReturn(true);
        Mockito.when(routineLoadJob1.getName()).thenReturn("routine_load_job_test1");
        Mockito.when(routineLoadJob2.getName()).thenReturn("routine_load_job");
        Mockito.when(routineLoadJob3.getName()).thenReturn("routine_load_job_test2");

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob1);
        idToRoutineLoadJob.put(2L, routineLoadJob2);
        idToRoutineLoadJob.put(3L, routineLoadJob3);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        List<RoutineLoadJob> result = routineLoadManager.getJob(null, null, true, null);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));

        PatternMatcher matcher = PatternMatcher.createMysqlPattern("%test%", true);
        result = routineLoadManager.getJob(null, null, true, matcher);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(routineLoadJob1, result.get(0));
        Assert.assertEquals(routineLoadJob3, result.get(1));
    }

    @Test
    public void testGetJobIncludeHistory() throws MetaNotFoundException {
        RoutineLoadJob routineLoadJob1 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob2 = Mockito.mock(RoutineLoadJob.class);
        RoutineLoadJob routineLoadJob3 = Mockito.mock(RoutineLoadJob.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            Mockito.when(routineLoadJob1.isFinal()).thenReturn(true);
            Mockito.when(routineLoadJob2.isFinal()).thenReturn(false);
            Mockito.when(routineLoadJob3.isFinal()).thenReturn(true);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable(Mockito.anyString())).thenReturn(database);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyString());
            Mockito.when(database.getId()).thenReturn(1L);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            routineLoadJobList.add(routineLoadJob1);
            routineLoadJobList.add(routineLoadJob2);
            routineLoadJobList.add(routineLoadJob3);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);
            List<RoutineLoadJob> result = routineLoadManager.getJob("", "", true, null);

            Assert.assertEquals(3, result.size());
            Assert.assertEquals(routineLoadJob2, result.get(0));
            Assert.assertEquals(routineLoadJob1, result.get(1));
            Assert.assertEquals(routineLoadJob3, result.get(2));
        }
    }

    @Test
    public void testPauseRoutineLoadJob() throws UserException {
        PauseRoutineLoadCommand pauseRoutineLoadCommand = Mockito.mock(PauseRoutineLoadCommand.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

            Mockito.when(pauseRoutineLoadCommand.getDbFullName()).thenReturn("");
            Mockito.when(pauseRoutineLoadCommand.getLabel()).thenReturn("");
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("")).thenReturn(database);
            Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(database);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyString());
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("");
            Mockito.when(database.getTableNullable(Mockito.anyLong())).thenReturn(tbl);
            Mockito.doReturn(tbl).when(database).getTableOrAnalysisException(Mockito.anyLong());
            Mockito.doReturn(tbl).when(database).getTableOrMetaException(Mockito.anyLong());
            Mockito.when(tbl.getName()).thenReturn("tbl");
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

            routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadCommand);

            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());

            for (int i = 0; i < 3; i++) {
                Deencapsulation.setField(routineLoadJob, "pauseReason",
                        new ErrorReason(InternalErrorCode.REPLICA_FEW_ERR, ""));
                try {
                    Thread.sleep(((long) Math.pow(2, i) * 10 * 1000L));
                } catch (InterruptedException e) {
                    throw new UserException("thread sleep failed");
                }
                routineLoadManager.updateRoutineLoadJob();
                Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
            }
            routineLoadManager.updateRoutineLoadJob();
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        }
    }

    @Test
    public void testResumeRoutineLoadJob() throws UserException {
        ResumeRoutineLoadCommand resumeRoutineLoadCommand = Mockito.mock(ResumeRoutineLoadCommand.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Mockito.when(resumeRoutineLoadCommand.getDbFullName()).thenReturn("");
            Mockito.when(resumeRoutineLoadCommand.getLabel()).thenReturn("");
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("")).thenReturn(database);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyString());
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("");
            Mockito.doReturn(tbl).when(database).getTableOrAnalysisException(Mockito.anyLong());
            Mockito.doReturn(tbl).when(database).getTableOrMetaException(Mockito.anyLong());
            Mockito.when(tbl.getName()).thenReturn("tbl");
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

            routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadCommand);

            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
        }
    }

    @Test
    public void testStopRoutineLoadJob() throws UserException {
        StopRoutineLoadCommand stopRoutineLoadCommand = Mockito.mock(StopRoutineLoadCommand.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            EditLog editLog = Mockito.mock(EditLog.class);
            GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Mockito.when(stopRoutineLoadCommand.getDbFullName()).thenReturn("");
            Mockito.when(stopRoutineLoadCommand.getLabel()).thenReturn("");
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("")).thenReturn(database);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyString());
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("");
            Mockito.doReturn(tbl).when(database).getTableOrAnalysisException(Mockito.anyLong());
            Mockito.doReturn(tbl).when(database).getTableOrMetaException(Mockito.anyLong());
            Mockito.when(tbl.getName()).thenReturn("tbl");
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

            routineLoadManager.stopRoutineLoadJob(stopRoutineLoadCommand);

            Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
        }
    }

    @Test
    public void testCheckBeToTask() throws UserException {
        Env env = Mockito.mock(Env.class);
        SystemInfoService localSystemInfoService = Mockito.mock(SystemInfoService.class);
        GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(localSystemInfoService);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            RoutineLoadManager routineLoadManager = Mockito.spy(new RoutineLoadManager());
            Mockito.doReturn(Lists.newArrayList()).when(routineLoadManager)
                    .getAvailableBackendIds(Mockito.anyLong());

            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testjob",
                    10000, 10001, "192.168.1.1:9090", "testtopic", UserIdentity.ADMIN);
            routineLoadManager.addRoutineLoadJob(job, "testdb", "testtable");
            Assert.assertEquals(-1L, routineLoadManager.getAvailableBeForTask(1L, 1L));
        }
    }

    @Test
    public void testCleanOldRoutineLoadJobs() {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
            idToRoutineLoadJob.put(1L, routineLoadJob);
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Mockito.when(routineLoadJob.isExpired()).thenReturn(true);
            Mockito.when(routineLoadJob.getDbId()).thenReturn(1L);
            Mockito.when(routineLoadJob.getName()).thenReturn("");
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            routineLoadManager.cleanOldRoutineLoadJobs();

            Assert.assertEquals(0, dbToNameToRoutineLoadJob.size());
            Assert.assertEquals(0, idToRoutineLoadJob.size());
        }
    }

    @Test
    public void testCleanOverLimitRoutineLoadJobs() {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
            idToRoutineLoadJob.put(1L, routineLoadJob);
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Mockito.when(routineLoadJob.getId()).thenReturn(1L);
            Mockito.when(routineLoadJob.isFinal()).thenReturn(true);
            Mockito.when(routineLoadJob.getDbId()).thenReturn(1L);
            Mockito.when(routineLoadJob.getName()).thenReturn("");
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            Config.label_num_threshold = 0;

            routineLoadManager.cleanOverLimitRoutineLoadJobs();
            Assert.assertEquals(0, dbToNameToRoutineLoadJob.size());
            Assert.assertEquals(0, idToRoutineLoadJob.size());
        }
    }

    @Test
    public void testGetBeIdConcurrentTaskMaps() {
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Integer> beIdToConcurrenTaskNum = Maps.newHashMap();
        beIdToConcurrenTaskNum.put(1L, 1);

        Mockito.when(routineLoadJob.getState()).thenReturn(RoutineLoadJob.JobState.RUNNING);
        Mockito.when(routineLoadJob.getBeCurrentTasksNumMap()).thenReturn(beIdToConcurrenTaskNum);

        Map<Long, Integer> result = Deencapsulation.invoke(routineLoadManager, "getBeCurrentTasksNumMap");
        Assert.assertEquals(1, (int) result.get(1L));

    }

    @Test
    public void testReplayRemoveOldRoutineLoad() {
        RoutineLoadOperation operation = Mockito.mock(RoutineLoadOperation.class);
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        Mockito.when(routineLoadJob.getName()).thenReturn("");
        Mockito.when(routineLoadJob.getDbId()).thenReturn(1L);
        Mockito.when(operation.getId()).thenReturn(1L);

        routineLoadManager.replayRemoveOldRoutineLoad(operation);
        Assert.assertEquals(0, idToRoutineLoadJob.size());
    }

    @Test
    public void testReplayChangeRoutineLoadJob() {
        RoutineLoadOperation operation = Mockito.mock(RoutineLoadOperation.class);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "name", "");
        Deencapsulation.setField(routineLoadJob, "dbId", 1L);
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        Mockito.when(operation.getId()).thenReturn(1L);
        Mockito.when(operation.getJobState()).thenReturn(RoutineLoadJob.JobState.PAUSED);

        routineLoadManager.replayChangeRoutineLoadJob(operation);
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAlterRoutineLoadJob() throws UserException {
        StopRoutineLoadCommand stopRoutineLoadCommand = Mockito.mock(StopRoutineLoadCommand.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            EditLog editLog = Mockito.mock(EditLog.class);
            GlobalTransactionMgrIface globalTxnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            TxnStateCallbackFactory callbackFactory = Mockito.mock(TxnStateCallbackFactory.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            envStatic.when(Env::getCurrentGlobalTransactionMgr).thenReturn(globalTxnMgr);
            Mockito.when(globalTxnMgr.getCallbackFactory()).thenReturn(callbackFactory);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
            routineLoadJobList.add(routineLoadJob);
            nameToRoutineLoadJob.put("", routineLoadJobList);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Mockito.when(stopRoutineLoadCommand.getDbFullName()).thenReturn("");
            Mockito.when(stopRoutineLoadCommand.getLabel()).thenReturn("");
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable("")).thenReturn(database);
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyString());
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("");
            Mockito.doReturn(tbl).when(database).getTableOrAnalysisException(Mockito.anyLong());
            Mockito.doReturn(tbl).when(database).getTableOrMetaException(Mockito.anyLong());
            Mockito.when(tbl.getName()).thenReturn("tbl");
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);

            routineLoadManager.stopRoutineLoadJob(stopRoutineLoadCommand);

            Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
        }
    }

    @Test
    public void testPauseAndResumeAllRoutineLoadJob() throws UserException {
        PauseRoutineLoadCommand pauseRoutineLoadCommand = Mockito.mock(PauseRoutineLoadCommand.class);
        ResumeRoutineLoadCommand resumeRoutineLoadCommand = Mockito.mock(ResumeRoutineLoadCommand.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table tbl = Mockito.mock(Table.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> ctxStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            ctxStatic.when(ConnectContext::get).thenReturn(connectContext);
            mockSessionVariable(connectContext);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            RoutineLoadManager routineLoadManager = new RoutineLoadManager();
            Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();

            List<RoutineLoadJob> routineLoadJobList1 = Lists.newArrayList();
            RoutineLoadJob routineLoadJob1 = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob1, "id", 1000L);
            routineLoadJobList1.add(routineLoadJob1);

            List<RoutineLoadJob> routineLoadJobList2 = Lists.newArrayList();
            RoutineLoadJob routineLoadJob2 = new KafkaRoutineLoadJob();
            Deencapsulation.setField(routineLoadJob2, "id", 1002L);
            routineLoadJobList2.add(routineLoadJob2);

            nameToRoutineLoadJob.put("job1", routineLoadJobList1);
            nameToRoutineLoadJob.put("job2", routineLoadJobList2);
            dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
            Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob1.getState());
            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob1.getState());

            Mockito.when(pauseRoutineLoadCommand.isAll()).thenReturn(true);
            Mockito.when(pauseRoutineLoadCommand.getDbFullName()).thenReturn("");
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.doReturn(database).when(catalog).getDbOrDdlException(Mockito.anyString());
            Mockito.doReturn(database).when(catalog).getDbOrMetaException(Mockito.anyLong());
            Mockito.when(database.getId()).thenReturn(1L);
            Mockito.when(database.getFullName()).thenReturn("");
            Mockito.doReturn(tbl).when(database).getTableOrAnalysisException(Mockito.anyLong());
            Mockito.doReturn(tbl).when(database).getTableOrMetaException(Mockito.anyLong());
            Mockito.when(tbl.getName()).thenReturn("tbl");
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                    Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(true);
            Mockito.when(resumeRoutineLoadCommand.isAll()).thenReturn(true);
            Mockito.when(resumeRoutineLoadCommand.getDbFullName()).thenReturn("");

            routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadCommand);
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob1.getState());
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob2.getState());

            routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadCommand);
            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob1.getState());
            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob2.getState());
        }
    }

    @Test
    public void testCalAutoResumeInterval() throws Exception {
        RoutineLoadJob jobRoutine = new KafkaRoutineLoadJob();

        Field maxBackOffField = ScheduleRule.class.getDeclaredField("MAX_BACK_OFF_TIME_SEC");
        maxBackOffField.setAccessible(true);
        long maxBackOffTimeSec = (long) maxBackOffField.get(null);

        Field backOffBasicField = ScheduleRule.class.getDeclaredField("BACK_OFF_BASIC_TIME_SEC");
        backOffBasicField.setAccessible(true);
        long backOffTimeSec = (long) backOffBasicField.get(null);

        jobRoutine.autoResumeCount = 0;
        long interval = ScheduleRule.calAutoResumeInterval(jobRoutine);
        Assert.assertEquals(Math.min((long) Math.pow(2, 0) * backOffTimeSec, maxBackOffTimeSec), interval);

        jobRoutine.autoResumeCount = 1;
        interval = ScheduleRule.calAutoResumeInterval(jobRoutine);
        Assert.assertEquals(Math.min((long) Math.pow(2, 1) * backOffTimeSec, maxBackOffTimeSec), interval);

        jobRoutine.autoResumeCount = 5;
        interval = ScheduleRule.calAutoResumeInterval(jobRoutine);
        Assert.assertEquals(maxBackOffTimeSec, interval);

        jobRoutine.autoResumeCount = 1000;
        interval = ScheduleRule.calAutoResumeInterval(jobRoutine);
        Assert.assertEquals(maxBackOffTimeSec, interval);
    }
}
