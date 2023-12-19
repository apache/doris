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
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.PauseRoutineLoadStmt;
import org.apache.doris.analysis.ResumeRoutineLoadStmt;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.StopRoutineLoadStmt;
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
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RoutineLoadManagerTest {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadManagerTest.class);

    @Mocked
    private SystemInfoService systemInfoService;

    @Test
    public void testAddJobByStmt(@Injectable AccessControllerManager accessManager,
            @Injectable TResourceInfo tResourceInfo,
            @Mocked ConnectContext connectContext,
            @Mocked Env env) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        List<ParseNode> loadPropertyList = new ArrayList<>();
        Separator columnSeparator = new Separator(",");
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();
        String topicName = "topic1";
        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        String serverAddress = "http://127.0.0.1:8080";
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties,
                                                                                LoadTask.MergeType.APPEND, "");
        createRoutineLoadStmt.setOrigStmt(new OriginStatement("dummy", 0));

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                serverAddress, topicName, UserIdentity.ADMIN);

        new MockUp<KafkaRoutineLoadJob>() {
            @Mock
            public KafkaRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) {
                return kafkaRoutineLoadJob;
            }
        };

        new Expectations() {
            {
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.createRoutineLoadJob(createRoutineLoadStmt);

        Map<String, RoutineLoadJob> idToRoutineLoadJob =
                Deencapsulation.getField(routineLoadManager, "idToRoutineLoadJob");
        Assert.assertEquals(1, idToRoutineLoadJob.size());
        RoutineLoadJob routineLoadJob = idToRoutineLoadJob.values().iterator().next();
        Assert.assertEquals(1L, routineLoadJob.getDbId());
        Assert.assertEquals(jobName, routineLoadJob.getName());
        Assert.assertEquals(1L, routineLoadJob.getTableId());
        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
        Assert.assertEquals(true, routineLoadJob instanceof KafkaRoutineLoadJob);

        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob =
                Deencapsulation.getField(routineLoadManager, "dbToNameToRoutineLoadJob");
        Assert.assertEquals(1, dbToNameToRoutineLoadJob.size());
        Assert.assertEquals(Long.valueOf(1L), dbToNameToRoutineLoadJob.keySet().iterator().next());
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(1L);
        Assert.assertEquals(jobName, nameToRoutineLoadJob.keySet().iterator().next());
        Assert.assertEquals(1, nameToRoutineLoadJob.values().size());
        Assert.assertEquals(routineLoadJob, nameToRoutineLoadJob.values().iterator().next().get(0));
    }

    @Test
    public void testCreateJobAuthDeny(@Injectable AccessControllerManager accessManager,
            @Injectable TResourceInfo tResourceInfo,
            @Mocked ConnectContext connectContext,
            @Mocked Env env) {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        List<ParseNode> loadPropertyList = new ArrayList<>();
        Separator columnSeparator = new Separator(",");
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();
        String topicName = "topic1";
        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        String serverAddress = "http://127.0.0.1:8080";
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties,
                                                                                LoadTask.MergeType.APPEND, "");
        createRoutineLoadStmt.setOrigStmt(new OriginStatement("dummy", 0));


        new Expectations() {
            {
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, PrivPredicate.LOAD);
                minTimes = 0;
                result = false;
            }
        };
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        try {
            routineLoadManager.createRoutineLoadJob(createRoutineLoadStmt);
            Assert.fail();
        } catch (LoadException | DdlException e) {
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info("Access deny");
        } catch (UserException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateWithSameName(@Mocked ConnectContext connectContext) {
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
        } catch (DdlException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCreateWithSameNameOfStoppedJob(@Mocked ConnectContext connectContext,
                                                   @Mocked Env env,
                                                   @Mocked EditLog editLog) throws DdlException {
        String jobName = "job1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, jobName, 1L, 1L,
                serverAddress, topicName, UserIdentity.ADMIN);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();

        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

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

    @Test
    public void testGetMinTaskBeId(@Injectable RoutineLoadJob routineLoadJob) throws LoadException {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        new Expectations() {
            {
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };

        new MockUp<Env>() {
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
        beIdToConcurrentTaskMap.put(1L, 1);

        new Expectations(routineLoadJob) {
            {
                routineLoadJob.getBeCurrentTasksNumMap();
                result = beIdToConcurrentTaskMap;
                routineLoadJob.getState();
                result = RoutineLoadJob.JobState.RUNNING;
            }
        };

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        Assert.assertEquals(2L, routineLoadManager.getMinTaskBeId("default"));
    }

    @Test
    public void testGetMinTaskBeIdWhileClusterDeleted() {
        new Expectations() {
            {
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = null;
            }
        };

        new MockUp<Env>() {
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        try {
            routineLoadManager.getMinTaskBeId("default");
            Assert.fail();
        } catch (LoadException e) {
            // do nothing
        }

    }

    @Test
    public void testGetMinTaskBeIdWhileNoSlot(@Injectable RoutineLoadJob routineLoadJob) {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
        beIdToConcurrentTaskMap.put(1L, 11);

        new Expectations() {
            {
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = beIds;
                routineLoadJob.getBeCurrentTasksNumMap();
                minTimes = 0;
                result = beIdToConcurrentTaskMap;
                routineLoadJob.getState();
                minTimes = 0;
                result = RoutineLoadJob.JobState.RUNNING;
            }
        };

        new MockUp<Env>() {
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

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

    @Test
    public void testGetTotalIdleTaskNum(@Injectable RoutineLoadJob routineLoadJob) {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        new Expectations() {
            {
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };

        new MockUp<Env>() {
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, Integer> beIdToConcurrentTaskMap = Maps.newHashMap();
        beIdToConcurrentTaskMap.put(1L, 1);

        new Expectations(routineLoadJob) {
            {
                routineLoadJob.getBeCurrentTasksNumMap();
                result = beIdToConcurrentTaskMap;
                routineLoadJob.getState();
                result = RoutineLoadJob.JobState.RUNNING;
            }
        };

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        routineLoadManager.updateBeIdToMaxConcurrentTasks();
        Assert.assertEquals(Config.max_routine_load_task_num_per_be * 2 - 1,
                routineLoadManager.getClusterIdleSlotNum());
    }

    @Test
    public void testUpdateBeIdTaskMaps() {
        List<Long> oldBeIds = Lists.newArrayList();
        oldBeIds.add(1L);
        oldBeIds.add(2L);

        List<Long> newBeIds = Lists.newArrayList();
        newBeIds.add(1L);
        newBeIds.add(3L);

        new Expectations() {
            {
                systemInfoService.getAllBackendIds(true);
                minTimes = 0;
                returns(oldBeIds, newBeIds);
            }
        };

        new MockUp<Env>() {
            SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.updateBeIdToMaxConcurrentTasks();
    }

    @Test
    public void testGetJobByName(@Injectable RoutineLoadJob routineLoadJob1,
                                 @Injectable RoutineLoadJob routineLoadJob2,
                                 @Injectable RoutineLoadJob routineLoadJob3) {
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

        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameRoutineLoadList);
        List<RoutineLoadJob> result = routineLoadManager.getJobByName(jobName);

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(routineLoadJob2, result.get(0));
        Assert.assertEquals(routineLoadJob1, result.get(1));
        Assert.assertEquals(routineLoadJob3, result.get(2));

    }

    @Test
    public void testGetJob(@Injectable RoutineLoadJob routineLoadJob1,
            @Injectable RoutineLoadJob routineLoadJob2,
            @Injectable RoutineLoadJob routineLoadJob3) throws MetaNotFoundException,
            PatternMatcherException {

        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob1.getName();
                minTimes = 0;
                result = "routine_load_job_test1";
                routineLoadJob2.getName();
                minTimes = 0;
                result = "routine_load_job";
                routineLoadJob3.getName();
                minTimes = 0;
                result = "routine_load_job_test2";
            }
        };

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
    public void testGetJobIncludeHistory(@Injectable RoutineLoadJob routineLoadJob1,
            @Injectable RoutineLoadJob routineLoadJob2, @Injectable RoutineLoadJob routineLoadJob3, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Mocked Database database)
            throws MetaNotFoundException {
        new Expectations() {
            {
                routineLoadJob1.isFinal();
                minTimes = 0;
                result = true;
                routineLoadJob2.isFinal();
                minTimes = 0;
                result = false;
                routineLoadJob3.isFinal();
                minTimes = 0;
                result = true;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

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

    @Test
    public void testPauseRoutineLoadJob(@Injectable PauseRoutineLoadStmt pauseRoutineLoadStmt, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Mocked Database database, @Mocked Table tbl,
            @Mocked AccessControllerManager accessManager,
            @Mocked ConnectContext connectContext) throws UserException {
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

        new Expectations() {
            {
                pauseRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                pauseRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
                database.getTableOrAnalysisException(anyLong);
                minTimes = 0;
                result = tbl;
                tbl.getName();
                minTimes = 0;
                result = "tbl";
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());

        // 第一次自动恢复
        for (int i = 0; i < 3; i++) {
            Deencapsulation.setField(routineLoadJob, "pauseReason",
                    new ErrorReason(InternalErrorCode.REPLICA_FEW_ERR, ""));
            routineLoadManager.updateRoutineLoadJob();
            Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
            Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
            boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
            Assert.assertEquals(autoResumeLock, false);
        }
        // 第四次自动恢复 就会锁定
        routineLoadManager.updateRoutineLoadJob();
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
        boolean autoResumeLock = Deencapsulation.getField(routineLoadJob, "autoResumeLock");
        Assert.assertEquals(autoResumeLock, true);
    }

    @Test
    public void testResumeRoutineLoadJob(@Injectable ResumeRoutineLoadStmt resumeRoutineLoadStmt, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Mocked Database database, @Mocked Table tbl,
            @Mocked AccessControllerManager accessManager,
            @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                resumeRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                resumeRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
                database.getTableOrAnalysisException(anyLong);
                minTimes = 0;
                result = tbl;
                tbl.getName();
                minTimes = 0;
                result = "tbl";
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testStopRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Mocked Database database, @Mocked Table tbl,
            @Mocked AccessControllerManager accessManager,
            @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                stopRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                stopRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
                database.getTableOrAnalysisException(anyLong);
                minTimes = 0;
                result = tbl;
                tbl.getName();
                minTimes = 0;
                result = "tbl";
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
    }

    @Test
    public void testCheckBeToTask(@Mocked Env env,
                                  @Mocked SystemInfoService systemInfoService) throws LoadException, DdlException {
        List<Long> beIdsInCluster = Lists.newArrayList();
        beIdsInCluster.add(1L);
        Map<Long, Integer> beIdToMaxConcurrentTasks = Maps.newHashMap();
        beIdToMaxConcurrentTasks.put(1L, 10);
        new Expectations() {
            {
                systemInfoService.selectBackendIdsByPolicy((BeSelectionPolicy) any, anyInt);
                minTimes = 0;
                result = beIdsInCluster;
            }
        };

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "testjob",
                10000, 10001, "192.168.1.1:9090", "testtopic", UserIdentity.ADMIN);
        routineLoadManager.addRoutineLoadJob(job, "testdb", "testtable");
        Config.max_routine_load_task_num_per_be = 10;
        Deencapsulation.setField(routineLoadManager, "beIdToMaxConcurrentTasks", beIdToMaxConcurrentTasks);
        Assert.assertEquals(1L, routineLoadManager.getAvailableBeForTask(1L, 1L));
    }

    @Test
    public void testCleanOldRoutineLoadJobs(@Injectable RoutineLoadJob routineLoadJob,
                                            @Mocked Env env,
                                            @Mocked EditLog editLog) {
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

        new Expectations() {
            {
                routineLoadJob.needRemove();
                minTimes = 0;
                result = true;
                routineLoadJob.getDbId();
                minTimes = 0;
                result = 1L;
                routineLoadJob.getName();
                minTimes = 0;
                result = "";
                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        routineLoadManager.cleanOldRoutineLoadJobs();

        Assert.assertEquals(0, dbToNameToRoutineLoadJob.size());
        Assert.assertEquals(0, idToRoutineLoadJob.size());
    }

    @Test
    public void testGetBeIdConcurrentTaskMaps(@Injectable RoutineLoadJob routineLoadJob) {
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadJob.put(1L, routineLoadJob);
        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);
        Map<Long, Integer> beIdToConcurrenTaskNum = Maps.newHashMap();
        beIdToConcurrenTaskNum.put(1L, 1);

        new Expectations() {
            {
                routineLoadJob.getState();
                minTimes = 0;
                result = RoutineLoadJob.JobState.RUNNING;
                routineLoadJob.getBeCurrentTasksNumMap();
                minTimes = 0;
                result = beIdToConcurrenTaskNum;
            }
        };

        Map<Long, Integer> result = Deencapsulation.invoke(routineLoadManager, "getBeCurrentTasksNumMap");
        Assert.assertEquals(1, (int) result.get(1L));

    }

    @Test
    public void testReplayRemoveOldRoutineLoad(@Injectable RoutineLoadOperation operation,
                                               @Injectable RoutineLoadJob routineLoadJob) {
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

        new Expectations() {
            {
                routineLoadJob.getName();
                minTimes = 0;
                result = "";
                routineLoadJob.getDbId();
                minTimes = 0;
                result = 1L;
                operation.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        routineLoadManager.replayRemoveOldRoutineLoad(operation);
        Assert.assertEquals(0, idToRoutineLoadJob.size());
    }

    @Test
    public void testReplayChangeRoutineLoadJob(@Injectable RoutineLoadOperation operation) {
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

        new Expectations() {
            {
                operation.getId();
                minTimes = 0;
                result = 1L;
                operation.getJobState();
                minTimes = 0;
                result = RoutineLoadJob.JobState.PAUSED;
            }
        };

        routineLoadManager.replayChangeRoutineLoadJob(operation);
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAlterRoutineLoadJob(@Injectable StopRoutineLoadStmt stopRoutineLoadStmt, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Mocked Database database, @Mocked Table tbl,
            @Mocked AccessControllerManager accessManager,
            @Mocked ConnectContext connectContext) throws UserException {
        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newHashMap();
        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newHashMap();
        List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJobList.add(routineLoadJob);
        nameToRoutineLoadJob.put("", routineLoadJobList);
        dbToNameToRoutineLoadJob.put(1L, nameToRoutineLoadJob);
        Deencapsulation.setField(routineLoadManager, "dbToNameToRoutineLoadJob", dbToNameToRoutineLoadJob);

        new Expectations() {
            {
                stopRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                stopRoutineLoadStmt.getName();
                minTimes = 0;
                result = "";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
                database.getTableOrAnalysisException(anyLong);
                minTimes = 0;
                result = tbl;
                tbl.getName();
                minTimes = 0;
                result = "tbl";
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        routineLoadManager.stopRoutineLoadJob(stopRoutineLoadStmt);

        Assert.assertEquals(RoutineLoadJob.JobState.STOPPED, routineLoadJob.getState());
    }

    @Test
    public void testPauseAndResumeAllRoutineLoadJob(@Injectable PauseRoutineLoadStmt pauseRoutineLoadStmt,
            @Injectable ResumeRoutineLoadStmt resumeRoutineLoadStmt, @Mocked Env env, @Mocked InternalCatalog catalog,
            @Mocked Database database, @Mocked Table tbl, @Mocked AccessControllerManager accessManager,
            @Mocked ConnectContext connectContext) throws UserException {
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

        new Expectations() {
            {
                pauseRoutineLoadStmt.isAll();
                minTimes = 0;
                result = true;
                pauseRoutineLoadStmt.getDbFullName();
                minTimes = 0;
                result = "";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDb("");
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
                database.getTableOrAnalysisException(anyLong);
                minTimes = 0;
                result = tbl;
                tbl.getName();
                minTimes = 0;
                result = "tbl";
                env.getAccessManager();
                minTimes = 0;
                result = accessManager;
                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
                resumeRoutineLoadStmt.isAll();
                minTimes = 0;
                result = true;
            }
        };

        routineLoadManager.pauseRoutineLoadJob(pauseRoutineLoadStmt);
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob1.getState());
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob2.getState());

        routineLoadManager.resumeRoutineLoadJob(resumeRoutineLoadStmt);
        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob1.getState());
        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob2.getState());
    }
}
