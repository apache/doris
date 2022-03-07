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

package org.apache.doris.load;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.MasterTask;
import org.apache.doris.task.MasterTaskExecutor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mockit.Expectations;
import mockit.Mocked;

public class LoadCheckerTest {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long backendId;

    private String label;

    @Mocked
    private Catalog catalog;
    @Mocked
    private EditLog editLog;
    @Mocked
    private Load load;
    private Database db;
    
    @Before
    public void setUp() {
        dbId = 0L;
        tableId = 0L;
        partitionId = 0L;
        indexId = 0L;
        tabletId = 0L;
        backendId = 0L;
        
        label = "test_label";
 
        // mock catalog
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L);
        new Expectations() {
            {
                catalog.getDbNullable(dbId);
                minTimes = 0;
                result = db;

                catalog.getDbNullable(db.getFullName());
                minTimes = 0;
                result = db;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };

        AgentTaskQueue.clearAllTasks();
        Assert.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @After
    public void tearDown() {
        Config.load_running_job_num_limit = 0;
    }
    
    @Test
    public void testInit() throws Exception {
        LoadChecker.init(5L);

        // verify checkers
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Assert.assertEquals(4, checkers.size());
        
        // verify executors
        Field executorsField = LoadChecker.class.getDeclaredField("executors");
        executorsField.setAccessible(true);
        Map<JobState, MasterTaskExecutor> executors = 
                (Map<JobState, MasterTaskExecutor>) executorsField.get(LoadChecker.class);
        Assert.assertEquals(2, executors.size());
    }
    
    @Test
    public void testRunPendingJobs(@Mocked MasterTaskExecutor executor) throws Exception {
        List<LoadJob> pendingJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        pendingJobs.add(job);

        // mock load
        new Expectations() {
            {
                load.getLoadJobs(JobState.PENDING);
                times = 1;
                result = pendingJobs;

                catalog.getLoadInstance();
                times = 2;
                result = load;

                executor.submit((MasterTask) any);
                times = 1;
                result = true;
            }
        };
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runPendingJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runPendingJobs", new Class[] {});
        runPendingJobs.invoke(checkers.get(JobState.PENDING), new Object[] {});
    }

    @Test
    public void testRunPendingJobsWithLimit(@Mocked MasterTaskExecutor executor) throws Exception {
        List<LoadJob> pendingJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        pendingJobs.add(job);
        pendingJobs.add(job);
        Assert.assertEquals(2, pendingJobs.size());

        // mock load
        new Expectations() {
            {
                load.getLoadJobs(JobState.PENDING);
                times = 1;
                result = pendingJobs;

                load.getLoadJobs(JobState.ETL);
                times = 1;
                result = Lists.newArrayList(job);

                catalog.getLoadInstance();
                times = 1;
                result = load;

                executor.getTaskNum();
                times = 2;
                result = 1;

                executor.submit((MasterTask) any);
                times = 0;
                result = true;
            }
        };

        // init
        LoadChecker.init(5L);

        // only one allow submit (1 + 1 (pending executor running) + 1 (etl state) = 3)
        Config.load_running_job_num_limit = 3;

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runPendingJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runPendingJobs", new Class[] {});
        runPendingJobs.invoke(checkers.get(JobState.PENDING), new Object[] {});
    }

    @Test
    public void testRunEtlJobs(@Mocked MasterTaskExecutor executor) throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.ETL);
        etlJobs.add(job);

        // mock load
        new Expectations() {
            {
                load.getLoadJobs(JobState.ETL);
                times = 1;
                result = etlJobs;

                catalog.getLoadInstance();
                times = 2;
                result = load;

                executor.submit((MasterTask) any);
                times = 1;
                result = true;
            }
        };
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runEtlJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runEtlJobs", new Class[] {});
        runEtlJobs.invoke(checkers.get(JobState.ETL), new Object[] {});
    }
    
    @Test
    public void testRunLoadingJobs() throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.LOADING);
        job.setDbId(dbId);
        etlJobs.add(job);
        // set table family load infos
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
        Partition partition = table.getPartition(partitionId);
        long newVersion = partition.getVisibleVersion() + 1;
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(new ArrayList<Source>());
        partitionLoadInfo.setVersion(newVersion);
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<Long, PartitionLoadInfo>();
        idToPartitionLoadInfo.put(partitionId, partitionLoadInfo);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        tableLoadInfo.addIndexSchemaHash(partition.getBaseIndex().getId(), 0);
        Map<Long, TableLoadInfo> idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
        idToTableLoadInfo.put(tableId, tableLoadInfo);
        job.setIdToTableLoadInfo(idToTableLoadInfo);
        // set tablet load infos
        int replicaNum = 0;
        Map<Long, TabletLoadInfo> tabletLoadInfos = new HashMap<Long, TabletLoadInfo>();
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                replicaNum += tablet.getReplicas().size();
                TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("/label/path", 1L);
                tabletLoadInfos.put(tablet.getId(), tabletLoadInfo);
            }
        }
        job.setIdToTabletLoadInfo(tabletLoadInfos);

        // mock load
        new Expectations() {
            {
                load.getLoadJobs(JobState.LOADING);
                times = 2;
                result = etlJobs;

                load.updateLoadJobState(job, JobState.QUORUM_FINISHED);
                minTimes = 0;
                result = true;

                load.cancelLoadJob((LoadJob) any, (CancelType) any, anyString);
                minTimes = 0;
                result = true;

                catalog.getLoadInstance();
                times = 4;
                result = load;
            }
        };
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs send tasks
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runLoadingJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runLoadingJobs", new Class[] {});
        runLoadingJobs.invoke(checkers.get(JobState.LOADING), new Object[] {});
        Assert.assertEquals(0, AgentTaskQueue.getTaskNum());

        // update replica to new version
        for (MaterializedIndex olapIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : olapIndex.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateVersionInfo(newVersion, 0L, 0L);
                }
            }
        }       

        // verify
        runLoadingJobs.invoke(checkers.get(JobState.LOADING), new Object[] {});
        // clear agent tasks
        AgentTaskQueue.clearAllTasks();
    }
    
    @Test
    public void testRunQuorumFinishedJobs() throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.QUORUM_FINISHED);
        job.setDbId(dbId);
        etlJobs.add(job);
        // set table family load infos
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
        Partition partition = table.getPartition(partitionId);
        long newVersion = partition.getVisibleVersion() + 1;
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(new ArrayList<Source>());
        partitionLoadInfo.setVersion(newVersion);
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<Long, PartitionLoadInfo>();
        idToPartitionLoadInfo.put(partitionId, partitionLoadInfo);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        tableLoadInfo.addIndexSchemaHash(partition.getBaseIndex().getId(), 0);
        Map<Long, TableLoadInfo> idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
        idToTableLoadInfo.put(tableId, tableLoadInfo);
        job.setIdToTableLoadInfo(idToTableLoadInfo);
        // set tablet load infos
        Map<Long, TabletLoadInfo> tabletLoadInfos = new HashMap<Long, TabletLoadInfo>();
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateVersionInfo(newVersion, 0L, 0L);
                }
                TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("/label/path", 1L);
                tabletLoadInfos.put(tablet.getId(), tabletLoadInfo);
            }
        }
        job.setIdToTabletLoadInfo(tabletLoadInfos);

        // mock load
        new Expectations() {
            {
                load.getLoadJobs(JobState.QUORUM_FINISHED);
                minTimes = 0;
                result = etlJobs;

                load.updateLoadJobState(job, JobState.FINISHED);
                minTimes = 0;
                result = true;

                load.clearJob(job, JobState.QUORUM_FINISHED);
                minTimes = 0;

                catalog.getLoadInstance();
                minTimes = 0;
                result = load;
            }
        };
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runQuorumFinishedJobs = UnitTestUtil.getPrivateMethod(
                LoadChecker.class, "runQuorumFinishedJobs", new Class[] {});
        runQuorumFinishedJobs.invoke(checkers.get(JobState.QUORUM_FINISHED), new Object[] {});
        
        Assert.assertEquals(0, AgentTaskQueue.getTaskNum());
    }
    
    @Test
    public void testCheckTimeout() {
        LoadJob job = new LoadJob(label);
        long currentTimeMs = System.currentTimeMillis();
        job.setCreateTimeMs(currentTimeMs - 2000);

        // timeout is 0s
        job.setTimeoutSecond(0);
        Assert.assertFalse(LoadChecker.checkTimeout(job));
        
        // timeout is 1s
        job.setTimeoutSecond(1);
        Assert.assertTrue(LoadChecker.checkTimeout(job));
        
        // timeout is 10s
        job.setTimeoutSecond(10);
        Assert.assertFalse(LoadChecker.checkTimeout(job));
    }

}
