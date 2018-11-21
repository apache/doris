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

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LoadChecker.class, Catalog.class})
public class LoadCheckerTest {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long backendId;

    private String label;
    
    private Catalog catalog;
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
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L, 0L);
        catalog = EasyMock.createNiceMock(Catalog.class);
        EasyMock.expect(catalog.getDb(dbId)).andReturn(db).anyTimes();
        EasyMock.expect(catalog.getDb(db.getFullName())).andReturn(db).anyTimes();
        // mock editLog
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        // mock static getInstance
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
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
    public void testRunPendingJobs() throws Exception {
        List<LoadJob> pendingJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        pendingJobs.add(job);

        // mock load
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.getLoadJobs(JobState.PENDING)).andReturn(pendingJobs).times(1);
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).times(1);
        EasyMock.replay(catalog);
        
        // mock MasterTaskExecutor submit
        MasterTaskExecutor executor = EasyMock.createMock(MasterTaskExecutor.class);
        EasyMock.expect(executor.submit(EasyMock.isA(MasterTask.class))).andReturn(true).times(1);
        EasyMock.replay(executor);
        PowerMock.expectNew(MasterTaskExecutor.class, EasyMock.anyInt()).andReturn(executor).times(4);
        PowerMock.replay(MasterTaskExecutor.class);
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runPendingJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runPendingJobs", new Class[] {});
        runPendingJobs.invoke(checkers.get(JobState.PENDING), new Object[] {});
        
        // verify
        EasyMock.verify(executor);
        PowerMock.verifyAll();
    }

    @Test
    public void testRunPendingJobsWithLimit() throws Exception {
        List<LoadJob> pendingJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.PENDING);
        pendingJobs.add(job);
        pendingJobs.add(job);
        Assert.assertEquals(2, pendingJobs.size());

        // mock load
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.getLoadJobs(JobState.PENDING)).andReturn(pendingJobs).times(1);
        EasyMock.expect(load.getLoadJobs(JobState.ETL)).andReturn(Lists.newArrayList(job)).times(1);
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).times(1);
        EasyMock.replay(catalog);

        // mock MasterTaskExecutor submit
        MasterTaskExecutor executor = EasyMock.createMock(MasterTaskExecutor.class);
        EasyMock.expect(executor.getTaskNum()).andReturn(1).times(2);
        EasyMock.expect(executor.submit(EasyMock.isA(MasterTask.class))).andReturn(true).times(1);
        EasyMock.replay(executor);
        PowerMock.expectNew(MasterTaskExecutor.class, EasyMock.anyInt()).andReturn(executor).times(4);
        PowerMock.replay(MasterTaskExecutor.class);

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
    public void testRunEtlJobs() throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.ETL);
        etlJobs.add(job);

        // mock load
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.getLoadJobs(JobState.ETL)).andReturn(etlJobs).times(1);
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).times(1);
        EasyMock.replay(catalog);
        
        // mock MasterTaskExecutor submit
        MasterTaskExecutor executor = EasyMock.createMock(MasterTaskExecutor.class);
        EasyMock.expect(executor.submit(EasyMock.isA(MasterTask.class))).andReturn(true).times(1);
        EasyMock.replay(executor);
        PowerMock.expectNew(MasterTaskExecutor.class, EasyMock.anyInt()).andReturn(executor).times(4);
        PowerMock.replay(MasterTaskExecutor.class);
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runEtlJobs = UnitTestUtil.getPrivateMethod(LoadChecker.class, "runEtlJobs", new Class[] {});
        runEtlJobs.invoke(checkers.get(JobState.ETL), new Object[] {});
        
        // verify
        EasyMock.verify(executor);
        PowerMock.verifyAll();
    }
    
    @Test
    public void testRunLoadingJobs() throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.LOADING);
        job.setDbId(dbId);
        etlJobs.add(job);
        // set table family load infos
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        long newVersion = partition.getVisibleVersion() + 1;
        long newVersionHash = 1L;
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(new ArrayList<Source>());
        partitionLoadInfo.setVersion(newVersion);
        partitionLoadInfo.setVersionHash(newVersionHash);
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
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            for (Tablet tablet : index.getTablets()) {
                replicaNum += tablet.getReplicas().size();
                TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("/label/path", 1L);
                tabletLoadInfos.put(tablet.getId(), tabletLoadInfo);
            }
        }
        job.setIdToTabletLoadInfo(tabletLoadInfos);

        // mock load
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.getLoadJobs(JobState.LOADING)).andReturn(etlJobs).anyTimes();
        EasyMock.expect(load.updateLoadJobState(job, JobState.QUORUM_FINISHED)).andReturn(true).anyTimes();
        EasyMock.expect(load.cancelLoadJob((LoadJob) EasyMock.anyObject(), (CancelType) EasyMock.anyObject(),
                                           EasyMock.anyString())).andReturn(true).anyTimes();
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).times(4);
        EasyMock.replay(catalog);
        
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
        for (MaterializedIndex olapIndex : partition.getMaterializedIndices()) {
            for (Tablet tablet : olapIndex.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateVersionInfo(newVersion, newVersionHash, 0L, 0L);
                }
            }
        }       

        // verify
        runLoadingJobs.invoke(checkers.get(JobState.LOADING), new Object[] {});
        EasyMock.verify(load);
        EasyMock.verify(catalog);
        
        // clear agent tasks
        AgentTaskQueue.clearAllTasks();
    }
    
    @Test
    public void testRunQuorumFinishedJobs() throws Exception {
        List<LoadJob> etlJobs = new ArrayList<LoadJob>();
        List<AsyncDeleteJob> deleteJobs = Lists.newArrayList();
        LoadJob job = new LoadJob(label);
        job.setState(JobState.QUORUM_FINISHED);
        job.setDbId(dbId);
        etlJobs.add(job);
        // set table family load infos
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        long newVersion = partition.getVisibleVersion() + 1;
        long newVersionHash = 0L;
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(new ArrayList<Source>());
        partitionLoadInfo.setVersion(newVersion);
        partitionLoadInfo.setVersionHash(newVersionHash);
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<Long, PartitionLoadInfo>();
        idToPartitionLoadInfo.put(partitionId, partitionLoadInfo);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        tableLoadInfo.addIndexSchemaHash(partition.getBaseIndex().getId(), 0);
        Map<Long, TableLoadInfo> idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
        idToTableLoadInfo.put(tableId, tableLoadInfo);
        job.setIdToTableLoadInfo(idToTableLoadInfo);
        // set tablet load infos
        Map<Long, TabletLoadInfo> tabletLoadInfos = new HashMap<Long, TabletLoadInfo>();
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateVersionInfo(newVersion, newVersionHash, 0L, 0L);
                }
                TabletLoadInfo tabletLoadInfo = new TabletLoadInfo("/label/path", 1L);
                tabletLoadInfos.put(tablet.getId(), tabletLoadInfo);
            }
        }
        job.setIdToTabletLoadInfo(tabletLoadInfos);

        // mock load
        load = EasyMock.createMock(Load.class);
        EasyMock.expect(load.getLoadJobs(JobState.QUORUM_FINISHED)).andReturn(etlJobs).anyTimes();
        EasyMock.expect(load.getQuorumFinishedDeleteJobs()).andReturn(deleteJobs).anyTimes();
        EasyMock.expect(load.updateLoadJobState(job, JobState.FINISHED)).andReturn(true).anyTimes();
        load.clearJob(job, JobState.QUORUM_FINISHED);
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(load);
        EasyMock.expect(catalog.getLoadInstance()).andReturn(load).anyTimes();
        EasyMock.replay(catalog);
        
        // init
        LoadChecker.init(5L);

        // test runPendingJobs
        Field checkersField = LoadChecker.class.getDeclaredField("checkers");
        checkersField.setAccessible(true);
        Map<JobState, LoadChecker> checkers = (Map<JobState, LoadChecker>) checkersField.get(LoadChecker.class);
        Method runQuorumFinishedJobs = UnitTestUtil.getPrivateMethod(
                LoadChecker.class, "runQuorumFinishedJobs", new Class[] {});
        runQuorumFinishedJobs.invoke(checkers.get(JobState.QUORUM_FINISHED), new Object[] {});
        
        // verify
        EasyMock.verify(load);
        EasyMock.verify(catalog);
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
