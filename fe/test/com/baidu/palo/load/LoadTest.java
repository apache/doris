// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.load;

import com.baidu.palo.analysis.CancelLoadStmt;
import com.baidu.palo.analysis.ColumnSeparator;
import com.baidu.palo.analysis.DataDescription;
import com.baidu.palo.analysis.DeleteStmt;
import com.baidu.palo.analysis.LabelName;
import com.baidu.palo.analysis.LoadStmt;
import com.baidu.palo.analysis.Predicate;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.MarkedCountDownLatch;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.util.UnitTestUtil;
import com.baidu.palo.load.FailMsg.CancelType;
import com.baidu.palo.load.LoadJob.EtlJobType;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.metric.MetricRepo;
import com.baidu.palo.mysql.privilege.PaloAuth;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.QueryState;
import com.baidu.palo.qe.SessionVariable;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Load.class, Catalog.class, ConnectContext.class, SystemInfoService.class })
public class LoadTest {
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long backendId;

    private String label;
    private String columnSeparator;
    private List<String> filePathes;

    private Load load;
    private Database db;

    private ConnectContext connectContext;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
    }

    @Before
    public void setUp() throws DdlException {
        dbId = 0L;
        tableId = 0L;
        partitionId = 0L;
        indexId = 0L;
        tabletId = 0L;
        backendId = 0L;

        label = "test_label";
        columnSeparator = "\t";
        filePathes = new ArrayList<String>();
        filePathes.add("test_path");

        load = new Load();
        Config.load_running_job_num_limit = 0;

        // dpp configs
        UnitTestUtil.initDppConfig();

        // mock catalog
        db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, 1L, 0L);
        Catalog catalog = EasyMock.createNiceMock(Catalog.class);
        EasyMock.expect(catalog.getDb(dbId)).andReturn(db).anyTimes();
        EasyMock.expect(catalog.getDb(db.getFullName())).andReturn(db).anyTimes();
        // mock editLog
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        // mock auth
        PaloAuth auth = EasyMock.createNiceMock(PaloAuth.class);
        EasyMock.expect(auth.getLoadClusterInfo(EasyMock.anyString(), EasyMock.anyString()))
                .andReturn(Pair.create("cluster", new DppConfig())).anyTimes();
        EasyMock.expect(auth.checkTblPriv(EasyMock.isA(ConnectContext.class), EasyMock.anyString(),
                                          EasyMock.anyString(), EasyMock.isA(PrivPredicate.class)))
                .andReturn(true).anyTimes();
        EasyMock.expect(catalog.getAuth()).andReturn(auth).anyTimes();
        EasyMock.replay(auth);

        // mock backend
        Backend backend = EasyMock.createMock(Backend.class);
        EasyMock.expect(backend.isAlive()).andReturn(true).anyTimes();
        EasyMock.replay(backend);
        EasyMock.replay(catalog);

        // SystemInfoService
        SystemInfoService systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.expect(systemInfoService.checkBackendAvailable(EasyMock.anyLong())).andReturn(true).anyTimes();
        systemInfoService.checkClusterCapacity(EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(systemInfoService);

        // mock static getInstance
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalog()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        PowerMock.replay(Catalog.class);

        QueryState state = new QueryState();
        connectContext = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(connectContext.toResourceCtx()).andReturn(null).anyTimes();
        EasyMock.expect(connectContext.getSessionVariable()).andReturn(new SessionVariable()).anyTimes();
        EasyMock.expect(connectContext.getQualifiedUser()).andReturn("root").anyTimes();
        EasyMock.expect(connectContext.getRemoteIP()).andReturn("192.168.1.1").anyTimes();
        EasyMock.expect(connectContext.getState()).andReturn(state).anyTimes();
        EasyMock.replay(connectContext);

        PowerMock.mockStatic(ConnectContext.class);
        EasyMock.expect(ConnectContext.get()).andReturn(connectContext).anyTimes();
        PowerMock.replay(ConnectContext.class);
    }

    private void addLoadJob(String label) throws DdlException {
        LabelName labelName = new LabelName(db.getFullName(), label);
        List<DataDescription> dataDescriptions = new ArrayList<DataDescription>();
        DataDescription dataDescription = new DataDescription(UnitTestUtil.TABLE_NAME,
                Lists.newArrayList(UnitTestUtil.PARTITION_NAME), filePathes, null, new ColumnSeparator(columnSeparator),
                false, null);
        dataDescriptions.add(dataDescription);
        LoadStmt stmt = new LoadStmt(labelName, dataDescriptions, null, null, null);
        load.addLoadJob(stmt, EtlJobType.HADOOP, 0);
    }

    @Test
    public void testAddAndGetLoadJob() throws DdlException {
        // add load job success
        addLoadJob(label);

        // verify
        // getDbLoadJobs
        List<LoadJob> dbLoadJobs = load.getDbLoadJobs(dbId);
        Assert.assertEquals(1, dbLoadJobs.size());
        LoadJob job = dbLoadJobs.get(0);
        Assert.assertEquals("cluster", job.getHadoopCluster());
        Assert.assertEquals(Config.hadoop_load_default_timeout_second, job.getTimeoutSecond());

        // getLoadJobNumber
        Assert.assertEquals(1, load.getLoadJobNumber());

        // getIdToLoadJob
        Assert.assertEquals(1, load.getIdToLoadJob().size());

        // getDbToLoadJobs
        Map<Long, List<LoadJob>> dbToLoadJobs = load.getDbToLoadJobs();
        Assert.assertEquals(1, dbToLoadJobs.get(dbId).size());

        // getLoadJobs
        Assert.assertEquals(1, load.getLoadJobs(JobState.PENDING).size());

        // getLoadJob
        Assert.assertEquals(job, load.getLoadJob(job.getId()));

        // getLoadJobInfosByDb
        Assert.assertEquals(1, load.getLoadJobInfosByDb(db.getId(), db.getFullName(), null, false, null, null).size());
    }

    @Test
    public void testCancelLoadJob() throws Exception {
        // add load job success
        addLoadJob(label);

        // mock dppscheduler
        DppScheduler dppScheduler = EasyMock.createMock(DppScheduler.class);
        dppScheduler.deleteEtlOutputPath(EasyMock.anyString());
        EasyMock.expectLastCall().times(1);
        EasyMock.replay(dppScheduler);
        PowerMock.expectNew(DppScheduler.class, EasyMock.anyString()).andReturn(dppScheduler).times(1);
        PowerMock.replay(DppScheduler.class);

        // cancel success
        CancelLoadStmt cancelStmt = EasyMock.createMock(CancelLoadStmt.class);
        EasyMock.expect(cancelStmt.getDbName()).andReturn(db.getFullName()).anyTimes();
        EasyMock.expect(cancelStmt.getLabel()).andReturn(label).anyTimes();
        EasyMock.replay(cancelStmt);
        load.cancelLoadJob(cancelStmt);

        // verify
        List<LoadJob> dbLoadJobs = load.getDbLoadJobs(dbId);
        Assert.assertEquals(1, dbLoadJobs.size());
        LoadJob job = dbLoadJobs.get(0);
        Assert.assertEquals(JobState.CANCELLED, job.getState());
        Assert.assertEquals(CancelType.USER_CANCEL, job.getFailMsg().getCancelType());
    }

    @Test
    public void testQuorumFinished() throws Exception {
        // add load job success
        addLoadJob(label);

        // get job
        List<LoadJob> dbLoadJobs = load.getDbLoadJobs(dbId);
        Assert.assertEquals(1, dbLoadJobs.size());
        LoadJob job = dbLoadJobs.get(0);
        Assert.assertEquals(JobState.PENDING, job.getState());

        // update job state loading
        job.setState(JobState.LOADING);

        // update replica row count
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        long oldCommittedVersion = partition.getCommittedVersion();
        long versionHash = 0L;
        Map<Long, Long> indexIdToRowCount = new HashMap<Long, Long>();
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            long indexRowCount = 0L;
            for (Tablet tablet : index.getTablets()) {
                long rowCount = 10L;
                indexRowCount += rowCount;
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateInfo(oldCommittedVersion + 1, versionHash, 1, rowCount--);
                }
            }
            indexIdToRowCount.put(index.getId(), indexRowCount);
        }

        // test
        load.updateLoadJobState(job, JobState.QUORUM_FINISHED);

        // verify
        Assert.assertEquals(JobState.QUORUM_FINISHED, job.getState());
        Assert.assertEquals(100, job.getProgress());
        for (MaterializedIndex olapTable : partition.getMaterializedIndices()) {
            Assert.assertEquals((long) indexIdToRowCount.get(olapTable.getId()), olapTable.getRowCount());
        }
    }

    @Test
    public void testDelete() throws Exception {
        // get table family
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        long oldCommittedVersion = partition.getCommittedVersion();

        // mock CountDownLatch
        MarkedCountDownLatch latch = EasyMock.createMock(MarkedCountDownLatch.class);
        EasyMock.expect(latch.await(EasyMock.anyLong(), EasyMock.eq(TimeUnit.MILLISECONDS))).andReturn(true).times(1);
        latch.addMark(EasyMock.anyLong(), EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(latch);
        PowerMock.expectNew(MarkedCountDownLatch.class, EasyMock.anyInt()).andReturn(latch).times(1);
        PowerMock.replay(MarkedCountDownLatch.class);

        // mock delete stmt
        DeleteStmt deleteStmt = EasyMock.createMock(DeleteStmt.class);
        EasyMock.expect(deleteStmt.getDbName()).andReturn(db.getFullName()).times(1);
        EasyMock.expect(deleteStmt.getTableName()).andReturn(UnitTestUtil.TABLE_NAME).times(1);
        EasyMock.expect(deleteStmt.getDeleteConditions()).andReturn(new ArrayList<Predicate>()).times(1);
        EasyMock.expect(deleteStmt.getPartitionName()).andReturn(UnitTestUtil.TABLE_NAME).times(1);
        EasyMock.replay(deleteStmt);

        // mock random
        long versionHash = 1L;
        Random random = EasyMock.createMock(Random.class);
        EasyMock.expect(random.nextLong()).andReturn(versionHash).times(1);
        EasyMock.replay(random);
        PowerMock.expectNew(Random.class).andReturn(random).times(1);
        PowerMock.replay(Random.class);

        // update replica version and version hash
        for (MaterializedIndex index : partition.getMaterializedIndices()) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    replica.updateInfo(oldCommittedVersion + 1, versionHash, 1, 1);
                }
            }
        }

        // delete success
        load.delete(deleteStmt);

        // verify
        Assert.assertEquals(oldCommittedVersion + 1, partition.getCommittedVersion());
        PowerMock.verifyAll();
    }
}
