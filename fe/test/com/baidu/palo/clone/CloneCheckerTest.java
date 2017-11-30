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

package com.baidu.palo.clone;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.clone.CloneChecker.CapacityLevel;
import com.baidu.palo.clone.CloneJob.JobPriority;
import com.baidu.palo.clone.CloneJob.JobState;
import com.baidu.palo.clone.CloneJob.JobType;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.util.UnitTestUtil;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.thrift.TDisk;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest({ CloneChecker.class, Catalog.class, SystemInfoService.class })
public class CloneCheckerTest {
    private static final Logger LOG = LoggerFactory.getLogger(CloneCheckerTest.class);

    private CloneChecker checker;
    private Catalog catalog;
    private SystemInfoService systemInfoService;
    private long dbId;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long backendId;

    @Before
    public void setUp() {
        dbId = 0L;
        tableId = 0L;
        partitionId = 0L;
        indexId = 0L;
        backendId = 0L;

        // update conf
        Config.clone_high_priority_delay_second = 0;
        Config.clone_low_priority_delay_second = 600;
        Config.clone_distribution_balance_threshold = 0.2;
        Config.clone_capacity_balance_threshold = 0.2;
    }

    @After
    public void tearDown() throws Exception {
        // destory INSTANCE
        Field instanceField = CloneChecker.class.getDeclaredField("INSTANCE");
        instanceField.setAccessible(true);
        instanceField.set(CloneChecker.class, null);
    }

    private CloneJob createCloneJob() {
        long tabletId = 0L;
        JobType type = JobType.SUPPLEMENT;
        JobPriority priority = JobPriority.HIGH;
        long timeoutMs = 1000L;
        CloneJob job = new CloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId, type, priority,
                timeoutMs);
        job.setCreateTimeMs(System.currentTimeMillis() - 1);
        return job;
    }

    private Constructor getInnerClassConstructor(Class innerClass) {
        Constructor[] constructors = innerClass.getDeclaredConstructors();
        Constructor constructor = constructors[0];
        constructor.setAccessible(true);
        LOG.debug(constructor.toString());
        return constructor;
    }

    @Test
    public void testCheckTablets() throws Exception {
        // mock getBackend getCloneInstance getDb getDbNames
        long backendId1 = backendId;
        long backendId2 = backendId + 1;
        long backendId3 = backendId + 2;
        Backend onlineBackend = EasyMock.createMock(Backend.class);
        EasyMock.expect(onlineBackend.isAlive()).andReturn(true).anyTimes();
        EasyMock.replay(onlineBackend);
        catalog = EasyMock.createNiceMock(Catalog.class);

        Clone clone = new Clone();
        EasyMock.expect(catalog.getCloneInstance()).andReturn(clone).anyTimes();

        long tabletId = 0L;
        long version = 1L;
        long versionHash = 0L;
        Database db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, version,
                versionHash);
        db.setClusterName("testCluster");
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        MaterializedIndex index = partition.getBaseIndex();
        Tablet tablet = index.getTablet(tabletId);
        tablet.deleteReplicaByBackendId(backendId1);
        Assert.assertEquals(2, tablet.getReplicas().size());
        EasyMock.expect(catalog.getDb(dbId)).andReturn(db).anyTimes();

        List<String> dbNames = new ArrayList<String>();
        String dbName = db.getFullName();
        dbNames.add(dbName);
        EasyMock.expect(catalog.getDb(db.getFullName())).andReturn(db).anyTimes();
        EasyMock.expect(catalog.getDbNames()).andReturn(dbNames).anyTimes();

        EasyMock.replay(catalog);

        // SystemInfoService
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.expect(systemInfoService.getBackend(backendId2)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId1)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId3)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackendIds(true))
                .andReturn(Lists.newArrayList(backendId1, backendId2, backendId3)).anyTimes();
        EasyMock.replay(systemInfoService);

        // inverted index
        TabletInvertedIndex invertedIndex = EasyMock.createMock(TabletInvertedIndex.class);
        invertedIndex.deleteReplica(EasyMock.anyLong(), EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(invertedIndex);

        // mock catalog
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        PowerMock.replay(Catalog.class);

        // mock private method
        Map<Long, Object> backendInfos = new HashMap<Long, Object>();
        Map<CapacityLevel, Set<Long>> distributionLevelToBackendIds = new HashMap<CapacityLevel, Set<Long>>();
        Map<CapacityLevel, Set<Long>> capacityLevelToBackendIds = new HashMap<CapacityLevel, Set<Long>>();
        CloneChecker mockChecker = PowerMock.createPartialMock(CloneChecker.class, "initBackendInfos",
                "initBackendCapacityInfos", "initBackendDistributionInfos");
        PowerMock.expectPrivate(mockChecker, "initBackendInfos", "testCluster").andReturn(backendInfos).anyTimes();
        PowerMock.expectPrivate(mockChecker, "initBackendDistributionInfos", backendInfos)
                .andReturn(distributionLevelToBackendIds).anyTimes();
        PowerMock.expectPrivate(mockChecker, "initBackendCapacityInfos", backendInfos)
                .andReturn(capacityLevelToBackendIds).anyTimes();
        PowerMock.replay(mockChecker);

        // init backend infos
        Class backendInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$BackendInfo");
        Constructor constructor = getInnerClassConstructor(backendInfoClass);
        backendInfos.put(backendId1, constructor.newInstance(new Object[] { mockChecker, backendId1, 10L, 0L }));
        backendInfos.put(backendId2, constructor.newInstance(new Object[] { mockChecker, backendId2, 10L, 0L }));
        backendInfos.put(backendId3, constructor.newInstance(new Object[] { mockChecker, backendId3, 10L, 0L }));

        for (CapacityLevel level : CapacityLevel.values()) {
            distributionLevelToBackendIds.put(level, new HashSet<Long>());
            capacityLevelToBackendIds.put(level, new HashSet<Long>());
        }
        distributionLevelToBackendIds.get(CapacityLevel.LOW).add(backendId1);
        distributionLevelToBackendIds.get(CapacityLevel.MID).add(backendId2);
        distributionLevelToBackendIds.get(CapacityLevel.HIGH).add(backendId3);
        capacityLevelToBackendIds.get(CapacityLevel.LOW).add(backendId1);
        capacityLevelToBackendIds.get(CapacityLevel.MID).add(backendId2);
        capacityLevelToBackendIds.get(CapacityLevel.HIGH).add(backendId3);

        // test check tablet for supplment
        Assert.assertTrue(mockChecker.checkTabletForSupplement(dbId, tableId, partitionId, indexId, tabletId));
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        if (pendingJobs.size() == 1) {
            CloneJob job = pendingJobs.get(0);
            Assert.assertEquals(backendId1, job.getDestBackendId());
            Assert.assertEquals(tabletId, job.getTabletId());
            Assert.assertEquals(JobType.SUPPLEMENT, job.getType());
            clone.cancelCloneJob(job, "test");
            Assert.assertEquals(0, clone.getCloneJobs(JobState.PENDING).size());
            Assert.assertEquals(2, tablet.getReplicas().size());
        }

        // test check tablets
        Method checkTablets = UnitTestUtil.getPrivateMethod(CloneChecker.class, "checkTablets", new Class[] {});
        checkTablets.invoke(mockChecker, new Object[] {});
        pendingJobs = clone.getCloneJobs(JobState.PENDING);
        if (pendingJobs.size() == 1) {
            CloneJob job = pendingJobs.get(0);
            Assert.assertEquals(backendId1, job.getDestBackendId());
            Assert.assertEquals(tabletId, job.getTabletId());
            Assert.assertEquals(JobType.SUPPLEMENT, job.getType());
            clone.cancelCloneJob(job, "test");
            Assert.assertEquals(0, clone.getCloneJobs(JobState.PENDING).size());
            Assert.assertEquals(2, tablet.getReplicas().size());
        }
    }

    @Test
    public void testInitBackendAndCapacityInfos() throws Exception {
        // mock catalog editlog
        catalog = EasyMock.createNiceMock(Catalog.class);
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        EasyMock.replay(catalog);

        // mock catalog
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        PowerMock.replay(Catalog.class);

        // mock getBackend
        long backendId1 = backendId; // high
        long backendId2 = backendId + 1; // mid
        long backendId3 = backendId + 2; // low
        long totalCapacityB = 100L;
        long availableCapacityB = 50L;
        List<Long> backends = new ArrayList<Long>();
        backends.add(backendId1);
        backends.add(backendId2);
        backends.add(backendId3);
        Backend backend1 = UnitTestUtil.createBackend(backendId1, "127.0.0.1", 8000, 8001, 8003, totalCapacityB,
                availableCapacityB - 40);
        Backend backend2 = UnitTestUtil.createBackend(backendId1, "127.0.0.1", 8100, 8101, 8103, totalCapacityB,
                availableCapacityB);
        Backend backend3 = UnitTestUtil.createBackend(backendId1, "127.0.0.1", 8200, 8201, 8203, totalCapacityB,
                availableCapacityB + 40);
        backend1.setOwnerClusterName("testCluster");

        // SystemInfoService
        EasyMock.expect(systemInfoService.getBackend(backendId1)).andReturn(backend1).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId2)).andReturn(backend2).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId3)).andReturn(backend3).anyTimes();
        EasyMock.expect(systemInfoService.getBackendIds(true))
                .andReturn(Lists.newArrayList(backendId1, backendId2, backendId3)).anyTimes();
        EasyMock.replay(systemInfoService);

        // get initBackendInfos method
        checker = CloneChecker.getInstance();
        Method initBackendInfos = UnitTestUtil.getPrivateMethod(CloneChecker.class, "initBackendInfos",
                new Class[] { String.class });
        Method initBackendCapacityInfos = UnitTestUtil.getPrivateMethod(CloneChecker.class, "initBackendCapacityInfos",
                new Class[] { Map.class });

        // test
        Map<Long, Object> backendInfos = (Map<Long, Object>) initBackendInfos.invoke(checker, new Object[] { null });
        Map<CapacityLevel, Set<Long>> capacityLevelToBackendIds = (Map<CapacityLevel, Set<Long>>) initBackendCapacityInfos
                .invoke(checker, new Object[] { backendInfos });
        Assert.assertTrue(capacityLevelToBackendIds.get(CapacityLevel.HIGH).contains(backendId1));
        Assert.assertTrue(capacityLevelToBackendIds.get(CapacityLevel.MID).contains(backendId2));
        Assert.assertTrue(capacityLevelToBackendIds.get(CapacityLevel.LOW).contains(backendId3));
    }

    @Test
    public void testInitBackendDistributionInfos() throws Exception {
        // get inner class: BackendInfo
        checker = CloneChecker.getInstance();
        Class backendInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$BackendInfo");
        Constructor backendInfoConstructor = getInnerClassConstructor(backendInfoClass);
        Method setTableReplicaNum = UnitTestUtil.getPrivateMethod(backendInfoClass, "setTableReplicaNum",
                new Class[] { int.class });

        // init params
        long backendId1 = backendId; // high
        long backendId2 = backendId + 1; // mid
        long backendId3 = backendId + 2; // low
        long totalCapacityB = 100L;
        long availableCapacityB = 50L;
        Map<Long, Object> backendInfos = new HashMap<Long, Object>();
        Object backendInfo1 = backendInfoConstructor
                .newInstance(new Object[] { checker, backendId1, totalCapacityB, availableCapacityB });
        setTableReplicaNum.invoke(backendInfo1, new Object[] { 10 });
        backendInfos.put(backendId1, backendInfo1);
        Object backendInfo2 = backendInfoConstructor
                .newInstance(new Object[] { checker, backendId2, totalCapacityB, availableCapacityB });
        backendInfos.put(backendId2, backendInfo2);
        setTableReplicaNum.invoke(backendInfo2, new Object[] { 5 });
        Object backendInfo3 = backendInfoConstructor
                .newInstance(new Object[] { checker, backendId3, totalCapacityB, availableCapacityB });
        backendInfos.put(backendId3, backendInfo3);
        setTableReplicaNum.invoke(backendInfo3, new Object[] { 1 });

        // get initBackendDistributionInfos method
        Method initBackendDistributionInfos = UnitTestUtil.getPrivateMethod(CloneChecker.class,
                "initBackendDistributionInfos", new Class[] { Map.class });

        // test
        Map<CapacityLevel, Set<Long>> distributionLevelToBackendIds = (Map<CapacityLevel, Set<Long>>) initBackendDistributionInfos
                .invoke(checker, new Object[] { backendInfos });
        Assert.assertTrue(distributionLevelToBackendIds.get(CapacityLevel.HIGH).contains(backendId1));
        Assert.assertTrue(distributionLevelToBackendIds.get(CapacityLevel.MID).contains(backendId2));
        Assert.assertTrue(distributionLevelToBackendIds.get(CapacityLevel.LOW).contains(backendId3));
    }

    @Test
    public void testSelectRandomBackendId() throws Exception {
        // get method
        checker = CloneChecker.getInstance();
        Method selectRandomBackendId = UnitTestUtil.getPrivateMethod(CloneChecker.class, "selectRandomBackendId",
                new Class[] { List.class, Set.class });

        // fail
        List<Long> candidateBackendIds = new ArrayList<Long>();
        candidateBackendIds.add(0L);
        Set<Long> excludeBackendIds = new HashSet<Long>();
        excludeBackendIds.add(0L);
        Assert.assertEquals(-1L,
                selectRandomBackendId.invoke(checker, new Object[] { candidateBackendIds, excludeBackendIds }));

        // success
        candidateBackendIds.add(1L);
        Assert.assertEquals(1L,
                selectRandomBackendId.invoke(checker, new Object[] { candidateBackendIds, excludeBackendIds }));
    }

    @Test
    public void testSelectCloneReplicaBackendId() throws Exception {
        // get inner class: TabletInfo BackendInfo
        checker = CloneChecker.getInstance();
        Class tabletInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$TabletInfo");
        Constructor tabletInfoConstructor = getInnerClassConstructor(tabletInfoClass);
        Class backendInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$BackendInfo");
        Constructor backendInfoConstructor = getInnerClassConstructor(backendInfoClass);

        // init params
        long totalCapacityB = 100L;
        long availableCapacityB = 50L;
        long tabletId = 0L;
        short replicationNum = 3;
        short onlineReplicaNum = 3;
        long tabletSizeB = 20L;
        long backendId1 = backendId;
        long backendId2 = backendId + 1;
        Set<Long> backendIds = new HashSet<Long>();
        backendIds.add(backendId1);
        Object[] objects = new Object[] { checker, dbId, tableId, partitionId, indexId, tabletId, replicationNum,
                onlineReplicaNum, tabletSizeB, backendIds };
        Object tabletInfo = tabletInfoConstructor.newInstance(objects);

        Object backendInfo1 = backendInfoConstructor
                .newInstance(new Object[] { checker, backendId1, totalCapacityB, availableCapacityB });
        Object backendInfo2 = backendInfoConstructor
                .newInstance(new Object[] { checker, backendId2, totalCapacityB, availableCapacityB });
        Map<Long, Object> backendInfos = new HashMap<Long, Object>();
        backendInfos.put(backendId1, backendInfo1);
        backendInfos.put(backendId2, backendInfo2);

        Map<CapacityLevel, Set<Long>> distributionLevelToBackendIds = new HashMap<CloneChecker.CapacityLevel, Set<Long>>();
        Map<CapacityLevel, Set<Long>> capacityLevelToBackendIds = new HashMap<CloneChecker.CapacityLevel, Set<Long>>();
        for (CapacityLevel level : CapacityLevel.values()) {
            distributionLevelToBackendIds.put(level, new HashSet<Long>());
            capacityLevelToBackendIds.put(level, new HashSet<Long>());
        }
        distributionLevelToBackendIds.get(CapacityLevel.MID).add(backendId1);
        distributionLevelToBackendIds.get(CapacityLevel.HIGH).add(backendId2);
        capacityLevelToBackendIds.get(CapacityLevel.MID).add(backendId1);
        capacityLevelToBackendIds.get(CapacityLevel.MID).add(backendId2);

        // get method
        Method selectCloneReplicaBackendId = UnitTestUtil.getPrivateMethod(CloneChecker.class,
                "selectCloneReplicaBackendId",
                new Class[] { Map.class, Map.class, Map.class, tabletInfoClass, JobType.class, JobPriority.class });

        // test
        Assert.assertEquals(-1L,
                selectCloneReplicaBackendId.invoke(checker, new Object[] { distributionLevelToBackendIds,
                        capacityLevelToBackendIds, backendInfos, tabletInfo, JobType.MIGRATION, JobPriority.LOW }));
        Assert.assertEquals(backendId2,
                selectCloneReplicaBackendId.invoke(checker, new Object[] { distributionLevelToBackendIds,
                        capacityLevelToBackendIds, backendInfos, tabletInfo, JobType.SUPPLEMENT, JobPriority.LOW }));
    }

    @Test
    public void testDeleteRedundantReplicas() throws Exception {
        // mock getBackend
        long backendId1 = backendId; // normal
        long backendId2 = backendId + 1; // offline
        long backendId3 = backendId + 2; // clone state
        long backendId4 = backendId + 3; // low version
        long backendId5 = backendId + 4; // high distribution
        long backendId6 = backendId + 5; // normal

        Map<String, TDisk> diskInfos = new HashMap<String, TDisk>();

        Backend offlineBackend = EasyMock.createMock(Backend.class);
        EasyMock.expect(offlineBackend.isAlive()).andReturn(false).anyTimes();
        EasyMock.expect(offlineBackend.isDecommissioned()).andReturn(false).anyTimes();
        EasyMock.expect(offlineBackend.getTotalCapacityB()).andReturn(6000L).anyTimes();
        EasyMock.expect(offlineBackend.getAvailableCapacityB()).andReturn(3000L).anyTimes();
        EasyMock.replay(offlineBackend);
        Backend onlineBackend = EasyMock.createMock(Backend.class);
        EasyMock.expect(onlineBackend.isAlive()).andReturn(true).anyTimes();
        EasyMock.expect(onlineBackend.isDecommissioned()).andReturn(false).anyTimes();
        EasyMock.expect(onlineBackend.getTotalCapacityB()).andReturn(6000L).anyTimes();
        EasyMock.expect(onlineBackend.getAvailableCapacityB()).andReturn(3000L).anyTimes();
        EasyMock.replay(onlineBackend);
        catalog = EasyMock.createNiceMock(Catalog.class);

        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        EasyMock.replay(catalog);

        // SystemInfoService
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.expect(systemInfoService.getBackend(backendId2)).andReturn(offlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId1)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId3)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId4)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId5)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.getBackend(backendId6)).andReturn(onlineBackend).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId2)).andReturn(false).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId1)).andReturn(true).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId3)).andReturn(true).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId4)).andReturn(true).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId5)).andReturn(true).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(backendId6)).andReturn(true).anyTimes();
        EasyMock.expect(systemInfoService.getClusterBackendIds("testCluster", true))
                .andReturn(Lists.newArrayList(backendId1, backendId3, backendId4, backendId5, backendId6)).anyTimes();
        EasyMock.expect(systemInfoService.getBackendIds(true))
                .andReturn(Lists.newArrayList(backendId1, backendId2, backendId3, backendId4, backendId5, backendId6))
                .anyTimes();
        EasyMock.replay(systemInfoService);

        // mock inverted index
        TabletInvertedIndex invertedIndex = EasyMock.createMock(TabletInvertedIndex.class);
        invertedIndex.addReplica(EasyMock.anyLong(), EasyMock.anyObject(Replica.class));
        EasyMock.expectLastCall().anyTimes();
        invertedIndex.addTablet(EasyMock.anyLong(), EasyMock.anyObject(TabletMeta.class));
        EasyMock.expectLastCall().anyTimes();
        invertedIndex.deleteReplica(EasyMock.anyLong(), EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        invertedIndex.clear();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(invertedIndex);

        // mock catalog
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        PowerMock.replay(Catalog.class);

        // get inner class: TabletInfo
        checker = CloneChecker.getInstance();
        Class tabletInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$TabletInfo");
        Constructor tabletInfoConstructor = getInnerClassConstructor(tabletInfoClass);

        // Map<Long, Object> backendInfos = new HashMap<Long, Object>();
        // backendInfos.put(backendId1, onlineBackend);
        // backendInfos.put(backendId2, offlineBackend);
        // backendInfos.put(backendId3, onlineBackend);
        // backendInfos.put(backendId4, onlineBackend);
        // backendInfos.put(backendId5, onlineBackend);
        // backendInfos.put(backendId6, onlineBackend);
        // PowerMock.expectPrivate(checker, "initBackendInfos",
        // "testCluster").andReturn(backendInfos).anyTimes();
        // PowerMock.replay(checker);

        // init params
        long tabletId = 0L;
        short replicationNum = 3;
        short onlineReplicaNum = 3;
        long tabletSizeB = 20L;
        Set<Long> backendIds = new HashSet<Long>();
        backendIds.add(backendId1);
        Object[] objects = new Object[] { checker, dbId, tableId, partitionId, indexId, tabletId, replicationNum,
                onlineReplicaNum, tabletSizeB, backendIds };
        Object tabletInfo = tabletInfoConstructor.newInstance(objects);

        Map<CapacityLevel, Set<Long>> distributionLevelToBackendIds = new HashMap<CloneChecker.CapacityLevel, Set<Long>>();
        for (CapacityLevel level : CapacityLevel.values()) {
            distributionLevelToBackendIds.put(level, new HashSet<Long>());
        }
        Set<Long> midBackendIds = distributionLevelToBackendIds.get(CapacityLevel.MID);
        midBackendIds.add(backendId1);
        midBackendIds.add(backendId2);
        midBackendIds.add(backendId3);
        midBackendIds.add(backendId4);
        midBackendIds.add(backendId6);
        Set<Long> highBackendIds = distributionLevelToBackendIds.get(CapacityLevel.HIGH);
        highBackendIds.add(backendId5);

        long version = 1L;
        long versionHash = 0L;
        Database db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, version,
                versionHash);
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        MaterializedIndex index = partition.getBaseIndex();
        Tablet tablet = index.getTablet(tabletId);
        Replica replica4 = new Replica(3, backendId4, version - 1, versionHash, 0L, 0L, ReplicaState.NORMAL);
        Replica replica5 = new Replica(4, backendId5, version, versionHash, 0L, 0L, ReplicaState.NORMAL);
        Replica replica6 = new Replica(5, backendId6, version, versionHash, 0L, 0L, ReplicaState.NORMAL);
        tablet.addReplica(replica4);
        tablet.addReplica(replica5);
        tablet.addReplica(replica6);
        Replica replica2 = tablet.getReplicaByBackendId(backendId2);
        Replica replica3 = tablet.getReplicaByBackendId(backendId3);
        replica3.setState(ReplicaState.CLONE);

        // get method
        Method deleteRedundantReplicas = UnitTestUtil.getPrivateMethod(CloneChecker.class, "deleteRedundantReplicas",
                new Class[] { Database.class, tabletInfoClass, Map.class });

        // need not delete
        for (Replica replica : tablet.getReplicas()) {
            LOG.info(replica.toString());
        }
        table.getPartitionInfo().setReplicationNum(partition.getId(), (short) 6);
        deleteRedundantReplicas.invoke(checker, new Object[] { db, tabletInfo, distributionLevelToBackendIds });
        Assert.assertEquals(6, tablet.getReplicas().size());

        // delete offline
        table.getPartitionInfo().setReplicationNum(partition.getId(), (short) 4);
        deleteRedundantReplicas.invoke(checker, new Object[] { db, tabletInfo, distributionLevelToBackendIds });
        Assert.assertEquals(5, tablet.getReplicas().size());
        Assert.assertFalse(tablet.getReplicas().contains(replica2));

        // delete clone state
        table.getPartitionInfo().setReplicationNum(partition.getId(), (short) 3);
        deleteRedundantReplicas.invoke(checker, new Object[] { db, tabletInfo, distributionLevelToBackendIds });
        Assert.assertEquals(4, tablet.getReplicas().size());
        Assert.assertFalse(tablet.getReplicas().contains(replica3));

        // delete low version
        table.getPartitionInfo().setReplicationNum(partition.getId(), (short) 2);
        deleteRedundantReplicas.invoke(checker, new Object[] { db, tabletInfo, distributionLevelToBackendIds });
        Assert.assertEquals(2, tablet.getReplicas().size());
        Assert.assertFalse(tablet.getReplicas().contains(replica4));

        // delete high distribution
        table.getPartitionInfo().setReplicationNum(partition.getId(), (short) 1);
        deleteRedundantReplicas.invoke(checker, new Object[] { db, tabletInfo, distributionLevelToBackendIds });
        Assert.assertEquals(1, tablet.getReplicas().size());
        Assert.assertFalse(tablet.getReplicas().contains(replica5));
    }

    @Test
    public void testRunCloneJob() throws Exception {
        // mock catalog db
        long tabletId = 0L;
        long version = 1L;
        long versionHash = 0L;
        Database db = UnitTestUtil.createDb(dbId, tableId, partitionId, indexId, tabletId, backendId, version,
                versionHash);
        catalog = EasyMock.createNiceMock(Catalog.class);
        EasyMock.expect(catalog.getDb(EasyMock.anyLong())).andReturn(db).anyTimes();

        // mock editlog
        EditLog editLog = EasyMock.createMock(EditLog.class);
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();

        // mock clone
        Clone clone = new Clone();
        clone.addCloneJob(dbId, tableId, partitionId, indexId, tabletId, backendId, JobType.SUPPLEMENT,
                JobPriority.HIGH, 5000L);
        EasyMock.expect(catalog.getCloneInstance()).andReturn(clone).anyTimes();
        EasyMock.replay(catalog);

        // mock inverted index
        TabletInvertedIndex invertedIndex = EasyMock.createMock(TabletInvertedIndex.class);
        invertedIndex.addReplica(EasyMock.anyLong(), EasyMock.anyObject(Replica.class));
        EasyMock.expectLastCall().anyTimes();
        invertedIndex.deleteReplica(EasyMock.anyLong(), EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(invertedIndex);

        // mock catalog
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        systemInfoService = EasyMock.createMock(SystemInfoService.class);
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        PowerMock.replay(Catalog.class);

        // mock getBackend
        Backend backend = UnitTestUtil.createBackend(backendId, "127.0.0.1", 8000, 8001, 8003);

        // mock SystemInfoService
        EasyMock.expect(systemInfoService.getBackend(EasyMock.anyLong())).andReturn(backend).anyTimes();
        EasyMock.expect(systemInfoService.getBackendIds(true)).andReturn(Lists.newArrayList(10000L)).anyTimes();
        EasyMock.expect(systemInfoService.checkBackendAvailable(EasyMock.anyLong())).andReturn(true).anyTimes();
        EasyMock.replay(systemInfoService);

        // get method
        checker = CloneChecker.getInstance();
        Method runCloneJob = UnitTestUtil.getPrivateMethod(CloneChecker.class, "runCloneJob",
                new Class[] { CloneJob.class });

        // delete replica of backendId
        OlapTable table = (OlapTable) db.getTable(tableId);
        Partition partition = table.getPartition(partitionId);
        MaterializedIndex index = partition.getBaseIndex();
        index.getTablet(tabletId).deleteReplicaByBackendId(backendId);

        // run clone task and update jobstate to RUNNING
        List<CloneJob> pendingJobs = clone.getCloneJobs(JobState.PENDING);
        Assert.assertEquals(1, pendingJobs.size());
        CloneJob job = pendingJobs.get(0);
        // for avoiding running too fast
        job.setCreateTimeMs(System.currentTimeMillis() - 2000);
        runCloneJob.invoke(checker, new Object[] { job });
        LOG.warn(job.toString());
        Assert.assertEquals(JobState.RUNNING, job.getState());
        List<AgentTask> tasks = AgentTaskQueue.getDiffTasks(backendId, new HashMap<TTaskType, Set<Long>>());
        for (AgentTask task : tasks) {
            if (task.getTaskType() == TTaskType.CLONE) {
                long signature = tabletId;
                Assert.assertEquals(signature, task.getSignature());
                AgentTaskQueue.removeTask(backendId, TTaskType.CLONE, signature);
                Assert.assertNull(AgentTaskQueue.getTask(backendId, TTaskType.CLONE, signature));
            }
        }
    }

    @Test
    public void testCheckPassDelayTime() throws Exception {
        // get method
        checker = CloneChecker.getInstance();
        Method checkPassDelayTime = UnitTestUtil.getPrivateMethod(CloneChecker.class, "checkPassDelayTime",
                new Class[] { CloneJob.class });

        // create job
        CloneJob job = createCloneJob();

        // high priority
        job.setPriority(JobPriority.HIGH);
        Assert.assertTrue((Boolean) checkPassDelayTime.invoke(checker, new Object[] { job }));

        // low priority
        job.setPriority(JobPriority.LOW);
        Assert.assertFalse((Boolean) checkPassDelayTime.invoke(checker, new Object[] { job }));

        job.setCreateTimeMs(System.currentTimeMillis() - Config.clone_low_priority_delay_second * 1000L - 100);
        Assert.assertTrue((Boolean) checkPassDelayTime.invoke(checker, new Object[] { job }));
    }

    @Test
    public void testInnerTabletInfo() throws Exception {
        // get inner class
        checker = CloneChecker.getInstance();
        Class tabletInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$TabletInfo");
        Constructor constructor = getInnerClassConstructor(tabletInfoClass);
        LOG.debug(constructor.toString());
        try {
            long tabletId = 0L;
            short replicationNum = 3;
            short onlineReplicaNum = 2;
            long tabletSizeB = 100L;
            Set<Long> backendIds = new HashSet<Long>();
            Object[] objects = new Object[] { checker, dbId, tableId, partitionId, indexId, tabletId, replicationNum,
                    onlineReplicaNum, tabletSizeB, backendIds };
            Object tabletInfo = constructor.newInstance(objects);

            // getDbId method
            Method getDbId = UnitTestUtil.getPrivateMethod(tabletInfoClass, "getDbId", new Class[] {});
            Assert.assertEquals(dbId, getDbId.invoke(tabletInfo, new Object[] {}));

            // getTabletSizeB method
            Method getTabletSizeB = UnitTestUtil.getPrivateMethod(tabletInfoClass, "getTabletSizeB", new Class[] {});
            Assert.assertEquals(tabletSizeB, getTabletSizeB.invoke(tabletInfo, new Object[] {}));

            // getBackendIds method
            Method getBackendIds = UnitTestUtil.getPrivateMethod(tabletInfoClass, "getBackendIds", new Class[] {});
            Set<Long> backendResults = (Set<Long>) getBackendIds.invoke(tabletInfo, new Object[] {});
            Assert.assertEquals(0, backendResults.size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInnerBackendInfo() throws Exception {
        // get inner class
        checker = CloneChecker.getInstance();
        Class backendInfoClass = UnitTestUtil.getInnerClass(CloneChecker.class,
                "com.baidu.palo.clone.CloneChecker$BackendInfo");
        Constructor constructor = getInnerClassConstructor(backendInfoClass);
        LOG.debug(constructor.toString());
        try {
            long totalCapacityB = 100L;
            long availableCapacityB = 50L;
            Object backendInfo = constructor
                    .newInstance(new Object[] { checker, backendId, totalCapacityB, availableCapacityB });

            // setCloneCapacityB method
            long cloneCapacityB = 10L;
            Method setCloneCapacityB = UnitTestUtil.getPrivateMethod(backendInfoClass, "setCloneCapacityB",
                    new Class[] { long.class });
            setCloneCapacityB.invoke(backendInfo, new Object[] { cloneCapacityB });

            // canCloneByCapacity method (cloneCapacityB is 10)
            Method canCloneByCapacity = UnitTestUtil.getPrivateMethod(backendInfoClass, "canCloneByCapacity",
                    new Class[] { long.class });
            long tabletSizeB = 20L;
            Assert.assertFalse((Boolean) canCloneByCapacity.invoke(backendInfo, new Object[] { tabletSizeB }));
            tabletSizeB = 6L;
            Assert.assertTrue((Boolean) canCloneByCapacity.invoke(backendInfo, new Object[] { tabletSizeB }));

            // decreaseCloneCapacityB method
            Method decreaseCloneCapacityB = UnitTestUtil.getPrivateMethod(backendInfoClass, "decreaseCloneCapacityB",
                    new Class[] { long.class });
            decreaseCloneCapacityB.invoke(backendInfo, new Object[] { tabletSizeB });
            // check canCloneByCapacity (cloneCapacityB is 4)
            tabletSizeB = 6L;
            Assert.assertFalse((Boolean) canCloneByCapacity.invoke(backendInfo, new Object[] { tabletSizeB }));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

}
