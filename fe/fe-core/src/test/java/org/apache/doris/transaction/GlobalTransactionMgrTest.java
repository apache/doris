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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.KafkaTaskInfo;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.RoutineLoadTaskInfo;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import mockit.Injectable;
import mockit.Mocked;

public class GlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeCatalog fakeCatalog;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Catalog masterCatalog;
    private static Catalog slaveCatalog;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        fakeEditLog = new FakeEditLog();
        fakeCatalog = new FakeCatalog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        slaveCatalog = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_40);
        metaContext.setThreadLocalInfo();

        masterTransMgr = masterCatalog.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterCatalog.getEditLog());

        slaveTransMgr = slaveCatalog.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveCatalog.getEditLog());
    }

    @Test
    public void testBeginTransaction() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());
    }

    @Test
    public void testBeginTransactionWithSameLabel() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = 0;
        try {
            transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        } catch (AnalysisException | LabelAlreadyUsedException e) {
            e.printStackTrace();
        }
        TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());

        try {
            transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    // all replica committed success
    @Test
    public void testCommitTransaction1() throws UserException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // check status is committed
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        // slave replay new state and compare catalog
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }

    // commit with only two replicas
    @Test
    public void testCommitTransactionWithOneFailed() throws UserException {
        TransactionState transactionState = null;
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, transTablets);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        FakeCatalog.setCatalog(masterCatalog);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel2,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        try {
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2, transTablets);
            Assert.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }
        // check replica version
        Partition testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        // the transaction not committed, so that catalog should be equal
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2, transTablets);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());
        // check partition next version
        tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        Replica replcia1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replcia2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replcia3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        assertEquals(CatalogTestUtil.testStartVersion, replcia1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        // last success version not change, because not published
        assertEquals(CatalogTestUtil.testStartVersion, replcia1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia3.getLastSuccessVersion());
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }

    @Test
    public void testCommitRoutineLoadTransaction(@Injectable TabletCommitInfo tabletCommitInfo,
            @Mocked KafkaConsumer kafkaConsumer,
            @Mocked EditLog editLog)
            throws UserException {
        FakeCatalog.setCatalog(masterCatalog);

        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", "default_cluster", 1L, 1L, "host:port",
                "topic");
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, "default_cluster", 20000,
                partitionIdToOffset);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK, new TxnCoordinator(TxnSourceType.BE, "be1"), routineLoadJob.getId(),
                Config.stream_load_default_timeout_second);
        transactionState.setTransactionStatus(TransactionStatus.PREPARE);
        masterTransMgr.getCallbackFactory().addCallback(routineLoadJob);
        // Deencapsulation.setField(transactionState, "txnStateChangeListener", routineLoadJob);
        Map<Long, TransactionState> idToTransactionState = Maps.newHashMap();
        idToTransactionState.put(1L, transactionState);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Map<Integer, Long> oldKafkaProgressMap = Maps.newHashMap();
        oldKafkaProgressMap.put(1, 0L);
        KafkaProgress oldkafkaProgress = new KafkaProgress();
        Deencapsulation.setField(oldkafkaProgress, "partitionIdToOffset", oldKafkaProgressMap);
        Deencapsulation.setField(routineLoadJob, "progress", oldkafkaProgress);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);

        TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = new TRLTaskTxnCommitAttachment();
        rlTaskTxnCommitAttachment.setId(new TUniqueId());
        rlTaskTxnCommitAttachment.setLoadedRows(100);
        rlTaskTxnCommitAttachment.setFilteredRows(1);
        rlTaskTxnCommitAttachment.setJobId(Deencapsulation.getField(routineLoadJob, "id"));
        rlTaskTxnCommitAttachment.setLoadSourceType(TLoadSourceType.KAFKA);
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        Map<Integer, Long> kafkaProgress = Maps.newHashMap();
        kafkaProgress.put(1, 100L); // start from 0, so rows number is 101, and consumed offset is 100
        tKafkaRLTaskProgress.setPartitionCmtOffset(kafkaProgress);
        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);
        TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment(rlTaskTxnCommitAttachment);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1), "idToRunningTransactionState", idToTransactionState);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(1L, Lists.newArrayList(testTable1), 1L, transTablets, txnCommitAttachment);

        Assert.assertEquals(Long.valueOf(101), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));
        Assert.assertEquals(Long.valueOf(1), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assert.assertEquals(Long.valueOf(101L), ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        // todo(ml): change to assert queue
        // Assert.assertEquals(1, routineLoadManager.getNeedScheduleTasksQueue().size());
        // Assert.assertNotEquals("label", routineLoadManager.getNeedScheduleTasksQueue().peek().getId());
    }

    @Test
    public void testCommitRoutineLoadTransactionWithErrorMax(@Injectable TabletCommitInfo tabletCommitInfo,
            @Mocked EditLog editLog,
            @Mocked KafkaConsumer kafkaConsumer)
            throws UserException {

        FakeCatalog.setCatalog(masterCatalog);
        
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", "default_cluster", 1L, 1L, "host:port", "topic");
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, "defualt_cluster", 20000,
                partitionIdToOffset);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK, new TxnCoordinator(TxnSourceType.BE, "be1"), routineLoadJob.getId(),
                Config.stream_load_default_timeout_second);
        transactionState.setTransactionStatus(TransactionStatus.PREPARE);
        masterTransMgr.getCallbackFactory().addCallback(routineLoadJob);
        Map<Long, TransactionState> idToTransactionState = Maps.newHashMap();
        idToTransactionState.put(1L, transactionState);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Map<Integer, Long> oldKafkaProgressMap = Maps.newHashMap();
        oldKafkaProgressMap.put(1, 0L);
        KafkaProgress oldkafkaProgress = new KafkaProgress();
        Deencapsulation.setField(oldkafkaProgress, "partitionIdToOffset", oldKafkaProgressMap);
        Deencapsulation.setField(routineLoadJob, "progress", oldkafkaProgress);
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);

        TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = new TRLTaskTxnCommitAttachment();
        rlTaskTxnCommitAttachment.setId(new TUniqueId());
        rlTaskTxnCommitAttachment.setLoadedRows(100);
        rlTaskTxnCommitAttachment.setFilteredRows(11);
        rlTaskTxnCommitAttachment.setJobId(Deencapsulation.getField(routineLoadJob, "id"));
        rlTaskTxnCommitAttachment.setLoadSourceType(TLoadSourceType.KAFKA);
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        Map<Integer, Long> kafkaProgress = Maps.newHashMap();
        kafkaProgress.put(1, 110L); // start from 0, so rows number is 111, consumed offset is 110
        tKafkaRLTaskProgress.setPartitionCmtOffset(kafkaProgress);
        rlTaskTxnCommitAttachment.setKafkaRLTaskProgress(tKafkaRLTaskProgress);
        TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment(rlTaskTxnCommitAttachment);

        RoutineLoadManager routineLoadManager = new RoutineLoadManager();
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1), "idToRunningTransactionState", idToTransactionState);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(1L, Lists.newArrayList(testTable1), 1L, transTablets, txnCommitAttachment);

        // current total rows and error rows will be reset after job pause, so here they should be 0.
        Assert.assertEquals(Long.valueOf(0), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));
        Assert.assertEquals(Long.valueOf(0), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assert.assertEquals(Long.valueOf(111L),
                ((KafkaProgress) routineLoadJob.getProgress()).getOffsetByPartition(1));
        // todo(ml): change to assert queue
        // Assert.assertEquals(0, routineLoadManager.getNeedScheduleTasksQueue().size());
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testFinishTransaction() throws UserException {
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId1);
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, errorReplicaIds);
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion + 1, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getId() == CatalogTestUtil.testReplicaId1) {
                assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
            } else {
                assertEquals(CatalogTestUtil.testStartVersion + 1, replica.getVersion());
            }

        }
        // slave replay new state and compare catalog
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }

    @Test
    public void testFinishTransactionWithOneFailed() throws UserException {
        TransactionState transactionState = null;
        Partition testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        Table testTable1 = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId, transTablets);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        // master finish the transaction failed
        FakeCatalog.setCatalog(masterCatalog);
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId2);
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Replica replica1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replica2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replica3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        errorReplicaIds = Sets.newHashSet();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        FakeCatalog.setCatalog(masterCatalog);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel2,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        try {
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2, transTablets);
            Assert.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2, transTablets);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion + 1, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        // master finish the transaction2
        errorReplicaIds = Sets.newHashSet();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId2, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replica1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replica2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        assertEquals(CatalogTestUtil.testStartVersion + 2, replica1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replica2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replica3.getLastSuccessVersion());
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }
}
