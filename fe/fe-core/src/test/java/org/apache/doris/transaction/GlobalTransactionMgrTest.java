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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.KafkaProgress;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.KafkaTaskInfo;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.RoutineLoadStatistic;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class GlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(
            TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterEnv = CatalogTestUtil.createTestCatalog();
        slaveEnv = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        masterTransMgr = masterEnv.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterEnv.getEditLog());

        slaveTransMgr = slaveEnv.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveEnv.getEditLog());
    }

    @Test
    public void testBeginTransaction() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
        FakeEnv.setEnv(masterEnv);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId);
        Assert.assertNotNull(transactionState);
        Assert.assertEquals(transactionId, transactionState.getTransactionId());
        Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        Assert.assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        Assert.assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());
    }

    @Test
    public void testBeginTransactionWithSameLabel() throws LabelAlreadyUsedException, AnalysisException,
            BeginTransactionException, DuplicatedRequestException {
        FakeEnv.setEnv(masterEnv);
        long transactionId = 0;
        try {
            transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        } catch (AnalysisException | LabelAlreadyUsedException e) {
            e.printStackTrace();
        } catch (MetaNotFoundException e) {
            e.printStackTrace();
        } catch (QuotaExceedException e) {
            e.printStackTrace();
        }
        TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId);
        Assert.assertNotNull(transactionState);
        Assert.assertEquals(transactionId, transactionState.getTransactionId());
        Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        Assert.assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        Assert.assertEquals(transactionSource.toString(), transactionState.getCoordinator().toString());

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
        FakeEnv.setEnv(masterEnv);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 =
                new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 =
                new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 =
                new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets, null);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // check status is committed
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        // slave replay new state and compare catalog
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    // commit with only two replicas
    @Test
    public void testCommitTransactionWithOneFailed() throws UserException {
        TransactionState transactionState = null;
        FakeEnv.setEnv(masterEnv);
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
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets, null);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

        FakeEnv.setEnv(masterEnv);
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
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                    transTablets, null);
            Assert.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }
        // check replica version
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        // the transaction not committed, so that catalog should be equal
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                transTablets, null);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());
        // check partition next version
        tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
        }
        Replica replcia1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replcia2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replcia3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia3.getVersion());
        Assert.assertEquals(-1, replcia1.getLastFailedVersion());
        Assert.assertEquals(-1, replcia2.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        // last success version not change, because not published
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia1.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia2.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replcia3.getLastSuccessVersion());
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    @Test
    public void testCommitRoutineLoadTransaction(@Injectable TabletCommitInfo tabletCommitInfo,
            @Mocked KafkaConsumer kafkaConsumer,
            @Mocked EditLog editLog)
            throws UserException {
        FakeEnv.setEnv(masterEnv);

        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port",
                "topic", UserIdentity.ADMIN);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK,
                new TxnCoordinator(TxnSourceType.BE, 0, "be1", System.currentTimeMillis()),
                routineLoadJob.getId(),
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
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db", "table");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1), "idToRunningTransactionState", idToTransactionState);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(1L, Lists.newArrayList(testTable1), 1L, transTablets, txnCommitAttachment);
        RoutineLoadStatistic jobStatistic =  Deencapsulation.getField(routineLoadJob, "jobStatistic");

        Assert.assertEquals(Long.valueOf(101), Deencapsulation.getField(jobStatistic, "currentTotalRows"));
        Assert.assertEquals(Long.valueOf(1), Deencapsulation.getField(jobStatistic, "currentErrorRows"));
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

        FakeEnv.setEnv(masterEnv);

        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);

        KafkaRoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port", "topic",
                        UserIdentity.ADMIN);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, 20000,
                partitionIdToOffset, false, -1, false);
        Deencapsulation.setField(routineLoadTaskInfo, "txnId", 1L);
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        TransactionState transactionState = new TransactionState(1L, Lists.newArrayList(1L), 1L, "label", null,
                LoadJobSourceType.ROUTINE_LOAD_TASK,
                new TxnCoordinator(TxnSourceType.BE, 0, "be1", System.currentTimeMillis()),
                routineLoadJob.getId(),
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
        routineLoadManager.addRoutineLoadJob(routineLoadJob, "db", "table");

        Deencapsulation.setField(masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1), "idToRunningTransactionState", idToTransactionState);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(1L, Lists.newArrayList(testTable1), 1L, transTablets, txnCommitAttachment);

        // current total rows and error rows will be reset after job pause, so here they should be 0.
        RoutineLoadStatistic jobStatistic =  Deencapsulation.getField(routineLoadJob, "jobStatistic");
        Assert.assertEquals(Long.valueOf(0), Deencapsulation.getField(jobStatistic, "currentTotalRows"));
        Assert.assertEquals(Long.valueOf(0), Deencapsulation.getField(jobStatistic, "currentErrorRows"));
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
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets, null);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState,
                Lists.newArrayList(CatalogTestUtil.testBackendId1,
                        CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId3));
        transactionState.getPublishVersionTasks()
                .get(CatalogTestUtil.testBackendId1).getErrorTablets().add(CatalogTestUtil.testTabletId1);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId,
                partitionVisibleVersions, backendPartitions);
        transactionState = fakeEditLog.getTransaction(transactionId);
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        // check replica version
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check partition next version
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getId() == CatalogTestUtil.testReplicaId1) {
                Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
            } else {
                Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica.getVersion());
            }

        }

        Assert.assertEquals(ImmutableMap.of(testPartition.getId(), CatalogTestUtil.testStartVersion + 1),
                partitionVisibleVersions);
        Set<Long> partitionIds = Sets.newHashSet(testPartition.getId());
        Assert.assertEquals(partitionIds, backendPartitions.get(CatalogTestUtil.testBackendId1));
        Assert.assertEquals(partitionIds, backendPartitions.get(CatalogTestUtil.testBackendId2));
        Assert.assertEquals(partitionIds, backendPartitions.get(CatalogTestUtil.testBackendId3));

        // slave replay new state and compare catalog
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    @Test
    public void testFinishTransactionWithOneFailed() throws UserException {
        TransactionState transactionState = null;
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        FakeEnv.setEnv(masterEnv);
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
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets, null);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

        // master finish the transaction failed
        FakeEnv.setEnv(masterEnv);
        DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState,
                Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2));

        // backend2 publish failed
        transactionState.getPublishVersionTasks()
                .get(CatalogTestUtil.testBackendId2).getErrorTablets().add(CatalogTestUtil.testTabletId1);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId,
                partitionVisibleVersions, backendPartitions);
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Assert.assertTrue(partitionVisibleVersions.isEmpty());
        Assert.assertTrue(backendPartitions.isEmpty());
        Replica replica1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replica2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replica3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        // because after calling `finishTransaction`, the txn state is COMMITTED, not VISIBLE,
        // so all replicas' version are not changed.
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        // backend2 publish success
        Map<Long, Long> backend2SuccTablets = Maps.newHashMap();
        backend2SuccTablets.put(CatalogTestUtil.testTabletId1, 0L);
        transactionState.getPublishVersionTasks()
                .get(CatalogTestUtil.testBackendId2).setSuccTablets(backend2SuccTablets);
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId,
                partitionVisibleVersions, backendPartitions);
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

        FakeEnv.setEnv(masterEnv);
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
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                    transTablets, null);
            Assert.fail();
        } catch (TabletQuorumFailedException e) {
            transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId2);
            // check status is prepare, because the commit failed
            Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                transTablets, null);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is commit
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

        // master finish the transaction2
        DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState,
                Lists.newArrayList(CatalogTestUtil.testBackendId1,
                        CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId3));
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId2,
                partitionVisibleVersions, backendPartitions);
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, replica3.getLastFailedVersion());

        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica1.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica2.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica3.getLastSuccessVersion());
        // check partition version
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }
}
