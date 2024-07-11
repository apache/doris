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
import org.apache.doris.catalog.OlapTable;
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
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TKafkaRLTaskProgress;
import org.apache.doris.thrift.TLoadSourceType;
import org.apache.doris.thrift.TRLTaskTxnCommitAttachment;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.SubTransactionState.SubTransactionType;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class GlobalTransactionMgrTest {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgrTest.class);

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(
            TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());
    protected static List<Long> allBackends = Lists.newArrayList(CatalogTestUtil.testBackendId1,
            CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId3);

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

        masterTransMgr = (GlobalTransactionMgr) masterEnv.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterEnv.getEditLog());

        slaveTransMgr = (GlobalTransactionMgr) slaveEnv.getGlobalTransactionMgr();
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
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e instanceof LabelAlreadyUsedException);
        }
    }

    // all replica committed success
    @Test
    public void testCommitTransaction() throws UserException {
        FakeEnv.setEnv(masterEnv);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // check status is committed
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check replica version
        checkVersion(testTable1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 2,
                CatalogTestUtil.testStartVersion);
        // slave replay new state and compare catalog
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    // commit with only two replicas
    @Test
    public void testCommitTransactionWithOneFailed() throws UserException {
        FakeEnv.setEnv(masterEnv);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        // txn1
        if (true) {
            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            // commit a transaction with 1,2 success
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2));
            // commit txn
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                    transTablets);
            checkVersion(testTable1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion);
            // check table1 replica3 last success and failed version
            checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartition1,
                    CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, CatalogTestUtil.testReplicaId3,
                    CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);
            // follower catalog replay the transaction
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }

        // txn2
        long transactionId2;
        if (true) {
            FakeEnv.setEnv(masterEnv);
            // commit another transaction with 1,3 success
            transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel2,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId3));
            try {
                masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                        transactionId2, transTablets);
                Assert.fail();
            } catch (TabletQuorumFailedException e) {
                TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1, transactionId2);
                // check status is prepare, because the commit failed
                Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
            }
            // check replica version
            checkVersion(testTable1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion);
            // the transaction not committed, so that catalog should be equal
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }

        // txn3: commit the second transaction with 1,2,3 success
        if (true) {
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                    transTablets);
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId2);
            // check status is committed
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // check partition visible and next version; check replica version
            checkVersion(testTable1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 3,
                    CatalogTestUtil.testStartVersion);
            // check replica last success and failed version: last success version not change, because not published
            checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartition1,
                    CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, CatalogTestUtil.testReplicaId1,
                    CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartition1,
                    CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, CatalogTestUtil.testReplicaId2,
                    CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1, CatalogTestUtil.testPartition1,
                    CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1, CatalogTestUtil.testReplicaId3,
                    CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);

            transactionState = fakeEditLog.getTransaction(transactionId2);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
    }

    @Test
    public void testCommitRoutineLoadTransaction(@Injectable TabletCommitInfo tabletCommitInfo,
            @Mocked KafkaConsumer kafkaConsumer,
            @Mocked EditLog editLog)
            throws UserException {
        FakeEnv.setEnv(masterEnv);
        List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port",
                "topic", UserIdentity.ADMIN);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, 20000, 0,
                partitionIdToOffset, false);
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
        ConcurrentMap<Integer, Long> oldKafkaProgressMap = Maps.newConcurrentMap();
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
        List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
        KafkaRoutineLoadJob routineLoadJob =
                new KafkaRoutineLoadJob(1L, "test", 1L, 1L, "host:port", "topic",
                        UserIdentity.ADMIN);
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 0L);
        KafkaTaskInfo routineLoadTaskInfo = new KafkaTaskInfo(UUID.randomUUID(), 1L, 20000, 0,
                partitionIdToOffset, false);
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
        ConcurrentMap<Integer, Long> oldKafkaProgressMap = Maps.newConcurrentMap();
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
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
        OlapTable testTable1 = (OlapTable) (masterEnv.getInternalCatalog()
                .getDbOrMetaException(CatalogTestUtil.testDbId1).getTableOrMetaException(CatalogTestUtil.testTableId1));
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        checkTableVersion(testTable1, 1, 2);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        // finish transaction
        Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
        DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                Lists.newArrayList(CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId3),
                transactionState.getTransactionId(), CatalogTestUtil.testTabletId1, 14);
        DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState, allBackends, keyToSuccessTablets);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                backendPartitions);
        transactionState = fakeEditLog.getTransaction(transactionId);
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        // check partition version
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 1, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getNextVersion());
        // check replica version
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

        checkTableVersion(testTable1, 2, 3);
        // slave replay new state and compare catalog
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    @Test
    public void testFinishTransactionWithOneFailed() throws UserException {
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        Partition testPartition = testTable1.getPartition(CatalogTestUtil.testPartition1);
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        Replica replica1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replica2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replica3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();

        // commit a transaction with 1,2 success
        if (true) {
            FakeEnv.setEnv(masterEnv);
            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel1,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2));
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId,
                    transTablets);

            // follower catalog replay the transaction
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

            // master finish the transaction failed
            FakeEnv.setEnv(masterEnv);
            Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
            // backend2 publish failed
            DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1),
                    transactionState.getTransactionId(), CatalogTestUtil.testTabletId1, 14);
            DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2),
                    keyToSuccessTablets);
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            Assert.assertTrue(partitionVisibleVersions.isEmpty());
            Assert.assertTrue(backendPartitions.isEmpty());
            // because after calling `finishTransaction`, the txn state is COMMITTED, not VISIBLE,
            // so all replicas' version are not changed.
            checkReplicaVersion(replica1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(replica2, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(replica3, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);

            // backend2 publish success
            Map<Long, Long> backend2SuccTablets = Maps.newHashMap();
            backend2SuccTablets.put(CatalogTestUtil.testTabletId1, 14L);
            transactionState.getPublishVersionTasks()
                    .get(CatalogTestUtil.testBackendId2).get(0).setSuccTablets(backend2SuccTablets);
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
            checkReplicaVersion(replica1, CatalogTestUtil.testStartVersion + 1, CatalogTestUtil.testStartVersion + 1,
                    -1);
            checkReplicaVersion(replica2, CatalogTestUtil.testStartVersion + 1, CatalogTestUtil.testStartVersion + 1,
                    -1);
            checkReplicaVersion(replica3, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);

            // follower catalog replay the transaction
            transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }

        // commit another transaction with 1,3 success
        long transactionId2;
        if (true) {
            FakeEnv.setEnv(masterEnv);
            transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1),
                    CatalogTestUtil.testTxnLabel2,
                    transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId3));
            try {
                masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1),
                        transactionId2, transTablets);
                Assert.fail();
            } catch (TabletQuorumFailedException e) {
                TransactionState transactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1,
                        transactionId2);
                // check status is prepare, because the commit failed
                Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
            }
        }

        // commit the second transaction with 1,2,3 success
        if (true) {
            List<TabletCommitInfo> transTablets = generateTabletCommitInfos(CatalogTestUtil.testTabletId1, allBackends);
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId2,
                    transTablets);
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId2);
            // check status is commit
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // check partition version
            checkPartitionVersion(testPartition, CatalogTestUtil.testStartVersion + 1,
                    CatalogTestUtil.testStartVersion + 3);

            // follower catalog replay the transaction
            transactionState = fakeEditLog.getTransaction(transactionId2);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

            // master finish the transaction2
            Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
            // backend2 publish failed
            DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                    allBackends, transactionState.getTransactionId(), CatalogTestUtil.testTabletId1,
                    CatalogTestUtil.testStartVersion + 3);
            DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState, allBackends, keyToSuccessTablets);
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId2, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
            checkReplicaVersion(replica1, CatalogTestUtil.testStartVersion + 2, CatalogTestUtil.testStartVersion + 2,
                    -1);
            checkReplicaVersion(replica2, CatalogTestUtil.testStartVersion + 2, CatalogTestUtil.testStartVersion + 2,
                    -1);
            checkReplicaVersion(replica3, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion + 1);
            // check partition version
            checkPartitionVersion(testPartition, CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion + 3);

            transactionState = fakeEditLog.getTransaction(transactionId2);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
    }

    @Test
    public void testTransactionWithSubTxn() throws UserException {
        FakeEnv.setEnv(masterEnv);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.INSERT_STREAMING, Config.stream_load_default_timeout_second);
        // LoadJobSourceType.INSERT_STREAMING does not write edit log
        Assert.assertNull(fakeEditLog.getTransaction(transactionId));
        // check transaction status in memory
        TransactionState transactionState = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1)
                .getTransactionState(transactionId);
        Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
    }

    // all replica committed success
    @Test
    public void testCommitTransactionWithSubTxn() throws UserException {
        FakeEnv.setEnv(masterEnv);
        Table table1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        Table table2 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId2);
        // begin txn
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // commit table1, table2, table1
        List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends),
                new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends),
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends));
        List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                subTransactionInfos);
        transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                .collect(Collectors.toList()));
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2), transactionId,
                subTransactionStates, 300000);
        // check status is committed
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check partition version
        checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 3,
                CatalogTestUtil.testStartVersion);
        checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 2,
                CatalogTestUtil.testStartVersion);
        // slave replay new state and compare catalog
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    /**
     * commit with only two replicas
     * txn1 -> commit success
     *   sub_txn1: table1, replica: 1, 2 success
     *   sub_txn2: table2, all replicas success
     *   sub_txn3: table1, all replicas success
     * txn2 -> commit failed
     *   sub_txn1: table1, all replicas success
     *   sub_txn2: table1, replica: 1, 3 success
     *   sub_txn3: table2, all replicas success
     * txn3 -> commit success
     *   sub_txn1: table1, all replicas success
     *   sub_txn2: table1, all replicas success
     *   sub_txn3: table2, all replicas success
     *   sub_txn4: table2, all replicas success
     */
    @Test
    public void testCommitTransactionWithSubTxnAndOneFailed() throws UserException {
        FakeEnv.setEnv(masterEnv);
        Table table1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        Table table2 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId2);
        Replica replica13 = getReplica(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testReplicaId3);
        // txn1
        if (true) {
            // check partition version
            checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1,
                    CatalogTestUtil.testStartVersion);
            checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                    CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1,
                    CatalogTestUtil.testStartVersion);
            // begin txn
            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            // commit table1 with 1, 2 success; table2 with all success; table1 with all success
            List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                            Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2)),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends));
            List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                    subTransactionInfos);
            // commit txn
            transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                    .collect(Collectors.toList()));
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2),
                    transactionId,
                    subTransactionStates, 300000);
            // check status is committed
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // check partition version
            checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 3,
                    CatalogTestUtil.testStartVersion);
            checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                    CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion);
            // check table1 replica3 last success and failed version
            checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);
            // follower catalog replay the transaction
            transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
        // txn2
        long transactionId;
        if (true) {
            FakeEnv.setEnv(masterEnv);
            transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel2, transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            // commit table1 with all success; table1 with 1, 3 success; table2 with all success
            List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                            Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId3)),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends));
            List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                    subTransactionInfos);
            // commit txn
            try {
                transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                        .collect(Collectors.toList()));
                masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2),
                        transactionId, subTransactionStates, 300000);
                Assert.fail();
            } catch (TabletQuorumFailedException e) {
                // check status is prepare
                Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
            }
            // check partition version
            checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 3,
                    CatalogTestUtil.testStartVersion);
            checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                    CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion);
            // check table1 replica3 last success and failed version
            checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);
            // the transaction not committed, so that catalog should be equal
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
        // txn3
        if (true) {
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            // commit table1 with all success; table1 with all success; table2 with all success; table2 with all success
            List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends));
            List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                    subTransactionInfos);
            // commit txn
            transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                    .collect(Collectors.toList()));
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2),
                    transactionId, subTransactionStates, 300000);
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // check partition version
            checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 5,
                    CatalogTestUtil.testStartVersion);
            checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                    CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 4,
                    CatalogTestUtil.testStartVersion);
            // check table1 replica last success and failed version: last success version not change,
            // because not published
            if (true) {
                checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                        CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1,
                        CatalogTestUtil.testReplicaId1, CatalogTestUtil.testStartVersion,
                        CatalogTestUtil.testStartVersion, -1);
                checkReplicaVersion(CatalogTestUtil.testDbId1, CatalogTestUtil.testTableId1,
                        CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1, CatalogTestUtil.testTabletId1,
                        CatalogTestUtil.testReplicaId2, CatalogTestUtil.testStartVersion,
                        CatalogTestUtil.testStartVersion, -1);
                checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                        CatalogTestUtil.testStartVersion + 1);
            }
            // check table2 replica last success and failed version
            if (true) {
                Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                        .getTableOrMetaException(CatalogTestUtil.testTableId2)
                        .getPartition(CatalogTestUtil.testPartition2);
                Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId2)
                        .getTablet(CatalogTestUtil.testTabletId2);
                for (Replica replica : tablet.getReplicas()) {
                    Assert.assertEquals(-1, replica.getLastFailedVersion());
                    Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getLastSuccessVersion());
                }
            }

            transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
    }

    /**
     * txn -> TabletQuorumFailedException
     *   sub_txn1: table1, replica: 1, 2 success
     *   sub_txn2: table1, replica: 1, 3 success
     *   sub_txn3: table1, replica: 2, 3 success
     */
    @Test
    public void testCommitTransactionWithSubTxnAndReplicaFailed() throws UserException {
        FakeEnv.setEnv(masterEnv);
        Table table1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        // check partition version
        checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 1,
                CatalogTestUtil.testStartVersion);
        // begin txn
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // commit table1 with 1, 2 success; table2 with all success; table1 with all success
        List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                        Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2)),
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                        Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId3)),
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                        Lists.newArrayList(CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId2)));
        List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                subTransactionInfos);
        // commit txn
        try {
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1),
                    transactionId,
                    subTransactionStates, 300000);
        } catch (TabletQuorumFailedException e) {
            // check status is prepare, because the commit failed
            Assert.assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        }
    }

    @Test
    public void testFinishTransactionWithSubTransaction() throws UserException {
        FakeEnv.setEnv(masterEnv);
        OlapTable table1 = (OlapTable) (masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1));
        OlapTable table2 = (OlapTable) (masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId2));
        // check partition version
        checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 1,
                CatalogTestUtil.testStartVersion);
        checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 1,
                CatalogTestUtil.testStartVersion);
        // begin txn
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        // commit table1, table2, table1
        List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends),
                new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends),
                new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends));
        List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                subTransactionInfos);
        // commit txn
        transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                .collect(Collectors.toList()));
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2), transactionId,
                subTransactionStates, 300000);
        // check status is committed
        Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        // check partition version
        checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 3,
                CatalogTestUtil.testStartVersion);
        checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion + 2,
                CatalogTestUtil.testStartVersion);
        // slave replay new state and compare catalog
        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        checkTableVersion(table1, 1, 2);
        checkTableVersion(table2, 1, 2);

        // finish transaction
        Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
        DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets, allBackends,
                subTransactionStates.get(0).getSubTransactionId(), CatalogTestUtil.testTabletId1, 14);
        DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets, allBackends,
                subTransactionStates.get(1).getSubTransactionId(), CatalogTestUtil.testTabletId2, 13);
        DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                Lists.newArrayList(CatalogTestUtil.testBackendId2, CatalogTestUtil.testBackendId3),
                subTransactionStates.get(2).getSubTransactionId(), CatalogTestUtil.testTabletId1, 15);
        DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState, allBackends, keyToSuccessTablets);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                backendPartitions);
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());

        // check table1 partition version
        Partition testPartition = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1).getPartition(CatalogTestUtil.testPartition1);
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getVisibleVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());
        // check table1 replica version, table1 has 3 replicas
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        for (Replica replica : tablet.getReplicas()) {
            if (replica.getId() == CatalogTestUtil.testReplicaId1) {
                // TODO replica version is [CatalogTestUtil.testStartVersion + 1] is an improvement
                Assert.assertEquals(CatalogTestUtil.testStartVersion, replica.getVersion());
            } else {
                Assert.assertEquals(CatalogTestUtil.testStartVersion + 2, replica.getVersion());
            }
        }
        // check table2 version, table2 has 1 replicas
        checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion + 1,
                CatalogTestUtil.testStartVersion + 2, CatalogTestUtil.testStartVersion + 1);
        checkTableVersion(table1, 2, 3);
        checkTableVersion(table2, 2, 3);

        // slave replay new state and compare catalog
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
    }

    /**
     * commit with only two replicas
     * txn1 -> commit and publish success
     *   sub_txn1: table1, replica: 1, 2 success; replica1 publish failed and then succeed
     *   sub_txn2: table2, all replicas success
     *   sub_txn3: table1, all replicas success
     */
    @Test
    public void testFinishTransactionWithSubTransactionAndOneFailed() throws UserException {
        FakeEnv.setEnv(masterEnv);
        // table1
        OlapTable table1 = (OlapTable) (masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1));
        Partition partition1 = table1.getPartition(CatalogTestUtil.testPartition1);
        Tablet tablet1 = partition1.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        Replica replica11 = tablet1.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replica12 = tablet1.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replica13 = tablet1.getReplicaById(CatalogTestUtil.testReplicaId3);
        // table2
        OlapTable table2 = (OlapTable) (masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId2));
        Partition partition2 = table2.getPartition(CatalogTestUtil.testPartition2);
        Tablet tablet2 = partition2.getIndex(CatalogTestUtil.testIndexId2).getTablet(CatalogTestUtil.testTabletId2);
        Replica replica21 = tablet2.getReplicaById(CatalogTestUtil.testReplicaId4);
        // txn1
        if (true) {
            // begin txn
            long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                    Lists.newArrayList(CatalogTestUtil.testTableId1), CatalogTestUtil.testTxnLabel1, transactionSource,
                    LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
            TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
            // commit table1 with 1, 2 success; table2 with all success; table1 with all success
            List<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1,
                            Lists.newArrayList(CatalogTestUtil.testBackendId1, CatalogTestUtil.testBackendId2)),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends));
            List<SubTransactionState> subTransactionStates = generateSubTransactionStates(transactionState,
                    subTransactionInfos);
            // commit txn
            transactionState.setSubTxnIds(subTransactionStates.stream().map(SubTransactionState::getSubTransactionId)
                    .collect(Collectors.toList()));
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2),
                    transactionId, subTransactionStates, 300000);
            // check status is committed
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // check partition version
            checkVersion(table1, CatalogTestUtil.testPartition1, CatalogTestUtil.testIndexId1,
                    CatalogTestUtil.testTabletId1, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 3,
                    CatalogTestUtil.testStartVersion);
            checkVersion(table2, CatalogTestUtil.testPartition2, CatalogTestUtil.testIndexId2,
                    CatalogTestUtil.testTabletId2, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 2,
                    CatalogTestUtil.testStartVersion);
            // check table1 replica3 last success and failed version
            checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);

            // follower catalog replay the transaction
            transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));

            // master finish the transaction failed
            FakeEnv.setEnv(masterEnv);
            Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
            // backend2, backend3 publish failed
            DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                    Lists.newArrayList(CatalogTestUtil.testBackendId1),
                    subTransactionStates.get(0).getSubTransactionId(), CatalogTestUtil.testTabletId1, 14);
            DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets, allBackends,
                    subTransactionStates.get(1).getSubTransactionId(), CatalogTestUtil.testTabletId2, 13);
            DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets, allBackends,
                    subTransactionStates.get(2).getSubTransactionId(), CatalogTestUtil.testTabletId1, 15);
            DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState, allBackends, keyToSuccessTablets);
            LOG.info("publish tasks: {}", transactionState.getPublishVersionTasks());
            // finish transaction
            Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
            Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
            // because after calling `finishTransaction`, the txn state is COMMITTED, not VISIBLE,
            // so all replicas' version are not changed.
            checkReplicaVersion(replica11, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(replica12, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);
            checkReplicaVersion(replica21, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion, -1);
            checkTableVersion(table1, 1, 2);
            checkTableVersion(table2, 1, 2);

            // backend2 publish success (tabletId to version map)
            List<PublishVersionTask> publishVersionTasks = transactionState.getPublishVersionTasks()
                    .get(CatalogTestUtil.testBackendId2).stream()
                    .filter(t -> t.getTransactionId() == subTransactionStates.get(0).getSubTransactionId())
                    .collect(Collectors.toList());
            Assert.assertEquals(1, publishVersionTasks.size());
            PublishVersionTask publishVersionTask = publishVersionTasks.get(0);
            publishVersionTask.setSuccTablets(ImmutableMap.of(CatalogTestUtil.testTabletId1, 100L));
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
            checkReplicaVersion(replica11, CatalogTestUtil.testStartVersion + 2, CatalogTestUtil.testStartVersion + 2,
                    -1);
            checkReplicaVersion(replica12, CatalogTestUtil.testStartVersion + 2, CatalogTestUtil.testStartVersion + 2,
                    -1);
            checkReplicaVersion(replica13, CatalogTestUtil.testStartVersion, CatalogTestUtil.testStartVersion,
                    CatalogTestUtil.testStartVersion + 1);
            checkReplicaVersion(replica21, CatalogTestUtil.testStartVersion + 1, CatalogTestUtil.testStartVersion + 1,
                    -1);
            checkTableVersion(table1, 2, 3);
            checkTableVersion(table2, 2, 3);

            // follower catalog replay the transaction
            transactionState = fakeEditLog.getTransaction(transactionId);
            FakeEnv.setEnv(slaveEnv);
            slaveTransMgr.replayUpsertTransactionState(transactionState);
            Assert.assertTrue(CatalogTestUtil.compareCatalog(masterEnv, slaveEnv));
        }
    }

    protected static List<TabletCommitInfo> generateTabletCommitInfos(long tabletId, List<Long> backendIds) {
        return backendIds.stream().map(backendId -> new TabletCommitInfo(tabletId, backendId))
                .collect(Collectors.toList());
    }

    protected static List<TTabletCommitInfo> generateTTabletCommitInfos(long tabletId, List<Long> backendIds) {
        return backendIds.stream().map(backendId -> new TTabletCommitInfo(tabletId, backendId))
                .collect(Collectors.toList());
    }

    protected static class SubTransactionInfo {
        Table table;
        long tabletId;
        List<Long> backends;
        long subTxnId = -1;

        protected SubTransactionInfo(Table table, long tabletId, List<Long> backends) {
            this.table = table;
            this.tabletId = tabletId;
            this.backends = backends;
        }

        protected SubTransactionInfo(Table table, long tabletId, List<Long> backends, long subTxnId) {
            this(table, tabletId, backends);
            this.subTxnId = subTxnId;
        }
    }

    private static List<SubTransactionState> generateSubTransactionStates(TransactionState transactionState,
            List<SubTransactionInfo> subTransactionInfos) {
        return generateSubTransactionStates(masterTransMgr, transactionState, subTransactionInfos);
    }

    protected static List<SubTransactionState> generateSubTransactionStates(GlobalTransactionMgr masterTransMgr,
            TransactionState transactionState, List<SubTransactionInfo> subTransactionInfos) {
        List<SubTransactionState> subTransactionStates = new ArrayList<>();
        for (int i = 0; i < subTransactionInfos.size(); i++) {
            SubTransactionInfo subTransactionInfo = subTransactionInfos.get(i);
            Table table = subTransactionInfo.table;
            long tabletId = subTransactionInfo.tabletId;
            List<Long> backends = subTransactionInfo.backends;
            long subTxnId = i == 0 ? transactionState.getTransactionId()
                    : (subTransactionInfo.subTxnId != -1 ? subTransactionInfo.subTxnId
                            : masterTransMgr.getNextTransactionId());
            boolean addTableId = i == 0 ? false : true;
            subTransactionStates.add(generateSubTransactionState(transactionState, subTxnId, table,
                    tabletId, backends, addTableId));
        }
        transactionState.setSubTxnIds(
                subTransactionInfos.stream().map(sub -> sub.subTxnId).collect(Collectors.toList()));
        LOG.info("sub txn states={}", subTransactionInfos);
        return subTransactionStates;
    }

    private static SubTransactionState generateSubTransactionState(TransactionState transactionState,
            long subTransactionId, Table table, long tabletId, List<Long> backendIds, boolean addTableId) {
        if (addTableId) {
            transactionState.addTableId(table.getId());
        }
        return new SubTransactionState(subTransactionId, table, generateTTabletCommitInfos(tabletId, backendIds),
                SubTransactionType.INSERT);
    }

    private void checkTableVersion(OlapTable olapTable, long visibleVersion, long nextVersion) {
        LOG.info("table={}, visibleVersion={}, nextVersion={}", olapTable.getName(), olapTable.getVisibleVersion(),
                olapTable.getNextVersion());
        Assert.assertEquals(visibleVersion, olapTable.getVisibleVersion());
        Assert.assertEquals(nextVersion, olapTable.getNextVersion());
    }

    private void checkPartitionVersion(Partition partition, long visibleVersion, long nextVersion) {
        LOG.info("partition={}, visibleVersion={}, nextVersion={}, committedVersion={}", partition.getName(),
                partition.getVisibleVersion(), partition.getNextVersion(), partition.getCommittedVersion());
        Assert.assertEquals(visibleVersion, partition.getVisibleVersion());
        Assert.assertEquals(nextVersion, partition.getNextVersion());
    }

    // check partition visible and next version; check replica version
    private void checkVersion(Table table, String partitionName, long indexId, long tabletId,
            long partitionVisibleVersion, long partitionNextVersion, long replicaVersion) {
        Partition partition = table.getPartition(partitionName);
        // check partition version
        checkPartitionVersion(partition, partitionVisibleVersion, partitionNextVersion);
        // check partition next version
        Tablet tablet = partition.getIndex(indexId).getTablet(tabletId);
        for (Replica replica : tablet.getReplicas()) {
            LOG.info("table={}, partition={}, index={}, tablet={}, replica={}, version={}, "
                            + "last_success_version={}, last_failed_version={}", table.getName(), partition.getName(),
                    indexId, tabletId, replica.getId(), replica.getVersion(), replica.getLastSuccessVersion(),
                    replica.getLastFailedVersion());
            Assert.assertEquals(replicaVersion, replica.getVersion());
        }
    }

    private Replica getReplica(long dbId, long tableId, String partitionName, long indexId, long tabletId,
            long replicaId) throws MetaNotFoundException {
        return masterEnv.getInternalCatalog().getDbOrMetaException(dbId).getTableOrMetaException(tableId)
                .getPartition(partitionName).getIndex(indexId).getTablet(tabletId).getReplicaById(replicaId);
    }

    // check replica last success and failed version
    private void checkReplicaVersion(long dbId, long tableId, String partitionName, long indexId, long tabletId,
            long replicaId, long version, long lastSuccessVersion, long lastFailedVersion) throws MetaNotFoundException {
        Replica replica = getReplica(dbId, tableId, partitionName, indexId, tabletId, replicaId);
        checkReplicaVersion(replica, version, lastSuccessVersion, lastFailedVersion);
    }

    private void checkReplicaVersion(Replica replica, long version, long lastSuccessVersion, long lastFailedVersion) {
        Assert.assertEquals(version, replica.getVersion());
        Assert.assertEquals(lastSuccessVersion, replica.getLastSuccessVersion());
        Assert.assertEquals(lastFailedVersion, replica.getLastFailedVersion());
    }
}

