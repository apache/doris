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

import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.transaction.GlobalTransactionMgrTest.SubTransactionInfo;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DatabaseTransactionMgrTest {
    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgrTest.class);
    private List<Long> allBackends = GlobalTransactionMgrTest.allBackends;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;
    private static Map<String, Long> LabelToTxnId;

    private TransactionState.TxnCoordinator transactionSource = new TransactionState.TxnCoordinator(
            TransactionState.TxnSourceType.FE, 0, "localfe", System.currentTimeMillis());

    public static void setTransactionFinishPublish(TransactionState transactionState, List<Long> backendIds) {
        setTransactionFinishPublish(transactionState, backendIds, new HashMap<>());
    }

    public static void setTransactionFinishPublish(TransactionState transactionState, List<Long> backendIds,
            Map<String, Map<Long, Long>> keyToSuccessTablets) {
        if (transactionState.getSubTxnIds() != null) {
            LOG.info("txnId={}, subTxnIdToTableCommitInfo={}", transactionState.getTransactionId(),
                    transactionState.getSubTxnIdToTableCommitInfo());
            /** the same with {@link PublishVersionDaemon#publishVersion} */
            for (Entry<Long, TableCommitInfo> entry : transactionState.getSubTxnIdToTableCommitInfo().entrySet()) {
                long subTxnId = entry.getKey();
                List<TPartitionVersionInfo> partitionVersionInfos = entry.getValue().generateTPartitionVersionInfos();
                LOG.info("add publish task, txnId={}, subTxnId={}, backends={}, partitionVersionInfos={}",
                        transactionState.getTransactionId(), subTxnId, backendIds, partitionVersionInfos);
                for (Long backendId : backendIds) {
                    PublishVersionTask task = new PublishVersionTask(backendId, subTxnId,
                            transactionState.getDbId(), partitionVersionInfos, System.currentTimeMillis());
                    task.setFinished(true);
                    task.setSuccTablets(
                            keyToSuccessTablets.getOrDefault(backendId + "-" + subTxnId, new HashMap<>()));
                    transactionState.addPublishVersionTask(backendId, task);
                }
            }
        } else {
            for (long backendId : backendIds) {
                PublishVersionTask task = new PublishVersionTask(backendId, transactionState.getTransactionId(),
                        transactionState.getDbId(), null, System.currentTimeMillis());
                task.setFinished(true);
                task.setSuccTablets(
                        keyToSuccessTablets.getOrDefault(backendId + "-" + transactionState.getTransactionId(),
                                new HashMap<>()));
                transactionState.addPublishVersionTask(backendId, task);
            }
        }
    }

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, UserException {
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

        LabelToTxnId = addTransactionToTransactionMgr();
    }

    public Map<String, Long> addTransactionToTransactionMgr() throws UserException {
        Map<String, Long> labelToTxnId = Maps.newHashMap();
        FakeEnv.setEnv(masterEnv);

        // txn1
        long transactionId1 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel1,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit and publish transaction
        List<TabletCommitInfo> transTablets = GlobalTransactionMgrTest.generateTabletCommitInfos(
                CatalogTestUtil.testTabletId1, allBackends);
        Table testTable1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(testTable1), transactionId1,
                transTablets);
        TransactionState transactionState1 = fakeEditLog.getTransaction(transactionId1);
        Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
        DatabaseTransactionMgrTest.setSuccessTablet(keyToSuccessTablets,
                allBackends, transactionState1.getTransactionId(), CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testStartVersion + 1);
        setTransactionFinishPublish(transactionState1, allBackends, keyToSuccessTablets);
        Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
        Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
        masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId1,
                partitionVisibleVersions, backendPartitions);
        labelToTxnId.put(CatalogTestUtil.testTxnLabel1, transactionId1);

        // txn 2, 3, 4
        TransactionState.TxnCoordinator beTransactionSource = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.BE, 0, "be1", System.currentTimeMillis());
        long transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel2,
                beTransactionSource,
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, Config.stream_load_default_timeout_second);
        long transactionId3 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel3,
                beTransactionSource,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, Config.stream_load_default_timeout_second);
        long transactionId4 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(CatalogTestUtil.testTableId1),
                CatalogTestUtil.testTxnLabel4,
                beTransactionSource,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, Config.stream_load_default_timeout_second);
        labelToTxnId.put(CatalogTestUtil.testTxnLabel2, transactionId2);
        labelToTxnId.put(CatalogTestUtil.testTxnLabel3, transactionId3);
        labelToTxnId.put(CatalogTestUtil.testTxnLabel4, transactionId4);

        FakeEnv.setEnv(slaveEnv);
        slaveTransMgr.replayUpsertTransactionState(transactionState1);
        return labelToTxnId;
    }

    @Test
    public void testNormal() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Assert.assertEquals(4, masterDbTransMgr.getTransactionNum());
        Assert.assertEquals(3, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(1, masterDbTransMgr.getFinishedTxnNums());
        DatabaseTransactionMgr slaveDbTransMgr = slaveTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Assert.assertEquals(1, slaveDbTransMgr.getTransactionNum());
        Assert.assertEquals(1, slaveDbTransMgr.getFinishedTxnNums());

        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel2).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel3).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel4).size());

        Long txnId1 = masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1).iterator().next();
        Assert.assertEquals(txnId1, LabelToTxnId.get(CatalogTestUtil.testTxnLabel1));
        TransactionState transactionState1 = masterDbTransMgr.getTransactionState(
                LabelToTxnId.get(CatalogTestUtil.testTxnLabel1));
        Assert.assertEquals(txnId1.longValue(), transactionState1.getTransactionId());
        Assert.assertEquals(TransactionStatus.VISIBLE, transactionState1.getTransactionStatus());

        Long txnId2 = masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel2).iterator().next();
        Assert.assertEquals(txnId2, LabelToTxnId.get(CatalogTestUtil.testTxnLabel2));
        TransactionState transactionState2 = masterDbTransMgr.getTransactionState(txnId2);
        Assert.assertEquals(txnId2.longValue(), transactionState2.getTransactionId());
        Assert.assertEquals(TransactionStatus.PREPARE, transactionState2.getTransactionStatus());
    }

    @Test
    public void testAbortTransaction() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);

        long txnId2 = LabelToTxnId.get(CatalogTestUtil.testTxnLabel2);
        masterDbTransMgr.abortTransaction(txnId2, "test abort transaction", null);
        Assert.assertEquals(2, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(2, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(4, masterDbTransMgr.getTransactionNum());

        long txnId3 = LabelToTxnId.get(CatalogTestUtil.testTxnLabel3);
        masterDbTransMgr.abortTransaction(txnId3, "test abort transaction", null);
        Assert.assertEquals(1, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(3, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(4, masterDbTransMgr.getTransactionNum());
    }

    @Test
    public void testAbortTransactionWithNotFoundException() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);

        long txnId1 = LabelToTxnId.get(CatalogTestUtil.testTxnLabel1);
        expectedEx.expect(UserException.class);
        expectedEx.expectMessage("transaction not found");
        masterDbTransMgr.abortTransaction(txnId1, "test abort transaction", null);
    }

    @Test
    public void testGetTransactionIdByCoordinateBe() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        List<Pair<Long, Long>> transactionInfoList = masterDbTransMgr.getPrepareTransactionIdByCoordinateBe(0, "be1", 10);
        Assert.assertEquals(3, transactionInfoList.size());
        Assert.assertEquals(CatalogTestUtil.testDbId1, transactionInfoList.get(0).first.longValue());
        Assert.assertEquals(TransactionStatus.PREPARE,
                masterDbTransMgr.getTransactionState(transactionInfoList.get(0).second).getTransactionStatus());
    }

    @Test
    public void testGetSingleTranInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        long txnId = LabelToTxnId.get(CatalogTestUtil.testTxnLabel1);
        List<List<String>> singleTranInfos = masterDbTransMgr.getSingleTranInfo(CatalogTestUtil.testDbId1, txnId);
        Assert.assertEquals(1, singleTranInfos.size());
        List<String> txnInfo = singleTranInfos.get(0);
        Assert.assertEquals("1000", txnInfo.get(0));
        Assert.assertEquals(CatalogTestUtil.testTxnLabel1, txnInfo.get(1));
        Assert.assertEquals("FE: localfe", txnInfo.get(2));
        Assert.assertEquals("VISIBLE", txnInfo.get(3));
        Assert.assertEquals("FRONTEND", txnInfo.get(4));
        long currentTime = System.currentTimeMillis();
        Assert.assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(5)));
        Assert.assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(6)));
        Assert.assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(7)));
        Assert.assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(8)));
        Assert.assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(9)));
        Assert.assertEquals("", txnInfo.get(10));
        Assert.assertEquals("0", txnInfo.get(11));
        Assert.assertEquals("-1", txnInfo.get(12));
        Assert.assertEquals(String.valueOf(Config.stream_load_default_timeout_second * 1000), txnInfo.get(13));
    }

    @Test
    public void testRemoveExpiredTxns() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Config.label_keep_max_second = -1;
        long currentMillis = System.currentTimeMillis();
        masterDbTransMgr.removeUselessTxns(currentMillis);
        Assert.assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(3, masterDbTransMgr.getTransactionNum());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1));
    }

    @Test
    public void testRemoveOverLimitTxns() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Config.label_num_threshold = 0;
        masterDbTransMgr.removeUselessTxns(System.currentTimeMillis());
        Assert.assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(3, masterDbTransMgr.getTransactionNum());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1));
    }

    @Test
    public void testGetTableTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Long txnId = LabelToTxnId.get(CatalogTestUtil.testTxnLabel1);
        List<List<Comparable>> tableTransInfos = masterDbTransMgr.getTableTransInfo(txnId);
        Assert.assertEquals(1, tableTransInfos.size());
        List<Comparable> tableTransInfo = tableTransInfos.get(0);
        Assert.assertEquals(2, tableTransInfo.size());
        Assert.assertEquals(2L, tableTransInfo.get(0));
        Assert.assertEquals("3", tableTransInfo.get(1));
    }

    @Test
    public void testGetPartitionTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Long txnId = LabelToTxnId.get(CatalogTestUtil.testTxnLabel1);
        List<List<Comparable>> partitionTransInfos = masterDbTransMgr.getPartitionTransInfo(txnId,
                CatalogTestUtil.testTableId1);
        Assert.assertEquals(1, partitionTransInfos.size());
        List<Comparable> partitionTransInfo = partitionTransInfos.get(0);
        Assert.assertEquals(2, partitionTransInfo.size());
        Assert.assertEquals(3L, partitionTransInfo.get(0));
        Assert.assertEquals(13L, partitionTransInfo.get(1));
    }

    @Test
    public void testDeleteTransaction() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        long txnId = LabelToTxnId.get(CatalogTestUtil.testTxnLabel1);
        TransactionState transactionState = masterDbTransMgr.getTransactionState(txnId);
        masterDbTransMgr.replayDeleteTransaction(transactionState);
        Assert.assertEquals(3, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(3, masterDbTransMgr.getTransactionNum());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1));
    }

    @Test
    public void testSubTransaction() throws UserException {
        addSubTransaction();
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(
                CatalogTestUtil.testDbId1);
        Assert.assertEquals(4 + 4, masterDbTransMgr.getTransactionNum());
        Assert.assertEquals(3 + 2, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(1 + 2, masterDbTransMgr.getFinishedTxnNums());
        // LoadJobSourceType.INSERT_STREAMING does not write edit log when begin txn
        DatabaseTransactionMgr slaveDbTransMgr = slaveTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Assert.assertEquals(1, slaveDbTransMgr.getTransactionNum());

        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel5).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel6).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel7).size());
        Assert.assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel8).size());

        // test get transaction state by subTxnId
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(
                LabelToTxnId.get(CatalogTestUtil.testTxnLabel6));
        long transactionId6 = transactionState6.getTransactionId();
        long subTransactionId3 = transactionState6.getSubTxnIds().get(2);
        TransactionState subTransactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1,
                subTransactionId3);
        Assert.assertEquals(transactionState6, subTransactionState);
        // test show transaction state command
        List<List<String>> singleTranInfos = masterDbTransMgr.getSingleTranInfo(CatalogTestUtil.testDbId1,
                subTransactionId3);
        Assert.assertEquals(1, singleTranInfos.size());
        List<String> txnInfo = singleTranInfos.get(0);
        Assert.assertEquals(String.valueOf(transactionId6), txnInfo.get(0));

        // test get table transaction info: table_id to partition_id map
        List<List<Comparable>> tableTransInfos = masterDbTransMgr.getTableTransInfo(transactionId6);
        LOG.info("tableTransInfos: {}", tableTransInfos);
        Assert.assertEquals(3, tableTransInfos.size());
        List<Comparable> tableTransInfo0 = tableTransInfos.get(0);
        Assert.assertEquals(2, tableTransInfo0.size());
        Assert.assertEquals(2L, tableTransInfo0.get(0));
        Assert.assertEquals("3", tableTransInfo0.get(1));
        List<Comparable> tableTransInfo1 = tableTransInfos.get(1);
        Assert.assertEquals(2, tableTransInfo1.size());
        Assert.assertEquals(15L, tableTransInfo1.get(0));
        Assert.assertEquals("16", tableTransInfo1.get(1));
        List<Comparable> tableTransInfo2 = tableTransInfos.get(2);
        Assert.assertEquals(2, tableTransInfo2.size());
        Assert.assertEquals(2L, tableTransInfo2.get(0));
        Assert.assertEquals("3", tableTransInfo2.get(1));

        // test get partition transaction info
        List<List<Comparable>> partitionTransInfos1 = masterDbTransMgr.getPartitionTransInfo(transactionId6,
                CatalogTestUtil.testTableId1);
        LOG.info("partitionTransInfos for table1: {}", partitionTransInfos1);
        Assert.assertEquals(2, partitionTransInfos1.size());
        List<Comparable> partitionTransInfo0 = partitionTransInfos1.get(0);
        Assert.assertEquals(2, partitionTransInfo0.size());
        Assert.assertEquals(3L, partitionTransInfo0.get(0));
        Assert.assertEquals(14L, partitionTransInfo0.get(1));
        List<Comparable> partitionTransInfo1 = partitionTransInfos1.get(1);
        Assert.assertEquals(2, partitionTransInfo1.size());
        Assert.assertEquals(3L, partitionTransInfo1.get(0));
        Assert.assertEquals(15L, partitionTransInfo1.get(1));
        List<List<Comparable>> partitionTransInfos2 = masterDbTransMgr.getPartitionTransInfo(transactionId6,
                CatalogTestUtil.testTableId2);
        LOG.info("partitionTransInfos for table2: {}", partitionTransInfos2);
        Assert.assertEquals(1, partitionTransInfos2.size());
        List<Comparable> partitionTransInfo3 = partitionTransInfos2.get(0);
        Assert.assertEquals(2, partitionTransInfo3.size());
        Assert.assertEquals(16L, partitionTransInfo3.get(0));
        Assert.assertEquals(13L, partitionTransInfo3.get(1));

        // test delete transaction
        masterDbTransMgr.replayDeleteTransaction(transactionState6);
        Assert.assertEquals(4 + 3, masterDbTransMgr.getTransactionNum());
        Assert.assertEquals(3 + 2, masterDbTransMgr.getRunningTxnNums());
        Assert.assertEquals(1 + 1, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel6));
        Assert.assertNull(masterDbTransMgr.getTransactionState(subTransactionId3));
    }

    @Test
    public void testRemoveExpiredTxnsWithSubTxn() throws UserException {
        addSubTransaction();
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        Config.label_keep_max_second = -1;
        Config.streaming_label_keep_max_second = -1;
        long currentMillis = System.currentTimeMillis();
        masterDbTransMgr.removeUselessTxns(currentMillis);
        Assert.assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(3 + 2, masterDbTransMgr.getTransactionNum());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1));
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel6));
    }

    @Test
    public void testRemoveOverLimitTxnsWithSubTxn() throws UserException {
        addSubTransaction();
        // TODO: the 0 does not work
        Config.label_num_threshold = 0;
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1);
        masterDbTransMgr.removeUselessTxns(System.currentTimeMillis());
        /*Assert.assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        Assert.assertEquals(3 + 2, masterDbTransMgr.getTransactionNum());
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel1));
        Assert.assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(CatalogTestUtil.testTxnLabel6));*/
    }

    private Pair<TransactionState, List<Long>> beginTransactionWithSubTxn(String label, List<Long> tableIds)
            throws UserException {
        Assert.assertTrue(tableIds.size() > 0);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                Lists.newArrayList(tableIds.get(0)),
                label, transactionSource, LoadJobSourceType.INSERT_STREAMING,
                Config.stream_load_default_timeout_second);
        TransactionState transactionState = masterTransMgr.getDatabaseTransactionMgr(CatalogTestUtil.testDbId1)
                .getTransactionState(transactionId);
        List<Long> subTxnIds = new ArrayList<>();
        subTxnIds.add(transactionId);
        for (int i = 1; i < tableIds.size(); i++) {
            /** add sub txn, the same as {@link TransactionEntry#beginTransaction} */
            transactionState.addTableId(tableIds.get(i));
            Long subTransactionId = masterTransMgr.getNextTransactionId();
            subTxnIds.add(subTransactionId);
            masterTransMgr.addSubTransaction(CatalogTestUtil.testDbId1, transactionId, subTransactionId);

            // get transaction state by subTransactionId
            TransactionState subTransactionState = masterTransMgr.getTransactionState(CatalogTestUtil.testDbId1,
                    subTransactionId);
            Assert.assertEquals(subTransactionState, transactionState);
        }
        return Pair.of(transactionState, subTxnIds);
    }

    protected static void setSuccessTablet(Map<String, Map<Long, Long>> keyToSuccessTablets, List<Long> beIds,
            long txnId, long tabletId, long tabletVersion) {
        for (Long beId : beIds) {
            String key = beId + "-" + txnId;
            Map<Long, Long> tabletVersionMap = keyToSuccessTablets.get(key);
            if (tabletVersionMap == null) {
                tabletVersionMap = new HashMap<>();
                keyToSuccessTablets.put(key, tabletVersionMap);
            }
            tabletVersionMap.put(tabletId, tabletVersion);
        }
    }

    /**
     * txn with label5: prepare
     *   sub_txn: table1
     *   sub_txn: table1
     *   sub_txn: table2
     * txn with label6: visible
     *   sub_txn: table1
     *   sub_txn: table2
     *   sub_txn: table1, load fail
     *   sub_txn: table1
     * txn with label7: aborted
     *   sub_txn: table1
     *   sub_txn: table2
     *   sub_txn: table2
     * txn with label7: committed
     *   sub_txn: table1
     *   sub_txn: table2
     *   sub_txn: table1
     */
    private void addSubTransaction() throws UserException {
        FakeEnv.setEnv(masterEnv);
        Table table1 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId1);
        Table table2 = masterEnv.getInternalCatalog().getDbOrMetaException(CatalogTestUtil.testDbId1)
                .getTableOrMetaException(CatalogTestUtil.testTableId2);

        // txn with label5
        TransactionState transactionState5 = beginTransactionWithSubTxn(CatalogTestUtil.testTxnLabel5,
                Lists.newArrayList(CatalogTestUtil.testTableId1, CatalogTestUtil.testTableId1,
                        CatalogTestUtil.testTableId2)).first;

        // txn with label6
        Pair<TransactionState, List<Long>> txnInfo6 = beginTransactionWithSubTxn(CatalogTestUtil.testTxnLabel6,
                Lists.newArrayList(CatalogTestUtil.testTableId1));
        TransactionState transactionState6 = txnInfo6.first;
        if (true) {
            long transactionId = transactionState6.getTransactionId();
            // add sub txn2
            long subTxnId2 = masterTransMgr.getNextTransactionId();
            transactionState6.addTableId(CatalogTestUtil.testTableId2);
            masterTransMgr.addSubTransaction(CatalogTestUtil.testDbId1, transactionId, subTxnId2);
            // add sub txn3
            long subTxnId3 = masterTransMgr.getNextTransactionId();
            transactionState6.addTableId(CatalogTestUtil.testTableId1);
            masterTransMgr.addSubTransaction(CatalogTestUtil.testDbId1, transactionId, subTxnId3);
            // sub txn3 fail
            transactionState6.removeTableId(CatalogTestUtil.testTableId1);
            Env.getCurrentGlobalTransactionMgr().removeSubTransaction(CatalogTestUtil.testDbId1, subTxnId3);
            // add sub txn4
            long subTxnId4 = masterTransMgr.getNextTransactionId();
            transactionState6.addTableId(CatalogTestUtil.testTableId1);
            masterTransMgr.addSubTransaction(CatalogTestUtil.testDbId1, transactionId, subTxnId4);

            // commit transaction
            ArrayList<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends, transactionId),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends, subTxnId2),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends, subTxnId4));
            Map<String, Map<Long, Long>> keyToSuccessTablets = new HashMap<>();
            setSuccessTablet(keyToSuccessTablets, allBackends, transactionId, CatalogTestUtil.testTabletId1, 14);
            setSuccessTablet(keyToSuccessTablets, allBackends, subTxnId2, CatalogTestUtil.testTabletId2, 13);
            setSuccessTablet(keyToSuccessTablets, allBackends, subTxnId4, CatalogTestUtil.testTabletId1, 15);
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2, table1),
                    transactionState6.getTransactionId(),
                    GlobalTransactionMgrTest.generateSubTransactionStates(masterTransMgr, transactionState6,
                            subTransactionInfos), 300000);
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState6.getTransactionStatus());

            // finish transaction
            DatabaseTransactionMgrTest.setTransactionFinishPublish(transactionState6, allBackends, keyToSuccessTablets);
            Map<Long, Long> partitionVisibleVersions = Maps.newHashMap();
            Map<Long, Set<Long>> backendPartitions = Maps.newHashMap();
            masterTransMgr.finishTransaction(CatalogTestUtil.testDbId1, transactionId, partitionVisibleVersions,
                    backendPartitions);
            Assert.assertEquals(TransactionStatus.VISIBLE, transactionState6.getTransactionStatus());
        }

        // txn with label7
        TransactionState transactionState7 = beginTransactionWithSubTxn(CatalogTestUtil.testTxnLabel7,
                Lists.newArrayList(CatalogTestUtil.testTableId1, CatalogTestUtil.testTableId2,
                        CatalogTestUtil.testTableId2)).first;
        // abort transaction
        masterTransMgr.abortTransaction(CatalogTestUtil.testDbId1, transactionState7.getTransactionId(),
                "user rollback");
        Assert.assertEquals(TransactionStatus.ABORTED, transactionState7.getTransactionStatus());

        // txn with label8
        Pair<TransactionState, List<Long>> txnInfo8 = beginTransactionWithSubTxn(CatalogTestUtil.testTxnLabel8,
                Lists.newArrayList(CatalogTestUtil.testTableId1, CatalogTestUtil.testTableId2,
                        CatalogTestUtil.testTableId1));
        TransactionState transactionState8 = txnInfo8.first;
        if (true) {
            List<Long> subTxnIds8 = txnInfo8.second;
            // commit transaction
            ArrayList<SubTransactionInfo> subTransactionInfos = Lists.newArrayList(
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends, subTxnIds8.get(0)),
                    new SubTransactionInfo(table2, CatalogTestUtil.testTabletId2, allBackends, subTxnIds8.get(1)),
                    new SubTransactionInfo(table1, CatalogTestUtil.testTabletId1, allBackends, subTxnIds8.get(2)));
            masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, Lists.newArrayList(table1, table2),
                    transactionState8.getTransactionId(),
                    GlobalTransactionMgrTest.generateSubTransactionStates(masterTransMgr, transactionState8,
                            subTransactionInfos), 300000);
            Assert.assertEquals(TransactionStatus.COMMITTED, transactionState8.getTransactionStatus());
        }

        LabelToTxnId.put(CatalogTestUtil.testTxnLabel5, transactionState5.getTransactionId());
        LabelToTxnId.put(CatalogTestUtil.testTxnLabel6, transactionState6.getTransactionId());
        LabelToTxnId.put(CatalogTestUtil.testTxnLabel7, transactionState7.getTransactionId());
        LabelToTxnId.put(CatalogTestUtil.testTxnLabel8, transactionState8.getTransactionId());
    }
}
