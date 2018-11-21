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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

public class GlobalTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeCatalog fakeCatalog;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static Catalog masterCatalog;
    private static Catalog slaveCatalog;

    private String transactionSource = "localfe";
    

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        fakeEditLog = new FakeEditLog();
        fakeCatalog = new FakeCatalog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        slaveCatalog = CatalogTestUtil.createTestCatalog();
        masterCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        slaveCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        masterTransMgr = masterCatalog.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterCatalog.getEditLog());

        slaveTransMgr = slaveCatalog.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveCatalog.getEditLog());
    }

    @Test
    public void testBeginTransaction() throws LabelAlreadyExistsException, AnalysisException, 
        BeginTransactionException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource, transactionState.getCoordinator());
    }
    
    @Test
    public void testBeginTransactionWithSameLabel() throws LabelAlreadyExistsException, AnalysisException, 
        BeginTransactionException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = 0;
        Throwable throwable = null;
        try {
            transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1, 
                    CatalogTestUtil.testTxnLable1, 
                    transactionSource,
                    LoadJobSourceType.FRONTEND);
        } catch (AnalysisException e) {
            e.printStackTrace();
        } catch (LabelAlreadyExistsException e) {
            e.printStackTrace();
        }
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource, transactionState.getCoordinator());

        try {
        transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    // all replica committed success
    @Test
    public void testCommitTransaction1() throws MetaNotFoundException,
            TransactionCommitFailedException,
            IllegalTransactionParameterException, LabelAlreadyExistsException, 
            AnalysisException, BeginTransactionException {
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);
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
    public void testCommitTransactionWithOneFailed() throws MetaNotFoundException,
            TransactionCommitFailedException,
            IllegalTransactionParameterException, LabelAlreadyExistsException, 
            AnalysisException, BeginTransactionException {
        TransactionState transactionState = null;
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        FakeCatalog.setCatalog(masterCatalog);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable2,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId2, transTablets);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is prepare, because the commit failed
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId2, transTablets);
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

    public void testFinishTransaction() throws MetaNotFoundException, TransactionCommitFailedException,
            IllegalTransactionParameterException, LabelAlreadyExistsException, 
            AnalysisException, BeginTransactionException {
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
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
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId1);
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
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
            assertEquals(CatalogTestUtil.testStartVersion + 1, replica.getVersion());
        }
        // slave replay new state and compare catalog
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }

    @Test
    public void testFinishTransactionWithOneFailed() throws MetaNotFoundException,
            TransactionCommitFailedException,
            IllegalTransactionParameterException, LabelAlreadyExistsException, 
            AnalysisException, BeginTransactionException {
        TransactionState transactionState = null;
        Partition testPartition = masterCatalog.getDb(CatalogTestUtil.testDbId1).getTable(CatalogTestUtil.testTableId1)
                .getPartition(CatalogTestUtil.testPartition1);
        Tablet tablet = testPartition.getIndex(CatalogTestUtil.testIndexId1).getTablet(CatalogTestUtil.testTabletId1);
        FakeCatalog.setCatalog(masterCatalog);
        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        // commit a transaction with 1,2 success
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId, transTablets);

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        // master finish the transaction failed
        FakeCatalog.setCatalog(masterCatalog);
        Set<Long> errorReplicaIds = Sets.newHashSet();
        errorReplicaIds.add(CatalogTestUtil.testReplicaId2);
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getTransactionStatus());
        Replica replcia1 = tablet.getReplicaById(CatalogTestUtil.testReplicaId1);
        Replica replcia2 = tablet.getReplicaById(CatalogTestUtil.testReplicaId2);
        Replica replcia3 = tablet.getReplicaById(CatalogTestUtil.testReplicaId3);
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        errorReplicaIds = Sets.newHashSet();
        masterTransMgr.finishTransaction(transactionId, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        // follower catalog replay the transaction
        transactionState = fakeEditLog.getTransaction(transactionId);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));

        FakeCatalog.setCatalog(masterCatalog);
        // commit another transaction with 1,3 success
        long transactionId2 = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable2,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1,
                CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId2, transTablets);
        transactionState = fakeEditLog.getTransaction(transactionId2);
        // check status is prepare, because the commit failed
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());

        // commit the second transaction with 1,2,3 success
        tabletCommitInfo1 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId1);
        tabletCommitInfo2 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId2);
        tabletCommitInfo3 = new TabletCommitInfo(CatalogTestUtil.testTabletId1, CatalogTestUtil.testBackendId3);
        transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(CatalogTestUtil.testDbId1, transactionId2, transTablets);
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
        masterTransMgr.finishTransaction(transactionId2, errorReplicaIds);
        assertEquals(TransactionStatus.VISIBLE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replcia1.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replcia2.getVersion());
        assertEquals(CatalogTestUtil.testStartVersion, replcia3.getVersion());
        assertEquals(-1, replcia1.getLastFailedVersion());
        assertEquals(-1, replcia2.getLastFailedVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 1, replcia3.getLastFailedVersion());

        assertEquals(CatalogTestUtil.testStartVersion + 2, replcia1.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replcia2.getLastSuccessVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 2, replcia3.getLastSuccessVersion());
        // check partition version
        assertEquals(CatalogTestUtil.testStartVersion + 2, testPartition.getVisibleVersion());
        assertEquals(CatalogTestUtil.testStartVersion + 3, testPartition.getNextVersion());

        transactionState = fakeEditLog.getTransaction(transactionId2);
        FakeCatalog.setCatalog(slaveCatalog);
        slaveTransMgr.replayUpsertTransactionState(transactionState);
        assertTrue(CatalogTestUtil.compareCatalog(masterCatalog, slaveCatalog));
    }

    @Test
    public void testDeleteTransaction() throws LabelAlreadyExistsException, 
        AnalysisException, BeginTransactionException {

        long transactionId = masterTransMgr.beginTransaction(CatalogTestUtil.testDbId1,
                CatalogTestUtil.testTxnLable1,
                transactionSource,
                LoadJobSourceType.FRONTEND);
        TransactionState transactionState = fakeEditLog.getTransaction(transactionId);
        assertNotNull(transactionState);
        assertEquals(transactionId, transactionState.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState.getTransactionStatus());
        assertEquals(CatalogTestUtil.testDbId1, transactionState.getDbId());
        assertEquals(transactionSource, transactionState.getCoordinator());

        masterTransMgr.deleteTransaction(transactionId);
        transactionState = fakeEditLog.getTransaction(transactionId);
        assertNull(transactionState);
        transactionState = masterTransMgr.getTransactionState(transactionId);
        assertNull(transactionState);
    }
}
