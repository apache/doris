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

package org.apache.doris.qe;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TimeBasedChangeVisibleWaiterTest {
    private static final List<String> TABLE_QUALIFIER = ImmutableList.of("internal", "db", "tbl");
    private static final long DB_ID = 100L;
    private static final long TABLE_ID = 200L;
    private static final long CURRENT_PHYSICAL_TIME_MS = 1700000000000L;
    private static final long CURRENT_TSO = TSOTimestamp.composeTimestamp(CURRENT_PHYSICAL_TIME_MS, 17L);

    @Test
    public void testCollectChangeReadInfoWithoutEndTimestamp() {
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);

        TimeBasedChangeVisibleWaiter.ChangeReadInfo result = TimeBasedChangeVisibleWaiter.collectChangeReadInfo(
                mockContext(), newChangeRelation(1, ImmutableMap.of()),
                ImmutableMap.of(TABLE_QUALIFIER, table));

        Assertions.assertEquals(ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)), result.getDbToTableIds());
        Assertions.assertNull(result.getMaxEndTimestampMs());
    }

    @Test
    public void testCollectChangeReadInfoMergesMaximumEndTimestamp() {
        String endTimestamp1 = "2024-01-01 00:00:00";
        String endTimestamp2 = "2024-01-02 00:00:00";
        Plan plan = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, endTimestamp1)),
                newChangeRelation(2, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, endTimestamp2)),
                null);
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);

        TimeBasedChangeVisibleWaiter.ChangeReadInfo result = TimeBasedChangeVisibleWaiter.collectChangeReadInfo(
                mockContext(), plan, ImmutableMap.of(TABLE_QUALIFIER, table));

        Assertions.assertEquals(ImmutableList.of(TABLE_ID), result.getDbToTableIds().get(DB_ID));
        Assertions.assertEquals(OlapScanNode.parseChangeTimestamp(endTimestamp2), result.getMaxEndTimestampMs());
    }

    @Test
    public void testFenceCapturesTsoBeforeTransactionWatermark() throws Exception {
        Env env = mockMasterEnv();
        TSOService tsoService = mockTsoService(env, CURRENT_TSO);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        long txnIdWatermark = 301L;
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(txnIdWatermark);
        Mockito.when(txnMgr.isPreviousTransactionsFinished(
                txnIdWatermark, DB_ID, ImmutableList.of(TABLE_ID))).thenReturn(false, true);

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);

            TimeBasedChangeVisibleWaiter.ChangeReadFence fence =
                    TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                            ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)),
                            CURRENT_PHYSICAL_TIME_MS, 1000L, true);

            Assertions.assertEquals(CURRENT_TSO, fence.getCurrentTso());
        }

        InOrder inOrder = Mockito.inOrder(tsoService, txnMgr);
        inOrder.verify(tsoService).getStatusSnapshot();
        inOrder.verify(txnMgr).getTransactionIdWatermark();
        Mockito.verify(txnMgr, Mockito.times(2)).isPreviousTransactionsFinished(
                txnIdWatermark, DB_ID, ImmutableList.of(TABLE_ID));
    }

    @Test
    public void testFenceAcceptsCurrentTsoPhysicalTime() throws Exception {
        Env env = mockMasterEnv();
        mockTsoService(env, CURRENT_TSO);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TimeBasedChangeVisibleWaiter.ChangeReadFence fence =
                    TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                            ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)),
                            CURRENT_PHYSICAL_TIME_MS, 1000L, false);

            Assertions.assertEquals(CURRENT_TSO, fence.getCurrentTso());
        }
    }

    @Test
    public void testFenceRejectsEndAfterCurrentTsoBeforeWatermark() throws Exception {
        Env env = mockMasterEnv();
        mockTsoService(env, CURRENT_TSO);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                            ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)),
                            CURRENT_PHYSICAL_TIME_MS + 1, 1000L, true));

            Assertions.assertTrue(exception.getDetailMessage().contains(
                    "CURRENT_TSO_PHYSICAL_TIME=" + CURRENT_PHYSICAL_TIME_MS));
        }

        Mockito.verify(txnMgr, Mockito.never()).getTransactionIdWatermark();
    }

    @Test
    public void testClassicFenceWaitsWatermarkAndSynchronizesPublisherLock() throws Exception {
        Env env = mockMasterEnv();
        mockTsoService(env, CURRENT_TSO);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        long txnIdWatermark = 300L;
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(txnIdWatermark);
        Mockito.when(txnMgr.isPreviousTransactionsFinished(
                txnIdWatermark, DB_ID, ImmutableList.of(TABLE_ID))).thenReturn(true);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(catalog.getDbOrMetaException(DB_ID)).thenReturn(database);
        Mockito.when(database.getTablesOnIdOrderIfExist(ImmutableList.of(TABLE_ID)))
                .thenReturn(ImmutableList.of(table));
        Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenReturn(true);

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                    ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)), null, 1000L, true);
        }

        Mockito.verify(txnMgr).getTransactionIdWatermark();
        Mockito.verify(table).tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS));
        Mockito.verify(table).readUnlock();
    }

    @Test
    public void testFenceFailsWhenConflictCheckFails() throws Exception {
        Env env = mockMasterEnv();
        mockTsoService(env, CURRENT_TSO);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        long txnIdWatermark = 301L;
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(txnIdWatermark);
        Mockito.when(txnMgr.isPreviousTransactionsFinished(
                txnIdWatermark, DB_ID, ImmutableList.of(TABLE_ID)))
                .thenThrow(new AnalysisException("check transaction conflict failed"));

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);

            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                            ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)), null, 1000L, true));
            Assertions.assertTrue(exception.getDetailMessage().contains("check previous transactions failed"));
        }
    }

    private ConnectContext mockContext() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setChangeVisibleTimeoutMs(1000);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        return context;
    }

    private Env mockMasterEnv() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getMaxJournalId()).thenReturn(123L);
        return env;
    }

    private TSOService mockTsoService(Env env, long currentTso) {
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, CURRENT_PHYSICAL_TIME_MS + 1000));
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        return tsoService;
    }

    private UnboundRelation newChangeRelation(int relationId, Map<String, String> mapParams) {
        return new UnboundRelation(new RelationId(relationId), TABLE_QUALIFIER,
                ImmutableList.of(), false, ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(),
                new TableScanParams(TableScanParams.INCREMENTAL_READ, mapParams, ImmutableList.of()),
                Optional.empty());
    }

    private OlapTable mockOlapTable(long dbId, long tableId) {
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getId()).thenReturn(dbId);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getId()).thenReturn(tableId);
        return table;
    }
}
