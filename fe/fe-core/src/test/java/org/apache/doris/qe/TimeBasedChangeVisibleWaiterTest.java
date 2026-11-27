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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TAcquireTimeBasedChangeReadFenceRequest;
import org.apache.doris.thrift.TAcquireTimeBasedChangeReadFenceResult;
import org.apache.doris.thrift.TIncrWindowNotReady;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
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
        Assertions.assertFalse(result.isAllEndsExplicit());
    }

    @Test
    public void testRowTtlReadDoesNotAcquireFence() throws Exception {
        assertRowTtlReadDoesNotAcquireFence(ImmutableMap.of());
    }

    @Test
    public void testBoundedRowTtlReadDoesNotAcquireFence() throws Exception {
        assertRowTtlReadDoesNotAcquireFence(
                ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, "2024-01-01 00:00:00"));
    }

    private void assertRowTtlReadDoesNotAcquireFence(Map<String, String> params) throws Exception {
        OlapTable table = mockOlapTable(DB_ID, TABLE_ID);
        Mockito.when(table.hasRowTtl()).thenReturn(true);
        ConnectContext context = mockContext();

        TimeBasedChangeVisibleWaiter.waitForVisible(context, newChangeRelation(1, params),
                ImmutableMap.of(TABLE_QUALIFIER, table));

        Mockito.verify(context, Mockito.never()).getEnv();
        Mockito.verify(table, Mockito.never()).getDatabase();
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
        Assertions.assertTrue(result.isAllEndsExplicit());
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
    public void testClassicVisibleWaitTimeoutsReportVisibleWaitTimeout() throws Exception {
        // One case times out draining earlier transactions, the other acquiring the publisher lock.
        for (boolean drainTimesOut : new boolean[] {true, false}) {
            Env master = mockMasterEnv();
            mockTsoService(master, CURRENT_TSO);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(300L);
            Mockito.when(txnMgr.isPreviousTransactionsFinished(300L, DB_ID, ImmutableList.of(TABLE_ID)))
                    .thenReturn(!drainTimesOut);
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            Database database = Mockito.mock(Database.class);
            Table table = Mockito.mock(Table.class);
            Mockito.when(catalog.getDbOrMetaException(DB_ID)).thenReturn(database);
            Mockito.when(database.getTablesOnIdOrderIfExist(ImmutableList.of(TABLE_ID)))
                    .thenReturn(ImmutableList.of(table));
            Mockito.when(table.getId()).thenReturn(TABLE_ID);
            Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenReturn(false);
            Map<Long, List<Long>> tables = ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID));
            TAcquireTimeBasedChangeReadFenceRequest request = new TAcquireTimeBasedChangeReadFenceRequest();
            request.setDbToTableIds(tables);
            request.setEndTimestampMs(CURRENT_PHYSICAL_TIME_MS);
            request.setTimeoutMs(100);
            request.setWaitForTransactions(true);
            IncrWindowNotReadyException direct;
            TAcquireTimeBasedChangeReadFenceResult response;
            try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                    MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
                config.when(Config::isCloudMode).thenReturn(false);
                mocked.when(Env::getCurrentEnv).thenReturn(master);
                mocked.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
                mocked.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
                direct = Assertions.assertThrows(IncrWindowNotReadyException.class,
                        () -> TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                                tables, CURRENT_PHYSICAL_TIME_MS, 100, true), "drainTimesOut=" + drainTimesOut);
                response = new FrontendServiceImpl(null).acquireTimeBasedChangeReadFence(request);
            }
            // 5101 is the code TSOService.waitForReadableWindow reports when a bounded cloud read
            // times out waiting for visibility; a classic visibility-wait timeout must report the same.
            Assertions.assertEquals(ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT, direct.getMysqlErrorCode());
            Assertions.assertTrue(direct.getReason().startsWith(
                    drainTimesOut ? "PREVIOUS_TRANSACTIONS_NOT_FINISHED" : "PUBLISHER_LOCK_TIMEOUT"),
                    direct.getReason());
            // The remaining fields come from the request and the mocked TSO snapshot; no retry delay is suggested.
            Assertions.assertEquals(CURRENT_PHYSICAL_TIME_MS, direct.getRequestedEndTimestampMs());
            Assertions.assertEquals(CURRENT_TSO, direct.getCurrentTso());
            Assertions.assertEquals(100, direct.getTimeoutMs());
            Assertions.assertEquals(0, direct.getRetryAfterMs());
            Assertions.assertEquals(ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT.getCode(),
                    response.getWindowNotReady().getErrorCode());
            TAcquireTimeBasedChangeReadFenceResult decoded = new TAcquireTimeBasedChangeReadFenceResult();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(response));
            IncrWindowNotReadyException propagated = receiveWindowErrorOnFollower(decoded, false);
            Assertions.assertEquals(ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT, propagated.getMysqlErrorCode());
            Assertions.assertEquals(direct.getReason(), propagated.getReason());
        }
    }

    @Test
    public void testClassicFollowerJournalReplayTimeoutReportsVisibleWaitTimeout() throws Exception {
        Env follower = Mockito.mock(Env.class);
        Mockito.when(follower.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(follower.getMasterRpcPort()).thenReturn(9020);
        Mockito.when(follower.getReplayedJournalId()).thenReturn(100L);
        ConnectContext context = mockContext();
        Mockito.when(context.getEnv()).thenReturn(follower);
        TAcquireTimeBasedChangeReadFenceResult response = new TAcquireTimeBasedChangeReadFenceResult();
        response.setStatus(new TStatus(TStatusCode.OK));
        response.setCurrentTso(CURRENT_TSO);
        response.setMaxJournalId(123L);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        // The master answers after the session timeout, 1000 ms (SessionVariable.MIN_CHANGE_VISIBLE_TIMEOUT_MS),
        // so no time is left for journal replay.
        Mockito.when(client.acquireTimeBasedChangeReadFence(Mockito.any())).thenAnswer(invocation -> {
            Thread.sleep(1100);
            return response;
        });
        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendPool;
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(Mockito.any(), Mockito.anyInt())).thenReturn(client);
        ClientPool.frontendPool = pool;
        String endTimestamp = "2024-01-01 00:00:00";
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(false);
            IncrWindowNotReadyException error = Assertions.assertThrows(IncrWindowNotReadyException.class,
                    () -> TimeBasedChangeVisibleWaiter.waitForVisible(context,
                            newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, endTimestamp)),
                            ImmutableMap.of(TABLE_QUALIFIER, mockOlapTable(DB_ID, TABLE_ID))));
            // Same code as the master-side visibility-wait timeouts; the other fields come from the
            // request, the master's fence response and the session timeout.
            Assertions.assertEquals(ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT, error.getMysqlErrorCode());
            Assertions.assertEquals("FOLLOWER_JOURNAL_REPLAY_TIMEOUT maxJournalId=123", error.getReason());
            Assertions.assertEquals(OlapScanNode.parseChangeTimestamp(endTimestamp),
                    error.getRequestedEndTimestampMs());
            Assertions.assertEquals(CURRENT_TSO, error.getCurrentTso());
            Assertions.assertEquals(1000, error.getTimeoutMs());
            Mockito.verify(follower, Mockito.never()).getJournalObservable();
        } finally {
            ClientPool.frontendPool = originalPool;
        }
    }

    @Test
    public void testEventualConsistentFollowerFenceKeepsSessionSocketTimeout() throws Exception {
        Env follower = Mockito.mock(Env.class);
        Mockito.when(follower.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(follower.getMasterRpcPort()).thenReturn(9020);
        ConnectContext context = mockContext();
        context.getSessionVariable().setEnableEventualConsistentChange(true);
        Mockito.when(context.getEnv()).thenReturn(follower);
        TAcquireTimeBasedChangeReadFenceResult response = new TAcquireTimeBasedChangeReadFenceResult();
        response.setStatus(new TStatus(TStatusCode.OK));
        response.setCurrentTso(CURRENT_TSO);
        response.setMaxJournalId(123L);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        Mockito.when(client.acquireTimeBasedChangeReadFence(Mockito.any())).thenReturn(response);
        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendPool;
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(Mockito.any(), Mockito.anyInt())).thenReturn(client);
        ClientPool.frontendPool = pool;
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(false);
            TimeBasedChangeVisibleWaiter.waitForVisible(context,
                    newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, "2024-01-01 00:00:00")),
                    ImmutableMap.of(TABLE_QUALIFIER, mockOlapTable(DB_ID, TABLE_ID)));
            // An eventual-consistent read does not wait for transactions on the master, so its socket
            // timeout stays at the 1000 ms session timeout without the 1000 ms grace.
            Mockito.verify(pool).borrowObject(Mockito.any(), Mockito.eq(1000));
        } finally {
            ClientPool.frontendPool = originalPool;
        }
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

    @Test
    public void testHistoricalWindowDoesNotWaitForTransactionsOutsideWindow() throws Exception {
        Env env = mockMasterEnv();
        TSOService service = mockTsoService(env, CURRENT_TSO);
        long committedTso = TSOTimestamp.composeTimestamp(CURRENT_PHYSICAL_TIME_MS - 1000, 17);
        Mockito.when(service.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, CURRENT_TSO, CURRENT_PHYSICAL_TIME_MS + 1000, committedTso));
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(301L);
        // A same-table write started after the historical end and remains running.
        Mockito.when(txnMgr.isPreviousTransactionsFinished(301L, DB_ID, ImmutableList.of(TABLE_ID)))
                .thenReturn(false);
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            mocked.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            TimeBasedChangeVisibleWaiter.ChangeReadFence fence = TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                    ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)), CURRENT_PHYSICAL_TIME_MS - 1000, 0, true, true);
            Assertions.assertEquals(committedTso, fence.getCommittedTso());
            Mockito.verifyNoInteractions(txnMgr);
            // Negative control: the legacy table-wide drain cannot distinguish this later write.
            Assertions.assertThrows(UserException.class, () -> TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                    ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)), CURRENT_PHYSICAL_TIME_MS - 1000, 0, true));
        }
    }

    @Test
    public void testBoundedReadDelegatesRegisteredTransactionWait() throws Exception {
        Env env = mockMasterEnv();
        TSOService service = mockTsoService(env, CURRENT_TSO);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Map<Long, List<Long>> tables = ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID));
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            mocked.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
            for (long committed : new long[] {0, TSOTimestamp.composeTimestamp(CURRENT_PHYSICAL_TIME_MS - 1, 17)}) {
                Mockito.when(service.getStatusSnapshot()).thenReturn(
                        new TSOService.TSOStatusSnapshot(true, CURRENT_TSO, CURRENT_PHYSICAL_TIME_MS + 1000, committed));
                TimeBasedChangeVisibleWaiter.ChangeReadFence result = TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                        tables, CURRENT_PHYSICAL_TIME_MS, 1000, true, true);
                Assertions.assertEquals(committed, result.getCommittedTso());
            }
            Mockito.verify(service, Mockito.times(2)).waitForReadableWindow(tables, CURRENT_PHYSICAL_TIME_MS, 1000);
            Mockito.verifyNoInteractions(txnMgr);
        }
    }

    @Test
    public void testMixedBoundedAndUnboundedRelationsRetainDrain() {
        Plan plan = new LogicalJoin<>(JoinType.INNER_JOIN,
                newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, "2024-01-01 00:00:00")),
                newChangeRelation(2, ImmutableMap.of()), null);
        TimeBasedChangeVisibleWaiter.ChangeReadInfo info = TimeBasedChangeVisibleWaiter.collectChangeReadInfo(
                mockContext(), plan, ImmutableMap.of(TABLE_QUALIFIER, mockOlapTable(DB_ID, TABLE_ID)));
        Assertions.assertNotNull(info.getMaxEndTimestampMs());
        Assertions.assertFalse(info.isAllEndsExplicit());
    }

    @Test
    public void testMasterRpcAndFollowerPreserveBothWindowErrors() throws Exception {
        for (ErrorCode code : new ErrorCode[] {ErrorCode.ERR_INCR_WINDOW_NOT_READY,
                ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT}) {
            Env master = mockMasterEnv();
            TSOService service = mockTsoService(master, CURRENT_TSO);
            String reason = code == ErrorCode.ERR_INCR_WINDOW_NOT_READY ? "TSO_MASTER_CHANGED" : "VISIBLE_WAIT_TIMEOUT";
            IncrWindowNotReadyException failure = new IncrWindowNotReadyException(code, reason,
                    CURRENT_PHYSICAL_TIME_MS, CURRENT_TSO, 0, 1000, 1000);
            Mockito.doThrow(failure).when(service)
                    .waitForReadableWindow(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyLong());
            TAcquireTimeBasedChangeReadFenceRequest request = new TAcquireTimeBasedChangeReadFenceRequest();
            request.setDbToTableIds(ImmutableMap.of(DB_ID, ImmutableList.of(TABLE_ID)));
            request.setEndTimestampMs(CURRENT_PHYSICAL_TIME_MS);
            request.setTimeoutMs(1000);
            request.setWaitForTransactions(true);
            request.setAllEndsExplicit(true);
            TAcquireTimeBasedChangeReadFenceResult response;
            try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class);
                    MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
                mocked.when(Env::getCurrentEnv).thenReturn(master);
                config.when(Config::isCloudMode).thenReturn(true);
                response = new FrontendServiceImpl(null).acquireTimeBasedChangeReadFence(request);
            }
            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, response.getStatus().getStatusCode());
            Assertions.assertEquals(code.getCode(), response.getWindowNotReady().getErrorCode());
            // Exercise the optional fields over the actual Thrift encoding before the follower receives them.
            TAcquireTimeBasedChangeReadFenceResult decoded = new TAcquireTimeBasedChangeReadFenceResult();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(response));
            IncrWindowNotReadyException propagated = receiveWindowErrorOnFollower(decoded, true);
            Assertions.assertEquals(code, propagated.getMysqlErrorCode());
            Assertions.assertEquals(CURRENT_TSO, propagated.getCurrentTso());
            Assertions.assertEquals(1000, propagated.getTimeoutMs());
            Assertions.assertEquals(reason, propagated.getReason());
        }
    }

    @Test
    public void testFollowerAcceptsWindowErrorFromOlderMaster() throws Exception {
        TAcquireTimeBasedChangeReadFenceResult response = new TAcquireTimeBasedChangeReadFenceResult();
        response.setStatus(new TStatus(TStatusCode.ANALYSIS_ERROR));
        response.setWindowNotReady(new TIncrWindowNotReady(CURRENT_PHYSICAL_TIME_MS, 0, 1000));
        IncrWindowNotReadyException propagated = receiveWindowErrorOnFollower(response, true);
        Assertions.assertEquals(ErrorCode.ERR_INCR_WINDOW_NOT_READY, propagated.getMysqlErrorCode());
        Assertions.assertEquals("WINDOW_NOT_READY", propagated.getReason());
    }

    /**
     * Runs waitForVisible on a non-master FE whose master Thrift client is mocked to return the given response.
     */
    private IncrWindowNotReadyException receiveWindowErrorOnFollower(TAcquireTimeBasedChangeReadFenceResult response,
            boolean cloudMode) throws Exception {
        Env follower = Mockito.mock(Env.class);
        Mockito.when(follower.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(follower.getMasterRpcPort()).thenReturn(9020);
        ConnectContext context = mockContext();
        Mockito.when(context.getEnv()).thenReturn(follower);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        Mockito.when(client.acquireTimeBasedChangeReadFence(Mockito.any())).thenReturn(response);
        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendPool;
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(Mockito.any(), Mockito.anyInt())).thenReturn(client);
        ClientPool.frontendPool = pool;
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(cloudMode);
            IncrWindowNotReadyException error = Assertions.assertThrows(IncrWindowNotReadyException.class,
                    () -> TimeBasedChangeVisibleWaiter.waitForVisible(context,
                            newChangeRelation(1, ImmutableMap.of(OlapScanNode.OLAP_END_TIMESTAMP, "2024-01-01 00:00:00")),
                            ImmutableMap.of(TABLE_QUALIFIER, mockOlapTable(DB_ID, TABLE_ID))));
            Assertions.assertEquals(CURRENT_PHYSICAL_TIME_MS, error.getRequestedEndTimestampMs());
            Assertions.assertEquals(0, error.getCommittedTso());
            // The socket timeout is the 1000 ms session timeout plus 1000 ms, so the master's typed
            // timeout response arrives first in both modes.
            Mockito.verify(pool).borrowObject(Mockito.any(), Mockito.eq(2000));
            Mockito.verify(pool).returnObject(Mockito.any(), Mockito.eq(client));
            return error;
        } finally {
            ClientPool.frontendPool = originalPool;
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

    private TSOService mockTsoService(Env env, long currentTso) throws UserException {
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, CURRENT_PHYSICAL_TIME_MS + 1000));
        Mockito.when(tsoService.waitForReadableWindow(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyLong()))
                .thenAnswer(invocation -> tsoService.getStatusSnapshot());
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
