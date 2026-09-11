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

package org.apache.doris.tso;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.IncrWindowNotReadyException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.TimeBasedChangeVisibleWaiter;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unit tests for TSOService class.
 */
public class TSOServiceTest {

    private TSOService tsoService;
    private Env env;
    private MockedStatic<Env> mockedEnv;

    private int originalMaxGetTSORetryCount;
    private int originalMaxUpdateRetryCount;
    private int originalUpdateIntervalMs;
    private boolean originalEnableFeatureBinlog;
    private long originalClockBackwardThresholdMs;

    @BeforeEach
    public void setUp() {
        mockedEnv = Mockito.mockStatic(Env.class);

        originalMaxGetTSORetryCount = Config.tso_max_get_retry_count;
        originalMaxUpdateRetryCount = Config.tso_max_update_retry_count;
        originalUpdateIntervalMs = Config.tso_service_update_interval_ms;
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        originalClockBackwardThresholdMs = Config.tso_clock_backward_startup_threshold_ms;

        Config.tso_max_get_retry_count = 1;
        Config.tso_max_update_retry_count = 1;
        Config.tso_service_update_interval_ms = 1;
        Config.enable_feature_binlog = true;
        Config.tso_clock_backward_startup_threshold_ms = 30L * 60 * 1000;

        env = Mockito.mock(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        tsoService = new TSOService();
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        Config.tso_max_get_retry_count = originalMaxGetTSORetryCount;
        Config.tso_max_update_retry_count = originalMaxUpdateRetryCount;
        Config.tso_service_update_interval_ms = originalUpdateIntervalMs;
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
        Config.tso_clock_backward_startup_threshold_ms = originalClockBackwardThresholdMs;
    }

    @Test
    public void testConstructor() {
        TSOService service = new TSOService();
        Assertions.assertNotNull(service);
    }

    @Test
    public void testGetCurrentTSO() {
        TSOService service = new TSOService();
        long currentTSO = service.getCurrentTSO();
        // Should be 0 since not initialized
        Assertions.assertEquals(0L, currentTSO);
    }

    @Test
    public void testGetTSOThrowsWhenEnvNotReady() {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, true);
            Mockito.when(env.isReady()).thenReturn(false);
            try {
                tsoService.getTSO();
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("Failed to get TSO"));
            }
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testGetTSOThrowsWhenNotCalibrated() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(true);
            try {
                tsoService.getTSO();
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("not calibrated"));
            }
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testGetTSOThrowsOnLogicalOverflow() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, true);
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(true);
            setGlobalTimestamp(tsoService, 100L, TSOTimestamp.MAX_LOGICAL_COUNTER);
            try {
                tsoService.getTSO();
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("Failed to get TSO"));
                Assertions.assertNotNull(e.getCause());
                Assertions.assertTrue(e.getCause().getMessage().contains("logical counter overflow"));
                Assertions.assertEquals(TSOTimestamp.MAX_LOGICAL_COUNTER, getGlobalLogicalCounter(tsoService));
            }
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testGetTSOAcceptsLogicalCounterUpperBound() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, true);
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(true);
            setGlobalTimestamp(tsoService, 100L, TSOTimestamp.MAX_LOGICAL_COUNTER - 1);
            long tso = tsoService.getTSO();
            Assertions.assertEquals(TSOTimestamp.composeTimestamp(100L, TSOTimestamp.MAX_LOGICAL_COUNTER), tso);
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testRunAfterCatalogReadySetsIntervalTo50WhenDisabled() {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            setInitializedFlag(tsoService, true);
            Config.enable_feature_binlog = false;
            tsoService.runAfterCatalogReady();
            Assertions.assertEquals(1L, tsoService.getInterval());
            try {
                tsoService.getTSO();
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("feature is disabled"));
            }
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testRunAfterCatalogReadyDoesNotResetFatalClockBackwardFlagWhenDisabled() {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = false;
            setFatalClockBackwardReportedFlag(tsoService, true);

            tsoService.runAfterCatalogReady();

            Assertions.assertTrue(getFatalClockBackwardReportedFlag(tsoService));
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testRunAfterCatalogReadyUsesAtLeastOneRetryWhenConfigNonPositive() {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            Config.tso_max_update_retry_count = 0;
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(true);
            mockPersistReady();
            tsoService.runAfterCatalogReady();
            Assertions.assertTrue(tsoService.getTSO() > 0);
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testRunAfterCatalogReadyUpdateFailureDoesNotTouchMetricWhenNotInit() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        boolean originalMetricInit = MetricRepo.isInit;
        LongCounterMetric originalUpdateFailedMetric = MetricRepo.COUNTER_TSO_CLOCK_UPDATE_FAILED;
        try {
            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, true);
            setGlobalTimestamp(tsoService, 100L, 1L);
            MetricRepo.isInit = false;
            MetricRepo.COUNTER_TSO_CLOCK_UPDATE_FAILED = null;
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenThrow(new RuntimeException("injected update failure"));
            tsoService.runAfterCatalogReady();
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
            MetricRepo.isInit = originalMetricInit;
            MetricRepo.COUNTER_TSO_CLOCK_UPDATE_FAILED = originalUpdateFailedMetric;
        }
    }

    @Test
    public void testReplayWindowEndTSOUpdatesServiceState() {
        long windowEnd = 12345L;
        tsoService.replayWindowEndTSO(new TSOServiceState(windowEnd, 0L));
        Assertions.assertEquals(windowEnd, tsoService.getWindowEndTSO());
    }

    @Test
    public void testSaveTSOPersistsWindowEndWhenBinlogEnabled() throws IOException {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            long windowEnd = 12345L;
            tsoService.replayWindowEndTSO(new TSOServiceState(windowEnd, 0L));

            byte[] bytes = saveTSOBytes(tsoService);
            Assertions.assertTrue(bytes.length > 0);

            TSOService recoveredService = new TSOService();
            long checksum = recoveredService.loadTSO(new DataInputStream(new ByteArrayInputStream(bytes)), 0L);
            Assertions.assertEquals(windowEnd, checksum);
            Assertions.assertEquals(windowEnd, recoveredService.getWindowEndTSO());
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testSaveTSOSkipsWhenWindowEndIsZero() throws IOException {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long checksum;
            try (CountingDataOutputStream dos = new CountingDataOutputStream(out, 0)) {
                checksum = tsoService.saveTSO(dos, 7L);
                dos.flush();
            }
            Assertions.assertEquals(7L, checksum);
            Assertions.assertEquals(0, out.size());
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testWriteTimestampToBdbJeSkipsWhenBinlogDisabled() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = false;
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.isReady()).thenReturn(false);
            Mockito.when(env.getEditLog()).thenReturn(editLog);

            invokeWriteTimestampToBdbJe(tsoService, 123L);
            Mockito.verifyNoInteractions(editLog);
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testWriteTimestampToBdbJeWritesWhenEnabledAndJournalReady() throws Exception {
        EditLog editLog = Mockito.mock(EditLog.class);
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.getJournal()).thenReturn(journal);

        invokeWriteTimestampToBdbJe(tsoService, 123L);
        Mockito.verify(editLog).logTSOTimestampWindowEnd(Mockito.any(TSOServiceState.class));
    }

    @Test
    public void testWriteTimestampToBdbJeThrowsWhenEnabledAndEnvNotReady() throws Exception {
        Mockito.when(env.isReady()).thenReturn(false);
        try {
            invokeWriteTimestampToBdbJe(tsoService, 123L);
            Assertions.fail();
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("Env is not ready"));
        }
    }

    @Test
    public void testCalibrateTimestampThrowsWhenPersistWriteFailsAndKeepNotInitialized() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            Mockito.when(env.isReady()).thenReturn(true);
            Mockito.when(env.isMaster()).thenReturn(true);
            Mockito.when(env.getEditLog()).thenReturn(null);

            try {
                invokeCalibrateTimestamp(tsoService);
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("EditLog is null"));
            }

            TSOService.TSOStatusSnapshot statusSnapshot = tsoService.getStatusSnapshot();
            Assertions.assertFalse(statusSnapshot.isInitialized());
            Assertions.assertTrue(statusSnapshot.getCurrentTso() > 0L);
            Assertions.assertEquals(0L, statusSnapshot.getWindowEndPhysicalTime());

            try {
                tsoService.getTSO();
                Assertions.fail();
            } catch (RuntimeException e) {
                Assertions.assertTrue(e.getMessage().contains("not calibrated"));
            }
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testCalibrateTimestampThrowsWhenClockBackwardExceedsThreshold() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        long now = System.currentTimeMillis() + Config.tso_time_offset_debug_mode;
        tsoService.replayWindowEndTSO(new TSOServiceState(
                now + Config.tso_clock_backward_startup_threshold_ms + 60_000, 0L));
        try {
            invokeCalibrateTimestamp(tsoService);
            Assertions.fail();
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("clock backward too much"));
        }
    }

    @Test
    public void testCalibrateTimestampResetsFatalClockBackwardReportedOnSuccess() throws Exception {
        setFatalClockBackwardReportedFlag(tsoService, true);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        mockPersistReady();

        invokeCalibrateTimestamp(tsoService);

        Assertions.assertFalse(getFatalClockBackwardReportedFlag(tsoService));
    }

    @Test
    public void testRunAfterCatalogReadySkipsWhenBinlogDisabled() throws Exception {
        Config.enable_feature_binlog = false;
        setInitializedFlag(tsoService, true);
        tsoService.runAfterCatalogReady();
        Assertions.assertEquals(0L, tsoService.getCurrentTSO());
    }

    @Test
    public void testUpdateTimestampReturnsEarlyWhenNotCalibrated() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        long initialWindowEnd = 12345L;
        tsoService.replayWindowEndTSO(new TSOServiceState(initialWindowEnd, 0L));

        invokeUpdateTimestamp(tsoService);

        Assertions.assertEquals(0L, tsoService.getCurrentTSO());
        Assertions.assertEquals(initialWindowEnd, tsoService.getWindowEndTSO());
    }

    @Test
    public void testGenerateTSOReturnsZeroWhenDisabledOrNotInitialized() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            setGlobalTimestamp(tsoService, 100L, 1L);

            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, false);
            Pair<Long, Long> pairWhenNotInitialized = invokeGenerateTSO(tsoService);
            Assertions.assertEquals(0L, (long) pairWhenNotInitialized.first);
            Assertions.assertEquals(0L, (long) pairWhenNotInitialized.second);

            Config.enable_feature_binlog = false;
            setInitializedFlag(tsoService, true);
            Pair<Long, Long> pairWhenDisabled = invokeGenerateTSO(tsoService);
            Assertions.assertEquals(0L, (long) pairWhenDisabled.first);
            Assertions.assertEquals(0L, (long) pairWhenDisabled.second);
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
        }
    }

    @Test
    public void testReadableWindowDoesNotRequireAnotherCommittedTsoFlush() throws Exception {
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        setInitializedFlag(tsoService, true);
        setGlobalTimestamp(tsoService, 1000, 17);
        long committed = TSOTimestamp.composeTimestamp(900, 1);
        tsoService.replayWindowEndTSO(new TSOServiceState(2000, committed));
        Field trackerField = TSOService.class.getDeclaredField("transactionTracker");
        trackerField.setAccessible(true);
        TSOTransactionTracker tracker = (TSOTransactionTracker) trackerField.get(tsoService);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                    .thenReturn(TSOTransactionTrackerTest.recoveryBatch(ByteString.EMPTY));
        tracker.checkTransactions(txnMgr, Long.MAX_VALUE);
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            TimeBasedChangeVisibleWaiter.ChangeReadFence fence = TimeBasedChangeVisibleWaiter.acquireFenceOnMaster(
                    Collections.singletonMap(1L, Collections.singletonList(2L)), 950L, 0, true, true);
            Assertions.assertEquals(committed, fence.getCommittedTso());
        }
    }

    private void prepareWindowRead(boolean finishRecovery) throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        setInitializedFlag(tsoService, true);
        setGlobalTimestamp(tsoService, 100, 0);
        tsoService.replayWindowEndTSO(new TSOServiceState(2000, TSOTimestamp.composeTimestamp(80, 1)));
        if (finishRecovery) {
            Field field = TSOService.class.getDeclaredField("transactionTracker");
            field.setAccessible(true);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
            Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                    .thenReturn(TSOTransactionTrackerTest.recoveryBatch(ByteString.EMPTY));
            ((TSOTransactionTracker) field.get(tsoService)).checkTransactions(txnMgr, Long.MAX_VALUE);
        }
    }

    @Test
    public void testSlowTableDoesNotBlockAnotherTableOrAnEarlierEnd() throws Exception {
        prepareWindowRead(true);
        long pending = tsoService.getCommitTSO(1, 10, Set.of(100L, 101L));
        setGlobalTimestamp(tsoService, 150, 17);
        TSOService.TSOStatusSnapshot unrelated = tsoService.waitForReadableWindow(
                Map.of(1L, Collections.singletonList(200L)), 120, 0);
        Assertions.assertTrue(unrelated.getCommittedTso() < pending);
        tsoService.waitForReadableWindow(Map.of(1L, Collections.singletonList(100L)), 100, 0);
        for (long table : new long[] {100, 101}) {
            IncrWindowNotReadyException error = Assertions.assertThrows(IncrWindowNotReadyException.class,
                    () -> tsoService.waitForReadableWindow(Map.of(1L, Collections.singletonList(table)), 120, 0));
            Assertions.assertEquals(ErrorCode.ERR_INCR_VISIBLE_WAIT_TIMEOUT, error.getMysqlErrorCode());
            Assertions.assertEquals("VISIBLE_WAIT_TIMEOUT", error.getReason());
            Assertions.assertEquals(120, error.getRequestedEndTimestampMs());
            Assertions.assertEquals(tsoService.getCurrentTSO(), error.getCurrentTso());
            Assertions.assertEquals(unrelated.getCommittedTso(), error.getCommittedTso());
        }
        tsoService.transactionFinished(1, 10);
        TSOService.TSOStatusSnapshot finished = tsoService.waitForReadableWindow(
                Map.of(1L, Collections.singletonList(100L)), 120, 0);
        // A table-specific successful read does not advance the global durable prefix.
        Assertions.assertEquals(unrelated.getCommittedTso(), finished.getCommittedTso());
    }

    @Test
    public void testFutureWindowAndRecoveryHaveDifferentReasonsFromWaitTimeout() throws Exception {
        prepareWindowRead(false);
        Map<Long, List<Long>> tables = Map.of(1L, Collections.singletonList(100L));
        IncrWindowNotReadyException future = Assertions.assertThrows(IncrWindowNotReadyException.class,
                () -> tsoService.waitForReadableWindow(tables, 101, 0));
        Assertions.assertEquals(ErrorCode.ERR_INCR_WINDOW_NOT_READY, future.getMysqlErrorCode());
        Assertions.assertEquals("END_AFTER_CURRENT_TSO", future.getReason());
        IncrWindowNotReadyException recovering = Assertions.assertThrows(IncrWindowNotReadyException.class,
                () -> tsoService.waitForReadableWindow(tables, 90, 0));
        Assertions.assertEquals(ErrorCode.ERR_INCR_WINDOW_NOT_READY, recovering.getMysqlErrorCode());
        Assertions.assertEquals("TSO_RECOVERING", recovering.getReason());
        tsoService.waitForReadableWindow(tables, 80, 0);
    }

    @Test
    public void testInterruptedReadWaitRestoresInterruptFlag() throws Exception {
        prepareWindowRead(true);
        tsoService.getCommitTSO(1, 10, Collections.singleton(100L));
        setGlobalTimestamp(tsoService, 150, 17);
        try {
            Thread.currentThread().interrupt();
            UserException error = Assertions.assertThrows(UserException.class, () -> tsoService.waitForReadableWindow(
                    Map.of(1L, Collections.singletonList(100L)), 120, 1000));
            Assertions.assertTrue(error.getDetailMessage().contains("interrupted"));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testCommittedPrefixIsPublishedOnlyAfterJournalSuccess() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(editLog.getJournal()).thenReturn(Mockito.mock(Journal.class));
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        tsoService.replayWindowEndTSO(new TSOServiceState(200, 80));
        setGlobalTimestamp(tsoService, 100, 10);
        Field trackerField = TSOService.class.getDeclaredField("transactionTracker");
        trackerField.setAccessible(true);
        TSOTransactionTracker tracker = (TSOTransactionTracker) trackerField.get(tsoService);
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                    .thenReturn(TSOTransactionTrackerTest.recoveryBatch(ByteString.EMPTY));
        tracker.checkTransactions(txnMgr, Long.MAX_VALUE);
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            Mockito.doAnswer(invocation -> {
                Assertions.assertEquals(80, tsoService.getStatusSnapshot().getCommittedTso());
                Assertions.assertEquals(200, tsoService.getStatusSnapshot().getWindowEndPhysicalTime());
                throw new RuntimeException("injected journal failure");
            }).when(editLog).logTSOTimestampWindowEnd(Mockito.any());
            Assertions.assertThrows(RuntimeException.class, () -> invokeWriteTimestampToBdbJe(tsoService, 300));
            Assertions.assertEquals(80, tsoService.getStatusSnapshot().getCommittedTso());
            Assertions.assertEquals(200, tsoService.getStatusSnapshot().getWindowEndPhysicalTime());
            Mockito.doNothing().when(editLog).logTSOTimestampWindowEnd(Mockito.any());
            invokeWriteTimestampToBdbJe(tsoService, 300);
            Assertions.assertEquals(tsoService.getCurrentTSO(), tsoService.getStatusSnapshot().getCommittedTso());
            Assertions.assertEquals(300, tsoService.getStatusSnapshot().getWindowEndPhysicalTime());
        }
    }

    @Test
    public void testRecoveryFreezesPrefixAndPeriodicFlushWorksWithUnchangedWindow() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        mockPersistReady();
        long oldWindow = System.currentTimeMillis() + 60_000;
        long oldCommitted = TSOTimestamp.composeTimestamp(oldWindow - 1000, 7);
        tsoService.replayWindowEndTSO(new TSOServiceState(oldWindow, oldCommitted));
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            invokeCalibrateTimestamp(tsoService);
            Assertions.assertEquals(oldCommitted, tsoService.getStatusSnapshot().getCommittedTso());
            long pendingTso = tsoService.getCommitTSO(1, 10, Collections.singleton(2L));
            Field trackerField = TSOService.class.getDeclaredField("transactionTracker");
            trackerField.setAccessible(true);
            TSOTransactionTracker tracker = (TSOTransactionTracker) trackerField.get(tsoService);
            GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
            Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
            Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                    .thenReturn(TSOTransactionTrackerTest.recoveryBatch(ByteString.EMPTY));
            long afterRecoveryDelay = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(Config.tso_service_window_duration_ms + 1001L);
            tracker.checkTransactions(txnMgr, afterRecoveryDelay);
            long reservedWindow = tsoService.getWindowEndTSO();
            Field lastPersist = TSOService.class.getDeclaredField("lastPersistNanos");
            lastPersist.setAccessible(true);
            lastPersist.setLong(tsoService, System.nanoTime()
                    - TimeUnit.MILLISECONDS.toNanos(Config.tso_service_window_duration_ms + 1L));
            invokeUpdateTimestamp(tsoService);
            Assertions.assertEquals(reservedWindow, tsoService.getWindowEndTSO());
            Assertions.assertEquals(pendingTso - 1, tsoService.getStatusSnapshot().getCommittedTso());
            tsoService.transactionFinished(1, 10);
            lastPersist.setLong(tsoService, System.nanoTime()
                    - TimeUnit.MILLISECONDS.toNanos(Config.tso_service_window_duration_ms + 1L));
            invokeUpdateTimestamp(tsoService);
            Assertions.assertEquals(reservedWindow, tsoService.getWindowEndTSO());
            Assertions.assertEquals(pendingTso, tsoService.getStatusSnapshot().getCommittedTso());
        }
    }

    @Test
    public void testStateImageJournalAndOldTimestampCompatibility() throws Exception {
        long committed = TSOTimestamp.composeTimestamp(100, 17);
        tsoService.replayWindowEndTSO(new TSOServiceState(200, committed));
        TSOService restored = new TSOService();
        Assertions.assertEquals(200, restored.loadTSO(
                new DataInputStream(new ByteArrayInputStream(saveTSOBytes(tsoService))), 0));
        Assertions.assertEquals(committed, restored.getStatusSnapshot().getCommittedTso());
        Assertions.assertFalse(restored.getStatusSnapshot().isInitialized());
        for (boolean legacy : new boolean[] {true, false}) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeShort(OperationType.OP_TSO_TIMESTAMP_WINDOW_END);
            if (legacy) {
                new TSOTimestamp(200, 0).write(out);
            } else {
                new TSOServiceState(200, committed).write(out);
            }
            JournalEntity entity = new JournalEntity();
            entity.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            TSOServiceState state = (TSOServiceState) entity.getData();
            Assertions.assertEquals(200, state.getPhysicalTimestamp());
            Assertions.assertEquals(legacy ? 0 : committed, state.getCommittedTso());
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new TSOServiceState(200, committed).write(new DataOutputStream(bytes));
        Assertions.assertEquals(200, TSOTimestamp.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))).getPhysicalTimestamp());
    }

    @Test
    public void testAllocationAndRegistrationShareSnapshotLock() throws Exception {
        setInitializedFlag(tsoService, true);
        setGlobalTimestamp(tsoService, 100, 0);
        Field lockField = TSOService.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        ReentrantLock lock = (ReentrantLock) lockField.get(tsoService);
        Field trackerField = TSOService.class.getDeclaredField("transactionTracker");
        trackerField.setAccessible(true);
        TSOTransactionTracker tracker = Mockito.spy((TSOTransactionTracker) trackerField.get(tsoService));
        trackerField.set(tsoService, tracker);
        CountDownLatch enteredRegistration = new CountDownLatch(1);
        CountDownLatch releaseRegistration = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            enteredRegistration.countDown();
            Assertions.assertTrue(releaseRegistration.await(30, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(tracker).register(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anySet());
        Method generate = TSOService.class.getDeclaredMethod("generateTSO", Pair.class, Set.class);
        generate.setAccessible(true);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> allocation = executor.submit(() -> {
                try {
                    generate.invoke(tsoService, Pair.of(1L, 10L), Collections.singleton(2L));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Assertions.assertTrue(enteredRegistration.await(30, TimeUnit.SECONDS));
            boolean snapshotLockAcquired = lock.tryLock();
            if (snapshotLockAcquired) {
                lock.unlock();
            }
            Assertions.assertFalse(snapshotLockAcquired, "snapshot must not pass an unregistered allocation");
            releaseRegistration.countDown();
            allocation.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(TSOTimestamp.composeTimestamp(100, 1), tracker.getOldestPendingTso());
        } finally {
            releaseRegistration.countDown();
            executor.shutdownNow();
        }
    }

    private static void invokeWriteTimestampToBdbJe(TSOService service, long timestamp) throws Exception {
        Method m = TSOService.class.getDeclaredMethod("writeTimestampToBDBJE", long.class);
        m.setAccessible(true);
        try {
            m.invoke(service, timestamp);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static void invokeCalibrateTimestamp(TSOService service) throws Exception {
        Method m = TSOService.class.getDeclaredMethod("calibrateTimestamp");
        m.setAccessible(true);
        try {
            m.invoke(service);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static void invokeUpdateTimestamp(TSOService service) throws Exception {
        Method m = TSOService.class.getDeclaredMethod("updateTimestamp");
        m.setAccessible(true);
        m.invoke(service);
    }

    private static Pair<Long, Long> invokeGenerateTSO(TSOService service) throws Exception {
        Method m = TSOService.class.getDeclaredMethod("generateTSO");
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Pair<Long, Long> pair = (Pair<Long, Long>) m.invoke(service);
        return pair;
    }

    private static void setGlobalTimestamp(TSOService service, long physical, long logical) throws Exception {
        Field f = TSOService.class.getDeclaredField("globalTimestamp");
        f.setAccessible(true);
        TSOTimestamp timestamp = (TSOTimestamp) f.get(service);
        timestamp.setPhysicalTimestamp(physical);
        timestamp.setLogicalCounter(logical);
    }

    private static long getGlobalLogicalCounter(TSOService service) throws Exception {
        Field f = TSOService.class.getDeclaredField("globalTimestamp");
        f.setAccessible(true);
        TSOTimestamp timestamp = (TSOTimestamp) f.get(service);
        return timestamp.getLogicalCounter();
    }

    private static void setInitializedFlag(TSOService service, boolean initialized) {
        try {
            Field f = TSOService.class.getDeclaredField("isInitialized");
            f.setAccessible(true);
            ((java.util.concurrent.atomic.AtomicBoolean) f.get(service)).set(initialized);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setFatalClockBackwardReportedFlag(TSOService service, boolean reported) {
        try {
            Field f = TSOService.class.getDeclaredField("fatalClockBackwardReported");
            f.setAccessible(true);
            ((java.util.concurrent.atomic.AtomicBoolean) f.get(service)).set(reported);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean getFatalClockBackwardReportedFlag(TSOService service) {
        try {
            Field f = TSOService.class.getDeclaredField("fatalClockBackwardReported");
            f.setAccessible(true);
            return ((java.util.concurrent.atomic.AtomicBoolean) f.get(service)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void mockPersistReady() {
        EditLog editLog = Mockito.mock(EditLog.class);
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.getJournal()).thenReturn(journal);
    }

    private static byte[] saveTSOBytes(TSOService service) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (CountingDataOutputStream dos = new CountingDataOutputStream(out, 0)) {
            long checksum = service.saveTSO(dos, 0L);
            dos.flush();
            Assertions.assertEquals(service.getWindowEndTSO(), checksum);
        }
        return out.toByteArray();
    }

}
