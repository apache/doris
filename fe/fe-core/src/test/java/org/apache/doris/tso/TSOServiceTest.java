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
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.journal.Journal;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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

    @Before
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

    @After
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
        Assert.assertNotNull(service);
    }

    @Test
    public void testGetCurrentTSO() {
        TSOService service = new TSOService();
        long currentTSO = service.getCurrentTSO();
        // Should be 0 since not initialized
        Assert.assertEquals(0L, currentTSO);
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
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Failed to get TSO"));
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
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("not calibrated"));
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
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("Failed to get TSO"));
                Assert.assertNotNull(e.getCause());
                Assert.assertTrue(e.getCause().getMessage().contains("logical counter overflow"));
                Assert.assertEquals(TSOTimestamp.MAX_LOGICAL_COUNTER, getGlobalLogicalCounter(tsoService));
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
            Assert.assertEquals(TSOTimestamp.composeTimestamp(100L, TSOTimestamp.MAX_LOGICAL_COUNTER), tso);
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
            Assert.assertEquals(1L, tsoService.getInterval());
            try {
                tsoService.getTSO();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("feature is disabled"));
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

            Assert.assertTrue(getFatalClockBackwardReportedFlag(tsoService));
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
            Assert.assertTrue(tsoService.getTSO() > 0);
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
        tsoService.replayWindowEndTSO(new TSOTimestamp(windowEnd, 0L));
        Assert.assertEquals(windowEnd, tsoService.getWindowEndTSO());
    }

    @Test
    public void testSaveTSOPersistsWindowEndWhenBinlogEnabled() throws IOException {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            Config.enable_feature_binlog = true;
            long windowEnd = 12345L;
            tsoService.replayWindowEndTSO(new TSOTimestamp(windowEnd, 0L));

            byte[] bytes = saveTSOBytes(tsoService);
            Assert.assertTrue(bytes.length > 0);

            TSOService recoveredService = new TSOService();
            long checksum = recoveredService.loadTSO(new DataInputStream(new ByteArrayInputStream(bytes)), 0L);
            Assert.assertEquals(windowEnd, checksum);
            Assert.assertEquals(windowEnd, recoveredService.getWindowEndTSO());
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
            Assert.assertEquals(7L, checksum);
            Assert.assertEquals(0, out.size());
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
        Mockito.verify(editLog).logTSOTimestampWindowEnd(Mockito.any(TSOTimestamp.class));
    }

    @Test
    public void testWriteTimestampToBdbJeThrowsWhenEnabledAndEnvNotReady() throws Exception {
        Mockito.when(env.isReady()).thenReturn(false);
        try {
            invokeWriteTimestampToBdbJe(tsoService, 123L);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Env is not ready"));
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
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("EditLog is null"));
            }

            Assert.assertEquals(0L, tsoService.getWindowEndTSO());

            try {
                tsoService.getTSO();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("not calibrated"));
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
        tsoService.replayWindowEndTSO(new TSOTimestamp(
                now + Config.tso_clock_backward_startup_threshold_ms + 60_000, 0L));
        try {
            invokeCalibrateTimestamp(tsoService);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("clock backward too much"));
        }
    }

    @Test
    public void testCalibrateTimestampResetsFatalClockBackwardReportedOnSuccess() throws Exception {
        setFatalClockBackwardReportedFlag(tsoService, true);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        mockPersistReady();

        invokeCalibrateTimestamp(tsoService);

        Assert.assertFalse(getFatalClockBackwardReportedFlag(tsoService));
    }

    @Test
    public void testRunAfterCatalogReadySkipsWhenBinlogDisabled() throws Exception {
        Config.enable_feature_binlog = false;
        setInitializedFlag(tsoService, true);
        tsoService.runAfterCatalogReady();
        Assert.assertEquals(0L, tsoService.getCurrentTSO());
    }

    @Test
    public void testUpdateTimestampReturnsEarlyWhenNotCalibrated() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        long initialWindowEnd = 12345L;
        tsoService.replayWindowEndTSO(new TSOTimestamp(initialWindowEnd, 0L));

        invokeUpdateTimestamp(tsoService);

        Assert.assertEquals(0L, tsoService.getCurrentTSO());
        Assert.assertEquals(initialWindowEnd, tsoService.getWindowEndTSO());
    }

    @Test
    public void testGenerateTSOReturnsZeroWhenDisabledOrNotInitialized() throws Exception {
        boolean originalEnableFeatureBinlog = Config.enable_feature_binlog;
        try {
            setGlobalTimestamp(tsoService, 100L, 1L);

            Config.enable_feature_binlog = true;
            setInitializedFlag(tsoService, false);
            Pair<Long, Long> pairWhenNotInitialized = invokeGenerateTSO(tsoService);
            Assert.assertEquals(0L, (long) pairWhenNotInitialized.first);
            Assert.assertEquals(0L, (long) pairWhenNotInitialized.second);

            Config.enable_feature_binlog = false;
            setInitializedFlag(tsoService, true);
            Pair<Long, Long> pairWhenDisabled = invokeGenerateTSO(tsoService);
            Assert.assertEquals(0L, (long) pairWhenDisabled.first);
            Assert.assertEquals(0L, (long) pairWhenDisabled.second);
        } finally {
            Config.enable_feature_binlog = originalEnableFeatureBinlog;
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
            Assert.assertEquals(service.getWindowEndTSO(), checksum);
        }
        return out.toByteArray();
    }

}
