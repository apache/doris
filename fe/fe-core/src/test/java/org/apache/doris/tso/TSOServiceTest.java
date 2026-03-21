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
import org.apache.doris.journal.Journal;
import org.apache.doris.persist.EditLog;

import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for TSOService class.
 */
public class TSOServiceTest {

    private TSOService tsoService;
    private Env env;

    private int originalMaxGetTSORetryCount;
    private int originalUpdateIntervalMs;
    private boolean originalEnableTsoPersistJournal;
    private long originalClockBackwardThresholdMs;

    @Before
    public void setUp() {
        new EnvMockUp();

        originalMaxGetTSORetryCount = Config.max_get_tso_retry_count;
        originalUpdateIntervalMs = Config.tso_service_update_interval_ms;
        originalEnableTsoPersistJournal = Config.enable_tso_persist_journal;
        originalClockBackwardThresholdMs = Config.tso_clock_backward_startup_threshold_ms;

        Config.max_get_tso_retry_count = 1;
        Config.tso_service_update_interval_ms = 1;
        Config.enable_tso_persist_journal = false;
        Config.tso_clock_backward_startup_threshold_ms = 30L * 60 * 1000;

        env = Mockito.mock(Env.class);
        EnvMockUp.CURRENT_ENV.set(env);

        tsoService = new TSOService();
    }

    @After
    public void tearDown() {
        EnvMockUp.CURRENT_ENV.set(null);
        Config.max_get_tso_retry_count = originalMaxGetTSORetryCount;
        Config.tso_service_update_interval_ms = originalUpdateIntervalMs;
        Config.enable_tso_persist_journal = originalEnableTsoPersistJournal;
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
        setInitializedFlag(tsoService, true);
        Mockito.when(env.isReady()).thenReturn(false);
        try {
            tsoService.getTSO();
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get TSO"));
        }
    }

    @Test
    public void testGetTSOThrowsWhenNotCalibrated() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        try {
            tsoService.getTSO();
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("not calibrated"));
        }
    }

    @Test
    public void testGetTSOThrowsOnLogicalOverflow() throws Exception {
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
        }
    }

    @Test
    public void testRunAfterCatalogReadySetsIntervalTo50WhenDisabled() {
        boolean originalEnableTsoFeature = Config.enable_feature_tso;
        try {
            setInitializedFlag(tsoService, true);
            Config.enable_feature_tso = false;
            tsoService.runAfterCatalogReady();
            Assert.assertEquals(1L, tsoService.getInterval());
            try {
                tsoService.getTSO();
                Assert.fail();
            } catch (RuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("not calibrated"));
            }
        } finally {
            Config.enable_feature_tso = originalEnableTsoFeature;
        }
    }

    @Test
    public void testReplayWindowEndTSOUpdatesEnv() {
        long windowEnd = 12345L;
        tsoService.replayWindowEndTSO(new TSOTimestamp(windowEnd, 0L));
        Mockito.verify(env).setWindowEndTSO(windowEnd);
    }

    @Test
    public void testWriteTimestampToBdbJeSkipsWhenEnvNotReady() throws Exception {
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.isReady()).thenReturn(false);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        invokeWriteTimestampToBdbJe(tsoService, 123L);
        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    public void testWriteTimestampToBdbJeWritesWhenEnabledAndJournalReady() throws Exception {
        EditLog editLog = Mockito.mock(EditLog.class);
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.getJournal()).thenReturn(journal);

        Config.enable_tso_persist_journal = true;
        invokeWriteTimestampToBdbJe(tsoService, 123L);
        Mockito.verify(editLog).logTSOTimestampWindowEnd(Mockito.any(TSOTimestamp.class));
    }

    @Test
    public void testCalibrateTimestampThrowsWhenClockBackwardExceedsThreshold() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        long now = System.currentTimeMillis() + Config.tso_time_offset_debug_mode;
        Mockito.when(env.getWindowEndTSO()).thenReturn(now + Config.tso_clock_backward_startup_threshold_ms + 60_000);
        try {
            invokeCalibrateTimestamp(tsoService);
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("clock backward too much"));
        }
    }

    private static void invokeWriteTimestampToBdbJe(TSOService service, long timestamp) throws Exception {
        Method m = TSOService.class.getDeclaredMethod("writeTimestampToBDBJE", long.class);
        m.setAccessible(true);
        m.invoke(service, timestamp);
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

    private static void setGlobalTimestamp(TSOService service, long physical, long logical) throws Exception {
        Field f = TSOService.class.getDeclaredField("globalTimestamp");
        f.setAccessible(true);
        TSOTimestamp timestamp = (TSOTimestamp) f.get(service);
        timestamp.setPhysicalTimestamp(physical);
        timestamp.setLogicalCounter(logical);
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

    private static final class EnvMockUp extends MockUp<Env> {
        private static final AtomicReference<Env> CURRENT_ENV = new AtomicReference<>();

        @Mock
        public static Env getCurrentEnv() {
            return CURRENT_ENV.get();
        }
    }
}
