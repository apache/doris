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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ExportHistoryPersistenceTest {
    private static final long NOW = 20_000_000L;

    @Test
    public void testStateTransferRealBytesPreserveAllFields() throws Exception {
        ExportJob source = job(7, "transfer", ExportJobState.FINISHED, 1_234_567L);
        Deencapsulation.setField(source, "startTimeMs", 765_432L);
        Deencapsulation.setField(source, "outfileInfo", "out-info");
        ExportJobStateTransfer transfer = new ExportJobStateTransfer(source, ExportJobState.FINISHED);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        transfer.write(new DataOutputStream(bytes));
        ExportJobStateTransfer restored = ExportJobStateTransfer.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertEquals(7, restored.getJobId());
        Assertions.assertEquals(ExportJobState.FINISHED, restored.getState());
        Assertions.assertEquals(765_432L, restored.getStartTimeMs());
        Assertions.assertEquals(1_234_567L, restored.getFinishTimeMs());
        Assertions.assertEquals("out-info", restored.getOutFileInfo());
    }

    @Test
    public void testEnvImageChecksumPartialCheckpointTerminalReplayAndFinalEviction() throws Exception {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 0;
            Config.max_export_history_job_num = 1000;

            Env sourceEnv = new Env(false);
            ExportJob pending = job(1, "partial", ExportJobState.PENDING, -1);
            sourceEnv.getExportMgr().unprotectAddJob(pending);
            Image image1 = save(sourceEnv, 91L);

            Env restoredEnv = new Env(false);
            long loadedChecksum1 = restoredEnv.loadExportJob(
                    new DataInputStream(new ByteArrayInputStream(image1.bytes)), 91L);
            Assertions.assertEquals(image1.checksum, loadedChecksum1);
            ExportJob recovered = restoredEnv.getExportMgr().getJob(1);
            Assertions.assertNotNull(recovered);
            Assertions.assertEquals(ExportJobState.CANCELLED, recovered.getState());
            Assertions.assertEquals(-1, recovered.getFinishTimeMs());
            Assertions.assertEquals("partial", recovered.getLabel());

            ExportJob terminal = job(1, "partial", ExportJobState.FINISHED,
                    System.currentTimeMillis() - 200_000);
            Deencapsulation.setField(terminal, "startTimeMs", 222L);
            Deencapsulation.setField(terminal, "outfileInfo", "terminal-out");
            restoredEnv.getExportMgr().replayUpdateJobState(
                    new ExportJobStateTransfer(terminal, ExportJobState.FINISHED));
            ExportJob replayed = restoredEnv.getExportMgr().getJob(1);
            Assertions.assertNotNull(replayed, "missing-job guard must not hide a valid state replay");
            Assertions.assertEquals(ExportJobState.FINISHED, replayed.getState());
            Assertions.assertEquals(terminal.getFinishTimeMs(), replayed.getFinishTimeMs());
            Assertions.assertEquals(222L, replayed.getStartTimeMs());
            Assertions.assertEquals("terminal-out", replayed.getOutfileInfo());
            Assertions.assertEquals("partial", replayed.getLabel());

            Image image2 = save(restoredEnv, 127L);
            Env secondRestore = new Env(false);
            long loadedChecksum2 = secondRestore.loadExportJob(
                    new DataInputStream(new ByteArrayInputStream(image2.bytes)), 127L);
            Assertions.assertEquals(image2.checksum, loadedChecksum2);
            ExportJob second = secondRestore.getExportMgr().getJob(1);
            Assertions.assertNotNull(second);
            Assertions.assertEquals(ExportJobState.FINISHED, second.getState());
            Assertions.assertEquals(terminal.getFinishTimeMs(), second.getFinishTimeMs());
            Assertions.assertEquals(222L, second.getStartTimeMs());
            Assertions.assertEquals("terminal-out", second.getOutfileInfo());
            Assertions.assertEquals("partial", second.getLabel());

            Deencapsulation.setField(second, "finishTimeMs", System.currentTimeMillis() - 300_001);
            Image finalImage = save(secondRestore, 255L);
            Env finalRestore = new Env(false);
            Assertions.assertEquals(finalImage.checksum, finalRestore.loadExportJob(
                    new DataInputStream(new ByteArrayInputStream(finalImage.bytes)), 255L));
            Assertions.assertNull(finalRestore.getExportMgr().getJob(1));
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testImageDropsMatureButRetainsYoungUnknownFutureAndActive() throws Exception {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 0;
            Config.max_export_history_job_num = 1000;
            long now = System.currentTimeMillis();
            Env source = new Env(false);
            source.getExportMgr().unprotectAddJob(job(1, "mature", ExportJobState.FINISHED, now - 600_000));
            source.getExportMgr().unprotectAddJob(job(2, "young", ExportJobState.CANCELLED, now - 200_000));
            source.getExportMgr().unprotectAddJob(job(3, "unknown", ExportJobState.CANCELLED, -1));
            source.getExportMgr().unprotectAddJob(job(4, "future", ExportJobState.FINISHED, now + 60_000));
            source.getExportMgr().unprotectAddJob(job(5, "active", ExportJobState.PENDING, -1));

            Image image = save(source, 0);
            Env restored = new Env(false);
            Assertions.assertEquals(image.checksum, restored.loadExportJob(
                    new DataInputStream(new ByteArrayInputStream(image.bytes)), 0));
            Assertions.assertNull(restored.getExportMgr().getJob(1));
            for (long id = 2; id <= 5; id++) {
                Assertions.assertNotNull(restored.getExportMgr().getJob(id), "job " + id);
            }
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testEnvImageAndOnlineCleanupEvictByFinishTimeAcrossTerminalStates() throws Exception {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 3600;
            Config.max_export_history_job_num = 3;
            long now = System.currentTimeMillis();
            Env source = new Env(false);
            ExportMgr mgr = source.getExportMgr();
            mgr.unprotectAddJob(job(61, "expired", ExportJobState.FINISHED, now - 3_700_000));
            mgr.unprotectAddJob(job(62, "older-finished", ExportJobState.FINISHED, now - 600_000));
            mgr.unprotectAddJob(job(63, "newer-cancelled", ExportJobState.CANCELLED, now - 400_000));
            mgr.unprotectAddJob(job(64, "protected", ExportJobState.FINISHED, now - 100_000));
            mgr.unprotectAddJob(job(65, "active", ExportJobState.PENDING, -1));

            Image image = save(source, 31L);
            Assertions.assertEquals(5, mgr.getJobs().size());
            Env restored = new Env(false);
            Assertions.assertEquals(image.checksum, restored.loadExportJob(
                    new DataInputStream(new ByteArrayInputStream(image.bytes)), 31L));
            mgr.removeOldExportJobs(now, Config.history_job_keep_max_second, Config.max_export_history_job_num);
            for (ExportMgr actual : Arrays.asList(mgr, restored.getExportMgr())) {
                Assertions.assertEquals(3, actual.getJobs().size());
                Assertions.assertNull(actual.getJob(61));
                Assertions.assertNull(actual.getJob(62));
                for (long id = 63; id <= 65; id++) {
                    Assertions.assertNotNull(actual.getJob(id), "job " + id);
                }
                Assertions.assertEquals(ExportJobState.CANCELLED, actual.getJob(63).getState());
                Assertions.assertEquals(ExportJobState.FINISHED, actual.getJob(64).getState());
            }
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testEnvImageBothLoadOrdersKeepNonCancelledLabel() throws Exception {
        long now = System.currentTimeMillis();
        ExportJob cancelled = job(1, "same", ExportJobState.CANCELLED, now - 100_000);
        ExportJob finished = job(2, "same", ExportJobState.FINISHED, now - 100_000);
        assertImageLabelRepresentative(Arrays.asList(cancelled, finished), 2L);
        assertImageLabelRepresentative(Arrays.asList(finished, cancelled), 2L);
    }

    @Test
    public void testBackfillThenEnvImageRestoreKeepsConfirmedTime() throws Exception {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 0;
            Config.max_export_history_job_num = 1000;

            Env sourceEnv = new Env(false);
            ExportJob pending = job(31, "backfilled", ExportJobState.PENDING, -1);
            Deencapsulation.setField(pending, "startTimeMs", 654_321L);
            Deencapsulation.setField(pending, "outfileInfo", "partial-out");
            sourceEnv.getExportMgr().unprotectAddJob(pending);
            Image preBackfillImage = save(sourceEnv, 511L);

            Env recoveryA = restore(preBackfillImage, 511L);
            Env recoveryB = restore(preBackfillImage, 511L);
            ExportJob missingA = recoveryA.getExportMgr().getJob(31);
            ExportJob missingB = recoveryB.getExportMgr().getJob(31);
            assertRecoveredUnknownJob(missingA);
            assertRecoveredUnknownJob(missingB);
            assertLabel(recoveryA, "backfilled", 31L);
            assertLabel(recoveryB, "backfilled", 31L);

            Env servingA = Mockito.mock(Env.class);
            EditLog editLogA = Mockito.mock(EditLog.class);
            Mockito.when(servingA.isReady()).thenReturn(true);
            Mockito.when(servingA.isMaster()).thenReturn(true);
            Mockito.when(servingA.getEditLog()).thenReturn(editLogA);
            AtomicReference<byte[]> transferBytes = new AtomicReference<>();
            Mockito.doAnswer(invocation -> {
                ExportJob loggedJob = invocation.getArgument(0);
                ExportJobState loggedState = invocation.getArgument(1);
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                new ExportJobStateTransfer(loggedJob, loggedState).write(new DataOutputStream(bytes));
                transferBytes.set(bytes.toByteArray());
                return null;
            }).when(editLogA).logExportUpdateState(Mockito.any(ExportJob.class), Mockito.any(ExportJobState.class));

            long confirmedTime = System.currentTimeMillis();
            recoveryA.getExportMgr().backfillMissingFinishTimes(servingA, servingA, false, confirmedTime);
            Mockito.verify(editLogA).logExportUpdateState(missingA, ExportJobState.CANCELLED);
            Assertions.assertNotNull(transferBytes.get(), "the real journal callback must freeze transfer bytes");

            ExportJobStateTransfer restoredTransfer = ExportJobStateTransfer.read(
                    new DataInputStream(new ByteArrayInputStream(transferBytes.get())));
            Assertions.assertEquals(31L, restoredTransfer.getJobId());
            Assertions.assertEquals(ExportJobState.CANCELLED, restoredTransfer.getState());
            Assertions.assertEquals(654_321L, restoredTransfer.getStartTimeMs());
            Assertions.assertEquals(confirmedTime, restoredTransfer.getFinishTimeMs());
            Assertions.assertEquals(missingA.getFailMsg().getCancelType(),
                    restoredTransfer.getFailMsg().getCancelType());
            Assertions.assertEquals(missingA.getFailMsg().getMsg(), restoredTransfer.getFailMsg().getMsg());
            Assertions.assertEquals("partial-out", restoredTransfer.getOutFileInfo());

            recoveryB.getExportMgr().replayUpdateJobState(restoredTransfer);
            ExportJob replayed = recoveryB.getExportMgr().getJob(31);
            Assertions.assertNotNull(replayed, "missing-job guard must not hide the backfill replay");
            assertTransferFields(replayed, restoredTransfer);
            Assertions.assertEquals("backfilled", replayed.getLabel());
            assertLabel(recoveryB, "backfilled", 31L);

            Env servingB = Mockito.mock(Env.class);
            EditLog editLogB = Mockito.mock(EditLog.class);
            Mockito.when(servingB.isReady()).thenReturn(true);
            Mockito.when(servingB.isMaster()).thenReturn(true);
            Mockito.when(servingB.getEditLog()).thenReturn(editLogB);
            recoveryB.getExportMgr().backfillMissingFinishTimes(
                    servingB, servingB, false, confirmedTime + 1);
            Mockito.verifyNoInteractions(editLogB);
            Assertions.assertEquals(confirmedTime, replayed.getFinishTimeMs());

            Image postReplayImage = save(recoveryB, 733L);
            Env finalRecovery = restore(postReplayImage, 733L);
            ExportJob finalJob = finalRecovery.getExportMgr().getJob(31);
            Assertions.assertNotNull(finalJob);
            assertTransferFields(finalJob, restoredTransfer);
            Assertions.assertEquals("backfilled", finalJob.getLabel());
            assertLabel(finalRecovery, "backfilled", 31L);

            finalRecovery.getExportMgr().removeOldExportJobs(confirmedTime + 299_999, 0, 1000);
            Assertions.assertSame(finalJob, finalRecovery.getExportMgr().getJob(31));
            assertLabel(finalRecovery, "backfilled", 31L);
            finalRecovery.getExportMgr().removeOldExportJobs(confirmedTime + 300_000, 0, 1000);
            Assertions.assertNull(finalRecovery.getExportMgr().getJob(31));
            Assertions.assertTrue(labels(finalRecovery).isEmpty());
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testOnlyReadyServingMasterBackfillsOnceWithoutChangingOtherFields() {
        ExportMgr mgr = new ExportMgr();
        ExportJob missing = job(1, "missing", ExportJobState.CANCELLED, -1);
        Deencapsulation.setField(missing, "startTimeMs", 123L);
        Deencapsulation.setField(missing, "outfileInfo", "unchanged");
        ExportFailMsg failMsg = new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL, "original");
        Deencapsulation.setField(missing, "failMsg", failMsg);
        ExportJob future = job(2, "future", ExportJobState.FINISHED, NOW + 1);
        mgr.unprotectAddJob(missing);
        mgr.unprotectAddJob(future);

        Env env = Mockito.mock(Env.class);
        Env other = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        mgr.backfillMissingFinishTimes(env, other, false, NOW);
        mgr.backfillMissingFinishTimes(env, env, true, NOW);
        Assertions.assertEquals(-1, missing.getFinishTimeMs());

        Mockito.when(env.isReady()).thenReturn(false);
        mgr.backfillMissingFinishTimes(env, env, false, NOW);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(false);
        mgr.backfillMissingFinishTimes(env, env, false, NOW);
        Assertions.assertEquals(-1, missing.getFinishTimeMs());

        Mockito.when(env.isMaster()).thenReturn(true);
        mgr.backfillMissingFinishTimes(env, env, false, NOW);
        mgr.backfillMissingFinishTimes(env, env, false, NOW + 1);
        Assertions.assertEquals(NOW, missing.getFinishTimeMs());
        Assertions.assertEquals(NOW + 1, future.getFinishTimeMs());
        Assertions.assertEquals(123L, missing.getStartTimeMs());
        Assertions.assertSame(failMsg, missing.getFailMsg());
        Assertions.assertEquals("unchanged", missing.getOutfileInfo());
        Assertions.assertEquals(ExportJobState.CANCELLED, missing.getState());
        Mockito.verify(editLog, Mockito.times(1))
                .logExportUpdateState(missing, ExportJobState.CANCELLED);
    }

    @Test
    public void testPublicCleanerBackfillsOnlyReadyServingMaster() {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 0;
            Config.max_export_history_job_num = 0;

            long now = System.currentTimeMillis();
            ExportMgr mgr = new ExportMgr();
            ExportJob missing = job(41, "missing-public", ExportJobState.CANCELLED, -1);
            Deencapsulation.setField(missing, "startTimeMs", 321L);
            Deencapsulation.setField(missing, "outfileInfo", "public-out");
            ExportFailMsg failMsg =
                    new ExportFailMsg(ExportFailMsg.CancelType.RUN_FAIL, "public-original");
            Deencapsulation.setField(missing, "failMsg", failMsg);
            ExportJob young = job(42, "young-public", ExportJobState.FINISHED, now - 1);
            ExportJob future = job(43, "future-public", ExportJobState.CANCELLED, now + 600_000);
            ExportJob active = job(44, "active-public", ExportJobState.PENDING, now - 600_000);
            mgr.unprotectAddJob(missing);
            mgr.unprotectAddJob(young);
            mgr.unprotectAddJob(future);
            mgr.unprotectAddJob(active);

            Env current = Mockito.mock(Env.class);
            Env otherServing = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(current.getEditLog()).thenReturn(editLog);
            Mockito.when(current.isReady()).thenReturn(true);
            Mockito.when(current.isMaster()).thenReturn(true);
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(current);
                envStatic.when(Env::getServingEnv).thenReturn(otherServing);
                envStatic.when(Env::isCheckpointThread).thenReturn(false);
                mgr.removeOldExportJobs();
                Assertions.assertEquals(-1, missing.getFinishTimeMs());
                Mockito.verifyNoInteractions(editLog);

                envStatic.when(Env::getServingEnv).thenReturn(current);
                envStatic.when(Env::isCheckpointThread).thenReturn(true);
                mgr.removeOldExportJobs();
                Assertions.assertEquals(-1, missing.getFinishTimeMs());
                Mockito.verifyNoInteractions(editLog);

                envStatic.when(Env::isCheckpointThread).thenReturn(false);
                Mockito.when(current.isReady()).thenReturn(false);
                mgr.removeOldExportJobs();
                Assertions.assertEquals(-1, missing.getFinishTimeMs());
                Mockito.verifyNoInteractions(editLog);

                Mockito.when(current.isReady()).thenReturn(true);
                Mockito.when(current.isMaster()).thenReturn(false);
                mgr.removeOldExportJobs();
                Assertions.assertEquals(-1, missing.getFinishTimeMs());
                Mockito.verifyNoInteractions(editLog);

                ExportJob mature = job(
                        45, "mature-public", ExportJobState.FINISHED, now - 600_000);
                mgr.unprotectAddJob(mature);
                Mockito.when(current.isMaster()).thenReturn(true);
                long beforeBackfill = System.currentTimeMillis();
                mgr.removeOldExportJobs();
                long afterBackfill = System.currentTimeMillis();

                long confirmedTime = missing.getFinishTimeMs();
                Assertions.assertTrue(confirmedTime >= beforeBackfill);
                Assertions.assertTrue(confirmedTime <= afterBackfill);
                Assertions.assertEquals(ExportJobState.CANCELLED, missing.getState());
                Assertions.assertEquals(321L, missing.getStartTimeMs());
                Assertions.assertSame(failMsg, missing.getFailMsg());
                Assertions.assertEquals("public-out", missing.getOutfileInfo());
                Assertions.assertNull(mgr.getJob(45));
                Assertions.assertSame(missing, mgr.getJob(41));
                Assertions.assertSame(young, mgr.getJob(42));
                Assertions.assertSame(future, mgr.getJob(43));
                Assertions.assertSame(active, mgr.getJob(44));

                mgr.removeOldExportJobs();
                Assertions.assertEquals(confirmedTime, missing.getFinishTimeMs());
                Mockito.verify(editLog, Mockito.times(1))
                        .logExportUpdateState(missing, ExportJobState.CANCELLED);
                Mockito.verifyNoMoreInteractions(editLog);
            }
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testPublicCleanerPropagatesJournalFailureBeforeCleanup() {
        int oldTtl = Config.history_job_keep_max_second;
        int oldMax = Config.max_export_history_job_num;
        try {
            Config.history_job_keep_max_second = 0;
            Config.max_export_history_job_num = 0;

            long now = System.currentTimeMillis();
            ExportMgr mgr = new ExportMgr();
            ExportJob missing = job(51, "journal-failure", ExportJobState.CANCELLED, -1);
            ExportJob mature = job(
                    52, "cleanup-must-not-run", ExportJobState.FINISHED, now - 600_000);
            mgr.unprotectAddJob(missing);
            mgr.unprotectAddJob(mature);

            Env current = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            IllegalStateException journalFailure =
                    new IllegalStateException("public cleaner journal failure");
            Mockito.when(current.getEditLog()).thenReturn(editLog);
            Mockito.when(current.getExportMgr()).thenReturn(mgr);
            Mockito.when(current.isReady()).thenReturn(true);
            Mockito.when(current.isMaster()).thenReturn(true);
            Mockito.doThrow(journalFailure).when(editLog)
                    .logExportUpdateState(missing, ExportJobState.CANCELLED);
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(current);
                envStatic.when(Env::getServingEnv).thenReturn(current);
                envStatic.when(Env::isCheckpointThread).thenReturn(false);
                try {
                    mgr.removeOldExportJobs();
                    Assertions.fail("the public cleaner must propagate the journal failure");
                } catch (IllegalStateException actual) {
                    Assertions.assertSame(journalFailure, actual);
                }
            }

            Mockito.verify(editLog).logExportUpdateState(missing, ExportJobState.CANCELLED);
            Assertions.assertSame(missing, mgr.getJob(51));
            Assertions.assertSame(mature, mgr.getJob(52));
            assertLabel(current, "journal-failure", 51L);
            assertLabel(current, "cleanup-must-not-run", 52L);
        } finally {
            Config.history_job_keep_max_second = oldTtl;
            Config.max_export_history_job_num = oldMax;
        }
    }

    @Test
    public void testJournalBlocksEligibilityAndFailureStopsCleanup() throws Exception {
        ExportMgr mgr = new ExportMgr();
        ExportJob missing = job(1, "blocked", ExportJobState.CANCELLED, -1);
        mgr.unprotectAddJob(missing);
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        CountDownLatch journalEntered = new CountDownLatch(1);
        CountDownLatch journalDone = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            journalEntered.countDown();
            Assertions.assertTrue(journalDone.await(10, TimeUnit.SECONDS));
            return null;
        }).when(editLog).logExportUpdateState(missing, ExportJobState.CANCELLED);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch eligibilityStarted = new CountDownLatch(1);
        AtomicReference<Thread> backfillThread = new AtomicReference<>();
        AtomicReference<Thread> eligibilityThread = new AtomicReference<>();
        Future<?> backfill = null;
        Future<Boolean> eligibility = null;
        Throwable failure = null;
        try {
            backfill = executor.submit(() -> {
                backfillThread.set(Thread.currentThread());
                mgr.backfillMissingFinishTimes(env, env, false, NOW);
            });
            Assertions.assertTrue(journalEntered.await(10, TimeUnit.SECONDS));
            eligibility = executor.submit(() -> {
                eligibilityThread.set(Thread.currentThread());
                eligibilityStarted.countDown();
                return missing.isRemovableAt(NOW + 300_000, 0);
            });
            Assertions.assertTrue(eligibilityStarted.await(10, TimeUnit.SECONDS));
            ThreadInfo blocked = awaitBlockedOnMonitor(
                    eligibilityThread.get(), missing, backfillThread.get());
            Assertions.assertEquals(Thread.State.BLOCKED, blocked.getThreadState());
            Assertions.assertFalse(backfill.isDone(), "journal holder must still be blocked in the edit log");

            journalDone.countDown();
            backfill.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(eligibility.get(10, TimeUnit.SECONDS));
        } catch (Throwable t) {
            failure = t;
        } finally {
            ExportHistoryRetentionTest.ConcurrentTestCleanup.finish(
                    journalDone, executor, failure, backfill, eligibility);
        }

        ExportMgr failingMgr = new ExportMgr();
        ExportJob failing = job(2, "failure", ExportJobState.FINISHED, -1);
        failingMgr.unprotectAddJob(failing);
        Env failingEnv = Mockito.mock(Env.class);
        EditLog failingLog = Mockito.mock(EditLog.class);
        Mockito.when(failingEnv.isReady()).thenReturn(true);
        Mockito.when(failingEnv.isMaster()).thenReturn(true);
        Mockito.when(failingEnv.getEditLog()).thenReturn(failingLog);
        Mockito.doThrow(new IllegalStateException("journal failure"))
                .when(failingLog).logExportUpdateState(failing, ExportJobState.FINISHED);
        try {
            failingMgr.backfillMissingFinishTimes(failingEnv, failingEnv, false, NOW);
            Assertions.fail("journal failure must propagate");
        } catch (IllegalStateException expected) {
            Assertions.assertEquals("journal failure", expected.getMessage());
        }
        Assertions.assertSame(failing, failingMgr.getJob(2));
    }

    private static Image save(Env env, long initialChecksum) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CountingDataOutputStream out = new CountingDataOutputStream(bytes);
        long checksum = env.saveExportJob(out, initialChecksum);
        out.flush();
        return new Image(bytes.toByteArray(), checksum);
    }

    private static Env restore(Image image, long initialChecksum) throws Exception {
        Env restored = new Env(false);
        Assertions.assertEquals(image.checksum, restored.loadExportJob(
                new DataInputStream(new ByteArrayInputStream(image.bytes)), initialChecksum));
        return restored;
    }

    private static void assertRecoveredUnknownJob(ExportJob job) {
        Assertions.assertNotNull(job);
        Assertions.assertEquals(31L, job.getId());
        Assertions.assertEquals(ExportJobState.CANCELLED, job.getState());
        Assertions.assertEquals(-1L, job.getFinishTimeMs());
        Assertions.assertEquals(654_321L, job.getStartTimeMs());
        Assertions.assertEquals("partial-out", job.getOutfileInfo());
        Assertions.assertEquals("backfilled", job.getLabel());
        Assertions.assertEquals(ExportFailMsg.CancelType.RUN_FAIL, job.getFailMsg().getCancelType());
        Assertions.assertTrue(job.getFailMsg().getMsg().contains("FE restarted or Master changed"));
    }

    private static void assertTransferFields(ExportJob job, ExportJobStateTransfer transfer) {
        Assertions.assertEquals(transfer.getJobId(), job.getId());
        Assertions.assertEquals(transfer.getState(), job.getState());
        Assertions.assertEquals(transfer.getStartTimeMs(), job.getStartTimeMs());
        Assertions.assertEquals(transfer.getFinishTimeMs(), job.getFinishTimeMs());
        Assertions.assertEquals(transfer.getFailMsg().getCancelType(), job.getFailMsg().getCancelType());
        Assertions.assertEquals(transfer.getFailMsg().getMsg(), job.getFailMsg().getMsg());
        Assertions.assertEquals(transfer.getOutFileInfo(), job.getOutfileInfo());
    }

    private static void assertLabel(Env env, String label, long expectedId) {
        Assertions.assertEquals(Long.valueOf(expectedId), labels(env).get(1L).get(label));
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Map<String, Long>> labels(Env env) {
        return Deencapsulation.getField(env.getExportMgr(), "dbTolabelToExportJobId");
    }

    private static ThreadInfo awaitBlockedOnMonitor(
            Thread contender, Object monitor, Thread owner) throws InterruptedException {
        Assertions.assertNotNull(contender);
        Assertions.assertNotNull(owner);
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        ThreadInfo info;
        while (true) {
            info = threads.getThreadInfo(contender.getId(), 32);
            if (info != null && info.getThreadState() == Thread.State.BLOCKED) {
                LockInfo lock = info.getLockInfo();
                if (lock != null
                        && lock.getClassName().equals(monitor.getClass().getName())
                        && lock.getIdentityHashCode() == System.identityHashCode(monitor)
                        && info.getLockOwnerId() == owner.getId()) {
                    return info;
                }
            }
            if (System.nanoTime() >= deadline) {
                Assertions.fail("eligibility thread never blocked on the target job monitor");
            }
            Thread.yield();
        }
    }

    private static void assertImageLabelRepresentative(List<ExportJob> jobs, long expectedId) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeInt(jobs.size());
        long checksum = jobs.size();
        for (ExportJob job : jobs) {
            output.writeLong(job.getId());
            checksum ^= job.getId();
            job.write(output);
        }
        output.flush();

        Env restored = new Env(false);
        Assertions.assertEquals(checksum, restored.loadExportJob(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())), 0));
        Map<Long, Map<String, Long>> labels =
                Deencapsulation.getField(restored.getExportMgr(), "dbTolabelToExportJobId");
        Assertions.assertEquals(Long.valueOf(expectedId), labels.get(1L).get("same"));
        Assertions.assertNotNull(restored.getExportMgr().getJob(1));
        Assertions.assertNotNull(restored.getExportMgr().getJob(2));
    }

    private static ExportJob job(long id, String label, ExportJobState state, long finishTimeMs) {
        ExportJob job = new ExportJob(id);
        Deencapsulation.setField(job, "dbId", 1L);
        Deencapsulation.setField(job, "label", label);
        Deencapsulation.setField(job, "state", state);
        Deencapsulation.setField(job, "finishTimeMs", finishTimeMs);
        return job;
    }

    private static class Image {
        private final byte[] bytes;
        private final long checksum;

        private Image(byte[] bytes, long checksum) {
            this.bytes = bytes;
            this.checksum = checksum;
        }
    }
}
