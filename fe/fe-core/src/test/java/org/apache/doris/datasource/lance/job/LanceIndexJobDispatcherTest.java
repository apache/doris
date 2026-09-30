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

package org.apache.doris.datasource.lance.job;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.GenericPool;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Fault-matrix coverage for {@link LanceIndexJobDispatcher}. The two test seams
 * are the manager's edit-log capture (the 3B pattern) and the send seam: a
 * subclass records every journal record and every send with their ordering,
 * injects clean error statuses or transport failures, and never opens a real
 * client pool. The pinned invariants: one round runs the five phases in the
 * fixed order (deadline sweep, epoch sweep, refresh, dispatch); request
 * preparation runs before markRunning, so a preparation failure leaves the job
 * PENDING without a journal record; the markRunning journal record always
 * precedes the send (durable-before-send); an attempt whose compare-and-set
 * lost never sends and never reuses its invocation id; a clean send error, a
 * client borrow failure, or an UNKNOWN_METHOD answer converges NOT_COMMITTED
 * and releases the possible-live slot in the same transition, while a transport
 * failure after the invocation may have started converges UNKNOWN with the
 * slot still held; only a changed backend process epoch releases a slot;
 * per-backend capacity counts possible-live slot ownership rather than RUNNING
 * state, so a released slot stops blocking while a deadline-expired UNKNOWN job
 * still holding its slot keeps occupying capacity; the per-round budget counts
 * only jobs actually made RUNNING, so permanently ineligible jobs are scanned
 * past and never crowd out later ids; a backend at capacity is scanned past for
 * a schedule-available one with a free slot, and the selection expectation
 * spans every registered backend so a full compute backend cannot hide an idle
 * mix peer; a local-filesystem dataset is dispatched only on the asserted
 * single-node topology, one frontend and exactly one REGISTERED backend —
 * heartbeat loss on a multi-BE deployment proves nothing about shared
 * local-file identity; and an idle round writes no journal record. Storage
 * options reach the wire but never a durable record, while the per-dispatch
 * invocation secret reaches both the wire and the journal but never any
 * rendered form (events, toString, logs). A dispatch payload that fails its
 * pre-send bound validation converges NOT_COMMITTED with the internal
 * NEVER_LAUNCHED proof (determined-never-sent evidence, distinct from the
 * FE-proven no-enqueue channel), and the admitted-bound snapshot rides the
 * dispatch only when the record carries it.
 */
public class LanceIndexJobDispatcherTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BE1_ID = 1001L;
    private static final long BE2_ID = 1002L;
    private static final long BE_EPOCH = 55L;
    private static final long REPLACED_BE_EPOCH = 77L;
    private static final long FAR_DEADLINE_MS = System.currentTimeMillis() + 3600_000L;
    private static final String INVOCATION_SECRET = "a3f1c02d97b64e8fad0c31b9e75d2468";
    /** Fake markers that must never surface in any durable or logged form. */
    private static final String FAKE_ACCESS_KEY = "test-ak-marker-not-a-real-credential";
    private static final String FAKE_SECRET_KEY = "test-sk-marker-not-a-real-credential";

    private final List<String> events = new ArrayList<>();
    private MockedStatic<Env> mockedEnv;
    private Env env;
    private SystemInfoService systemInfo;
    private RefreshManager refreshManager;
    private LanceExternalCatalog catalog;
    private AbstractS3CompatibleProperties storageProperties;
    private CatalogMgr catalogMgr;
    private TestManager manager;
    private TestDispatcher dispatcher;

    private int originalIntervalSecond;
    private int originalMaxDispatchPerRound;
    private int originalMaxInflightPerBackend;
    private long originalExecuteDeadlineSecond;
    private boolean originalLocalFileMutation;
    private boolean originalDispatcherPaused;

    @BeforeEach
    public void setUp() throws Exception {
        events.clear();
        mockedEnv = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo = Mockito.mock(SystemInfoService.class));
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(-1)))
                .thenReturn(Collections.singletonList(BE1_ID));
        Mockito.when(systemInfo.getBackend(BE1_ID)).thenReturn(backend(BE1_ID, BE_EPOCH));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, BE_EPOCH));
        // The registered topology of the default single-BE deployment: the selection
        // expectation and the local-file guard both read this view.
        Mockito.when(systemInfo.getAllBackendIds(false)).thenReturn(Collections.singletonList(BE1_ID));
        Mockito.when(env.getFrontends(Mockito.any()))
                .thenReturn(Collections.singletonList(Mockito.mock(Frontend.class)));

        catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(catalog.getName()).thenReturn("lance_cat");
        // The refresh driver proves DONE through the local db/table resolution before
        // it refreshes; the resolved target also keeps the dispatch-side storage-option
        // resolution unaffected (it reads the catalog property, not the relations).
        @SuppressWarnings("unchecked")
        ExternalDatabase<ExternalTable> catalogDb = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(catalogDb).when(catalog).getDbNullable("db1");
        Mockito.doReturn(Mockito.mock(ExternalTable.class)).when(catalogDb).getTableNullable("tbl1");
        storageProperties = Mockito.mock(AbstractS3CompatibleProperties.class);
        Mockito.when(storageProperties.getAccessKey()).thenReturn(FAKE_ACCESS_KEY);
        Mockito.when(storageProperties.getSecretKey()).thenReturn(FAKE_SECRET_KEY);
        Mockito.when(storageProperties.getEndpoint()).thenReturn("http://minio.example:9000");
        Mockito.when(storageProperties.getRegion()).thenReturn("us-east-1");
        org.apache.doris.datasource.CatalogProperty catalogProperty =
                Mockito.mock(org.apache.doris.datasource.CatalogProperty.class);
        Mockito.when(catalogProperty.getOrderedStoragePropertiesList())
                .thenReturn(Collections.singletonList(storageProperties));
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        CatalogMgr mgr = new CatalogMgr();
        java.lang.reflect.Field catalogs = CatalogMgr.class.getDeclaredField("idToCatalog");
        catalogs.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, CatalogIf> registered = (Map<Long, CatalogIf>) catalogs.get(mgr);
        registered.put(CATALOG_ID, catalog);
        catalogMgr = mgr;
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        refreshManager = Mockito.mock(RefreshManager.class);
        Mockito.doAnswer(invocation -> {
            events.add("refresh:" + invocation.getArgument(1) + "." + invocation.getArgument(2));
            return null;
        }).when(refreshManager).handleRefreshTable(Mockito.anyLong(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean());
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);

        manager = new TestManager(events);
        dispatcher = new TestDispatcher(manager, events);

        originalIntervalSecond = Config.lance_index_job_dispatch_interval_second;
        originalMaxDispatchPerRound = Config.lance_index_job_max_dispatch_per_round;
        originalMaxInflightPerBackend = Config.lance_index_job_max_inflight_per_backend;
        originalExecuteDeadlineSecond = Config.lance_index_job_execute_deadline_second;
        originalLocalFileMutation = Config.enable_lance_index_local_file_mutation;
        originalDispatcherPaused = Config.lance_index_job_dispatcher_paused;
        Config.lance_index_job_dispatcher_paused = false;
    }

    @AfterEach
    public void tearDown() {
        Config.lance_index_job_dispatch_interval_second = originalIntervalSecond;
        Config.lance_index_job_max_dispatch_per_round = originalMaxDispatchPerRound;
        Config.lance_index_job_max_inflight_per_backend = originalMaxInflightPerBackend;
        Config.lance_index_job_execute_deadline_second = originalExecuteDeadlineSecond;
        Config.enable_lance_index_local_file_mutation = originalLocalFileMutation;
        Config.lance_index_job_dispatcher_paused = originalDispatcherPaused;
        mockedEnv.close();
    }

    // ------------------------------------------------------------------
    // Round structure
    // ------------------------------------------------------------------

    @Test
    public void oneRoundRunsTheFivePhasesInFixedOrder() throws Exception {
        // This case pins the phase order, not the capacity rule: raise the per-backend
        // slot cap so the slot holders below (the deadline-swept UNKNOWN keeps its
        // slot, the COMMITTED one keeps its own) cannot crowd out job 4's dispatch.
        Config.lance_index_job_max_inflight_per_backend = 10;
        // Job 1: expired RUNNING, converged by the deadline sweep. Its backend keeps
        // the recorded epoch so the epoch sweep leaves it alone.
        admit(1L, "IdxDeadline", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, System.currentTimeMillis() - 1_000L));
        // Job 2: RUNNING on a backend whose process epoch was replaced.
        admit(2L, "IdxEpoch", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE2_ID, BE_EPOCH, "inv-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, REPLACED_BE_EPOCH));
        // Job 3: terminal with a refresh obligation.
        admit(3L, "IdxRefresh", LOCATOR);
        Assertions.assertTrue(manager.markRunning(3L, 0L, BE1_ID, BE_EPOCH, "inv-3",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(3L, 1L, "inv-3", BE_EPOCH, okResult()));
        // Job 4: PENDING, dispatched this round.
        admit(4L, "IdxPending", LOCATOR);

        dispatcher.runAfterCatalogReady();

        int deadlineIdx = events.indexOf(journal(1L, "UNKNOWN", "NOT_REQUIRED", true));
        int epochIdx = events.indexOf(journal(2L, "RUNNING", "NOT_REQUIRED", false));
        int refreshRunningIdx = events.indexOf(journal(3L, "COMMITTED", "RUNNING", true));
        int refreshDoneIdx = events.indexOf(journal(3L, "COMMITTED", "DONE", true));
        int dispatchIdx = events.indexOf(journal(4L, "RUNNING", "NOT_REQUIRED", true));
        int sendIdx = events.indexOf("send:4");
        Assertions.assertTrue(deadlineIdx >= 0, events.toString());
        Assertions.assertTrue(epochIdx >= 0, events.toString());
        Assertions.assertTrue(refreshRunningIdx >= 0, events.toString());
        Assertions.assertTrue(refreshDoneIdx >= 0, events.toString());
        Assertions.assertTrue(dispatchIdx >= 0, events.toString());
        Assertions.assertTrue(sendIdx >= 0, events.toString());
        Assertions.assertTrue(deadlineIdx < epochIdx, events.toString());
        Assertions.assertTrue(epochIdx < refreshRunningIdx, events.toString());
        Assertions.assertTrue(refreshRunningIdx < events.indexOf("refresh:db1.tbl1"), events.toString());
        Assertions.assertTrue(events.indexOf("refresh:db1.tbl1") < refreshDoneIdx, events.toString());
        Assertions.assertTrue(refreshDoneIdx < dispatchIdx, events.toString());
        Assertions.assertTrue(dispatchIdx < sendIdx, events.toString());

        // Phase outcomes: the deadline sweep keeps fence and slot, the epoch sweep only
        // releases the slot, the refresh completes, and the dispatch is durable RUNNING.
        LanceIndexJob swept = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, swept.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT, swept.getResult().getResultCode());
        Assertions.assertTrue(swept.holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.isFenceHeld(swept.fenceKey()));
        LanceIndexJob epochSwept = manager.getJob(2L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, epochSwept.getMutationState());
        Assertions.assertFalse(epochSwept.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE,
                epochSwept.getTerminationProof());
        Assertions.assertTrue(manager.isFenceHeld(epochSwept.fenceKey()));
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(3L).getRefreshState());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(4L).getMutationState());
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void markRunningJournalPrecedesTheSend() throws Exception {
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        int journalIdx = events.indexOf(journal(1L, "RUNNING", "NOT_REQUIRED", true));
        int sendIdx = events.indexOf("send:1");
        Assertions.assertTrue(journalIdx >= 0, events.toString());
        Assertions.assertTrue(sendIdx >= 0, events.toString());
        // Direct durable-before-send evidence: the only network path is the send seam,
        // and it fired strictly after the markRunning journal record.
        Assertions.assertTrue(journalIdx < sendIdx, events.toString());
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void dispatchRequestCarriesTheDurableDispatchIdentity() throws Exception {
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(1L, request.getJobId());
        Assertions.assertEquals(1L, request.getDispatchRevision());
        Assertions.assertEquals(stored.getInvocationId(), request.getInvocationId());
        // The dispatch secret travels with the identity and is the journaled one.
        Assertions.assertTrue(request.isSetInvocationSecret());
        Assertions.assertEquals(stored.getInvocationSecret(), request.getInvocationSecret());
        Assertions.assertEquals(BE_EPOCH, request.getBeProcessEpoch());
        Assertions.assertTrue(request.getDeadlineMs() > System.currentTimeMillis());
        Assertions.assertEquals(TLanceIndexMutationType.CREATE, request.getMutationType());
        Assertions.assertEquals("IdxA", request.getIndexName());
        Assertions.assertEquals("v", request.getColumnName());
        Assertions.assertEquals("IVF_PQ", request.getIndexType());
        Assertions.assertEquals(LOCATOR, request.getDatasetUri());
        Assertions.assertEquals(7L, request.getAdmittedDatasetVersion());
        Assertions.assertFalse(request.isIfNotExists());
        Assertions.assertFalse(request.isIfExists());
    }

    @Test
    public void invocationSecretIsFreshPerAttemptJournaledAndNeverRendered() throws Exception {
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        String firstSecret = dispatcher.sends.get(0).getInvocationSecret();
        // 128 bits hex-encoded: a 32-character token, independent of the UUID invocation
        // id (a secret derivable from a shown value would authorize reports like the
        // shown value does).
        Assertions.assertEquals(32, firstSecret.length());
        Assertions.assertNotEquals(manager.getJob(1L).getInvocationId(), firstSecret);
        // The journal recorded exactly the secret the wire carries.
        Assertions.assertEquals(firstSecret, manager.getJob(1L).getInvocationSecret());

        // A second attempt mints an independent secret: job 2's first markRunning loses
        // the compare-and-set (a fresh identity is built from scratch next round).
        admit(2L, "IdxB", LOCATOR);
        manager.rejectNextMarkRunning = true;
        dispatcher.runAfterCatalogReady();
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(2, dispatcher.sends.size());
        String secondSecret = dispatcher.sends.get(1).getInvocationSecret();
        Assertions.assertNotEquals(firstSecret, secondSecret);
        Assertions.assertEquals(secondSecret, manager.getJob(2L).getInvocationSecret());

        // Never rendered: the journal event stream and the record's toString form (the
        // two shapes every log line of the manager reuses) carry no secret material.
        for (String event : events) {
            Assertions.assertFalse(event.contains(firstSecret), "an event leaked the first secret");
            Assertions.assertFalse(event.contains(secondSecret), "an event leaked the second secret");
        }
        for (LanceIndexJob record : manager.editLog) {
            Assertions.assertFalse(record.toString().contains(firstSecret), "toString leaked the first secret");
            Assertions.assertFalse(record.toString().contains(secondSecret), "toString leaked the second secret");
        }
    }

    @Test
    public void idleRoundWritesNoJournalRecord() {
        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(manager.editLog.isEmpty(), manager.editLog.toString());
        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(0, manager.getJobCount());
    }

    @Test
    public void intervalIsRereadFromMutableConfigEachRound() {
        Config.lance_index_job_dispatch_interval_second = 7;
        dispatcher.runAfterCatalogReady();
        Assertions.assertEquals(7_000L, dispatcher.getInterval());

        Config.lance_index_job_dispatch_interval_second = 9;
        dispatcher.runAfterCatalogReady();
        Assertions.assertEquals(9_000L, dispatcher.getInterval());
    }

    @Test
    public void nonPositiveDispatchIntervalIsClampedAtConsumption() {
        // fe.conf bypasses the config validator; the consumption clamp keeps the daemon
        // thread alive instead of dying inside Thread.sleep.
        Config.lance_index_job_dispatch_interval_second = -5;

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1_000L, dispatcher.getInterval());
    }

    @Test
    public void longIntervalIsSleptInBoundedSlicesWhileTheRoundStillRuns() throws Exception {
        Config.lance_index_job_dispatch_interval_second = 3600;
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        // The daemon must not actually sleep an hour: the slice bound caps the sleep,
        // and the round itself still ran and dispatched the job.
        Assertions.assertEquals(10_000L, dispatcher.getInterval());
        Assertions.assertTrue(events.contains("send:1"), events.toString());
    }

    @Test
    public void longIntervalSkipsRoundsUntilItElapses() throws Exception {
        Config.lance_index_job_dispatch_interval_second = 3600;
        admit(1L, "IdxA", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:1"), events.toString());

        // A wake one slice later: the configured hour has not elapsed, so the round
        // is skipped and job 2 is not dispatched.
        admit(2L, "IdxB", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertFalse(events.contains("send:2"), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(10_000L, dispatcher.getInterval());

        // Once the configured interval has elapsed (seam clock, no real sleeping),
        // the next wake runs the round.
        dispatcher.nowOffsetMs = 3600_000L;
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:2"), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
    }

    @Test
    public void lengthenedIntervalTakesEffectAtTheNextWake() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:1"), events.toString());

        // Lengthen right after a round: the very next wake honors the new period and
        // skips, instead of running on the stale short period.
        Config.lance_index_job_dispatch_interval_second = 3600;
        admit(2L, "IdxB", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertFalse(events.contains("send:2"), events.toString());

        dispatcher.nowOffsetMs = 3600_000L;
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:2"), events.toString());
    }

    @Test
    public void shortenedIntervalTakesEffectWithinOneSlice() throws Exception {
        // A round adopting a one-hour period would previously sleep the whole hour no
        // matter what the config says later. With sliced sleeps the shortened config
        // bypasses the elapsed check and the very next wake dispatches.
        Config.lance_index_job_dispatch_interval_second = 3600;
        admit(1L, "IdxA", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:1"), events.toString());

        Config.lance_index_job_dispatch_interval_second = 10;
        admit(2L, "IdxB", LOCATOR);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:2"), events.toString());
        Assertions.assertEquals(10_000L, dispatcher.getInterval());
    }

    @Test
    public void pausedDispatcherSkipsDispatchButKeepsSweepsAndRefreshAlive() throws Exception {
        // Job 1: expired RUNNING, the deadline sweep still converges it under pause.
        admit(1L, "IdxDeadline", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, System.currentTimeMillis() - 1_000L));
        // Job 2: terminal with a refresh obligation, the refresh driver still runs.
        admit(2L, "IdxRefresh", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "inv-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(2L, 1L, "inv-2", BE_EPOCH, okResult()));
        // Job 3: PENDING; pause must hold it back.
        admit(3L, "IdxPending", LOCATOR);
        Config.lance_index_job_dispatcher_paused = true;

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(events.contains(journal(1L, "UNKNOWN", "NOT_REQUIRED", true)), events.toString());
        Assertions.assertTrue(events.contains(journal(2L, "COMMITTED", "DONE", true)), events.toString());
        Assertions.assertTrue(events.contains("refresh:db1.tbl1"), events.toString());
        Assertions.assertFalse(events.contains(journal(3L, "RUNNING", "NOT_REQUIRED", true)), events.toString());
        Assertions.assertFalse(events.contains("send:3"), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());
    }

    @Test
    public void pauseFlippedMidRoundSkipsTheRestWithoutConsumingBudget() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        admit(2L, "IdxB", LOCATOR);
        // Flip the switch as soon as the first dispatch reaches the send seam: the
        // per-job check must then stop the loop before job 2, while the per-round
        // budget (default 16, only 1 consumed) is provably not the blocker.
        dispatcher.onSend = () -> Config.lance_index_job_dispatcher_paused = true;

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(2L).getMutationState());
    }

    @Test
    public void unpauseResumesDispatch() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Config.lance_index_job_dispatcher_paused = true;
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());

        Config.lance_index_job_dispatcher_paused = false;
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(events.contains("send:1"), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void zeroDispatchCapsAreClampedSoProgressContinues() throws Exception {
        Config.lance_index_job_max_dispatch_per_round = 0;
        Config.lance_index_job_max_inflight_per_backend = 0;
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void nonPositiveExecuteDeadlineIsClamped() throws Exception {
        Config.lance_index_job_execute_deadline_second = 0L;
        admit(1L, "IdxA", LOCATOR);
        long beforeMs = System.currentTimeMillis();

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        long deadlineMs = dispatcher.sends.get(0).getDeadlineMs();
        Assertions.assertTrue(deadlineMs >= beforeMs + 1_000L, "deadline=" + deadlineMs + " before=" + beforeMs);
        Assertions.assertTrue(deadlineMs <= System.currentTimeMillis() + 1_000L);
    }

    @Test
    public void dispatchCarriesTheEpochCapturedAtMarkRunningNotALaterHeartbeat() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Backend selected = systemInfo.getBackend(BE1_ID);
        manager.afterMarkRunning = () -> selected.setLastStartTime(REPLACED_BE_EPOCH);

        dispatcher.runAfterCatalogReady();

        // A heartbeat landing between the durable record and the send must not split
        // the dispatch identity: the wire carries the same epoch the journal recorded,
        // which is what the callback matches and the epoch sweep releases against.
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(BE_EPOCH, dispatcher.sends.get(0).getBeProcessEpoch());
        Assertions.assertEquals(BE_EPOCH, manager.getJob(1L).getBeProcessEpoch().longValue());
    }

    @Test
    public void leadershipLossBeforeTheSendSendsNothing() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // The entry check passes, then mastership is lost before the pre-send recheck.
        Mockito.when(env.isMaster()).thenReturn(true, false);

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void checkpointThreadSkipsTheRound() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        mockedEnv.when(Env::isCheckpointThread).thenReturn(true);

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        // Only the admission record exists: the round never ran.
        Assertions.assertEquals(1, manager.editLog.size(), manager.editLog.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
    }

    // ------------------------------------------------------------------
    // No-second-dispatch
    // ------------------------------------------------------------------

    @Test
    public void markRunningCasLossSkipsTheSendAndRetriesNextRound() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        manager.rejectNextMarkRunning = true;

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(events.contains("markRunningRejected:1"), events.toString());
        Assertions.assertFalse(events.contains("send:1"), events.toString());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());

        // The next round builds a fresh identity from scratch and sends exactly once.
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(2, manager.editLog.size());
    }

    @Test
    public void runningJobIsNeverRedispatchedAcrossRounds() throws Exception {
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();
        dispatcher.runAfterCatalogReady();
        dispatcher.runAfterCatalogReady();

        // After the one send of the one durable RUNNING record, later rounds neither
        // resend nor rewrite anything: convergence belongs to the callback or sweeps.
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(2, manager.editLog.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void preSendRecheckFailureSendsNothing() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        manager.hijackedInvocationId = "concurrent-invocation";

        dispatcher.runAfterCatalogReady();

        // markRunning won, but the job was advanced concurrently before the send: the
        // recheck fails, nothing is sent, and no resend ever happens for the old identity.
        Assertions.assertTrue(events.contains("hijacked:1"), events.toString());
        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals("concurrent-invocation", stored.getInvocationId());
        // Exactly the create and markRunning records: the dispatcher neither converged
        // the job nor journaled anything else; the deadline sweep owns it now.
        Assertions.assertEquals(2, manager.editLog.size());
    }

    // ------------------------------------------------------------------
    // Send outcomes
    // ------------------------------------------------------------------

    @Test
    public void cleanSendErrorConvergesNotCommitted() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        dispatcher.statusToReturn = new TStatus(TStatusCode.INTERNAL_ERROR);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        // A clean error status proves the dispatch was never enqueued, so the
        // invocation is known never to have executed: NOT_COMMITTED, no refresh owed,
        // and the possible-live slot released in the same durable transition. The
        // bounded status-code name survives in the persisted reason, so SHOW can tell
        // an unavailable worker from a resource rejection; the raw backend message
        // never reaches the durable record.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                stored.getResult().getResultCode());
        Assertions.assertTrue(stored.getResult().getSanitizedMessage().contains("INTERNAL_ERROR"),
                stored.getResult().getSanitizedMessage());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.NOT_ENQUEUED, stored.getTerminationProof());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void borrowFailureConvergesNotCommittedAndReleasesTheSlot() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // The pool never handed out a client: no connection was established, so the
        // dispatch provably never reached the backend.
        dispatcher.sendException = new LanceIndexJobDispatcher.PreInvocationSendException(
                "no backend client could be borrowed; the dispatch was never sent",
                new RuntimeException("connection refused"));
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                stored.getResult().getResultCode());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.NOT_ENQUEUED, stored.getTerminationProof());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void unknownMethodFromAnOldBackendConvergesNotCommittedAndReleasesTheSlot() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // Rolling upgrade: an old backend answered UNKNOWN_METHOD for the new RPC, so
        // it provably never enqueued the dispatch.
        dispatcher.sendException = new LanceIndexJobDispatcher.PreInvocationSendException(
                "backend does not serve submitLanceIndexJob (rolling upgrade); not enqueued",
                new TApplicationException(TApplicationException.UNKNOWN_METHOD));
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                stored.getResult().getResultCode());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.NOT_ENQUEUED, stored.getTerminationProof());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void sendExecuteRequestClassifiesOnlyProvenPreInvocationFailures() throws Exception {
        // The real send path (no seam) against a swapped client pool: a borrow failure
        // and an UNKNOWN_METHOD answer come out wrapped as proven pre-invocation;
        // anything raised by the call itself propagates unwrapped as ambiguous.
        GenericPool<BackendService.Client> originalPool = ClientPool.backendPool;
        Backend backend = backend(BE1_ID, BE_EPOCH);
        TLanceIndexJobDispatch anyDispatch = new TLanceIndexJobDispatch();
        LanceIndexJobDispatcher realDispatcher = new LanceIndexJobDispatcher(manager);
        try {
            GenericPool<BackendService.Client> pool = Mockito.mock(GenericPool.class);
            Mockito.when(pool.borrowObject(Mockito.any(TNetworkAddress.class)))
                    .thenThrow(new RuntimeException("connection refused"));
            ClientPool.backendPool = pool;
            Assertions.assertThrows(LanceIndexJobDispatcher.PreInvocationSendException.class,
                    () -> realDispatcher.sendExecuteRequest(backend, anyDispatch));

            BackendService.Client oldBackend = Mockito.mock(BackendService.Client.class);
            Mockito.when(oldBackend.submitLanceIndexJob(Mockito.any()))
                    .thenThrow(new TApplicationException(TApplicationException.UNKNOWN_METHOD));
            Mockito.when(pool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(oldBackend);
            Assertions.assertThrows(LanceIndexJobDispatcher.PreInvocationSendException.class,
                    () -> realDispatcher.sendExecuteRequest(backend, anyDispatch));
            Mockito.verify(pool, Mockito.atLeastOnce()).invalidateObject(Mockito.any(TNetworkAddress.class),
                    Mockito.eq(oldBackend));

            BackendService.Client confusedBackend = Mockito.mock(BackendService.Client.class);
            TApplicationException ambiguous = new TApplicationException(
                    TApplicationException.INVALID_MESSAGE_TYPE);
            Mockito.when(confusedBackend.submitLanceIndexJob(Mockito.any())).thenThrow(ambiguous);
            Mockito.when(pool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(confusedBackend);
            TApplicationException thrown = Assertions.assertThrows(TApplicationException.class,
                    () -> realDispatcher.sendExecuteRequest(backend, anyDispatch));
            Assertions.assertSame(ambiguous, thrown);
        } finally {
            ClientPool.backendPool = originalPool;
        }
    }

    @Test
    public void sendWithoutAStatusConvergesUnknownNotCommitted() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        dispatcher.statusToReturn = null;
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        // The absence of a status is the absence of a trusted answer, not a clean
        // rejection: only a complete error status proves the dispatch was not
        // enqueued, so this converges UNKNOWN with everything still held.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                stored.getResult().getResultCode());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void sendFailureConvergesUnknownAndKeepsThePossibleLiveSlot() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        dispatcher.throwOnSend = true;
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        // The request may have reached the backend, so nothing about the outcome is
        // trusted: UNKNOWN through the only channel, with fence, quota, and the
        // possible-live slot all still held.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT, stored.getResult().getResultCode());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(containsJob(manager.getJobsHoldingPossibleLiveSlot(), 1L));
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void preparationFailureStaysPendingAndRecoversWhenTheCatalogReturns() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // The job's catalog resolves to nothing, as in the ALTER CATALOG RENAME window
        // where CatalogMgr has the catalog temporarily removed: storage options cannot
        // be resolved. Built before the stubbing: constructing it inside when(...)
        // triggers Mockito's unfinished-stubbing detection.
        CatalogMgr catalogless = new CatalogMgr();
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogless);
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        // Preparation runs before the durable boundary: nothing was marked, nothing
        // was sent, and no journal record exists — the job just waits for a later round.
        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals(0L, stored.getRevision());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(journalBefore, manager.editLog.size());

        // The rename finished and the catalog id resolves again, so the next round
        // dispatches the job normally.
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    // ------------------------------------------------------------------
    // Pre-send payload validation (D4 bounds)
    // ------------------------------------------------------------------

    @Test
    public void payloadValidationAcceptsAtLimitAndRejectsOneOver() {
        // Exactly at every bound the dispatch is legal...
        Map<String, String> atLimit = new java.util.HashMap<>();
        for (int i = 0; i < LanceIndexDispatchBounds.MAX_STORAGE_OPTIONS; i++) {
            atLimit.put(repeat('k', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_KEY_BYTES - 4) + i,
                    repeat('v', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES));
        }
        TLanceIndexJobDispatch maximal = minimalDispatch(1L).setStorageOptions(atLimit);
        LanceIndexDispatchBounds.validatePayload(maximal);

        // ...and one over any bound is rejected.
        Map<String, String> tooMany = new java.util.HashMap<>(atLimit);
        tooMany.put("one.too.many", "v");
        assertPayloadRejected(minimalDispatch(1L).setStorageOptions(tooMany));

        Map<String, String> keyTooLong = new java.util.HashMap<>();
        keyTooLong.put(repeat('k', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_KEY_BYTES + 1), "v");
        assertPayloadRejected(minimalDispatch(1L).setStorageOptions(keyTooLong));

        Map<String, String> valueTooLong = new java.util.HashMap<>();
        valueTooLong.put("k", repeat('v', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES + 1));
        assertPayloadRejected(minimalDispatch(1L).setStorageOptions(valueTooLong));

        // A serialized frame past the 512 KiB bound is rejected even with individually
        // legal options (the map bound alone keeps this unreachable in practice; the
        // total-size check is the second line of defense).
        Map<String, String> frameTooBig = new java.util.HashMap<>();
        for (int i = 0; i < LanceIndexDispatchBounds.MAX_STORAGE_OPTIONS; i++) {
            frameTooBig.put("key-" + i, repeat('v', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES));
        }
        TLanceIndexJobDispatch padded = minimalDispatch(1L).setStorageOptions(frameTooBig)
                .setSchemaContractJson(repeat('s', LanceIndexDispatchBounds.MAX_DISPATCH_BYTES));
        assertPayloadRejected(padded);
    }

    @Test
    public void oversizedPayloadConvergesNotCommittedWithoutASend() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // A credential value one byte past the protocol bound: resolution succeeds, the
        // pre-send payload validation fails, nothing is ever sent. Determined-never-sent
        // evidence that is also permanent (the same record rebuilds the same payload
        // every round), so the job converges NOT_COMMITTED with the internal
        // NEVER_LAUNCHED proof in one durable transition instead of looping PENDING.
        Mockito.when(storageProperties.getEndpoint())
                .thenReturn(repeat('e', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES + 1));

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                stored.getResult().getResultCode());
        Assertions.assertEquals(LanceIndexTerminationProof.NEVER_LAUNCHED, stored.getTerminationProof());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(3, manager.editLog.size());
    }

    @Test
    public void atLimitPayloadIsSent() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Mockito.when(storageProperties.getEndpoint())
                .thenReturn(repeat('e', LanceIndexDispatchBounds.MAX_STORAGE_OPTION_VALUE_BYTES));

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    // ------------------------------------------------------------------
    // Admitted-bound snapshot on the wire
    // ------------------------------------------------------------------

    @Test
    public void dispatchCarriesTheAdmittedBoundSnapshot() throws Exception {
        admitWithBounds(1L, "IdxSnap", LOCATOR, 64, 32);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        Assertions.assertTrue(request.isSetMaxNumPartitions());
        Assertions.assertEquals(64, request.getMaxNumPartitions());
        Assertions.assertTrue(request.isSetMaxNumSubVectors());
        Assertions.assertEquals(32, request.getMaxNumSubVectors());
    }

    @Test
    public void legacyRecordWithoutBoundSnapshotLeavesTheFieldsUnset() throws Exception {
        // The shared admit helper builds a record the pre-snapshot way: both bound fields
        // stay null, and the dispatch leaves the wire fields unset for the worker to
        // reject safely.
        admit(1L, "IdxLegacy", LOCATOR);
        Assertions.assertNull(manager.getJob(1L).getAdmittedMaxNumPartitions());
        Assertions.assertNull(manager.getJob(1L).getAdmittedMaxNumSubVectors());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        Assertions.assertFalse(request.isSetMaxNumPartitions());
        Assertions.assertFalse(request.isSetMaxNumSubVectors());
    }

    @Test
    public void createDispatchCarriesThePersistedSchemaContract() throws Exception {
        admitWithContract(1L, "IdxA", LOCATOR, LanceIndexJobMutationType.CREATE, false, true);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        Assertions.assertFalse(request.getSchemaContractJson().isEmpty());
        // The wire form is exactly the Gson serialization of the durable contract.
        Assertions.assertEquals(GsonUtils.GSON.toJson(manager.getJob(1L).getSchemaContract()),
                request.getSchemaContractJson());
        Assertions.assertEquals("IVF_PQ", request.getIndexType());
        Assertions.assertEquals("v", request.getColumnName());
    }

    @Test
    public void dropDispatchCarriesThePersistedSchemaContract() throws Exception {
        admitWithContract(1L, "IdxA", LOCATOR, LanceIndexJobMutationType.DROP, true, false);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        Assertions.assertEquals(TLanceIndexMutationType.DROP, request.getMutationType());
        // A DROP with a persisted contract sends it like a CREATE; only the build-definition
        // fields a DROP never carries travel as the empty string.
        Assertions.assertFalse(request.getSchemaContractJson().isEmpty());
        Assertions.assertEquals(GsonUtils.GSON.toJson(manager.getJob(1L).getSchemaContract()),
                request.getSchemaContractJson());
        Assertions.assertEquals("v", request.getColumnName());
        Assertions.assertEquals("", request.getIndexType());
        Assertions.assertFalse(request.isSetPropertiesJson());
    }

    @Test
    public void legacyDropRecordDispatchesWithEmptyContractAndColumn() throws Exception {
        // A DROP record journaled before DROP admissions persisted the schema contract has
        // null "sc"/"cn" slots. Its dispatch must carry column_name="" and
        // schema_contract_json="" — the FE-side wire premise for the new worker's safe
        // rejection (BE side, parse_schema_contract("") -> UNSUPPORTED is pinned by
        // DropContractSemantics). A regression in buildDispatch's null-to-empty mapping
        // would otherwise be caught only against a hand-built frame.
        manager.createJob(new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                "IdxLegacyDrop", LanceIndexNameNormalizer.normalize("IdxLegacyDrop"),
                LanceIndexJobMutationType.DROP, false, true, null, null, null, 7L, null),
                100, 100, 100);
        Assertions.assertNull(manager.getJob(1L).getSchemaContract());
        Assertions.assertNull(manager.getJob(1L).getColumnName());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        TLanceIndexJobDispatch request = dispatcher.sends.get(0);
        Assertions.assertEquals(TLanceIndexMutationType.DROP, request.getMutationType());
        Assertions.assertEquals("", request.getColumnName());
        Assertions.assertEquals("", request.getSchemaContractJson());
        Assertions.assertEquals("", request.getIndexType());
        Assertions.assertFalse(request.isSetPropertiesJson());
        // The legacy record also predates the admitted-bound snapshot: fields 17/18 stay
        // unset, same as the CREATE legacy shape.
        Assertions.assertFalse(request.isSetMaxNumPartitions());
        Assertions.assertFalse(request.isSetMaxNumSubVectors());
    }

    // ------------------------------------------------------------------
    // Backpressure
    // ------------------------------------------------------------------

    @Test
    public void perRoundCapDefersDispatchesBeyondTheLimit() throws Exception {
        Config.lance_index_job_max_dispatch_per_round = 2;
        // Roomy per-backend cap so only the per-round limit binds in this test.
        Config.lance_index_job_max_inflight_per_backend = 8;
        admit(1L, "IdxA", LOCATOR);
        admit(2L, "IdxB", LOCATOR);
        admit(3L, "IdxC", LOCATOR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(2, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(3, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(3L).getMutationState());
    }

    @Test
    public void noEnqueueRejectionReclaimsTheRoundsCapacity() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 1;
        admit(1L, "IdxA", LOCATOR);
        admit(2L, "IdxB", LOCATOR);
        // The shipped stub answers every dispatch with a clean rejection: the durable
        // no-enqueue transition releases the slot, and the round must hand its local
        // capacity straight back or only one of these jobs would dispatch per round.
        dispatcher.statusToReturn = new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(2, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, manager.getJob(2L).getMutationState());
        Assertions.assertFalse(manager.getJob(1L).holdsPossibleLiveSlot());
        Assertions.assertFalse(manager.getJob(2L).holdsPossibleLiveSlot());
    }

    @Test
    public void selectionPolicyIncludesSameHostAndComputeBackends() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        ArgumentCaptor<BeSelectionPolicy> policy = ArgumentCaptor.forClass(BeSelectionPolicy.class);

        dispatcher.runAfterCatalogReady();

        Mockito.verify(systemInfo).selectBackendIdsByPolicy(policy.capture(), Mockito.eq(-1));
        // Every usable worker must be selectable: the policy defaults hide all but one
        // backend per host and filter every compute-role backend out, and the default
        // expectation of zero hides every mix peer once any compute backend exists.
        Assertions.assertTrue(policy.getValue().needScheduleAvailable);
        Assertions.assertTrue(policy.getValue().allowOnSameHost);
        Assertions.assertTrue(policy.getValue().preferComputeNode);
        Assertions.assertEquals(1, policy.getValue().expectBeNum);
        Assertions.assertEquals(1, dispatcher.sends.size());
    }

    @Test
    public void fullComputeBackendFallsThroughToAnIdleMixPeer() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 1;
        // One compute-role backend and one mix backend, both registered: selection is
        // delegated to the real policy over them, so the compute-first-then-fill
        // contract of getCandidateBackends is what stands under test — a stubbed id
        // list could not tell the fixed expectation from the broken default.
        Backend computeBackend = backend(BE2_ID, BE_EPOCH);
        Map<String, String> computeTagMap = Tag.create(Tag.TYPE_LOCATION, "group_a").toMap();
        computeTagMap.put(Tag.TYPE_ROLE, Tag.VALUE_COMPUTATION);
        computeBackend.setTagMap(computeTagMap);
        Backend mixBackend = backend(BE1_ID, BE_EPOCH);
        List<Backend> registeredBackends = Arrays.asList(computeBackend, mixBackend);
        Mockito.when(systemInfo.getAllBackendIds(false)).thenReturn(Arrays.asList(BE2_ID, BE1_ID));
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(-1)))
                .thenAnswer(invocation -> {
                    List<Long> candidateIds = new ArrayList<>();
                    for (Backend candidate : ((BeSelectionPolicy) invocation.getArgument(0))
                            .getCandidateBackends(registeredBackends)) {
                        candidateIds.add(candidate.getId());
                    }
                    return candidateIds;
                });
        // The compute backend sits at the possible-live cap, the mix peer is idle.
        admit(1L, "IdxOccupied", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE2_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        admit(2L, "IdxWaiting", LOCATOR);

        dispatcher.runAfterCatalogReady();

        // The policy's zero expectation would return only the compute candidate and
        // leave this job PENDING every round despite the free backend; the registered
        // count lets the mix peer be filled in and the capacity loop dispatch to it.
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(BE1_ID, manager.getJob(2L).getBackendId().longValue());
    }

    @Test
    public void stalledSendBoundsTheRoundsBlockingDispatchWork() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 8;
        admit(1L, "IdxA", LOCATOR);
        admit(2L, "IdxB", LOCATOR);
        admit(3L, "IdxC", LOCATOR);
        // Job 1's RPC stalls for just over one backend RPC timeout (the client-pool
        // default the round budget is taken from); the seam clock jumps inside the
        // send, so no real sleeping is involved.
        dispatcher.onSend = () -> dispatcher.nowOffsetMs += 61_000L;

        dispatcher.runAfterCatalogReady();

        // The stalled send alone exhausts the round's blocking budget: jobs 2 and 3 are
        // deferred to the next round instead of compounding up to the per-round cap
        // times the timeout on the one thread that also runs the sweeps and the
        // refresh driver.
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());

        dispatcher.onSend = null;
        dispatcher.runAfterCatalogReady();

        // A fresh round gets a fresh budget, so the deferred jobs dispatch normally.
        Assertions.assertEquals(3, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(3L).getMutationState());
    }

    @Test
    public void perBackendInflightCapDefersDispatchUntilASlotFrees() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 2;
        admit(1L, "IdxOccupied1", LOCATOR);
        admit(2L, "IdxOccupied2", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "place-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "place-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        admit(3L, "IdxWaiting", LOCATOR);
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        // Both possible-live slots of the only selectable backend are taken: the round
        // attempts nothing and the job keeps waiting as PENDING.
        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());

        // A termination proof releases one slot while the job itself stays RUNNING:
        // capacity follows slot ownership, not the mutation state, so the next round
        // sends even though both placeholder jobs are still RUNNING.
        Assertions.assertTrue(manager.recordTerminationProof(1L, 1L, BE1_ID, BE_EPOCH, "place-1",
                LanceIndexTerminationProof.CHILD_REAPED));
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(3L).getMutationState());
    }

    @Test
    public void unknownJobHoldingItsSlotStillOccupiesBackendCapacity() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 2;
        admit(1L, "IdxExpired", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        // Deadline expiry converges the job UNKNOWN but never proves termination: its
        // possible-live slot stays held and must keep occupying backend capacity.
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, "inv-1", BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, "deadline expired", false)));
        admit(2L, "IdxCurrent", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "inv-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        admit(3L, "IdxWaiting", LOCATOR);

        dispatcher.runAfterCatalogReady();

        // One RUNNING plus one UNKNOWN slot holder fill the cap of two: a RUNNING-only
        // count would see a free slot here, the slot-ownership count does not.
        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot());
    }

    @Test
    public void fullBackendIsSkippedForOneWithAFreeSlot() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 1;
        admit(1L, "IdxOccupied", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        // The policy returns both backends with the full one first: the dispatcher must
        // scan past it instead of deferring the job for a whole round.
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(-1)))
                .thenReturn(Arrays.asList(BE1_ID, BE2_ID));
        admit(2L, "IdxWaiting", LOCATOR);

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
        Assertions.assertEquals(BE2_ID, manager.getJob(2L).getBackendId().longValue());
    }

    @Test
    public void noSelectableBackendKeepsTheJobPending() throws Exception {
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(-1)))
                .thenReturn(Collections.emptyList());
        admit(1L, "IdxA", LOCATOR);
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());
    }

    @Test
    public void ineligibleJobsNeverCrowdOutLaterDispatchableIds() throws Exception {
        Config.lance_index_job_max_dispatch_per_round = 1;
        Config.enable_lance_index_local_file_mutation = false;
        // More permanently ineligible jobs than the round budget: local-file datasets
        // while the operator assertion is off. They must be scanned past without
        // consuming the budget.
        for (long jobId = 1L; jobId <= 16L; jobId++) {
            admit(jobId, "IdxLocal" + jobId, "/tmp/local-dataset-" + jobId);
        }
        admit(17L, "IdxRemote1", LOCATOR);
        admit(18L, "IdxRemote2", LOCATOR);

        dispatcher.runAfterCatalogReady();

        // The single budgeted dispatch of the round reaches job 17 behind the sixteen
        // ineligible ones; the second remote job waits for the next round.
        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(17L, dispatcher.sends.get(0).getJobId());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(17L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(18L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(2, dispatcher.sends.size());
        Assertions.assertEquals(18L, dispatcher.sends.get(1).getJobId());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(18L).getMutationState());
    }

    @Test
    public void identityLessPendingJobIsNeverPickedUpForDispatch() throws Exception {
        admit(1L, "IdxHealthy", LOCATOR);
        // A corrupt identity-less PENDING record: queryable, but never dispatchable.
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"PENDING\"}", LanceIndexJob.class));

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(1L, dispatcher.sends.get(0).getJobId());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(5L).getMutationState());
        Assertions.assertEquals(Collections.singletonList(5L), manager.getCorruptUnresolvedJobIds());
    }

    // ------------------------------------------------------------------
    // Deadline sweep
    // ------------------------------------------------------------------

    @Test
    public void deadlineSweepConvergesOnlyTheExpiredRunningJob() throws Exception {
        admit(1L, "IdxExpired", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, System.currentTimeMillis() - 1_000L));
        admit(2L, "IdxCurrent", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "inv-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));

        dispatcher.runAfterCatalogReady();

        LanceIndexJob expired = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, expired.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT, expired.getResult().getResultCode());
        // Expiry bounds the wait only: slot, fence, and quota all stay held.
        Assertions.assertTrue(expired.holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.isFenceHeld(expired.fenceKey()));
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(2L).getMutationState());
        Assertions.assertTrue(dispatcher.sends.isEmpty());
    }

    @Test
    public void callbackArrivingBeforeTheDeadlineSweepOnlyWarns() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, System.currentTimeMillis() - 1_000L));
        LanceIndexJob staleSnapshot = manager.getJob(1L);
        // The matching callback converges the job first, and its refresh duty is
        // settled too, so the late sweep is the only remaining actor.
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, "inv-1", BE_EPOCH, okResult()));
        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));
        manager.staleExpiredJob = staleSnapshot;
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        // The sweep's late completeWithResult loses the identity check and only warns:
        // no state change, no journal record, and the round itself must not throw.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_OK, stored.getResult().getResultCode());
        Assertions.assertEquals(journalBefore, manager.editLog.size());
    }

    // ------------------------------------------------------------------
    // Epoch sweep
    // ------------------------------------------------------------------

    @Test
    public void epochSweepReleasesTheSlotOfAReplacedBackendProcess() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE2_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, REPLACED_BE_EPOCH));
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        // The proof releases only the slot: the mutation state, fence, and quota stay.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE, stored.getTerminationProof());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void epochSweepReleasesTheSlotOfAnUnknownJobToo() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE2_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, "inv-1", BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, "ambiguous", false)));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, REPLACED_BE_EPOCH));

        dispatcher.runAfterCatalogReady();

        // The slot-release proof is independent of the outcome, so an UNKNOWN job's
        // slot is released exactly like a RUNNING one's.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE, stored.getTerminationProof());
    }

    @Test
    public void missingBackendEntrySameEpochOrHeartbeatLossNeverReleasesTheSlot() throws Exception {
        // A backend entry that disappeared proves nothing: the worker may still run
        // behind a partition, so the slot stays until a stronger proof.
        admit(1L, "IdxGone", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE1_ID)).thenReturn(null);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot(), "backend entry must not release the slot");

        // Same epoch: the recorded process is still the one that received the dispatch.
        admit(2L, "IdxSameEpoch", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE2_ID, BE_EPOCH, "inv-2",
                INVOCATION_SECRET, FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE1_ID)).thenReturn(backend(BE1_ID, BE_EPOCH));
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.getJob(2L).holdsPossibleLiveSlot(), "same epoch must not release the slot");

        // Heartbeat loss without a process restart: same epoch, dead marker, no release.
        Backend notAlive = backend(BE2_ID, BE_EPOCH);
        notAlive.setAlive(false);
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(notAlive);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.getJob(2L).holdsPossibleLiveSlot(), "heartbeat loss must not release the slot");

        for (LanceIndexJob record : manager.editLog) {
            Assertions.assertNotEquals(LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE,
                    record.getTerminationProof());
        }
    }

    // ------------------------------------------------------------------
    // Local-filesystem datasets
    // ------------------------------------------------------------------

    @Test
    public void localFileDatasetStaysPendingWhileTheAssertionIsOff() throws Exception {
        admit(1L, "IdxLocal", "file:///data/dataset");
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());
    }

    @Test
    public void schemelessAbsolutePathIsAlsoALocalDataset() throws Exception {
        admit(1L, "IdxLocal", "/data/dataset");
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());
    }

    @Test
    public void assertedLocalMutationIsRefusedWithMultipleFrontends() throws Exception {
        Config.enable_lance_index_local_file_mutation = true;
        Mockito.when(env.getFrontends(Mockito.any()))
                .thenReturn(Arrays.asList(Mockito.mock(Frontend.class), Mockito.mock(Frontend.class)));
        admit(1L, "IdxLocal", "file:///data/dataset");

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void assertedLocalMutationIsRefusedWithTwoRegisteredBackendsEvenOneAlive() throws Exception {
        Config.enable_lance_index_local_file_mutation = true;
        // One FE and two REGISTERED backends whose second one lost its heartbeat:
        // heartbeat loss does not turn a multi-node deployment into a shared-local-file
        // single node, so the guard must read the registered topology, not the alive
        // one (an alive-based guard accepted exactly this cluster).
        Mockito.when(systemInfo.getAllBackendIds(false)).thenReturn(Arrays.asList(BE1_ID, BE2_ID));
        Mockito.when(systemInfo.getAllBackendIds(true)).thenReturn(Collections.singletonList(BE1_ID));
        admit(1L, "IdxLocal", "file:///data/dataset");

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void assertedLocalMutationDispatchesOnASingleNodeTopology() throws Exception {
        Config.enable_lance_index_local_file_mutation = true;
        admit(1L, "IdxLocal", "file:///data/dataset");

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals("file:///data/dataset", dispatcher.sends.get(0).getDatasetUri());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    // ------------------------------------------------------------------
    // Storage-option confidentiality
    // ------------------------------------------------------------------

    @Test
    public void storageOptionsReachTheWireButNeverADurableRecord() throws Exception {
        admit(1L, "IdxA", LOCATOR);

        dispatcher.runAfterCatalogReady();

        // The resolved credentials really did reach the wire request...
        Assertions.assertEquals(1, dispatcher.sends.size());
        Map<String, String> wireOptions = dispatcher.sends.get(0).getStorageOptions();
        Assertions.assertEquals(FAKE_ACCESS_KEY, wireOptions.get("aws_access_key_id"));
        Assertions.assertEquals(FAKE_SECRET_KEY, wireOptions.get("aws_secret_access_key"));
        // ...while no durable record, in journal form or in its log/toString rendering,
        // carries the credential key or value: storage options are never persisted.
        for (LanceIndexJob record : manager.editLog) {
            String json = GsonUtils.GSON.toJson(record);
            Assertions.assertFalse(json.contains(FAKE_ACCESS_KEY), "journal json leaked the access key");
            Assertions.assertFalse(json.contains(FAKE_SECRET_KEY), "journal json leaked the secret key");
            Assertions.assertFalse(record.toString().contains(FAKE_ACCESS_KEY), "toString leaked the access key");
            Assertions.assertFalse(record.toString().contains(FAKE_SECRET_KEY), "toString leaked the secret key");
        }
        String storedJson = GsonUtils.GSON.toJson(manager.getJob(1L));
        Assertions.assertFalse(storedJson.contains(FAKE_ACCESS_KEY));
        Assertions.assertFalse(storedJson.contains(FAKE_SECRET_KEY));
        Assertions.assertFalse(manager.getJob(1L).toString().contains("aws_access_key_id"));
    }

    /**
     * Image-restart regression: {@code Env.loadLanceIndexJobManager} replaces the
     * Env-owned manager with the object restored from the image, so the dispatcher
     * must resolve its manager per round. A dispatcher that captured the pre-image
     * instance would keep scanning it after the restart: the restored job would
     * never advance, with no error logged. This pins the rebinding by flipping the
     * Env-side reference to the image-restored manager between rounds.
     */
    @Test
    public void imageRestartDrivesJobsInTheRestoredManager() throws Exception {
        admit(1L, "IdxImage", LOCATOR);

        // The image cycle, verbatim: serialize the live manager and restore it into
        // a brand-new object, exactly what loadLanceIndexJobManager does on restart.
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        manager.write(new DataOutputStream(byteStream));
        LanceIndexJobManager restored = LanceIndexJobManager.read(
                new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));
        Assertions.assertTrue(containsJob(restored.getJobsNeedingDispatch(), 1L),
                "the restored image must carry the PENDING job");

        // The restored manager journals through the real base seam; route it to a mock.
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));

        // The Env field flips to the restored manager, as on the restart that loaded it.
        AtomicReference<LanceIndexJobManager> envManager = new AtomicReference<>(manager);
        TestDispatcher rebinding = new TestDispatcher(envManager::get, events);
        envManager.set(restored);

        rebinding.runAfterCatalogReady();

        // The round drove the restored manager: its job went RUNNING and was sent,
        // while the abandoned pre-image copy stayed PENDING and un-sent.
        LanceIndexJob driven = restored.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, driven.getMutationState());
        Assertions.assertEquals(1, rebinding.sends.size(), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private static Backend backend(long id, long processEpoch) {
        Backend backend = new Backend(id, "host-" + id, 9050);
        backend.setAlive(true);
        backend.setLastStartTime(processEpoch);
        return backend;
    }

    private void admit(long jobId, String displayName, String locator) throws Exception {
        manager.createJob(new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, locator,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null), 100, 100, 100);
    }

    private void admitWithBounds(long jobId, String displayName, String locator,
            int maxNumPartitions, int maxNumSubVectors) throws Exception {
        LanceIndexJob job = new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, locator,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
        job.setAdmittedMaxNumPartitions(maxNumPartitions);
        job.setAdmittedMaxNumSubVectors(maxNumSubVectors);
        manager.createJob(job, 100, 100, 100);
    }

    private void admitWithContract(long jobId, String displayName, String locator,
            LanceIndexJobMutationType mutationType, boolean ifExists, boolean withIndexType)
            throws Exception {
        LanceIndexSchemaContract contract = new LanceIndexSchemaContract(Collections.singletonList(
                new LanceIndexSchemaContract.IndexedField(1, "v", "fixed_size_list", false, 4,
                        "float32", true)));
        manager.createJob(new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, locator,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                mutationType, false, ifExists, withIndexType ? "IVF_PQ" : null, "v",
                null, 7L, contract), 100, 100, 100);
    }

    private static TLanceIndexJobDispatch minimalDispatch(long jobId) {
        return new TLanceIndexJobDispatch()
                .setJobId(jobId)
                .setDispatchRevision(1L)
                .setInvocationId("inv")
                .setBeProcessEpoch(BE_EPOCH)
                .setDeadlineMs(FAR_DEADLINE_MS)
                .setMutationType(TLanceIndexMutationType.CREATE)
                .setIndexName("idx")
                .setColumnName("v")
                .setIndexType("IVF_PQ")
                .setDatasetUri(LOCATOR)
                .setAdmittedDatasetVersion(7L)
                .setSchemaContractJson("{}");
    }

    private static void assertPayloadRejected(TLanceIndexJobDispatch dispatch) {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexDispatchBounds.validatePayload(dispatch));
    }

    private static String repeat(char c, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(c);
        }
        return builder.toString();
    }

    private static String journal(long jobId, String mutationState, String refreshState, boolean slot) {
        return "journal:" + jobId + ":" + mutationState + ":" + refreshState + ":" + (slot ? "slot" : "noslot");
    }

    private static boolean containsJob(List<LanceIndexJob> jobs, long jobId) {
        for (LanceIndexJob job : jobs) {
            if (job.getJobId() == jobId) {
                return true;
            }
        }
        return false;
    }

    private static LanceIndexJobResult okResult() {
        return new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                LanceIndexJobCompletionReason.NONE, "ok", false);
    }

    /**
     * Edit-log seam plus dispatch-boundary injections: captures every durable
     * record, can lose one markRunning compare-and-set, can replay a concurrent
     * higher-revision record right after a won markRunning (pre-send recheck), and
     * can hand the deadline sweep one stale snapshot after a callback converged
     * the job first.
     */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();
        private final List<String> events;
        private boolean rejectNextMarkRunning;
        private String hijackedInvocationId;
        private Runnable afterMarkRunning;
        private LanceIndexJob staleExpiredJob;

        TestManager(List<String> events) {
            this.events = events;
        }

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
            events.add("journal:" + job.getJobId() + ":" + job.getMutationState() + ":" + job.getRefreshState()
                    + ":" + (job.isPossibleLiveOwned() ? "slot" : "noslot"));
        }

        @Override
        public boolean markRunning(long jobId, long expectedRevision, long backendId, long beProcessEpoch,
                String invocationId, String invocationSecret, long deadlineMs) {
            if (rejectNextMarkRunning) {
                rejectNextMarkRunning = false;
                events.add("markRunningRejected:" + jobId);
                return false;
            }
            boolean marked = super.markRunning(jobId, expectedRevision, backendId, beProcessEpoch, invocationId,
                    invocationSecret, deadlineMs);
            if (marked && afterMarkRunning != null) {
                // Simulates a heartbeat landing right after the durable record, before
                // the dispatcher reads the backend again for the wire request.
                Runnable hook = afterMarkRunning;
                afterMarkRunning = null;
                hook.run();
            }
            if (marked && hijackedInvocationId != null) {
                String hijacker = hijackedInvocationId;
                hijackedInvocationId = null;
                LanceIndexJob concurrent = getJob(jobId);
                concurrent.setRevision(expectedRevision + 5);
                concurrent.setDispatchRevision(expectedRevision + 5);
                concurrent.setInvocationId(hijacker);
                replayUpsertJob(concurrent);
                events.add("hijacked:" + jobId);
            }
            return marked;
        }

        @Override
        public List<LanceIndexJob> getExpiredRunningJobs(long nowMs) {
            if (staleExpiredJob != null) {
                LanceIndexJob stale = staleExpiredJob;
                staleExpiredJob = null;
                return new ArrayList<>(Collections.singletonList(stale));
            }
            return super.getExpiredRunningJobs(nowMs);
        }
    }

    /**
     * Send seam: records the call order against the journal events and injects the
     * per-case send outcome. No client pool is ever touched, so a send event is the
     * earliest possible network activity of a dispatch attempt.
     */
    private static class TestDispatcher extends LanceIndexJobDispatcher {
        private final List<String> events;
        private final List<TLanceIndexJobDispatch> sends = new ArrayList<>();
        private TStatus statusToReturn = new TStatus(TStatusCode.OK);
        private boolean throwOnSend;
        private Exception sendException;
        /** Added to the wall clock by the round-period seam; lets tests skip hours. */
        private volatile long nowOffsetMs;
        /** Fired once per send, after the dispatch is durable; a fault-injection hook. */
        private volatile Runnable onSend;

        TestDispatcher(LanceIndexJobManager jobManager, List<String> events) {
            super(jobManager);
            this.events = events;
        }

        TestDispatcher(Supplier<LanceIndexJobManager> jobManagerSupplier, List<String> events) {
            super(jobManagerSupplier);
            this.events = events;
        }

        @Override
        protected long nowMs() {
            return System.currentTimeMillis() + nowOffsetMs;
        }

        @Override
        protected TStatus sendExecuteRequest(Backend backend, TLanceIndexJobDispatch dispatch) throws Exception {
            events.add("send:" + dispatch.getJobId());
            sends.add(dispatch);
            if (onSend != null) {
                onSend.run();
            }
            if (sendException != null) {
                throw sendException;
            }
            if (throwOnSend) {
                throw new RuntimeException("injected transport failure");
            }
            return statusToReturn;
        }
    }
}
