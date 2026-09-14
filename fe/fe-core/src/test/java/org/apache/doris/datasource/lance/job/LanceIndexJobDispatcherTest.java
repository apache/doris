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
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Fault-matrix coverage for {@link LanceIndexJobDispatcher}. The two test seams
 * are the manager's edit-log capture (the 3B pattern) and the send seam: a
 * subclass records every journal record and every send with their ordering,
 * injects clean error statuses or transport failures, and never opens a real
 * client pool. The pinned invariants: one round runs the five phases in the
 * fixed order (deadline sweep, epoch sweep, refresh, dispatch); the markRunning
 * journal record always precedes the send (durable-before-send); an attempt
 * whose compare-and-set lost never sends and never reuses its invocation id; a
 * clean send error converges NOT_COMMITTED while a transport failure converges
 * UNKNOWN with the possible-live slot still held; only a changed backend
 * process epoch releases a slot; a local-filesystem dataset is dispatched only
 * on the asserted single-node topology; and an idle round writes no journal
 * record. Storage options reach the wire but never a durable record.
 */
public class LanceIndexJobDispatcherTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BE1_ID = 1001L;
    private static final long BE2_ID = 1002L;
    private static final long BE_EPOCH = 55L;
    private static final long REPLACED_BE_EPOCH = 77L;
    private static final long FAR_DEADLINE_MS = System.currentTimeMillis() + 3600_000L;
    /** Fake markers that must never surface in any durable or logged form. */
    private static final String FAKE_ACCESS_KEY = "test-ak-marker-not-a-real-credential";
    private static final String FAKE_SECRET_KEY = "test-sk-marker-not-a-real-credential";

    private final List<String> events = new ArrayList<>();
    private MockedStatic<Env> mockedEnv;
    private Env env;
    private SystemInfoService systemInfo;
    private RefreshManager refreshManager;
    private LanceExternalCatalog catalog;
    private TestManager manager;
    private TestDispatcher dispatcher;

    private int originalIntervalSecond;
    private int originalMaxDispatchPerRound;
    private int originalMaxInflightPerBackend;
    private boolean originalLocalFileMutation;

    @BeforeEach
    public void setUp() throws Exception {
        events.clear();
        mockedEnv = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo = Mockito.mock(SystemInfoService.class));
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(1)))
                .thenReturn(Collections.singletonList(BE1_ID));
        Mockito.when(systemInfo.getBackend(BE1_ID)).thenReturn(backend(BE1_ID, BE_EPOCH));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, BE_EPOCH));
        Mockito.when(systemInfo.getAllBackendIds(true)).thenReturn(Collections.singletonList(BE1_ID));
        Mockito.when(env.getFrontends(Mockito.any()))
                .thenReturn(Collections.singletonList(Mockito.mock(Frontend.class)));

        catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(catalog.getName()).thenReturn("lance_cat");
        AbstractS3CompatibleProperties storageProperties = Mockito.mock(AbstractS3CompatibleProperties.class);
        Mockito.when(storageProperties.getAccessKey()).thenReturn(FAKE_ACCESS_KEY);
        Mockito.when(storageProperties.getSecretKey()).thenReturn(FAKE_SECRET_KEY);
        Mockito.when(storageProperties.getEndpoint()).thenReturn("http://minio.example:9000");
        Mockito.when(storageProperties.getRegion()).thenReturn("us-east-1");
        org.apache.doris.datasource.CatalogProperty catalogProperty =
                Mockito.mock(org.apache.doris.datasource.CatalogProperty.class);
        Mockito.when(catalogProperty.getOrderedStoragePropertiesList())
                .thenReturn(Collections.singletonList(storageProperties));
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        CatalogMgr catalogMgr = new CatalogMgr();
        java.lang.reflect.Field catalogs = CatalogMgr.class.getDeclaredField("idToCatalog");
        catalogs.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, CatalogIf> registered = (Map<Long, CatalogIf>) catalogs.get(catalogMgr);
        registered.put(CATALOG_ID, catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        refreshManager = Mockito.mock(RefreshManager.class);
        Mockito.doAnswer(invocation -> {
            events.add("refresh:" + invocation.getArgument(1) + "." + invocation.getArgument(2));
            return null;
        }).when(refreshManager).handleRefreshTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean());
        Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);

        manager = new TestManager(events);
        dispatcher = new TestDispatcher(manager, events);

        originalIntervalSecond = Config.lance_index_job_dispatch_interval_second;
        originalMaxDispatchPerRound = Config.lance_index_job_max_dispatch_per_round;
        originalMaxInflightPerBackend = Config.lance_index_job_max_inflight_per_backend;
        originalLocalFileMutation = Config.enable_lance_index_local_file_mutation;
    }

    @AfterEach
    public void tearDown() {
        Config.lance_index_job_dispatch_interval_second = originalIntervalSecond;
        Config.lance_index_job_max_dispatch_per_round = originalMaxDispatchPerRound;
        Config.lance_index_job_max_inflight_per_backend = originalMaxInflightPerBackend;
        Config.enable_lance_index_local_file_mutation = originalLocalFileMutation;
        mockedEnv.close();
    }

    // ------------------------------------------------------------------
    // Round structure
    // ------------------------------------------------------------------

    @Test
    public void oneRoundRunsTheFivePhasesInFixedOrder() throws Exception {
        // Job 1: expired RUNNING, converged by the deadline sweep. Its backend keeps
        // the recorded epoch so the epoch sweep leaves it alone.
        admit(1L, "IdxDeadline", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1",
                System.currentTimeMillis() - 1_000L));
        // Job 2: RUNNING on a backend whose process epoch was replaced.
        admit(2L, "IdxEpoch", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE2_ID, BE_EPOCH, "inv-2", FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE2_ID)).thenReturn(backend(BE2_ID, REPLACED_BE_EPOCH));
        // Job 3: terminal with a refresh obligation.
        admit(3L, "IdxRefresh", LOCATOR);
        Assertions.assertTrue(manager.markRunning(3L, 0L, BE1_ID, BE_EPOCH, "inv-3", FAR_DEADLINE_MS));
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
        // invocation is known never to have executed: NOT_COMMITTED, no refresh owed.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                stored.getResult().getResultCode());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
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
    public void preparationFailureConvergesUnknownWithoutASend() throws Exception {
        admit(1L, "IdxA", LOCATOR);
        // The job's catalog resolves to nothing: storage options cannot be resolved,
        // which is an FE-side failure, never a trusted worker rejection. Built before
        // the stubbing: constructing it inside when(...) triggers Mockito's
        // unfinished-stubbing detection.
        CatalogMgr catalogless = new CatalogMgr();
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogless);

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT, stored.getResult().getResultCode());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
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
    public void perBackendInflightCapDefersDispatchUntilASlotFrees() throws Exception {
        Config.lance_index_job_max_inflight_per_backend = 2;
        admit(1L, "IdxOccupied1", LOCATOR);
        admit(2L, "IdxOccupied2", LOCATOR);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "place-1", FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "place-2", FAR_DEADLINE_MS));
        admit(3L, "IdxWaiting", LOCATOR);
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        // Both in-flight slots of the only selectable backend are taken: the round
        // attempts nothing and the job keeps waiting as PENDING.
        Assertions.assertTrue(dispatcher.sends.isEmpty(), events.toString());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(3L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());

        // One placeholder converges, so the snapshot count drops and the next round sends.
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, "place-1", BE_EPOCH, okResult()));
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(1, dispatcher.sends.size());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(3L).getMutationState());
    }

    @Test
    public void noSelectableBackendKeepsTheJobPending() throws Exception {
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(1)))
                .thenReturn(Collections.emptyList());
        admit(1L, "IdxA", LOCATOR);
        int journalBefore = manager.editLog.size();

        dispatcher.runAfterCatalogReady();

        Assertions.assertTrue(dispatcher.sends.isEmpty());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(journalBefore, manager.editLog.size());
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
                System.currentTimeMillis() - 1_000L));
        admit(2L, "IdxCurrent", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE1_ID, BE_EPOCH, "inv-2", FAR_DEADLINE_MS));

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
                System.currentTimeMillis() - 1_000L));
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
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE2_ID, BE_EPOCH, "inv-1", FAR_DEADLINE_MS));
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
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE2_ID, BE_EPOCH, "inv-1", FAR_DEADLINE_MS));
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
        Assertions.assertTrue(manager.markRunning(1L, 0L, BE1_ID, BE_EPOCH, "inv-1", FAR_DEADLINE_MS));
        Mockito.when(systemInfo.getBackend(BE1_ID)).thenReturn(null);
        dispatcher.runAfterCatalogReady();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot(), "backend entry must not release the slot");

        // Same epoch: the recorded process is still the one that received the dispatch.
        admit(2L, "IdxSameEpoch", LOCATOR);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BE2_ID, BE_EPOCH, "inv-2", FAR_DEADLINE_MS));
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
    public void assertedLocalMutationIsRefusedWithMultipleAliveBackends() throws Exception {
        Config.enable_lance_index_local_file_mutation = true;
        Mockito.when(systemInfo.getAllBackendIds(true)).thenReturn(Arrays.asList(BE1_ID, BE2_ID));
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
                String invocationId, long deadlineMs) {
            if (rejectNextMarkRunning) {
                rejectNextMarkRunning = false;
                events.add("markRunningRejected:" + jobId);
                return false;
            }
            boolean marked = super.markRunning(jobId, expectedRevision, backendId, beProcessEpoch, invocationId,
                    deadlineMs);
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

        TestDispatcher(LanceIndexJobManager jobManager, List<String> events) {
            super(jobManager);
            this.events = events;
        }

        @Override
        protected TStatus sendExecuteRequest(Backend backend, TLanceIndexJobDispatch dispatch) throws Exception {
            events.add("send:" + dispatch.getJobId());
            sends.add(dispatch);
            if (throwOnSend) {
                throw new RuntimeException("injected transport failure");
            }
            return statusToReturn;
        }
    }
}
