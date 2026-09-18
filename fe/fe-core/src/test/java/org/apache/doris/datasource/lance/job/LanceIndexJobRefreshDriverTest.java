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
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Refresh-phase coverage for {@link LanceIndexJobDispatcher}. The external-table
 * refresh is mocked, the manager's edit-log seam captures every durable record,
 * and each round is one direct {@code runAfterCatalogReady} call, so the pinned
 * invariants are all observable: the driver runs markRefreshRunning, then
 * {@code handleRefreshTable(catalogName, db, table, ignoreIfNotExists=true)},
 * then DONE, which releases the fence and the unresolved quota; a
 * {@link DdlException} keeps the fence and retries on a later round; the retry
 * throttle delays only FAILED refreshes, never a first REQUIRED one; a silent
 * no-op refresh (a half-orphan target) still completes to DONE; force-released
 * jobs owe nothing; and a refresh stranded at RUNNING by a lost driver is
 * downgraded by the master-transfer sweep and then driven to DONE here.
 */
public class LanceIndexJobRefreshDriverTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final long FAR_DEADLINE_MS = System.currentTimeMillis() + 3600_000L;

    private final List<String> events = new ArrayList<>();
    private MockedStatic<Env> mockedEnv;
    private Env env;
    private RefreshManager refreshManager;
    private TestManager manager;
    private TestDispatcher dispatcher;

    private int originalRetrySecond;

    @BeforeEach
    public void setUp() throws Exception {
        events.clear();
        mockedEnv = Mockito.mockStatic(Env.class);
        env = Mockito.mock(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.selectBackendIdsByPolicy(Mockito.any(BeSelectionPolicy.class), Mockito.eq(1)))
                .thenReturn(Collections.emptyList());
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        Mockito.when(env.getFrontends(Mockito.any()))
                .thenReturn(Collections.singletonList(Mockito.mock(Frontend.class)));

        LanceExternalCatalog catalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(CATALOG_ID);
        Mockito.when(catalog.getName()).thenReturn("lance_cat");
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

        originalRetrySecond = Config.lance_index_job_refresh_retry_second;
    }

    @AfterEach
    public void tearDown() {
        Config.lance_index_job_refresh_retry_second = originalRetrySecond;
        mockedEnv.close();
    }

    @Test
    public void refreshRunsMarkRunningThenRefreshTableThenDoneWithExactParameters() throws Exception {
        admitTerminalCommitted(1L, "IdxA");

        dispatcher.runAfterCatalogReady();

        // Order: the durable refresh-RUNNING record precedes the external refresh,
        // and the DONE record follows it.
        int refreshRunningIdx = events.indexOf(journal(1L, "COMMITTED", "RUNNING", true));
        int refreshTableIdx = events.indexOf("refresh:db1.tbl1");
        int refreshDoneIdx = events.indexOf(journal(1L, "COMMITTED", "DONE", true));
        Assertions.assertTrue(refreshRunningIdx >= 0, events.toString());
        Assertions.assertTrue(refreshTableIdx >= 0, events.toString());
        Assertions.assertTrue(refreshDoneIdx >= 0, events.toString());
        Assertions.assertTrue(refreshRunningIdx < refreshTableIdx, events.toString());
        Assertions.assertTrue(refreshTableIdx < refreshDoneIdx, events.toString());

        ArgumentCaptor<String> catalogName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> dbName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tableName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Boolean> ignoreIfNotExists = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(refreshManager, Mockito.times(1)).handleRefreshTable(catalogName.capture(), dbName.capture(),
                tableName.capture(), ignoreIfNotExists.capture());
        Assertions.assertEquals("lance_cat", catalogName.getValue());
        Assertions.assertEquals("db1", dbName.getValue());
        Assertions.assertEquals("tbl1", tableName.getValue());
        // A half-orphan target is a legal input to the refresh call, not an error.
        Assertions.assertEquals(Boolean.TRUE, ignoreIfNotExists.getValue());

        // Completing the refresh duty releases the fence and the unresolved quota,
        // and a refresh round never dispatches anything.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, stored.getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
        Assertions.assertTrue(dispatcher.sendJobIds.isEmpty());
    }

    @Test
    public void ddlExceptionMarksRefreshFailedAndKeepsTheFenceForARetry() throws Exception {
        Config.lance_index_job_refresh_retry_second = 0;
        admitTerminalCommitted(1L, "IdxA");
        Mockito.doThrow(new DdlException("refresh exploded")).doAnswer(invocation -> {
            events.add("refresh:" + invocation.getArgument(1) + "." + invocation.getArgument(2));
            return null;
        }).when(refreshManager).handleRefreshTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean());
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        LanceIndexJob failed = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, failed.getRefreshState());
        // The fence and quota survive the failure: DONE is the only release.
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(containsJob(manager.getUnresolvedJobs(), 1L));
        Assertions.assertTrue(containsJob(manager.getJobsNeedingRefresh(), 1L));

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(1L).getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Mockito.verify(refreshManager, Mockito.times(2)).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void uncheckedExceptionStillMarksRefreshFailedInsteadOfStrandingRunning() throws Exception {
        Config.lance_index_job_refresh_retry_second = 0;
        admitTerminalCommitted(1L, "IdxA");
        Mockito.doThrow(new IllegalStateException("metadata path exploded")).doAnswer(invocation -> {
            events.add("refresh:" + invocation.getArgument(1) + "." + invocation.getArgument(2));
            return null;
        }).when(refreshManager).handleRefreshTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean());
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        // The failure face of the refresh path is not only the typed DdlException: an
        // unchecked exception must still leave the durable refresh state FAILED, or the
        // job would strand in refresh RUNNING until a master transfer.
        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, manager.getJob(1L).getRefreshState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertTrue(containsJob(manager.getJobsNeedingRefresh(), 1L));

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(1L).getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
    }

    @Test
    public void doneTransitionLosingTheCompareAndSetIsRetriedWithTheFreshRevision() throws Exception {
        Config.lance_index_job_refresh_retry_second = 0;
        FlakyDoneTestManager flaky = new FlakyDoneTestManager(events);
        manager = flaky;
        dispatcher = new TestDispatcher(flaky, events);
        admitTerminalCommitted(1L, "IdxA");
        flaky.failNextDone = 1;

        dispatcher.runAfterCatalogReady();

        // Losing the DONE compare-and-set once (as a concurrent revision bump would)
        // must not strand the refresh in RUNNING: the driver re-reads the revision and
        // retries within the same round.
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, stored.getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
    }

    @Test
    public void freshFailedRefreshIsThrottledUntilTheRetryIntervalElapses() throws Exception {
        Config.lance_index_job_refresh_retry_second = 300;
        admitTerminalCommitted(1L, "IdxA");
        Mockito.doThrow(new DdlException("refresh exploded")).when(refreshManager)
                .handleRefreshTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                        Mockito.anyBoolean());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, manager.getJob(1L).getRefreshState());
        int journalAfterFailure = manager.editLog.size();

        // The FAILED transition just bumped updateTimeMs: the immediately following
        // round is inside the retry window and must not even attempt the CAS.
        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, manager.getJob(1L).getRefreshState());
        Assertions.assertEquals(journalAfterFailure, manager.editLog.size());
        Mockito.verify(refreshManager, Mockito.times(1)).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
        Assertions.assertTrue(containsJob(manager.getJobsNeedingRefresh(), 1L));
    }

    @Test
    public void staleFailedRefreshIsRetriedOncePastTheThrottleWindow() throws Exception {
        Config.lance_index_job_refresh_retry_second = 300;
        // A FAILED refresh whose last transition aged past the retry window, built as
        // a replayed durable record so its updateTimeMs is controllable.
        LanceIndexJob stale = newCreateJob(1L, "IdxA");
        stale.setRevision(2L);
        stale.setMutationState(LanceIndexJobMutationState.COMMITTED);
        stale.setRefreshState(LanceIndexJobRefreshState.FAILED);
        long staleTime = System.currentTimeMillis()
                - (Config.lance_index_job_refresh_retry_second * 1000L + 60_000L);
        stale.setCreateTimeMs(staleTime);
        stale.setUpdateTimeMs(staleTime);
        manager.replayUpsertJob(stale);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(1L).getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Mockito.verify(refreshManager, Mockito.times(1)).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    public void halfOrphanTargetRefreshesSilentlyAndStillCompletes() throws Exception {
        admitTerminalCommitted(1L, "IdxA");
        // The target db/table was already dropped externally: the refresh call is a
        // silent no-op (nothing is left to invalidate), which is success here.
        Mockito.doNothing().when(refreshManager).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, stored.getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
    }

    @Test
    public void forceReleasedJobsOweNoRefresh() throws Exception {
        LanceIndexJob forced = newCreateJob(1L, "IdxA");
        forced.setRevision(2L);
        forced.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        forced.setRefreshState(LanceIndexJobRefreshState.REQUIRED);
        forced.setForceReleased(true);
        manager.replayUpsertJob(forced);

        dispatcher.runAfterCatalogReady();

        Mockito.verify(refreshManager, Mockito.never()).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(1L).getRefreshState());
        Assertions.assertTrue(manager.editLog.isEmpty());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().isEmpty());
    }

    @Test
    public void downgradedRunningRefreshIsDrivenToDoneAfterMasterTransfer() throws Exception {
        // A driver crashed between markRefreshRunning and the refresh call: the job
        // is terminal with a refresh stuck at RUNNING.
        TestManager source = new TestManager(new ArrayList<>());
        source.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        source.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, "inv-1", FAR_DEADLINE_MS);
        source.completeWithResult(1L, 1L, "inv-1", BE_EPOCH, okResult());
        source.markRefreshRunning(1L, 2L);
        Assertions.assertEquals(LanceIndexJobRefreshState.RUNNING, source.getJob(1L).getRefreshState());

        // A new master replays the journal, then its election sweep downgrades the
        // stranded RUNNING refresh back to REQUIRED before any daemon could start.
        for (LanceIndexJob record : source.editLog) {
            manager.replayUpsertJob(record);
        }
        manager.onTransferToMaster();
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(1L).getRefreshState());

        dispatcher.runAfterCatalogReady();

        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(1L).getRefreshState());
        Assertions.assertFalse(manager.isFenceHeld(manager.getJob(1L).fenceKey()));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void missingCatalogFailsTheRefreshClosedAndKeepsRetrying() throws Exception {
        Config.lance_index_job_refresh_retry_second = 0;
        admitTerminalCommitted(1L, "IdxA");
        // Built before the stubbing: constructing it inside when(...) triggers
        // Mockito's unfinished-stubbing detection.
        CatalogMgr catalogless = new CatalogMgr();
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogless);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        dispatcher.runAfterCatalogReady();
        dispatcher.runAfterCatalogReady();

        // Unreachable while the unresolved-job guard blocks catalog drops, but the
        // driver still transitions and retries instead of stranding the job.
        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, manager.getJob(1L).getRefreshState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Mockito.verify(refreshManager, Mockito.never()).handleRefreshTable(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean());
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /** Creates a job and walks it to COMMITTED with a REQUIRED refresh, the driver's input. */
    private void admitTerminalCommitted(long jobId, String displayName) throws Exception {
        manager.createJob(newCreateJob(jobId, displayName), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, "inv-" + jobId,
                FAR_DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(jobId, 1L, "inv-" + jobId, BE_EPOCH, okResult()));
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(jobId).getRefreshState());
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
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

    /** Edit-log seam: captures every durable record with its phase-observable label. */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();
        private final List<String> events;

        TestManager(List<String> events) {
            this.events = events;
        }

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
            events.add("journal:" + job.getJobId() + ":" + job.getMutationState() + ":" + job.getRefreshState()
                    + ":" + (job.isPossibleLiveOwned() ? "slot" : "noslot"));
        }
    }

    /** Fails the DONE transition a bounded number of times, as a concurrent revision bump would. */
    private static class FlakyDoneTestManager extends TestManager {
        private int failNextDone = 0;

        FlakyDoneTestManager(List<String> events) {
            super(events);
        }

        @Override
        public boolean markRefreshDone(long jobId, long expectedRevision) {
            if (failNextDone > 0) {
                failNextDone--;
                return false;
            }
            return super.markRefreshDone(jobId, expectedRevision);
        }
    }

    /** Send seam: refresh rounds must never dispatch; any send is a test failure. */
    private static class TestDispatcher extends LanceIndexJobDispatcher {
        private final List<String> events;
        private final List<Long> sendJobIds = new ArrayList<>();

        TestDispatcher(LanceIndexJobManager jobManager, List<String> events) {
            super(jobManager);
            this.events = events;
        }

        @Override
        protected TStatus sendExecuteRequest(Backend backend, TLanceIndexJobDispatch dispatch) throws Exception {
            events.add("send:" + dispatch.getJobId());
            sendJobIds.add(dispatch.getJobId());
            return new TStatus(TStatusCode.OK);
        }
    }
}
