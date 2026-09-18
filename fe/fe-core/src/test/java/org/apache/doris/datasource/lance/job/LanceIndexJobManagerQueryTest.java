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

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Coverage for the read-side queries of {@link LanceIndexJobManager}: the admission-slice
 * queries {@link LanceIndexJobManager#getAllJobsSnapshot()} (the data source of SHOW LANCE
 * INDEX JOBS: every job, copies only, ordered by job id) and
 * {@link LanceIndexJobManager#hasUnresolvedJobsForCatalog(long)} (the catalog DDL guard
 * probe), plus the dispatcher-slice queries: {@link LanceIndexJobManager#getJobsNeedingDispatch(int)}
 * (only PENDING records whose dispatch identity is complete, in job id order, at most
 * limit), {@link LanceIndexJobManager#getExpiredRunningJobs(long)} (only RUNNING past the
 * deadline), {@link LanceIndexJobManager#getJobsHoldingPossibleLiveSlot()} (slot holders
 * with complete identity, regardless of mutation state), and the force-release filter of
 * {@link LanceIndexJobManager#getJobsNeedingRefresh()}.
 */
public class LanceIndexJobManagerQueryTest {
    private static final long CATALOG_ID = 10L;
    private static final long OTHER_CATALOG_ID = 20L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;

    @Test
    public void snapshotReturnsEveryJobOrderedByJobId() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(3L, "IdxC", CATALOG_ID), 100, 100, 100);
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        manager.createJob(newCreateJob(2L, "IdxB", OTHER_CATALOG_ID), 100, 100, 100);

        List<LanceIndexJob> snapshot = manager.getAllJobsSnapshot();
        Assertions.assertEquals(3, snapshot.size());
        Assertions.assertEquals(1L, snapshot.get(0).getJobId());
        Assertions.assertEquals(2L, snapshot.get(1).getJobId());
        Assertions.assertEquals(3L, snapshot.get(2).getJobId());
    }

    @Test
    public void snapshotCopiesAreIsolatedFromStoredRecords() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);

        LanceIndexJob copy = manager.getAllJobsSnapshot().get(0);
        copy.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        copy.setDisplayIndexName("mutated");
        copy.setRevision(99L);

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals("IdxA", stored.getDisplayIndexName());
        Assertions.assertEquals(0L, stored.getRevision());

        // A later snapshot is unaffected by mutations of an earlier one.
        LanceIndexJob fresh = manager.getAllJobsSnapshot().get(0);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, fresh.getMutationState());
        Assertions.assertEquals("IdxA", fresh.getDisplayIndexName());
        Assertions.assertEquals(0L, fresh.getRevision());
    }

    @Test
    public void hasUnresolvedJobsForCatalogScopesByCatalogId() throws Exception {
        TestManager manager = new TestManager();
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(OTHER_CATALOG_ID));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(999L));
    }

    @Test
    public void releasedTerminalJobDoesNotCountAsUnresolved() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", CATALOG_ID), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, "ok", false)));
        // COMMITTED with refresh REQUIRED still holds the fence and quota.
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));

        // A released terminal job remains visible in the full snapshot.
        Assertions.assertEquals(1, manager.getAllJobsSnapshot().size());
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED,
                manager.getAllJobsSnapshot().get(0).getMutationState());
    }

    @Test
    public void unresolvedJobInAnotherCatalogDoesNotLeak() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA", OTHER_CATALOG_ID), 100, 100, 100);
        Assertions.assertFalse(manager.hasUnresolvedJobsForCatalog(CATALOG_ID));
        Assertions.assertTrue(manager.hasUnresolvedJobsForCatalog(OTHER_CATALOG_ID));
    }

    // ------------------------------------------------------------------
    // dispatcher-slice queries
    // ------------------------------------------------------------------

    @Test
    public void dispatchQueryReturnsPendingJobsWithCompleteTargetIdentity() throws Exception {
        TestManager manager = new TestManager();
        // A durable PENDING record carrying the full dispatch identity is dispatchable.
        manager.replayUpsertJob(dispatchablePending(1L, "IdxDispatchable"));
        // An admitted PENDING record (the form createJob produces) carries no dispatch quad
        // yet and is dispatchable too: the quad is written by markRunning, the very step
        // this query feeds, so only the target identity is required here.
        manager.createJob(newCreateJob(2L, "IdxAdmitted", CATALOG_ID), 100, 100, 100);
        // Non-PENDING states are invisible to the dispatch sweep even with full identity.
        manager.replayUpsertJob(runningRecord(4L, "IdxRunning"));
        LanceIndexJob terminal = dispatchablePending(5L, "IdxTerminal");
        terminal.setMutationState(LanceIndexJobMutationState.COMMITTED);
        terminal.setRefreshState(LanceIndexJobRefreshState.DONE);
        manager.replayUpsertJob(terminal);
        // A corrupt identity-less PENDING record is never dispatchable (replayed last: it
        // also fail-closes new admissions, which the createJob above must not hit).
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":3,\"rev\":0,\"ms\":\"PENDING\"}", LanceIndexJob.class));

        List<LanceIndexJob> dispatchable = manager.getJobsNeedingDispatch(10);
        Assertions.assertEquals(2, dispatchable.size());
        Assertions.assertEquals(1L, dispatchable.get(0).getJobId());
        Assertions.assertEquals(2L, dispatchable.get(1).getJobId());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, dispatchable.get(0).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, dispatchable.get(1).getMutationState());
    }

    @Test
    public void dispatchQueryHonorsLimitAndJobIdOrder() {
        TestManager manager = new TestManager();
        // Insertion order deliberately scrambled; the sweep returns job id order (FIFO).
        for (long jobId : new long[]{5L, 1L, 4L, 2L, 3L}) {
            manager.replayUpsertJob(dispatchablePending(jobId, "Idx" + jobId));
        }

        List<LanceIndexJob> all = manager.getJobsNeedingDispatch(10);
        Assertions.assertEquals(5, all.size());
        for (int i = 0; i < all.size(); i++) {
            Assertions.assertEquals(i + 1L, all.get(i).getJobId());
        }

        // The limit keeps the smallest ids: ordering happens before truncation, so a
        // stable subset of undispatchable jobs can never crowd out later ids.
        List<LanceIndexJob> capped = manager.getJobsNeedingDispatch(3);
        Assertions.assertEquals(3, capped.size());
        for (int i = 0; i < capped.size(); i++) {
            Assertions.assertEquals(i + 1L, capped.get(i).getJobId());
            Assertions.assertEquals(LanceIndexJobMutationState.PENDING, capped.get(i).getMutationState());
        }

        Assertions.assertTrue(manager.getJobsNeedingDispatch(0).isEmpty());
    }

    @Test
    public void expiredQueryReturnsOnlyRunningJobsPastTheirDeadline() throws Exception {
        TestManager manager = new TestManager();
        long now = 1_000L;
        // RUNNING with an expired deadline is the sweep's only input.
        manager.replayUpsertJob(runningRecord(1L, "IdxExpired", 999L));
        // The boundary is strict: a deadline exactly at "now" has not expired.
        manager.replayUpsertJob(runningRecord(2L, "IdxAtBoundary", now));
        manager.replayUpsertJob(runningRecord(3L, "IdxStillWaiting", 1_001L));
        // RUNNING without a deadline (an old record) never expires on its own.
        LanceIndexJob deadlineless = runningRecord(4L, "IdxDeadlineless");
        deadlineless.setDeadlineMs(null);
        manager.replayUpsertJob(deadlineless);
        // Non-RUNNING states are invisible even with an expired deadline in the record.
        LanceIndexJob pending = dispatchablePending(5L, "IdxPending");
        pending.setDeadlineMs(1L);
        manager.replayUpsertJob(pending);
        LanceIndexJob unknown = runningRecord(6L, "IdxUnknown", 1L);
        unknown.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        manager.replayUpsertJob(unknown);

        List<LanceIndexJob> expired = manager.getExpiredRunningJobs(now);
        Assertions.assertEquals(1, expired.size());
        Assertions.assertEquals(1L, expired.get(0).getJobId());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, expired.get(0).getMutationState());
    }

    @Test
    public void possibleLiveQueryReturnsSlotHoldersWithCompleteIdentity() throws Exception {
        TestManager manager = new TestManager();
        // A RUNNING dispatch holds a slot.
        manager.replayUpsertJob(runningRecord(1L, "IdxRunning"));
        // An UNKNOWN converged from RUNNING holds its slot exactly the same way: the release
        // proof is independent of the outcome.
        LanceIndexJob unknown = runningRecord(2L, "IdxUnknown");
        unknown.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        unknown.setResult(new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                LanceIndexJobCompletionReason.NONE, "deadline expired", false));
        unknown.setRevision(2L);
        manager.replayUpsertJob(unknown);
        // A slot already released by a termination proof is gone.
        LanceIndexJob reaped = runningRecord(3L, "IdxReaped");
        reaped.setTerminationProof(LanceIndexTerminationProof.CHILD_REAPED);
        reaped.setPossibleLiveOwned(false);
        manager.replayUpsertJob(reaped);
        // A force-released record holds nothing.
        LanceIndexJob forced = runningRecord(4L, "IdxForced");
        forced.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        forced.setForceReleased(true);
        manager.replayUpsertJob(forced);
        // A corrupt record that claims a slot but lacks dispatch identity cannot be matched
        // by the epoch sweep and is skipped.
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":1,\"ms\":\"RUNNING\",\"plo\":true}", LanceIndexJob.class));

        List<LanceIndexJob> holders = manager.getJobsHoldingPossibleLiveSlot();
        Assertions.assertEquals(2, holders.size());
        // The query does not promise an order; assert membership and each state.
        List<Long> holderIds = new ArrayList<>();
        for (LanceIndexJob holder : holders) {
            holderIds.add(holder.getJobId());
            Assertions.assertTrue(holder.holdsPossibleLiveSlot());
        }
        Collections.sort(holderIds);
        Assertions.assertEquals(Arrays.asList(1L, 2L), holderIds);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, manager.getJob(2L).getMutationState());
    }

    @Test
    public void refreshQueryExcludesForceReleasedJobs() throws Exception {
        TestManager manager = new TestManager();
        // A terminal job owing its first refresh is the driver's input.
        manager.createJob(newCreateJob(1L, "IdxOwed", CATALOG_ID), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                        LanceIndexJobCompletionReason.NONE, "ok", false)));

        // The same terminal shape, but force-released: its fence is already gone and the
        // driver must never pick it up again.
        LanceIndexJob forced = newCreateJob(7L, "IdxForced", CATALOG_ID);
        forced.setMutationState(LanceIndexJobMutationState.COMMITTED);
        forced.setRefreshState(LanceIndexJobRefreshState.REQUIRED);
        forced.setRevision(2L);
        forced.setForceReleased(true);
        manager.replayUpsertJob(forced);
        // And the FAILED variant of a forced job, equally invisible.
        LanceIndexJob forcedFailed = newCreateJob(8L, "IdxForcedFailed", CATALOG_ID);
        forcedFailed.setMutationState(LanceIndexJobMutationState.NOT_COMMITTED);
        forcedFailed.setRefreshState(LanceIndexJobRefreshState.FAILED);
        forcedFailed.setRevision(3L);
        forcedFailed.setForceReleased(true);
        manager.replayUpsertJob(forcedFailed);

        List<LanceIndexJob> needing = manager.getJobsNeedingRefresh();
        Assertions.assertEquals(1, needing.size());
        Assertions.assertEquals(1L, needing.get(0).getJobId());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, needing.get(0).getRefreshState());
        Assertions.assertFalse(needing.get(0).isForceReleased());
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName, long catalogId) {
        return new LanceIndexJob(jobId, "tester", catalogId, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    /**
     * A durable PENDING record whose dispatch identity is complete, the form the dispatch
     * sweep accepts. Built through setters and replayed verbatim, like a follower applying
     * a journal record.
     */
    private static LanceIndexJob dispatchablePending(long jobId, String displayName) {
        LanceIndexJob job = newCreateJob(jobId, displayName, CATALOG_ID);
        job.setMutationState(LanceIndexJobMutationState.PENDING);
        job.setRefreshState(LanceIndexJobRefreshState.NOT_REQUIRED);
        job.setBackendId(BACKEND_ID);
        job.setBeProcessEpoch(BE_EPOCH);
        job.setInvocationId(INVOCATION_ID);
        job.setDispatchRevision(1L);
        job.setRevision(1L);
        return job;
    }

    private static LanceIndexJob runningRecord(long jobId, String displayName) {
        return runningRecord(jobId, displayName, DEADLINE_MS);
    }

    private static LanceIndexJob runningRecord(long jobId, String displayName, long deadlineMs) {
        LanceIndexJob job = dispatchablePending(jobId, displayName);
        job.setMutationState(LanceIndexJobMutationState.RUNNING);
        job.setPossibleLiveOwned(true);
        job.setDeadlineMs(deadlineMs);
        return job;
    }

    private static class TestManager extends LanceIndexJobManager {
        @Override
        protected void writeEditLog(LanceIndexJob job) {
            // No journal in a pure query unit test.
        }
    }
}
