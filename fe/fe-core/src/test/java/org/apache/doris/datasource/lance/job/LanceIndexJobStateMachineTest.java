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

import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Lifecycle state-machine coverage driven through the master write paths of
 * {@link LanceIndexJobManager} with the edit-log seam captured in memory. Covers the
 * legal mutation chain PENDING -&gt; RUNNING -&gt; (COMMITTED | NOT_COMMITTED |
 * UNKNOWN), the impossibility of leaving UNKNOWN, the independent refresh chain
 * REQUIRED -&gt; RUNNING -&gt; DONE / FAILED with FAILED -&gt; RUNNING retry, the
 * revision compare-and-set on every transition, and the fence/quota release timing
 * (immediately on a terminal outcome with refresh NOT_REQUIRED, only at refresh DONE
 * otherwise, never for UNKNOWN without FORCE).
 */
public class LanceIndexJobStateMachineTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;

    @Test
    public void createInitializesPendingRecord() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(0L, stored.getRevision());
        Assertions.assertFalse(stored.isPossibleLiveOwned());
        Assertions.assertEquals(LanceIndexTerminationProof.NONE, stored.getTerminationProof());
        Assertions.assertTrue(stored.getCreateTimeMs() > 0L);
        Assertions.assertTrue(manager.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(1L, manager.getQuota().getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(1L, manager.getQuota().getTableCount(stored.getTableQuotaKey()));
        Assertions.assertEquals(1, manager.getUnresolvedJobs().size());
        Assertions.assertEquals(1, manager.editLog.size());
    }

    @Test
    public void pendingToRunningToCommitted() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));

        LanceIndexJob running = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, running.getMutationState());
        Assertions.assertEquals(1L, running.getRevision());
        Assertions.assertEquals(BACKEND_ID, running.getBackendId().longValue());
        Assertions.assertEquals(BE_EPOCH, running.getBeProcessEpoch().longValue());
        Assertions.assertEquals(INVOCATION_ID, running.getInvocationId());
        Assertions.assertEquals(DEADLINE_MS, running.getDeadlineMs().longValue());
        Assertions.assertTrue(running.holdsPossibleLiveSlot());

        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        LanceIndexJob committed = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, committed.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, committed.getRefreshState());
        Assertions.assertEquals(2L, committed.getRevision());
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_OK, committed.getResult().getResultCode());
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, committed.getResult().getCompletionReason());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().contains(committed));
    }

    @Test
    public void pendingToRunningToNotCommitted() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.PRE_INVOCATION_CREDENTIAL_EXPIRED)));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(2L, stored.getRevision());
    }

    @Test
    public void pendingToRunningToUnknown() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NO_TRUSTED_RESULT)));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(2L, stored.getRevision());
    }

    @Test
    public void dropIfExistsNotFoundCompletesWithIfConditionNoop() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newDropJob(1L, "IdxA", true), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_NOT_FOUND)));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(LanceIndexJobCompletionReason.IF_CONDITION_NOOP,
                stored.getResult().getCompletionReason());
    }

    @Test
    public void markRunningRejectsSecondDispatch() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");

        Assertions.assertFalse(manager.markRunning(1L, 1L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(1L, manager.getJob(1L).getRevision());
    }

    @Test
    public void markRunningRejectsWrongRevisionAndUnknownJob() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        Assertions.assertFalse(manager.markRunning(1L, 5L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(manager.markRunning(404L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertEquals(1, manager.editLog.size());
    }

    @Test
    public void completeRejectsNonRunningJob() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        Assertions.assertFalse(manager.completeWithResult(1L, 0L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());

        createAndRun(manager, 2L, "IdxB");
        Assertions.assertTrue(manager.completeWithResult(2L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertFalse(manager.completeWithResult(2L, 2L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, manager.getJob(2L).getMutationState());
    }

    @Test
    public void unknownHasNoOutgoingTransitions() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NO_TRUSTED_RESULT)));
        int loggedRecords = manager.editLog.size();

        Assertions.assertFalse(manager.markRunning(1L, 2L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(manager.completeWithResult(1L, 2L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertFalse(manager.markRefreshRunning(1L, 2L));
        Assertions.assertFalse(manager.markRefreshDone(1L, 2L));
        Assertions.assertFalse(manager.markRefreshFailed(1L, 2L));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(2L, stored.getRevision());
        Assertions.assertEquals(loggedRecords, manager.editLog.size());
    }

    @Test
    public void refreshLifecycleReleasesFenceAndQuotaAtDone() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));

        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertEquals(LanceIndexJobRefreshState.RUNNING, manager.getJob(1L).getRefreshState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));

        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, stored.getRefreshState());
        Assertions.assertEquals(4L, stored.getRevision());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().isEmpty());
    }

    @Test
    public void refreshFailureKeepsFenceAndRetriesThroughRunning() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshFailed(1L, 3L));
        LanceIndexJob failed = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobRefreshState.FAILED, failed.getRefreshState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().contains(failed));

        // FAILED -> RUNNING is the retry entry through the idempotent refresh path.
        Assertions.assertTrue(manager.markRefreshRunning(1L, 4L));
        Assertions.assertEquals(LanceIndexJobRefreshState.RUNNING, manager.getJob(1L).getRefreshState());
        Assertions.assertTrue(manager.markRefreshDone(1L, 5L));
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void refreshTransitionsRejectedFromNotRequired() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED)));
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, manager.getJob(1L).getRefreshState());

        Assertions.assertFalse(manager.markRefreshRunning(1L, 2L));
        Assertions.assertFalse(manager.markRefreshDone(1L, 2L));
        Assertions.assertFalse(manager.markRefreshFailed(1L, 2L));
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, manager.getJob(1L).getRefreshState());
        Assertions.assertEquals(2L, manager.getJob(1L).getRevision());
    }

    @Test
    public void refreshTransitionsRejectedFromDone() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));

        Assertions.assertFalse(manager.markRefreshRunning(1L, 4L));
        Assertions.assertFalse(manager.markRefreshDone(1L, 4L));
        Assertions.assertFalse(manager.markRefreshFailed(1L, 4L));
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, manager.getJob(1L).getRefreshState());
    }

    @Test
    public void refreshTransitionIsRevisionGuarded() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));

        Assertions.assertFalse(manager.markRefreshRunning(1L, 99L));
        Assertions.assertFalse(manager.markRefreshRunning(404L, 2L));
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(1L).getRefreshState());
        Assertions.assertEquals(2L, manager.getJob(1L).getRevision());
    }

    @Test
    public void fenceAndQuotaReleasedImmediatelyWhenRefreshNotRequired() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));

        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.PRE_INVOCATION_STALE_ADMISSION)));

        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, manager.getJob(1L).getMutationState());
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(0L, manager.getQuota().getCatalogCount(CATALOG_ID));
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
    }

    @Test
    public void fenceAndQuotaSurviveUnknown() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_IO)));

        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, manager.getJob(1L).getMutationState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(1, manager.getUnresolvedJobs().size());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().isEmpty());
    }

    @Test
    public void terminationProofReleasesSlotOnly() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot());

        Assertions.assertFalse(manager.recordTerminationProof(1L, 1L, LanceIndexTerminationProof.NONE));
        Assertions.assertFalse(manager.recordTerminationProof(1L, 99L, LanceIndexTerminationProof.CHILD_REAPED));
        Assertions.assertTrue(manager.recordTerminationProof(1L, 1L, LanceIndexTerminationProof.CHILD_REAPED));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals(LanceIndexTerminationProof.CHILD_REAPED, stored.getTerminationProof());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        // A slot may be proven exactly once.
        Assertions.assertFalse(manager.recordTerminationProof(1L, 2L, LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE));
    }

    @Test
    public void everyAcceptedTransitionWritesExactlyOneEditLogRecord() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH, result(LanceIndexJobResultCode.NATIVE_OK));
        manager.markRefreshRunning(1L, 2L);
        manager.markRefreshDone(1L, 3L);
        Assertions.assertEquals(5, manager.editLog.size());

        // Rejected transitions never reach the journal.
        Assertions.assertFalse(manager.markRefreshDone(1L, 4L));
        Assertions.assertFalse(manager.markRunning(1L, 4L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(5, manager.editLog.size());
    }

    @Test
    public void markRunningRejectsBlankInvocationId() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        // A null/blank invocation identity would match a null field under Objects.equals
        // in completeWithResult and silently defeat the stale-callback guard.
        Assertions.assertFalse(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, null, DEADLINE_MS));
        Assertions.assertFalse(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, "", DEADLINE_MS));
        Assertions.assertFalse(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, "  \t\n", DEADLINE_MS));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals(0L, stored.getRevision());
        Assertions.assertNull(stored.getInvocationId());
        Assertions.assertEquals(1, manager.editLog.size());

        // A well-formed dispatch is still accepted afterwards.
        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, manager.getJob(1L).getMutationState());
    }

    @Test
    public void failedRefreshJobStaysVisibleToTheRefreshDriver() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshFailed(1L, 3L));

        // FAILED still owes the idempotent retry: the driver must see the job.
        Assertions.assertTrue(manager.getJobsNeedingRefresh().contains(manager.getJob(1L)));

        Assertions.assertTrue(manager.markRefreshRunning(1L, 4L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 5L));
        Assertions.assertTrue(manager.getJobsNeedingRefresh().isEmpty());

        // Terminal jobs with refresh DONE or NOT_REQUIRED never show up.
        createAndRun(manager, 2L, "IdxB");
        Assertions.assertTrue(manager.completeWithResult(2L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED)));
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, manager.getJob(2L).getRefreshState());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().isEmpty());
    }

    @Test
    public void createJobResetsLifecycleFieldsAndPublishesAPrivateCopy() throws DdlException {
        LanceIndexJob dirty = newCreateJob(1L, "IdxA");
        dirty.setRevision(9L);
        dirty.setMutationState(LanceIndexJobMutationState.RUNNING);
        dirty.setRefreshState(LanceIndexJobRefreshState.RUNNING);
        dirty.setResult(result(LanceIndexJobResultCode.NATIVE_OK));
        dirty.setBackendId(BACKEND_ID);
        dirty.setBeProcessEpoch(BE_EPOCH);
        dirty.setInvocationId(INVOCATION_ID);
        dirty.setDeadlineMs(DEADLINE_MS);
        dirty.setPossibleLiveOwned(true);
        dirty.setTerminationProof(LanceIndexTerminationProof.CHILD_REAPED);
        dirty.setForceReleased(true);
        dirty.setForceActor("admin");
        dirty.setForceTimeMs(7L);
        dirty.setForceNote("note");
        dirty.setForceWarning("warning");

        TestManager manager = new TestManager();
        manager.createJob(dirty, 100, 100, 100);

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertNotSame(dirty, stored);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(0L, stored.getRevision());
        Assertions.assertNull(stored.getResult());
        Assertions.assertNull(stored.getBackendId());
        Assertions.assertNull(stored.getBeProcessEpoch());
        Assertions.assertNull(stored.getInvocationId());
        Assertions.assertNull(stored.getDeadlineMs());
        Assertions.assertFalse(stored.isPossibleLiveOwned());
        Assertions.assertEquals(LanceIndexTerminationProof.NONE, stored.getTerminationProof());
        Assertions.assertFalse(stored.isForceReleased());
        Assertions.assertNull(stored.getForceActor());
        Assertions.assertNull(stored.getForceTimeMs());
        Assertions.assertNull(stored.getForceNote());
        Assertions.assertNull(stored.getForceWarning());

        // Mutating the caller's object after admission touches nothing inside the manager.
        dirty.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        dirty.setNormalizedIndexName("idxb");
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, manager.getJob(1L).getMutationState());
        Assertions.assertTrue(manager.isFenceHeld(newCreateJob(2L, "idxa").fenceKey()));
        Assertions.assertFalse(manager.isFenceHeld(newCreateJob(3L, "IdxB").fenceKey()));
    }

    @Test
    public void refreshDoneOrFailedRequiresAPrecedingRefreshRunning() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(1L).getRefreshState());

        // No direct REQUIRED -> DONE / FAILED: a refresh must actually run first.
        Assertions.assertFalse(manager.markRefreshDone(1L, 2L));
        Assertions.assertFalse(manager.markRefreshFailed(1L, 2L));
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, manager.getJob(1L).getRefreshState());
        Assertions.assertEquals(2L, manager.getJob(1L).getRevision());
        Assertions.assertEquals(3, manager.editLog.size());
    }

    @Test
    public void terminationProofNeedsASlotAndStillLandsAfterTheTerminalResult() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        // PENDING owns no possible-live slot: a proof has nothing to release.
        Assertions.assertFalse(manager.recordTerminationProof(1L, 0L, LanceIndexTerminationProof.CHILD_REAPED));
        Assertions.assertEquals(0L, manager.getJob(1L).getRevision());
        Assertions.assertEquals(1, manager.editLog.size());

        Assertions.assertTrue(manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        // The terminal outcome does not release the slot; the proof still lands afterwards.
        LanceIndexJob committed = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, committed.getMutationState());
        Assertions.assertTrue(committed.holdsPossibleLiveSlot());
        LanceIndexFenceKey fenceKey = committed.fenceKey();

        Assertions.assertTrue(manager.recordTerminationProof(1L, 2L, LanceIndexTerminationProof.CHILD_REAPED));
        LanceIndexJob proven = manager.getJob(1L);
        Assertions.assertFalse(proven.holdsPossibleLiveSlot());
        // Fence and quota still follow the refresh rule, not the proof.
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        Assertions.assertTrue(manager.markRefreshRunning(1L, 3L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 4L));
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void notCommittedWithRefreshRequiredReleasesFenceAtRefreshDone() throws DdlException {
        TestManager manager = new TestManager();
        createAndRun(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT,
                        LanceIndexJobCompletionReason.NONE, "commit conflict", false)));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
        LanceIndexFenceKey fenceKey = stored.fenceKey();
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getJobsNeedingRefresh().contains(stored));

        Assertions.assertTrue(manager.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(manager.markRefreshDone(1L, 3L));
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static LanceIndexJob newDropJob(long jobId, String displayName, boolean ifExists) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.DROP, false, ifExists, null, "v",
                null, 7L, null);
    }

    private static LanceIndexJobResult result(LanceIndexJobResultCode code) {
        return new LanceIndexJobResult(code, LanceIndexJobCompletionReason.NONE, "sanitized message", false);
    }

    private static LanceIndexJob createAndRun(TestManager manager, long jobId, String displayName) throws DdlException {
        manager.createJob(newCreateJob(jobId, displayName), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        return manager.getJob(jobId);
    }

    /**
     * Edit-log seam: captures every durable record instead of writing the journal.
     */
    private static class TestManager extends LanceIndexJobManager {
        private final List<LanceIndexJob> editLog = new ArrayList<>();

        @Override
        protected void writeEditLog(LanceIndexJob job) {
            editLog.add(job);
        }
    }
}
