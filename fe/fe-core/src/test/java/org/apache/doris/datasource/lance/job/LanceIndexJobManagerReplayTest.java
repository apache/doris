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
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Replay and master-transfer coverage for {@link LanceIndexJobManager}. The journal
 * records of a scenario are captured from a source manager through the edit-log seam
 * and replayed verbatim into a fresh target manager, exactly like a follower tailing
 * the edit log. The pinned invariants: replay is a verbatim replace with a monotonic
 * revision guard (no state transformation on followers); a replayed PENDING permits
 * exactly one dispatch; only the master-election sweep turns a durable RUNNING into
 * UNKNOWN, after which redispatch is permanently refused; an unresolved UNKNOWN keeps
 * its fence and quota across replay while a FORCE-released UNKNOWN frees both; stale
 * callbacks (revision / invocation id / BE epoch) never change state; and a rejected
 * admission leaves no job, fence, quota charge, or journal record behind.
 */
public class LanceIndexJobManagerReplayTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;

    @Test
    public void replayedPendingPermitsExactlyOneDispatch() throws DdlException {
        List<LanceIndexJob> records = runningRecords(1L, "IdxA");
        TestManager target = new TestManager();
        target.replayUpsertJob(records.get(0));

        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, target.getJob(1L).getMutationState());
        Assertions.assertTrue(target.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(target.markRunning(1L, 1L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(target.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, target.getJob(1L).getMutationState());
    }

    @Test
    public void replayedRunningStaysRunningWithoutSweep() throws DdlException {
        List<LanceIndexJob> records = runningRecords(1L, "IdxA");
        TestManager target = new TestManager();
        target.replayUpsertJob(records.get(0));
        target.replayUpsertJob(records.get(1));

        LanceIndexJob stored = target.getJob(1L);
        // A follower tailing a live master must not transform a fresh RUNNING record.
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals(1L, stored.getRevision());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
        Assertions.assertTrue(target.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
        // Replay itself never writes the journal.
        Assertions.assertTrue(target.editLog.isEmpty());
    }

    @Test
    public void transferToMasterConvertsRunningToUnknownAndNeverRedispatches() throws DdlException {
        List<LanceIndexJob> records = runningRecords(1L, "IdxA");
        TestManager target = new TestManager();
        target.replayUpsertJob(records.get(0));
        target.replayUpsertJob(records.get(1));
        LanceIndexFenceKey fenceKey = target.getJob(1L).fenceKey();

        target.onTransferToMaster();

        LanceIndexJob swept = target.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, swept.getMutationState());
        Assertions.assertEquals(2L, swept.getRevision());
        Assertions.assertEquals(LanceIndexJobResultCode.NO_TRUSTED_RESULT, swept.getResult().getResultCode());
        // Fence, quota, and the possible-live slot survive the sweep: only FORCE releases them.
        Assertions.assertTrue(target.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
        Assertions.assertTrue(swept.holdsPossibleLiveSlot());
        Assertions.assertTrue(target.getUnresolvedJobs().contains(swept));
        Assertions.assertEquals(1, target.editLog.size());

        Assertions.assertFalse(target.markRunning(1L, 2L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(target.markRunning(1L, 1L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(target.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, target.getJob(1L).getMutationState());
    }

    @Test
    public void transferToMasterDowngradesRunningRefreshToRequired() throws DdlException {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        source.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        source.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH, okResult());
        source.markRefreshRunning(1L, 2L);

        TestManager target = new TestManager();
        for (LanceIndexJob record : source.editLog) {
            target.replayUpsertJob(record);
        }
        Assertions.assertEquals(LanceIndexJobRefreshState.RUNNING, target.getJob(1L).getRefreshState());

        target.onTransferToMaster();

        LanceIndexJob swept = target.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, swept.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, swept.getRefreshState());
        Assertions.assertEquals(4L, swept.getRevision());
        Assertions.assertTrue(target.getJobsNeedingRefresh().contains(swept));
        Assertions.assertTrue(target.isFenceHeld(swept.fenceKey()));
    }

    @Test
    public void replayedTerminalWithRefreshRequiredOnlyAllowsRefreshPath() throws DdlException {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        source.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        source.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH, okResult());

        TestManager target = new TestManager();
        for (LanceIndexJob record : source.editLog) {
            target.replayUpsertJob(record);
        }
        LanceIndexJob stored = target.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
        LanceIndexFenceKey fenceKey = stored.fenceKey();

        // The mutation lifecycle is closed; only the refresh transitions remain.
        Assertions.assertFalse(target.markRunning(1L, 2L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertFalse(target.completeWithResult(1L, 2L, INVOCATION_ID, BE_EPOCH, okResult()));
        Assertions.assertTrue(target.getJobsNeedingRefresh().contains(stored));
        Assertions.assertTrue(target.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());

        Assertions.assertTrue(target.markRefreshRunning(1L, 2L));
        Assertions.assertTrue(target.markRefreshDone(1L, 3L));
        Assertions.assertFalse(target.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());
        Assertions.assertTrue(target.getUnresolvedJobs().isEmpty());
    }

    @Test
    public void replayedUnresolvedUnknownFencesTheSameName() throws DdlException {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        source.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        source.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, "ambiguous", false));

        TestManager target = new TestManager();
        for (LanceIndexJob record : source.editLog) {
            target.replayUpsertJob(record);
        }
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, target.getJob(1L).getMutationState());

        // The unforced UNKNOWN still holds the same-name fence and the quota.
        Assertions.assertThrows(DdlException.class,
                () -> target.createJob(newCreateJob(9L, "IdxA"), 100, 100, 100));
        Assertions.assertThrows(DdlException.class,
                () -> target.createJob(newCreateJob(9L, "idxa"), 100, 100, 100));
        target.createJob(newCreateJob(9L, "IdxB"), 100, 100, 100);
        Assertions.assertEquals(2L, target.getQuota().getGlobalCount());
    }

    @Test
    public void replayedForceReleasedUnknownFreesNameAndQuota() {
        TestManager target = new TestManager();
        target.replayUpsertJob(forceReleasedUnknownJob(7L, "idxforce"));

        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());
        Assertions.assertFalse(target.isFenceHeld(target.getJob(7L).fenceKey()));
        Assertions.assertTrue(target.getUnresolvedJobs().isEmpty());

        Assertions.assertDoesNotThrow(() -> target.createJob(newCreateJob(8L, "IdxForce"), 100, 100, 100));
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
        Assertions.assertEquals(1, target.editLog.size());
    }

    @Test
    public void staleCallbacksAreRejectedWithoutStateChange() throws DdlException {
        List<LanceIndexJob> records = runningRecords(1L, "IdxA");
        TestManager target = new TestManager();
        target.replayUpsertJob(records.get(0));
        target.replayUpsertJob(records.get(1));

        Assertions.assertFalse(target.completeWithResult(1L, 0L, INVOCATION_ID, BE_EPOCH, okResult()));
        Assertions.assertFalse(target.completeWithResult(1L, 1L, "invocation-x", BE_EPOCH, okResult()));
        Assertions.assertFalse(target.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH + 1, okResult()));
        Assertions.assertFalse(target.completeWithResult(1L, 1L, INVOCATION_ID, null, okResult()));
        Assertions.assertFalse(target.completeWithResult(404L, 1L, INVOCATION_ID, BE_EPOCH, okResult()));

        LanceIndexJob stored = target.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals(1L, stored.getRevision());
        Assertions.assertNull(stored.getResult());
        Assertions.assertTrue(target.editLog.isEmpty());

        Assertions.assertTrue(target.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH, okResult()));
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, target.getJob(1L).getMutationState());
    }

    @Test
    public void replayIsIdempotentForTheSameRecord() throws DdlException {
        List<LanceIndexJob> records = runningRecords(1L, "IdxA");
        TestManager target = new TestManager();
        target.replayUpsertJob(records.get(0));
        target.replayUpsertJob(records.get(0));
        target.replayUpsertJob(records.get(1));
        target.replayUpsertJob(records.get(1));

        Assertions.assertEquals(1, target.getJobCount());
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, target.getJob(1L).getMutationState());
        Assertions.assertEquals(1L, target.getJob(1L).getRevision());
    }

    @Test
    public void lowerRevisionRecordNeverOverwritesHigherRevision() throws DdlException {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        source.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        source.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH, okResult());
        // Records: PENDING rev0, RUNNING rev1, COMMITTED+REQUIRED rev2.

        TestManager target = new TestManager();
        target.replayUpsertJob(source.editLog.get(2));
        target.replayUpsertJob(source.editLog.get(0));
        target.replayUpsertJob(source.editLog.get(1));

        LanceIndexJob stored = target.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(2L, stored.getRevision());
        Assertions.assertTrue(target.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
        Assertions.assertTrue(target.editLog.isEmpty());

        // An equal revision replaces verbatim (idempotent re-delivery).
        target.replayUpsertJob(source.editLog.get(2));
        Assertions.assertEquals(2L, target.getJob(1L).getRevision());
        Assertions.assertEquals(1L, target.getQuota().getGlobalCount());
    }

    @Test
    public void replayToleratesNullAndIdentityLessRecords() {
        TestManager target = new TestManager();
        target.replayUpsertJob(null);
        Assertions.assertEquals(0, target.getJobCount());

        // A corrupt record without fence identity stays queryable but out of the books.
        LanceIndexJob sparse = GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"PENDING\"}", LanceIndexJob.class);
        target.replayUpsertJob(sparse);
        Assertions.assertEquals(1, target.getJobCount());
        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, target.getJob(5L).getMutationState());
    }

    @Test
    public void replayingOverAnIdentityLessRecordNeverThrows() {
        TestManager target = new TestManager();
        LanceIndexJob sparse = GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"PENDING\"}", LanceIndexJob.class);
        target.replayUpsertJob(sparse);
        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());

        // Re-delivering the same identity-less record must not key the release side on it.
        Assertions.assertDoesNotThrow(() -> target.replayUpsertJob(sparse));
        Assertions.assertEquals(1, target.getJobCount());
        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());

        // Neither must a higher-revision identity-less upsert of the same job id.
        LanceIndexJob unknown = GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":1,\"ms\":\"UNKNOWN\"}", LanceIndexJob.class);
        Assertions.assertDoesNotThrow(() -> target.replayUpsertJob(unknown));
        LanceIndexJob stored = target.getJob(5L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(1L, stored.getRevision());
        Assertions.assertEquals(1, target.getJobCount());
        Assertions.assertEquals(0L, target.getQuota().getGlobalCount());
    }

    @Test
    public void transferToMasterSweepsOnlyTheRunningJobInAMixedPopulation() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxPending"), 100, 100, 100);
        manager.createJob(newCreateJob(2L, "IdxCommitted"), 100, 100, 100);
        manager.markRunning(2L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        manager.completeWithResult(2L, 1L, INVOCATION_ID, BE_EPOCH, okResult());
        manager.markRefreshRunning(2L, 2L);
        manager.markRefreshDone(2L, 3L);
        manager.createJob(newCreateJob(3L, "IdxRunning"), 100, 100, 100);
        manager.markRunning(3L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        // Journal so far: create(1), create+run+complete+refreshRun+refreshDone(2), create+run(3).
        Assertions.assertEquals(8, manager.editLog.size());

        manager.onTransferToMaster();

        // PENDING and the settled terminal job are replay-faithful: untouched.
        LanceIndexJob pending = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, pending.getMutationState());
        Assertions.assertEquals(0L, pending.getRevision());
        LanceIndexJob committed = manager.getJob(2L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, committed.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.DONE, committed.getRefreshState());
        Assertions.assertEquals(4L, committed.getRevision());

        // Only the durable RUNNING becomes UNKNOWN, through exactly one journal record.
        LanceIndexJob swept = manager.getJob(3L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, swept.getMutationState());
        Assertions.assertEquals(2L, swept.getRevision());
        Assertions.assertEquals(9, manager.editLog.size());
    }

    @Test
    public void fenceRejectionLeavesNoJobNoQuotaAndNoJournalRecord() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> manager.createJob(newCreateJob(2L, "idxa"), 100, 100, 100));
        // The rejection must not disclose the dataset locator to an unauthorized caller.
        Assertions.assertFalse(exception.getMessage().contains("bucket"));
        Assertions.assertFalse(exception.getMessage().contains(LOCATOR));

        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        // Only the original job's fence exists; nothing new was registered for the rejection.
        Assertions.assertFalse(manager.isFenceHeld(newCreateJob(3L, "IdxB").fenceKey()));
    }

    @Test
    public void quotaRejectionLeavesNoJobNoFenceAndNoJournalRecord() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 1, 1, 1);

        Assertions.assertThrows(DdlException.class,
                () -> manager.createJob(newCreateJob(2L, "IdxB"), 1, 1, 1));

        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertFalse(manager.isFenceHeld(newCreateJob(2L, "IdxB").fenceKey()));
    }

    @Test
    public void duplicateJobIdIsRejectedBeforeAnythingElse() throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);

        Assertions.assertThrows(DdlException.class,
                () -> manager.createJob(newCreateJob(1L, "IdxB"), 100, 100, 100));
        Assertions.assertEquals(1, manager.getJobCount());
        Assertions.assertEquals(1, manager.editLog.size());
        Assertions.assertFalse(manager.isFenceHeld(newCreateJob(1L, "IdxB").fenceKey()));
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static LanceIndexJobResult okResult() {
        return new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                LanceIndexJobCompletionReason.NONE, "ok", false);
    }

    /**
     * Runs create + dispatch on a throwaway source manager and returns its journal
     * records: [PENDING rev0, RUNNING rev1].
     */
    private static List<LanceIndexJob> runningRecords(long jobId, String displayName) throws DdlException {
        TestManager source = new TestManager();
        source.createJob(newCreateJob(jobId, displayName), 100, 100, 100);
        source.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS);
        return source.editLog;
    }

    /**
     * Builds the durable form of a FORCE-released UNKNOWN job. The FORCE slice owns the
     * transition itself; here the record only exists as replay input, so it is built
     * from its JSON journal form.
     */
    private static LanceIndexJob forceReleasedUnknownJob(long jobId, String normalizedName) {
        String json = "{\"jid\":" + jobId + ",\"cr\":\"tester\",\"rev\":2,\"cid\":" + CATALOG_ID
                + ",\"dbn\":\"db1\",\"tbn\":\"tbl1\",\"prv\":\"" + LanceIndexFenceKey.PROVIDER_DIRECTORY
                + "\",\"loc\":\"" + LOCATOR + "\",\"din\":\"" + normalizedName + "\",\"nin\":\"" + normalizedName
                + "\",\"mt\":\"CREATE\",\"ms\":\"UNKNOWN\",\"rs\":\"NOT_REQUIRED\",\"fr\":true,"
                + "\"fa\":\"admin\",\"ftm\":12345,\"fn\":\"operator note\"}";
        LanceIndexJob job = GsonUtils.GSON.fromJson(json, LanceIndexJob.class);
        Assertions.assertTrue(job.isForceReleased());
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, job.getMutationState());
        return job;
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
