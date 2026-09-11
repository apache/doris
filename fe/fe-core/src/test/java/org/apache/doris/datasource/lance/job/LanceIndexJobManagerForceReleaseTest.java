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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FORCE_RELEASE transition coverage for {@link LanceIndexJobManager#forceRelease},
 * driven through the master write paths with the edit-log seam captured in memory.
 * The pinned invariants: only an UNKNOWN job may be force-released; the release is
 * durable (exactly one upsert journal record carrying the five FORCE audit fields
 * and a bumped revision/update time) and atomically releases the same-name fence,
 * the unresolved quota, and the possible-live slot without rewriting the UNKNOWN
 * outcome; a retry is idempotent and returns the existing release record even with
 * the pre-release revision (the short-circuit deliberately precedes the revision
 * CAS, unlike the stale-callback convention); a stale revision or an oversized
 * bounded text field changes nothing and writes no journal; a force-released name
 * is immediately reusable; the release of an identity-less (corrupt) record lifts
 * the fail-closed admission blockade by id; and a force-released job leaves the
 * refresh-driver queue.
 */
public class LanceIndexJobManagerForceReleaseTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long DEADLINE_MS = 9999L;
    private static final String ACTOR = "admin";
    private static final String NOTE = "worker lost after commit; outcome unverifiable";
    private static final String WARNING = "the old worker may still overwrite the index";

    @Test
    public void forceReleaseOfUnknownReleasesFenceQuotaAndSlotAtomically() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxA");
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();
        Assertions.assertTrue(manager.getJob(1L).holdsPossibleLiveSlot());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(3, manager.editLog.size());

        Assertions.assertTrue(manager.forceRelease(1L, 2L, ACTOR, NOTE, WARNING));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertTrue(stored.isForceReleased());
        Assertions.assertEquals(3L, stored.getRevision());
        Assertions.assertEquals(ACTOR, stored.getForceActor());
        Assertions.assertEquals(NOTE, stored.getForceNote());
        Assertions.assertEquals(WARNING, stored.getForceWarning());
        Assertions.assertNotNull(stored.getForceTimeMs());
        // The release bumps the durable update time onto the force time.
        Assertions.assertEquals(stored.getForceTimeMs().longValue(), stored.getUpdateTimeMs());
        // The UNKNOWN outcome itself is never rewritten.
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        // possibleLiveOwned keeps its value for audit; the derived slot is released.
        Assertions.assertTrue(stored.isPossibleLiveOwned());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());

        // Fence, quota, and the unresolved books settle with the same record swap.
        Assertions.assertFalse(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(0L, manager.getQuota().getCatalogCount(CATALOG_ID));
        Assertions.assertEquals(0L, manager.getQuota().getTableCount(stored.getTableQuotaKey()));
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());

        // The force release is durable: exactly one upsert record carrying the audit fields.
        Assertions.assertEquals(4, manager.editLog.size());
        LanceIndexJob logged = manager.editLog.get(3);
        Assertions.assertTrue(logged.isForceReleased());
        Assertions.assertEquals(3L, logged.getRevision());
        Assertions.assertEquals(ACTOR, logged.getForceActor());
        Assertions.assertEquals(NOTE, logged.getForceNote());
        Assertions.assertEquals(WARNING, logged.getForceWarning());
    }

    @Test
    public void repeatedForceReleaseReturnsTheExistingReleaseRecord() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxA");
        Assertions.assertTrue(manager.forceRelease(1L, 2L, ACTOR, NOTE, WARNING));
        LanceIndexJob first = manager.getJob(1L);
        int journalSize = manager.editLog.size();

        // A retry still observes the existing release record, even carrying the
        // pre-release revision: the idempotent short-circuit precedes the revision CAS.
        Assertions.assertTrue(manager.forceRelease(1L, 2L, "other-actor", "other note", "other warning"));
        Assertions.assertTrue(manager.forceRelease(1L, 3L, "other-actor", "other note", "other warning"));

        // No new journal record, and the first release record is kept verbatim.
        Assertions.assertEquals(journalSize, manager.editLog.size());
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(ACTOR, stored.getForceActor());
        Assertions.assertEquals(NOTE, stored.getForceNote());
        Assertions.assertEquals(WARNING, stored.getForceWarning());
        Assertions.assertEquals(first.getForceTimeMs(), stored.getForceTimeMs());
        Assertions.assertEquals(3L, stored.getRevision());
    }

    @Test
    public void forceReleaseRejectsNonUnknownStatesWithoutAJournalRecord() throws DdlException {
        TestManager manager = new TestManager();
        // PENDING at revision 0.
        manager.createJob(newCreateJob(1L, "IdxPending"), 100, 100, 100);
        // RUNNING at revision 1.
        manager.createJob(newCreateJob(2L, "IdxRunning"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(2L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        // COMMITTED at revision 2.
        manager.createJob(newCreateJob(3L, "IdxCommitted"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(3L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(3L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NATIVE_OK)));
        // NOT_COMMITTED at revision 2.
        manager.createJob(newCreateJob(4L, "IdxNotCommitted"), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(4L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(4L, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.PRE_INVOCATION_CREDENTIAL_EXPIRED)));
        Assertions.assertEquals(9, manager.editLog.size());

        Assertions.assertFalse(manager.forceRelease(1L, 0L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(2L, 1L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(3L, 2L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(4L, 2L, ACTOR, NOTE, WARNING));

        // A rejection is warn + false: no journal record and no field changes.
        Assertions.assertEquals(9, manager.editLog.size());
        for (long jobId = 1L; jobId <= 4L; jobId++) {
            LanceIndexJob stored = manager.getJob(jobId);
            Assertions.assertFalse(stored.isForceReleased(), "job " + jobId);
            Assertions.assertNull(stored.getForceActor(), "job " + jobId);
            Assertions.assertNull(stored.getForceTimeMs(), "job " + jobId);
        }
        Assertions.assertEquals(0L, manager.getJob(1L).getRevision());
        Assertions.assertEquals(1L, manager.getJob(2L).getRevision());
        Assertions.assertEquals(2L, manager.getJob(3L).getRevision());
        Assertions.assertEquals(2L, manager.getJob(4L).getRevision());
        // Three charges remain: job 4 settled NOT_COMMITTED with refresh NOT_REQUIRED
        // and released its quota already at completion time.
        Assertions.assertEquals(3L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void forceReleaseRejectsAStaleRevisionOrAnUnknownId() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxA");
        Assertions.assertEquals(3, manager.editLog.size());

        Assertions.assertFalse(manager.forceRelease(1L, 0L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(1L, 1L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(1L, 3L, ACTOR, NOTE, WARNING));
        Assertions.assertFalse(manager.forceRelease(404L, 2L, ACTOR, NOTE, WARNING));

        Assertions.assertEquals(3, manager.editLog.size());
        Assertions.assertFalse(manager.getJob(1L).isForceReleased());
        Assertions.assertEquals(2L, manager.getJob(1L).getRevision());
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(1L).fenceKey()));

        // The matching revision still wins afterwards.
        Assertions.assertTrue(manager.forceRelease(1L, 2L, ACTOR, NOTE, WARNING));
        Assertions.assertTrue(manager.getJob(1L).isForceReleased());
    }

    @Test
    public void forceReleaseRejectsOversizedBoundedText() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxA");
        Assertions.assertEquals(3, manager.editLog.size());

        String oversized = StringUtils.repeat("n", LanceIndexJob.MAX_FORCE_TEXT_BYTES + 1);
        DdlException overNote = Assertions.assertThrows(DdlException.class,
                () -> manager.forceRelease(1L, 2L, ACTOR, oversized, WARNING));
        Assertions.assertTrue(overNote.getMessage().contains("force release"));
        // The warning field is bounded by the same limit.
        Assertions.assertThrows(DdlException.class,
                () -> manager.forceRelease(1L, 2L, ACTOR, NOTE, oversized));
        // Multibyte characters count as UTF-8 bytes, not chars.
        Assertions.assertThrows(DdlException.class,
                () -> manager.forceRelease(1L, 2L, ACTOR, StringUtils.repeat("é", 513), WARNING));

        // A rejected release leaves no journal record and keeps the job unresolved.
        Assertions.assertEquals(3, manager.editLog.size());
        Assertions.assertFalse(manager.getJob(1L).isForceReleased());
        Assertions.assertEquals(2L, manager.getJob(1L).getRevision());
        Assertions.assertTrue(manager.isFenceHeld(manager.getJob(1L).fenceKey()));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        // Exactly at the byte limit is accepted.
        Assertions.assertTrue(manager.forceRelease(1L, 2L, ACTOR,
                StringUtils.repeat("n", LanceIndexJob.MAX_FORCE_TEXT_BYTES), WARNING));
    }

    @Test
    public void sameNameAdmissionSucceedsAfterForceRelease() throws DdlException {
        TestManager manager = new TestManager();
        createRunAndLoseResult(manager, 1L, "IdxA");
        // The unresolved UNKNOWN still fences the name, case-insensitively.
        Assertions.assertThrows(DdlException.class,
                () -> manager.createJob(newCreateJob(9L, "idxa"), 100, 100, 100));

        Assertions.assertTrue(manager.forceRelease(1L, 2L, ACTOR, NOTE, WARNING));
        manager.createJob(newCreateJob(9L, "idxa"), 100, 100, 100);

        LanceIndexJob admitted = manager.getJob(9L);
        Assertions.assertEquals(LanceIndexJobMutationState.PENDING, admitted.getMutationState());
        Assertions.assertTrue(manager.isFenceHeld(admitted.fenceKey()));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(2, manager.getJobCount());
    }

    @Test
    public void forceReleaseLiftsTheAdmissionBlockOfAnIdentityLessRecord() throws DdlException {
        TestManager manager = new TestManager();
        // A replayed corrupt record without fence identity blocks every admission fail-closed.
        manager.replayUpsertJob(GsonUtils.GSON.fromJson(
                "{\"jid\":5,\"rev\":0,\"ms\":\"UNKNOWN\"}", LanceIndexJob.class));
        Assertions.assertEquals(Collections.singletonList(5L), manager.getCorruptUnresolvedJobIds());
        DdlException blocked = Assertions.assertThrows(DdlException.class,
                () -> manager.createJob(newCreateJob(9L, "IdxB"), 100, 100, 100));
        Assertions.assertTrue(blocked.getMessage().contains("smallest job id 5"));

        // The release resolves the job by id, without needing its fence key.
        Assertions.assertTrue(manager.forceRelease(5L, 0L, ACTOR, NOTE, WARNING));

        Assertions.assertTrue(manager.getJob(5L).isForceReleased());
        Assertions.assertTrue(manager.getCorruptUnresolvedJobIds().isEmpty());
        Assertions.assertEquals(1, manager.editLog.size());
        // The identity-less record was never charged; admission is open again.
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
        manager.createJob(newCreateJob(9L, "IdxB"), 100, 100, 100);
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
    }

    @Test
    public void forceReleasedJobLeavesTheRefreshQueue() throws DdlException {
        TestManager manager = new TestManager();
        manager.replayUpsertJob(unknownNeedingRefresh(5L, "idxrf"));
        Assertions.assertTrue(containsJob(manager.getJobsNeedingRefresh(), 5L));

        Assertions.assertTrue(manager.forceRelease(5L, 2L, ACTOR, NOTE, WARNING));

        // A force-released job owes no refresh; picking it up would only add audit noise.
        Assertions.assertFalse(containsJob(manager.getJobsNeedingRefresh(), 5L));
        Assertions.assertTrue(manager.getUnresolvedJobs().isEmpty());
        Assertions.assertEquals(0L, manager.getQuota().getGlobalCount());
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static boolean containsJob(List<LanceIndexJob> jobs, long jobId) {
        return jobs.stream().anyMatch(job -> job.getJobId() == jobId);
    }

    private static LanceIndexJobResult result(LanceIndexJobResultCode code) {
        return new LanceIndexJobResult(code, LanceIndexJobCompletionReason.NONE, "sanitized message", false);
    }

    /**
     * Drives a job to a durable UNKNOWN at revision 2 through the lifecycle channel:
     * create (rev0) + dispatch (rev1) + an ambiguous result (rev2), fence/quota held.
     */
    private static void createRunAndLoseResult(TestManager manager, long jobId, String displayName)
            throws DdlException {
        manager.createJob(newCreateJob(jobId, displayName), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, DEADLINE_MS));
        Assertions.assertTrue(manager.completeWithResult(jobId, 1L, INVOCATION_ID, BE_EPOCH,
                result(LanceIndexJobResultCode.NO_TRUSTED_RESULT)));
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, manager.getJob(jobId).getMutationState());
    }

    /**
     * Builds the durable form of an unforced UNKNOWN job whose refresh is still REQUIRED
     * (only reachable on a corrupt/older journal record: the lifecycle channel always
     * lands UNKNOWN with refresh NOT_REQUIRED). Carries full fence identity.
     */
    private static LanceIndexJob unknownNeedingRefresh(long jobId, String normalizedName) {
        String json = "{\"jid\":" + jobId + ",\"cr\":\"tester\",\"rev\":2,\"cid\":" + CATALOG_ID
                + ",\"dbn\":\"db1\",\"tbn\":\"tbl1\",\"prv\":\"" + LanceIndexFenceKey.PROVIDER_DIRECTORY
                + "\",\"loc\":\"" + LOCATOR + "\",\"din\":\"" + normalizedName + "\",\"nin\":\"" + normalizedName
                + "\",\"mt\":\"CREATE\",\"ms\":\"UNKNOWN\",\"rs\":\"REQUIRED\"}";
        LanceIndexJob job = GsonUtils.GSON.fromJson(json, LanceIndexJob.class);
        Assertions.assertFalse(job.isForceReleased());
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, job.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, job.getRefreshState());
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
