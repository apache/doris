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
import java.util.List;

/**
 * Record-level coverage for {@link LanceIndexJob}: the derived fence and quota keys,
 * the full {@link LanceIndexJob#isUnresolved()} matrix that keeps fence and quota
 * alive together, the possible-live slot semantics (released only by a matching
 * termination proof or a durable FORCE_RELEASE, never by a deadline), corrupt-record
 * fallbacks toward the safe direction, and the deep-copy constructor.
 */
public class LanceIndexJobTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";

    @Test
    public void fenceKeyCarriesIdentityAndHidesLocator() {
        LanceIndexJob job = newCreateJob(1L, "IdxA");
        LanceIndexFenceKey key = job.fenceKey();

        Assertions.assertEquals(new LanceIndexFenceKey(CATALOG_ID, LanceIndexFenceKey.PROVIDER_DIRECTORY,
                LOCATOR, "idxa"), key);
        Assertions.assertEquals(new LanceIndexFenceKey(CATALOG_ID, LanceIndexFenceKey.PROVIDER_DIRECTORY,
                LOCATOR, "idxa").hashCode(), key.hashCode());
        Assertions.assertNotEquals(new LanceIndexFenceKey(20L, LanceIndexFenceKey.PROVIDER_DIRECTORY,
                LOCATOR, "idxa"), key);
        Assertions.assertNotEquals(new LanceIndexFenceKey(CATALOG_ID, "REST", LOCATOR, "idxa"), key);
        Assertions.assertNotEquals(new LanceIndexFenceKey(CATALOG_ID, LanceIndexFenceKey.PROVIDER_DIRECTORY,
                "s3://bucket/other", "idxa"), key);
        Assertions.assertNotEquals(new LanceIndexFenceKey(CATALOG_ID, LanceIndexFenceKey.PROVIDER_DIRECTORY,
                LOCATOR, "idxb"), key);

        // Fence-conflict messages may surface to users without target privileges.
        Assertions.assertFalse(key.toString().contains(LOCATOR));
        Assertions.assertTrue(key.toString().contains("idxa"));
    }

    @Test
    public void fenceKeyIsStableAcrossJobsWithTheSameIdentity() {
        LanceIndexJob first = newCreateJob(1L, "IdxA");
        LanceIndexJob second = newCreateJob(2L, "idxa");
        // Display case differs, normalized identity does not.
        Assertions.assertEquals(first.fenceKey(), second.fenceKey());
        Assertions.assertEquals(first.fenceKey().hashCode(), second.fenceKey().hashCode());
    }

    @Test
    public void tableQuotaKeyDerivesFromCatalogAndLocator() {
        LanceIndexJob job = newCreateJob(1L, "IdxA");
        Assertions.assertEquals(new LanceIndexJobQuota.TableQuotaKey(CATALOG_ID, LOCATOR), job.getTableQuotaKey());
    }

    @Test
    public void activeStatesAreAlwaysUnresolved() {
        for (LanceIndexJobRefreshState refresh : LanceIndexJobRefreshState.values()) {
            Assertions.assertTrue(jobInState(LanceIndexJobMutationState.PENDING, refresh, false).isUnresolved());
            Assertions.assertTrue(jobInState(LanceIndexJobMutationState.RUNNING, refresh, false).isUnresolved());
        }
    }

    @Test
    public void knownTerminalStatesFollowTheRefreshState() {
        for (LanceIndexJobMutationState terminal : new LanceIndexJobMutationState[]{
                LanceIndexJobMutationState.COMMITTED, LanceIndexJobMutationState.NOT_COMMITTED}) {
            Assertions.assertFalse(jobInState(terminal, LanceIndexJobRefreshState.NOT_REQUIRED, false).isUnresolved());
            Assertions.assertFalse(jobInState(terminal, LanceIndexJobRefreshState.DONE, false).isUnresolved());
            Assertions.assertTrue(jobInState(terminal, LanceIndexJobRefreshState.REQUIRED, false).isUnresolved());
            Assertions.assertTrue(jobInState(terminal, LanceIndexJobRefreshState.RUNNING, false).isUnresolved());
            // A failed refresh still holds the fence: the retry goes through the idempotent path.
            Assertions.assertTrue(jobInState(terminal, LanceIndexJobRefreshState.FAILED, false).isUnresolved());
        }
    }

    @Test
    public void unknownIsUnresolvedUntilForceReleased() {
        for (LanceIndexJobRefreshState refresh : LanceIndexJobRefreshState.values()) {
            Assertions.assertTrue(jobInState(LanceIndexJobMutationState.UNKNOWN, refresh, false).isUnresolved());
            Assertions.assertFalse(jobInState(LanceIndexJobMutationState.UNKNOWN, refresh, true).isUnresolved());
        }
    }

    @Test
    public void nullStatesFallBackToTheSafeDirection() {
        // A corrupt record missing its states must keep the fence (treated as UNKNOWN)
        // and must never become redispatchable PENDING.
        LanceIndexJob job = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"ms\":null,\"rs\":null}", LanceIndexJob.class);
        Assertions.assertNull(job.getMutationState());
        Assertions.assertNull(job.getRefreshState());
        Assertions.assertTrue(job.isUnresolved());

        LanceIndexJob forced = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"ms\":null,\"rs\":null,\"fr\":true}", LanceIndexJob.class);
        Assertions.assertFalse(forced.isUnresolved());

        // A terminal record missing its refresh state owes one (treated as REQUIRED).
        LanceIndexJob committedNoRefresh = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"ms\":\"COMMITTED\",\"rs\":null}", LanceIndexJob.class);
        Assertions.assertTrue(committedNoRefresh.isUnresolved());
    }

    @Test
    public void missingRefreshStateKeyFallsBackToRequired() {
        // A corrupt terminal record without the "rs" key at all keeps the fence: the
        // field initial value is REQUIRED, the same safe direction as the null fallback.
        LanceIndexJob missingKey = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"ms\":\"COMMITTED\"}", LanceIndexJob.class);
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, missingKey.getRefreshState());
        Assertions.assertTrue(missingKey.isUnresolved());

        // A legal terminal record always carries the key explicitly.
        LanceIndexJob legal = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"ms\":\"COMMITTED\",\"rs\":\"NOT_REQUIRED\"}", LanceIndexJob.class);
        Assertions.assertFalse(legal.isUnresolved());
    }

    @Test
    public void possibleLiveSlotMatrix() {
        LanceIndexJob job = new LanceIndexJob();
        Assertions.assertFalse(job.holdsPossibleLiveSlot());

        job.setPossibleLiveOwned(true);
        Assertions.assertTrue(job.holdsPossibleLiveSlot());

        job.setTerminationProof(LanceIndexTerminationProof.CHILD_REAPED);
        Assertions.assertFalse(job.holdsPossibleLiveSlot());

        job.setTerminationProof(LanceIndexTerminationProof.NONE);
        job.setForceReleased(true);
        Assertions.assertFalse(job.holdsPossibleLiveSlot());

        // A corrupt record missing the proof is treated as NONE (slot still owned).
        LanceIndexJob nullProof = GsonUtils.GSON.fromJson(
                "{\"jid\":1,\"plo\":true,\"tp\":null}", LanceIndexJob.class);
        Assertions.assertNull(nullProof.getTerminationProof());
        Assertions.assertTrue(nullProof.holdsPossibleLiveSlot());
    }

    @Test
    public void terminationProofClearsSlotButKeepsFenceAndOutcome() throws Exception {
        TestManager manager = new TestManager();
        manager.createJob(newCreateJob(1L, "IdxA"), 100, 100, 100);
        manager.markRunning(1L, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID, 9999L);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        Assertions.assertTrue(manager.recordTerminationProof(1L, 1L, BACKEND_ID, BE_EPOCH, INVOCATION_ID,
                LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE));
        LanceIndexJob proven = manager.getJob(1L);
        Assertions.assertFalse(proven.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, proven.getMutationState());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        // The ambiguous result still lands afterwards: UNKNOWN keeps the fence, and the
        // already-recorded proof keeps the slot released.
        Assertions.assertTrue(manager.completeWithResult(1L, 1L, INVOCATION_ID, BE_EPOCH,
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, "ambiguous", false)));
        LanceIndexJob unknown = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, unknown.getMutationState());
        Assertions.assertFalse(unknown.holdsPossibleLiveSlot());
        Assertions.assertTrue(unknown.isUnresolved());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
    }

    @Test
    public void copyConstructorDuplicatesEveryFieldIndependently() {
        LanceIndexJob original = newCreateJob(1L, "IdxA");
        original.setRevision(3L);
        original.setMutationState(LanceIndexJobMutationState.RUNNING);
        original.setRefreshState(LanceIndexJobRefreshState.RUNNING);
        original.setResult(new LanceIndexJobResult(LanceIndexJobResultCode.NATIVE_OK,
                LanceIndexJobCompletionReason.NONE, "ok", false));
        original.setBackendId(BACKEND_ID);
        original.setBeProcessEpoch(BE_EPOCH);
        original.setDispatchRevision(1L);
        original.setInvocationId(INVOCATION_ID);
        original.setDeadlineMs(123L);
        original.setPossibleLiveOwned(true);
        original.setForceActor("admin");
        original.setForceTimeMs(9L);
        original.setForceNote("note");
        original.setForceWarning("warning");

        LanceIndexJob copy = new LanceIndexJob(original);
        Assertions.assertEquals(GsonUtils.GSON.toJson(original), GsonUtils.GSON.toJson(copy));

        copy.setRevision(99L);
        copy.setMutationState(LanceIndexJobMutationState.UNKNOWN);
        copy.setPossibleLiveOwned(false);
        Assertions.assertEquals(3L, original.getRevision());
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, original.getMutationState());
        Assertions.assertTrue(original.isPossibleLiveOwned());
    }

    @Test
    public void admissionConstructorRejectsNullIdentity() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        null, LOCATOR, "IdxA", "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, null, "IdxA", "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR, "IdxA", "idxa",
                        null, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR, null, "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR, "IdxA", null,
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
    }

    @Test
    public void admissionRejectsPseudoCanonicalFenceIdentity() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        "directory", LOCATOR, "IdxA", "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, "S3://bucket/dataset/", "IdxA", "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR, "IdxA", "IdxA",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceIndexJob(1L, "tester", CATALOG_ID, "db1", "tbl1",
                        LanceIndexFenceKey.PROVIDER_DIRECTORY,
                        "https://bucket.example/ds?X-Amz-Signature=secret", "IdxA", "idxa",
                        LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v", null, 7L, null));
    }

    @Test
    public void toStringNamesTheIndexButHidesTheLocator() {
        LanceIndexJob job = newCreateJob(1L, "IdxA");
        Assertions.assertTrue(job.toString().contains("IdxA"));
        Assertions.assertFalse(job.toString().contains("bucket"));
        Assertions.assertFalse(job.toString().contains(LOCATOR));
    }

    private static LanceIndexJob newCreateJob(long jobId, String displayName) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                LanceIndexJobMutationType.CREATE, false, false, "IVF_PQ", "v",
                null, 7L, null);
    }

    private static LanceIndexJob jobInState(LanceIndexJobMutationState mutationState,
            LanceIndexJobRefreshState refreshState, boolean forceReleased) {
        LanceIndexJob job = new LanceIndexJob();
        job.setMutationState(mutationState);
        job.setRefreshState(refreshState);
        job.setForceReleased(forceReleased);
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
