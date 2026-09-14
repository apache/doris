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
import org.apache.doris.thrift.TLanceIndexCompletionReason;
import org.apache.doris.thrift.TLanceIndexJobReport;
import org.apache.doris.thrift.TLanceIndexJobResultCode;
import org.apache.doris.thrift.TLanceIndexTerminationProof;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * Coverage for {@link LanceIndexJobReportHandler}, the thin shim applying one typed
 * result envelope to the durable job record. The pinned invariants: every wire result
 * code lands the classified (mutationState, refreshState, completionReason) triple of
 * the classification table; a stale report (wrong dispatch revision, invocation id, BE
 * process epoch, or an already-terminal job) only warns and changes nothing; a
 * CHILD_REAPED proof releases exactly the possible-live slot (never the outcome, never
 * the fence); and a malformed envelope (missing or unknown result code, sanitized
 * message past the durable bound) is dropped whole so the dispatcher's deadline sweep
 * converges the job. Message text is stored verbatim and never inspected to infer an
 * outcome, and NO_TRUSTED_RESULT never arrives on the wire.
 */
public class LanceIndexJobReportHandlerTest {
    private static final long CATALOG_ID = 10L;
    private static final String LOCATOR = "s3://bucket/dataset";
    private static final long BACKEND_ID = 1001L;
    private static final long BE_EPOCH = 55L;
    private static final String INVOCATION_ID = "invocation-1";
    private static final long NOT_EXPIRED_DEADLINE_MS = Long.MAX_VALUE;

    // ------------------------------------------------------------------
    // result_code x mutation_type matrix
    // ------------------------------------------------------------------

    @Test
    public void everyWireCodeLandsTheClassifiedTripleForEveryMutationType() throws DdlException {
        for (TLanceIndexJobResultCode wireCode : TLanceIndexJobResultCode.values()) {
            for (LanceIndexJobMutationType type : LanceIndexJobMutationType.values()) {
                for (boolean ifExists : new boolean[]{false, true}) {
                    for (boolean advanced : new boolean[]{false, true}) {
                        assertOneLandedTriple(wireCode, type, ifExists, advanced);
                    }
                }
            }
        }
    }

    private void assertOneLandedTriple(TLanceIndexJobResultCode wireCode, LanceIndexJobMutationType type,
            boolean ifExists, boolean advanced) throws DdlException {
        TestManager manager = runningManager(1L, "Idx" + wireCode + type + ifExists + advanced, type, ifExists);
        LanceIndexJobReportHandler handler = new LanceIndexJobReportHandler(manager);
        handler.handle(matchingReport(wireCode)
                .setExternalMetadataAdvanced(advanced));

        LanceIndexJob stored = manager.getJob(1L);
        String context = "code=" + wireCode + ", type=" + type + ", ifExists=" + ifExists
                + ", advanced=" + advanced;
        // The FE enum mirrors the wire enum name for name; the landed triple must be exactly
        // the classification of that code (the exhaustive restatement lives in
        // LanceIndexJobResultClassifyTest, so here the oracle is the classifier itself and
        // the pinned families below restate the load-bearing rows with literals).
        LanceIndexJobResultCode feCode = LanceIndexJobResultCode.valueOf(wireCode.name());
        LanceIndexJobResultCode.Classification expected =
                LanceIndexJobResultCode.classify(type, feCode, ifExists, advanced);
        Assertions.assertEquals(expected.getMutationState(), stored.getMutationState(), context);
        Assertions.assertEquals(expected.getRefreshState(), stored.getRefreshState(), context);
        Assertions.assertEquals(expected.getCompletionReason(), stored.getResult().getCompletionReason(), context);
        Assertions.assertEquals(feCode, stored.getResult().getResultCode(), context);
        Assertions.assertEquals(advanced, stored.getResult().isExternalMetadataAdvanced(), context);
        // One completion journal record, exactly one.
        Assertions.assertEquals(1, manager.editLog.size(), context);
    }

    @Test
    public void preInvocationRejectionsAreNotCommittedAndOweRefreshOnlyOnAdvancement() throws DdlException {
        for (TLanceIndexJobResultCode code : new TLanceIndexJobResultCode[]{
                TLanceIndexJobResultCode.PRE_INVOCATION_STALE_ADMISSION,
                TLanceIndexJobResultCode.PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
                TLanceIndexJobResultCode.PRE_INVOCATION_CREDENTIAL_EXPIRED,
                TLanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED}) {
            TestManager quiet = runningManager(1L, "IdxQuiet", LanceIndexJobMutationType.CREATE, false);
            new LanceIndexJobReportHandler(quiet).handle(matchingReport(code));
            LanceIndexJob stored = quiet.getJob(1L);
            Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState(),
                    "code=" + code);
            Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState(),
                    "code=" + code);

            TestManager advanced = runningManager(1L, "IdxAdvanced", LanceIndexJobMutationType.CREATE, false);
            new LanceIndexJobReportHandler(advanced).handle(matchingReport(code)
                    .setExternalMetadataAdvanced(true));
            Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED,
                    advanced.getJob(1L).getMutationState(), "code=" + code);
            Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED,
                    advanced.getJob(1L).getRefreshState(), "code=" + code);
        }
    }

    @Test
    public void nativeOkCommitsAndOwesRefresh() throws DdlException {
        TestManager manager = runningManager(1L, "IdxOk", LanceIndexJobMutationType.CREATE, false);
        new LanceIndexJobReportHandler(manager).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setSanitizedMessage("built"));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_OK, stored.getResult().getResultCode());
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, stored.getResult().getCompletionReason());
    }

    @Test
    public void commitConflictIsNotCommittedButOwesRefresh() throws DdlException {
        TestManager manager = runningManager(1L, "IdxConflict", LanceIndexJobMutationType.REPLACE, false);
        new LanceIndexJobReportHandler(manager).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_COMMIT_CONFLICT));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, stored.getRefreshState());
    }

    @Test
    public void notFoundAttributionDependsOnMutationTypeAndIfExists() throws DdlException {
        for (LanceIndexJobMutationType type : new LanceIndexJobMutationType[]{
                LanceIndexJobMutationType.CREATE, LanceIndexJobMutationType.REPLACE}) {
            TestManager manager = runningManager(1L, "Idx" + type, type, false);
            new LanceIndexJobReportHandler(manager).handle(
                    matchingReport(TLanceIndexJobResultCode.NATIVE_NOT_FOUND));
            LanceIndexJob stored = manager.getJob(1L);
            Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState(),
                    "type=" + type);
            Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState(),
                    "type=" + type);
        }

        TestManager plainDrop = runningManager(1L, "IdxDrop", LanceIndexJobMutationType.DROP, false);
        new LanceIndexJobReportHandler(plainDrop).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_NOT_FOUND));
        LanceIndexJob dropped = plainDrop.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, dropped.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, dropped.getRefreshState());
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, dropped.getResult().getCompletionReason());

        TestManager ifExistsDrop = runningManager(1L, "IdxDropIf", LanceIndexJobMutationType.DROP, true);
        new LanceIndexJobReportHandler(ifExistsDrop).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_NOT_FOUND)
                        .setCompletionReason(TLanceIndexCompletionReason.IF_CONDITION_NOOP));
        LanceIndexJob noop = ifExistsDrop.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.NOT_COMMITTED, noop.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, noop.getRefreshState());
        Assertions.assertEquals(LanceIndexJobCompletionReason.IF_CONDITION_NOOP,
                noop.getResult().getCompletionReason());
    }

    @Test
    public void invalidArgumentIsUnknownWithoutRefresh() throws DdlException {
        TestManager manager = runningManager(1L, "IdxBadArg", LanceIndexJobMutationType.CREATE, false);
        new LanceIndexJobReportHandler(manager).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.NOT_REQUIRED, stored.getRefreshState());
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT,
                stored.getResult().getResultCode());
    }

    @Test
    public void noTrustedResultNeverArrivesOnTheWire() {
        // NO_TRUSTED_RESULT is FE-side only (deadline sweep, send failure, master-transfer
        // sweep); the wire enum must not carry it, or a worker could report it and bypass
        // the ambiguity rule.
        Assertions.assertEquals(12, TLanceIndexJobResultCode.values().length);
        for (TLanceIndexJobResultCode code : TLanceIndexJobResultCode.values()) {
            Assertions.assertNotEquals("NO_TRUSTED_RESULT", code.name());
        }
        // The FE enum is exactly the wire enum plus that one FE-only code, so every wire
        // name resolves through the name-based mapping in the handler.
        Assertions.assertEquals(13, LanceIndexJobResultCode.values().length);
        for (TLanceIndexJobResultCode code : TLanceIndexJobResultCode.values()) {
            Assertions.assertEquals(code.name(), LanceIndexJobResultCode.valueOf(code.name()).name());
        }
    }

    // ------------------------------------------------------------------
    // stale reports
    // ------------------------------------------------------------------

    @Test
    public void staleReportsOnlyWarnAndChangeNothing() throws DdlException {
        TestManager manager = runningManager(1L, "IdxA", LanceIndexJobMutationType.CREATE, false);
        LanceIndexJobReportHandler handler = new LanceIndexJobReportHandler(manager);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        // Wrong dispatch revision, wrong invocation id, wrong BE process epoch, unknown job.
        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setDispatchRevision(0L));
        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setInvocationId("invocation-x"));
        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setBeProcessEpoch(BE_EPOCH + 1));
        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setJobId(404L));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals(1L, stored.getRevision());
        Assertions.assertNull(stored.getResult());
        Assertions.assertTrue(manager.editLog.isEmpty());
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());

        // A matched report still completes afterwards; a duplicate then arrives for a job
        // that is already terminal and is dropped the same way.
        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK));
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, manager.getJob(1L).getMutationState());
        handler.handle(matchingReport(TLanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED));
        LanceIndexJob terminal = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, terminal.getMutationState());
        Assertions.assertEquals(LanceIndexJobRefreshState.REQUIRED, terminal.getRefreshState());
        Assertions.assertEquals(2L, terminal.getRevision());
        Assertions.assertEquals(1, manager.editLog.size());
    }

    // ------------------------------------------------------------------
    // CHILD_REAPED termination proof
    // ------------------------------------------------------------------

    @Test
    public void childReapedProofReleasesOnlyThePossibleLiveSlot() throws DdlException {
        TestManager manager = runningManager(1L, "IdxA", LanceIndexJobMutationType.CREATE, false);
        LanceIndexJobReportHandler handler = new LanceIndexJobReportHandler(manager);
        LanceIndexFenceKey fenceKey = manager.getJob(1L).fenceKey();

        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT)
                .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED));

        LanceIndexJob stored = manager.getJob(1L);
        // The proof says the child process ended, which still says nothing about the outcome.
        Assertions.assertEquals(LanceIndexJobMutationState.UNKNOWN, stored.getMutationState());
        Assertions.assertEquals(LanceIndexTerminationProof.CHILD_REAPED, stored.getTerminationProof());
        Assertions.assertFalse(stored.holdsPossibleLiveSlot());
        // Only the slot was released: fence and quota survive until FORCE.
        Assertions.assertTrue(manager.isFenceHeld(fenceKey));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
        Assertions.assertEquals(2, manager.editLog.size());
    }

    @Test
    public void childReapedProofWithMismatchedIdentityReleasesNothing() throws DdlException {
        TestManager manager = runningManager(1L, "IdxA", LanceIndexJobMutationType.CREATE, false);
        LanceIndexJobReportHandler handler = new LanceIndexJobReportHandler(manager);

        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT)
                .setInvocationId("invocation-x")
                .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED));

        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
        Assertions.assertEquals(LanceIndexTerminationProof.NONE, stored.getTerminationProof());
        Assertions.assertTrue(manager.editLog.isEmpty());
    }

    @Test
    public void childReapedProofForUnknownJobChangesNothing() {
        TestManager manager = new TestManager();
        LanceIndexJobReportHandler handler = new LanceIndexJobReportHandler(manager);

        handler.handle(matchingReport(TLanceIndexJobResultCode.NATIVE_INVALID_ARGUMENT)
                .setJobId(404L)
                .setTerminationProof(TLanceIndexTerminationProof.CHILD_REAPED));

        Assertions.assertEquals(0, manager.getJobCount());
        Assertions.assertTrue(manager.editLog.isEmpty());
    }

    // ------------------------------------------------------------------
    // malformed envelopes
    // ------------------------------------------------------------------

    @Test
    public void malformedEnvelopesAreDroppedForTheDeadlineSweep() throws DdlException {
        TestManager missingCode = runningManager(1L, "IdxMissing", LanceIndexJobMutationType.CREATE, false);
        TLanceIndexJobReport withoutCode = matchingReport(TLanceIndexJobResultCode.NATIVE_OK);
        withoutCode.unsetResultCode();
        new LanceIndexJobReportHandler(missingCode).handle(withoutCode);
        assertManagerUnchangedByDroppedEnvelope(missingCode);

        TestManager overlong = runningManager(1L, "IdxOverlong", LanceIndexJobMutationType.CREATE, false);
        StringBuilder over = new StringBuilder();
        for (int i = 0; i < LanceIndexJobResult.MAX_MESSAGE_BYTES + 1; i++) {
            over.append('x');
        }
        new LanceIndexJobReportHandler(overlong).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_OK).setSanitizedMessage(over.toString()));
        assertManagerUnchangedByDroppedEnvelope(overlong);

        // A later valid envelope for the same job still completes it: the drop was not a
        // terminal transition, and the deadline sweep is only the fallback.
        new LanceIndexJobReportHandler(overlong).handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK));
        Assertions.assertEquals(LanceIndexJobMutationState.COMMITTED, overlong.getJob(1L).getMutationState());
    }

    @Test
    public void unknownWireResultCodeIsDroppedLikeAnyMalformedEnvelope() throws DdlException {
        TestManager manager = runningManager(1L, "IdxUnknownCode", LanceIndexJobMutationType.CREATE, false);
        // A code this FE does not know can only exist as enum skew on the wire; model it by
        // a wire-code instance whose name resolves to nothing on the FE side.
        TLanceIndexJobResultCode skew = Mockito.mock(TLanceIndexJobResultCode.class);
        Mockito.when(skew.name()).thenReturn("SOMETHING_ONLY_THE_BE_KNOWS");
        Mockito.when(skew.getValue()).thenReturn(9999);

        new LanceIndexJobReportHandler(manager).handle(matchingReport(skew));

        assertManagerUnchangedByDroppedEnvelope(manager);
    }

    @Test
    public void nullReportIsDroppedWithoutThrowing() {
        TestManager manager = new TestManager();
        new LanceIndexJobReportHandler(manager).handle(null);
        Assertions.assertEquals(0, manager.getJobCount());
        Assertions.assertTrue(manager.editLog.isEmpty());
    }

    // ------------------------------------------------------------------
    // toResult passthrough
    // ------------------------------------------------------------------

    @Test
    public void messageAndOptionalFlagsPassThroughUntouched() throws DdlException {
        TestManager manager = runningManager(1L, "IdxPassthrough", LanceIndexJobMutationType.DROP, true);
        new LanceIndexJobReportHandler(manager).handle(
                matchingReport(TLanceIndexJobResultCode.NATIVE_NOT_FOUND)
                        .setCompletionReason(TLanceIndexCompletionReason.IF_CONDITION_NOOP)
                        .setSanitizedMessage("index absent on the provider")
                        .setExternalMetadataAdvanced(false));

        LanceIndexJobResult result = manager.getJob(1L).getResult();
        Assertions.assertEquals(LanceIndexJobResultCode.NATIVE_NOT_FOUND, result.getResultCode());
        Assertions.assertEquals(LanceIndexJobCompletionReason.IF_CONDITION_NOOP, result.getCompletionReason());
        Assertions.assertEquals("index absent on the provider", result.getSanitizedMessage());
        Assertions.assertFalse(result.isExternalMetadataAdvanced());
    }

    @Test
    public void unsetOptionalFlagsDefaultToNoneAndFalse() throws DdlException {
        TestManager manager = runningManager(1L, "IdxDefaults", LanceIndexJobMutationType.CREATE, false);
        new LanceIndexJobReportHandler(manager).handle(matchingReport(TLanceIndexJobResultCode.NATIVE_OK));

        LanceIndexJobResult result = manager.getJob(1L).getResult();
        Assertions.assertEquals(LanceIndexJobCompletionReason.NONE, result.getCompletionReason());
        Assertions.assertFalse(result.isExternalMetadataAdvanced());
        Assertions.assertNull(result.getSanitizedMessage());
    }

    // ------------------------------------------------------------------
    // fixtures
    // ------------------------------------------------------------------

    private static TLanceIndexJobReport matchingReport(TLanceIndexJobResultCode resultCode) {
        return new TLanceIndexJobReport()
                .setJobId(1L)
                .setDispatchRevision(1L)
                .setInvocationId(INVOCATION_ID)
                .setBeProcessEpoch(BE_EPOCH)
                .setResultCode(resultCode);
    }

    private static void assertManagerUnchangedByDroppedEnvelope(TestManager manager) {
        LanceIndexJob stored = manager.getJob(1L);
        Assertions.assertNotNull(stored, "the dropped envelope must not delete the job");
        Assertions.assertEquals(LanceIndexJobMutationState.RUNNING, stored.getMutationState());
        Assertions.assertEquals(1L, stored.getRevision());
        Assertions.assertNull(stored.getResult());
        Assertions.assertTrue(stored.holdsPossibleLiveSlot());
        // Zero change anywhere in the manager: no journal record, fence and quota intact.
        Assertions.assertTrue(manager.editLog.isEmpty());
        Assertions.assertTrue(manager.isFenceHeld(stored.fenceKey()));
        Assertions.assertEquals(1L, manager.getQuota().getGlobalCount());
    }

    /**
     * A manager holding exactly one durable RUNNING job for the given mutation type,
     * dispatched with the standard test identity (backend {@link #BACKEND_ID}, epoch
     * {@link #BE_EPOCH}, invocation {@link #INVOCATION_ID}, dispatch revision 1). The
     * setup journal records are cleared so tests can assert on post-setup writes alone.
     */
    private static TestManager runningManager(long jobId, String displayName,
            LanceIndexJobMutationType type, boolean ifExists) throws DdlException {
        TestManager manager = new TestManager();
        manager.createJob(newJob(jobId, displayName, type, ifExists), 100, 100, 100);
        Assertions.assertTrue(manager.markRunning(jobId, 0L, BACKEND_ID, BE_EPOCH, INVOCATION_ID,
                NOT_EXPIRED_DEADLINE_MS));
        manager.editLog.clear();
        return manager;
    }

    private static LanceIndexJob newJob(long jobId, String displayName,
            LanceIndexJobMutationType type, boolean ifExists) {
        return new LanceIndexJob(jobId, "tester", CATALOG_ID, "db1", "tbl1",
                LanceIndexFenceKey.PROVIDER_DIRECTORY, LOCATOR,
                displayName, LanceIndexNameNormalizer.normalize(displayName),
                type, false, ifExists, "IVF_PQ", "v", null, 7L, null);
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
