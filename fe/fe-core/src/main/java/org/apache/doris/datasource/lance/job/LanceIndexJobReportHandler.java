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

import org.apache.doris.thrift.TLanceIndexJobReport;
import org.apache.doris.thrift.TLanceIndexJobTerminationReport;
import org.apache.doris.thrift.TLanceIndexTerminationProof;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Applies one typed result envelope reported by a backend to the durable job
 * record. The dispatch identity has two halves, checked at two layers:
 * the journaled identity quad (dispatch revision, invocation id, BE process
 * epoch, backend) is matched inside {@link LanceIndexJobManager} and rejects
 * reports that are merely stale, while the per-dispatch invocation secret is
 * matched here, before the manager is touched at all, and rejects FORGED ones.
 * The second half exists because the FE thrift server cannot authenticate its
 * caller and SHOW LANCE INDEX JOB publishes every other identity field, so a
 * client that can merely reach the port could otherwise assemble a
 * well-matching envelope; only the secret - handed to the selected BE inside
 * the dispatch request and never shown or logged - is unforgeable. An
 * envelope whose secret echo is missing, blank, or wrong, or whose durable
 * record carries no secret (a legacy record from before the field existed),
 * is unauthenticated: the whole envelope is dropped, its termination proof
 * included, because a forged CHILD_REAPED would release the possible-live
 * slot of a worker that may still be live.
 *
 * <p>Beyond authentication this is a thin shim over the manager transitions:
 * result classification lives in {@link LanceIndexJobManager}, so a stale or
 * identity-mismatched report only logs a warning and changes nothing. A
 * malformed envelope (missing result code, a code this FE does not know, or a
 * sanitized message past the durable bound) has its result dropped rather
 * than trusted; the job then converges through the dispatcher's deadline
 * sweep. Only the typed codes are read: message text is never inspected to
 * infer an outcome.
 *
 * <p>A termination proof is validated independently of the result, so a
 * CHILD_REAPED or NEVER_LAUNCHED proof is recorded first and still lands when
 * the result of the same envelope is malformed: the proof states the
 * invocation's worker process ended or never existed, and dropping that proof
 * together with the result would strand the possible-live slot until the
 * backend process is replaced.
 *
 * <p>Invocations that produced no trusted result code at all (kill,
 * wall-clock timeout, OOM, or a panic) report through the separate
 * termination-only channel, {@link #handleTermination}, which carries the
 * invocation identity and one proof value and nothing else.
 *
 * <p>The handler runs on the report RPC thread and performs no I/O beyond the
 * manager's own edit-log write. It starts no refresh: the metadata refresh a
 * completed job may owe is driven by the dispatcher daemon, not here.
 */
public class LanceIndexJobReportHandler {

    private static final Logger LOG = LogManager.getLogger(LanceIndexJobReportHandler.class);

    private final LanceIndexJobManager jobManager;

    public LanceIndexJobReportHandler(LanceIndexJobManager jobManager) {
        this.jobManager = Objects.requireNonNull(jobManager, "jobManager");
    }

    /**
     * Handles one report: authentication comes first and gates everything else,
     * then a matched report completes the job with its classified result, and a
     * CHILD_REAPED or NEVER_LAUNCHED termination proof additionally releases
     * the possible-live slot, because the proof states the invocation's worker
     * process ended or never existed (which still says nothing about the
     * outcome). The proof is recorded before the result is parsed: the two are
     * validated independently, and a malformed result must not take a valid
     * proof down with it.
     */
    public void handle(TLanceIndexJobReport report) {
        if (report == null) {
            LOG.warn("dropping null lance index job report");
            return;
        }
        if (!isAuthenticated(report)) {
            // Deliberately names no secret material, neither the expected nor the
            // presented one: the log only records that the envelope was rejected.
            LOG.warn("dropping unauthenticated lance index job report for job {}: invocation secret mismatch",
                    report.getJobId());
            return;
        }
        LanceIndexTerminationProof proof = toProof(report.getTerminationProof());
        if (proof != null) {
            recordWireProof(report.getJobId(), report.getDispatchRevision(), report.getInvocationId(),
                    report.getBeProcessEpoch(), proof);
        }
        LanceIndexJobResult result;
        try {
            result = toResult(report);
        } catch (IllegalArgumentException e) {
            LOG.warn("dropping malformed lance index job report for job {}: {}", report.getJobId(), e.getMessage());
            return;
        }
        boolean completed = jobManager.completeWithResult(report.getJobId(), report.getDispatchRevision(),
                report.getInvocationId(), report.getBeProcessEpoch(), result);
        if (!completed) {
            LOG.warn("dropping stale lance index job report for job {}", report.getJobId());
        }
    }

    /**
     * Whether the reporter proved it is the BE this dispatch was sent to: the
     * durable record's journaled secret must exist and be echoed by the report.
     * A blank or missing echo, a wrong value, or a durable record with no secret
     * at all (a legacy record replayed from before the field existed) fails
     * closed: such an envelope is unauthenticated and nothing in it may be
     * applied. The comparison is constant-time over the UTF-8 bytes so a forger
     * cannot mine the secret through timing; the secret values themselves never
     * reach a log line.
     */
    private boolean isAuthenticated(TLanceIndexJobReport report) {
        LanceIndexJob job = jobManager.getJob(report.getJobId());
        if (job == null) {
            return false;
        }
        String expected = job.getInvocationSecret();
        String presented = report.getInvocationSecret();
        if (expected == null || expected.isEmpty() || presented == null || presented.isEmpty()) {
            return false;
        }
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                presented.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Handles one termination-only report: an invocation without a trusted result
     * code (kill/timeout/OOM/panic), or the supervisor-side proof of never-launch.
     * A matched proof releases only the possible-live slot — it never changes an
     * UNKNOWN outcome and never releases the fence. A malformed report (missing or
     * unknown proof value) and a stale or identity-mismatched one are dropped with
     * a warning and change nothing.
     */
    public void handleTermination(TLanceIndexJobTerminationReport report) {
        if (report == null) {
            LOG.warn("dropping null lance index job termination report");
            return;
        }
        LanceIndexTerminationProof proof = toProof(report.getProof());
        if (proof == null) {
            LOG.warn("dropping malformed lance index job termination report for job {}: proof {} is not a"
                    + " termination proof this FE accepts", report.getJobId(), report.getProof());
            return;
        }
        recordWireProof(report.getJobId(), report.getDispatchRevision(), report.getInvocationId(),
                report.getBeProcessEpoch(), proof);
    }

    /**
     * Maps a wire proof to the durable enum, keeping only the proofs a backend may
     * send. NONE is the absence of a proof rather than a proof, an unknown wire
     * value deserializes as null, and BE_PROCESS_EPOCH_GONE is FE-derived and never
     * accepted from the wire (NOT_ENQUEUED likewise never travels, because a
     * backend cannot prove its own non-enqueue this way); all four yield null here
     * and the caller drops them.
     */
    private static LanceIndexTerminationProof toProof(TLanceIndexTerminationProof wireProof) {
        if (wireProof == null) {
            return null;
        }
        switch (wireProof) {
            case CHILD_REAPED:
                return LanceIndexTerminationProof.CHILD_REAPED;
            case NEVER_LAUNCHED:
                return LanceIndexTerminationProof.NEVER_LAUNCHED;
            default:
                return null;
        }
    }

    /**
     * Releases the possible-live slot on a wire proof. The report carries the
     * invocation identity but not the backend id, so the durable record is its
     * source; the quad match inside recordTerminationProof still rejects anything
     * stale.
     */
    private void recordWireProof(long jobId, long dispatchRevision, String invocationId, long beProcessEpoch,
            LanceIndexTerminationProof proof) {
        LanceIndexJob job = jobManager.getJob(jobId);
        if (job == null || job.getBackendId() == null) {
            LOG.warn("dropping {} proof of lance index job {}: no durable dispatch identity", proof, jobId);
            return;
        }
        boolean recorded = jobManager.recordTerminationProof(jobId, dispatchRevision,
                job.getBackendId(), beProcessEpoch, invocationId, proof);
        if (!recorded) {
            LOG.warn("dropping stale {} proof of lance index job {}", proof, jobId);
        }
    }

    /**
     * Converts the wire envelope to the durable result value, rejecting
     * anything that cannot be represented: a missing result code, a code this
     * FE does not know, or a sanitized message past the durable bound.
     * NO_TRUSTED_RESULT is FE-side only and absent from the wire enum, so it
     * can never arrive here.
     */
    private static LanceIndexJobResult toResult(TLanceIndexJobReport report) {
        if (report.getResultCode() == null) {
            throw new IllegalArgumentException("report carries no result code");
        }
        LanceIndexJobResultCode resultCode;
        try {
            resultCode = LanceIndexJobResultCode.valueOf(report.getResultCode().name());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("report carries an unknown result code " + report.getResultCode());
        }
        LanceIndexJobCompletionReason completionReason = LanceIndexJobCompletionReason.NONE;
        if (report.isSetCompletionReason() && report.getCompletionReason() != null) {
            completionReason = LanceIndexJobCompletionReason.valueOf(report.getCompletionReason().name());
        }
        boolean externalMetadataAdvanced =
                report.isSetExternalMetadataAdvanced() && report.isExternalMetadataAdvanced();
        // Throws IllegalArgumentException when the message is past the durable bound.
        return new LanceIndexJobResult(resultCode, completionReason,
                report.getSanitizedMessage(), externalMetadataAdvanced);
    }
}
