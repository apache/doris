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
 * CHILD_REAPED proof is recorded first and still lands when the result of the
 * same envelope is malformed: reaping the exact child process proves that
 * process ended, and dropping that proof together with the result would
 * strand the possible-live slot until the backend process is replaced.
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
     * CHILD_REAPED termination proof additionally releases the possible-live
     * slot, because reaping the exact child process proves that process ended
     * (which still says nothing about the outcome). The proof is recorded
     * before the result is parsed: the two are validated independently, and a
     * malformed result must not take a valid proof down with it.
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
        if (report.getTerminationProof() == TLanceIndexTerminationProof.CHILD_REAPED) {
            recordChildReaped(report);
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
     * Releases the possible-live slot on a CHILD_REAPED proof. The report
     * carries the invocation identity but not the backend id, so the durable
     * record is its source; the quad match inside recordTerminationProof still
     * rejects anything stale.
     */
    private void recordChildReaped(TLanceIndexJobReport report) {
        LanceIndexJob job = jobManager.getJob(report.getJobId());
        if (job == null || job.getBackendId() == null) {
            LOG.warn("dropping CHILD_REAPED proof of lance index job {}: no durable dispatch identity",
                    report.getJobId());
            return;
        }
        boolean recorded = jobManager.recordTerminationProof(report.getJobId(), report.getDispatchRevision(),
                job.getBackendId(), report.getBeProcessEpoch(), report.getInvocationId(),
                LanceIndexTerminationProof.CHILD_REAPED);
        if (!recorded) {
            LOG.warn("dropping stale CHILD_REAPED proof of lance index job {}", report.getJobId());
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
