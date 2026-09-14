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

import java.util.Objects;

/**
 * Applies one typed result envelope reported by a backend to the durable job
 * record. This is a thin shim over the manager transitions: dispatch-identity
 * checking and result classification all live in {@link LanceIndexJobManager},
 * so a stale or identity-mismatched report only logs a warning and changes
 * nothing. A malformed envelope (missing result code, a code this FE does not
 * know, or a sanitized message past the durable bound) is dropped rather than
 * trusted; the job then converges through the dispatcher's deadline sweep.
 * Only the typed codes are read: message text is never inspected to infer an
 * outcome.
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
     * Handles one report: a matched report completes the job with its
     * classified result, and a CHILD_REAPED termination proof additionally
     * releases the possible-live slot, because reaping the exact child process
     * proves that process ended (which still says nothing about the outcome).
     */
    public void handle(TLanceIndexJobReport report) {
        if (report == null) {
            LOG.warn("dropping null lance index job report");
            return;
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
        if (report.getTerminationProof() == TLanceIndexTerminationProof.CHILD_REAPED) {
            recordChildReaped(report);
        }
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
