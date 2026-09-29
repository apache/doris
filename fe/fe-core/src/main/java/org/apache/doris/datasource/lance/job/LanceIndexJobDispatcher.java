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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.storage.LanceStorageOptions;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLanceIndexJobDispatch;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TApplicationException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Master-only daemon that drives the durable Lance index job records through
 * the lifecycle after admission. Each round runs in a fixed order: converge
 * expired RUNNING jobs to UNKNOWN, release possible-live slots whose backend
 * process was replaced, drive the refresh a terminal job still owes, then
 * dispatch PENDING jobs. Every durable transition goes through
 * {@link LanceIndexJobManager} under its own lock; the daemon holds no catalog
 * or manager lock across any call.
 *
 * <p>The daemon does not read the admission gate: a job that is already durable
 * must be driven to its terminal state, whatever the gate says now, so the
 * thread runs unconditionally on the master and simply finds nothing to do
 * while no jobs exist. An idle round writes no journal record.
 *
 * <p>Dispatch follows the durable-before-send boundary: the whole request is
 * prepared first (so a preparation failure just leaves the job PENDING), then
 * the markRunning edit log is written and re-read before the first byte of
 * network I/O, and the invocation id of an attempt that lost the compare-and-set
 * is never reused. After a successful markRunning there is exactly one send;
 * from that point a job converges only through a matching result callback, the
 * deadline sweep, or the epoch sweep, never through a resend. A failure that
 * still proves the dispatch was never enqueued (a clean pre-enqueue error
 * status, a client-pool borrow failure, or an UNKNOWN_METHOD answer from an
 * old backend) converges it NOT_COMMITTED through the no-enqueue channel,
 * which releases the possible-live slot in the same durable transition;
 * anything ambiguous after the invocation may have started converges UNKNOWN
 * with the slot retained.
 *
 * <p>The manager is resolved from the supplier once per round rather than
 * captured at construction: {@code Env.loadLanceIndexJobManager} replaces the
 * Env-owned manager with a brand-new object on every image load, so a cached
 * reference would keep scanning the abandoned pre-image manager after an FE
 * restart while replay, admission and SHOW all move on to the restored one.
 * Every phase of one round shares the single resolved instance.
 */
public class LanceIndexJobDispatcher extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobDispatcher.class);

    private final Supplier<LanceIndexJobManager> jobManagerSupplier;

    public LanceIndexJobDispatcher(LanceIndexJobManager jobManager) {
        this(() -> jobManager);
    }

    public LanceIndexJobDispatcher(Supplier<LanceIndexJobManager> jobManagerSupplier) {
        super("lance index job dispatcher", dispatchIntervalMs());
        this.jobManagerSupplier = jobManagerSupplier;
    }

    /**
     * Values loaded from fe.conf bypass the config validator (only ADMIN SET runs
     * it), so the positive invariant is re-asserted where a non-positive value
     * would break the loop: a non-positive interval would kill this thread inside
     * {@code Thread.sleep} or busy-spin it, a non-positive deadline would sweep
     * every dispatched job UNKNOWN on the next round, and a zero cap would stall
     * dispatch forever. The refresh retry interval needs no such defense: a
     * non-positive value simply disengages the throttle.
     */
    private static long dispatchIntervalMs() {
        return Math.max(1, Config.lance_index_job_dispatch_interval_second) * 1000L;
    }

    private static long executeDeadlineMs(long nowMs) {
        long second = Math.max(1L, Config.lance_index_job_execute_deadline_second);
        return second > (Long.MAX_VALUE - nowMs) / 1000L ? Long.MAX_VALUE : nowMs + second * 1000L;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        setInterval(dispatchIntervalMs());
        try {
            runOneRound(jobManagerSupplier.get());
        } catch (Throwable t) {
            LOG.warn("Failed to process one round of the lance index job dispatcher", t);
        }
    }

    private void runOneRound(LanceIndexJobManager jobManager) {
        long nowMs = System.currentTimeMillis();
        sweepExpiredRunningJobs(jobManager, nowMs);
        sweepReplacedProcessEpochs(jobManager);
        driveRequiredRefreshes(jobManager, nowMs);
        dispatchPendingJobs(jobManager);
    }

    /**
     * Deadline sweep. A RUNNING job past its wait deadline has produced no
     * complete trusted result, so it converges to UNKNOWN through the same
     * completeWithResult channel a callback would use. Expiry bounds the wait
     * only: it never proves termination, so the possible-live slot, the
     * same-name fence, and the unresolved quota all stay held.
     */
    private void sweepExpiredRunningJobs(LanceIndexJobManager jobManager, long nowMs) {
        for (LanceIndexJob job : jobManager.getExpiredRunningJobs(nowMs)) {
            try {
                boolean completed = jobManager.completeWithResult(job.getJobId(),
                        dispatchRevisionOf(job), job.getInvocationId(), job.getBeProcessEpoch(),
                        new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                                LanceIndexJobCompletionReason.NONE,
                                "execute deadline expired without a complete trusted result", false));
                if (completed) {
                    LOG.info("lance index job {} converged RUNNING -> UNKNOWN on deadline expiry",
                            job.getJobId());
                } else {
                    LOG.warn("deadline sweep skipped lance index job {}: already converged by a callback or sweep",
                            job.getJobId());
                }
            } catch (Throwable t) {
                LOG.warn("failed to sweep expired lance index job " + job.getJobId(), t);
            }
        }
    }

    /**
     * Possible-live sweep. The only slot-release proof this daemon produces is
     * that the recorded backend process epoch no longer exists: a backend entry
     * reporting a different epoch proves the process that received the dispatch
     * was replaced. A missing backend entry or heartbeat loss proves nothing
     * (the worker may still be running behind a partition), so such a job keeps
     * its slot until a stronger proof or an operator force release. An epoch
     * change also proves nothing about the outcome, so the mutation state is
     * never touched here.
     */
    private void sweepReplacedProcessEpochs(LanceIndexJobManager jobManager) {
        for (LanceIndexJob job : jobManager.getJobsHoldingPossibleLiveSlot()) {
            try {
                Backend backend = Env.getCurrentSystemInfo().getBackend(job.getBackendId());
                if (backend == null || backend.getProcessEpoch() == job.getBeProcessEpoch()) {
                    continue;
                }
                boolean recorded = jobManager.recordTerminationProof(job.getJobId(),
                        dispatchRevisionOf(job), job.getBackendId(), job.getBeProcessEpoch(),
                        job.getInvocationId(), LanceIndexTerminationProof.BE_PROCESS_EPOCH_GONE);
                if (recorded) {
                    LOG.info("released possible-live slot of lance index job {}: backend process epoch was replaced",
                            job.getJobId());
                } else {
                    LOG.warn("epoch sweep skipped lance index job {}: dispatch identity already moved",
                            job.getJobId());
                }
            } catch (Throwable t) {
                LOG.warn("failed to sweep possible-live slot of lance index job " + job.getJobId(), t);
            }
        }
    }

    /**
     * Refresh driver for terminal jobs with an unfinished refresh obligation.
     * Completing the refresh is the protocol duty that releases the same-name
     * fence and the unresolved quota once DONE; it is not a read-visibility
     * action, because index metadata is never cached. Each job is driven
     * through markRefreshRunning, the idempotent external-table refresh, then
     * DONE or FAILED: a FAILED job keeps its fence and is retried, throttled to
     * one attempt per retry interval, while a first REQUIRED refresh is never
     * delayed. UNKNOWN jobs never appear here; they owe no refresh.
     */
    private void driveRequiredRefreshes(LanceIndexJobManager jobManager, long nowMs) {
        for (LanceIndexJob job : jobManager.getJobsNeedingRefresh()) {
            try {
                if (job.getRefreshState() == LanceIndexJobRefreshState.RUNNING) {
                    // In flight elsewhere; the master-transfer sweep downgrades a stale
                    // RUNNING back to REQUIRED, so a lost driver cannot strand it.
                    continue;
                }
                if (job.getRefreshState() == LanceIndexJobRefreshState.FAILED
                        && nowMs - job.getUpdateTimeMs()
                                < Config.lance_index_job_refresh_retry_second * 1000L) {
                    continue;
                }
                if (!jobManager.markRefreshRunning(job.getJobId(), job.getRevision())) {
                    // A concurrent driver won the compare-and-set; nothing to do here.
                    continue;
                }
                driveOneRefresh(jobManager, job);
            } catch (Throwable t) {
                LOG.warn("failed to drive the refresh of lance index job " + job.getJobId(), t);
            }
        }
    }

    private void driveOneRefresh(LanceIndexJobManager jobManager, LanceIndexJob job) {
        long refreshRevision = job.getRevision() + 1;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(job.getCatalogId());
        if (catalog == null) {
            // Unreachable while the unresolved-job guard blocks catalog drops; kept as a
            // fail-closed fallback so the job still transitions and retries later.
            LOG.warn("catalog of lance index job {} is gone; marking its refresh FAILED", job.getJobId());
            finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, false);
            return;
        }
        try {
            // A half-orphan target (its db or table already dropped externally) is a
            // silent no-op: nothing is left to invalidate, and DONE is the correct end
            // state for the job.
            Env.getCurrentEnv().getRefreshManager().handleRefreshTable(catalog.getName(),
                    job.getDbName(), job.getTableName(), true);
        } catch (Throwable t) {
            // The typed DdlException is the expected failure; an unchecked exception out
            // of the metadata path must still leave the durable refresh state, or the
            // job would strand in refresh RUNNING until the next master transfer.
            LOG.warn("refresh of lance index job {} failed; keeping the fence for a retry",
                    job.getJobId(), t);
            finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, false);
            return;
        }
        finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, true);
    }

    /**
     * Applies the DONE/FAILED transition with a bounded revision retry. A concurrent
     * termination-proof write can bump the revision after markRefreshRunning succeeded,
     * and silently losing that compare-and-set would leave the refresh RUNNING — a
     * state only the master-transfer sweep downgrades. Re-reading the revision and
     * retrying a few times converges it; a persistent loss is escalated.
     */
    private void finishRefreshTransition(LanceIndexJobManager jobManager, long jobId, long expectedRevision,
            boolean done) {
        long revision = expectedRevision;
        for (int attempt = 0; attempt < 3; attempt++) {
            boolean transitioned = done ? jobManager.markRefreshDone(jobId, revision)
                    : jobManager.markRefreshFailed(jobId, revision);
            if (transitioned) {
                return;
            }
            LanceIndexJob fresh = jobManager.getJob(jobId);
            if (fresh == null) {
                break;
            }
            revision = fresh.getRevision();
        }
        LOG.error("lance index job {} kept its refresh RUNNING: the DONE/FAILED transition kept losing the"
                + " compare-and-set; the master-transfer sweep will downgrade it", jobId);
    }

    /**
     * PENDING dispatch. Makes at most
     * {@link Config#lance_index_job_max_dispatch_per_round} fresh dispatches per
     * round, and only a job this round actually made RUNNING consumes that
     * budget: skipped jobs (an eligibility gate is closed, or every backend is
     * at capacity) are scanned past, so a stable subset of permanently
     * undispatchable jobs can never crowd out later ids. Per backend it never
     * exceeds {@link Config#lance_index_job_max_inflight_per_backend}
     * possible-live worker slots, counted from slot ownership (see
     * {@link LanceIndexJobManager#countPossibleLiveSlotsByBackend()}) plus the
     * jobs this round already made RUNNING. A job that cannot be dispatched
     * keeps waiting as PENDING: there is no dispatch-exhaustion terminal state
     * and no backoff beyond the daemon period.
     */
    private void dispatchPendingJobs(LanceIndexJobManager jobManager) {
        int maxPerRound = Math.max(1, Config.lance_index_job_max_dispatch_per_round);
        Map<Long, Integer> inflightByBackend = jobManager.countPossibleLiveSlotsByBackend();
        int dispatched = 0;
        for (LanceIndexJob job : jobManager.getJobsNeedingDispatch()) {
            if (dispatched >= maxPerRound) {
                break;
            }
            try {
                if (tryDispatch(jobManager, job, inflightByBackend)) {
                    dispatched++;
                }
            } catch (Throwable t) {
                LOG.warn("failed to dispatch lance index job " + job.getJobId(), t);
            }
        }
    }

    /**
     * One dispatch attempt for one PENDING job; returns true only when the
     * attempt made the job durable RUNNING (and so consumes this round's
     * dispatch budget). Every early return before markRunning leaves the job
     * PENDING for a later round: the eligibility gates, the backend and
     * capacity checks, and also the whole request preparation — storage-option
     * resolution and the wire request build run before the durable boundary,
     * so an FE-side failure there (for example a catalog id that resolves to
     * nothing while ALTER CATALOG RENAME has the catalog temporarily removed)
     * just retries next round instead of stranding the job UNKNOWN without a
     * single byte sent. Once markRunning succeeds the job is durable RUNNING
     * and this invocation id gets exactly one send attempt; after that only a
     * matching callback, the deadline sweep, or the epoch sweep can converge
     * the job.
     */
    private boolean tryDispatch(LanceIndexJobManager jobManager, LanceIndexJob job,
            Map<Long, Integer> inflightByBackend) {
        boolean localDataset = isLocalFileDataset(job.getNormalizedLocator());
        if (localDataset && !Config.enable_lance_index_local_file_mutation) {
            // Operator assertion is off: a local-filesystem mutation stays PENDING.
            return false;
        }
        if (localDataset && Env.getCurrentEnv().getFrontends(null).size() != 1) {
            // Local files are only shared by a single-node deployment.
            return false;
        }
        SystemInfoService systemInfo = Env.getCurrentSystemInfo();
        // All schedule-available backends, shuffled by the selection policy: the
        // first one with a free possible-live slot takes the job, so a full
        // backend defers this attempt only when every selectable backend is at
        // the cap, never just because the randomly picked one is.
        List<Long> backendIds = systemInfo.selectBackendIdsByPolicy(
                new BeSelectionPolicy.Builder().needScheduleAvailable().build(), -1);
        int perBackendCap = Math.max(1, Config.lance_index_job_max_inflight_per_backend);
        Backend backend = null;
        for (Long backendId : backendIds) {
            Backend candidate = systemInfo.getBackend(backendId);
            if (candidate == null) {
                continue;
            }
            if (localDataset && !isOnlyAliveBackend(systemInfo, candidate.getId())) {
                continue;
            }
            Integer inflight = inflightByBackend.get(candidate.getId());
            if (inflight != null && inflight >= perBackendCap) {
                continue;
            }
            backend = candidate;
            break;
        }
        if (backend == null) {
            return false;
        }
        String invocationId = UUID.randomUUID().toString();
        // The process epoch is captured once, and the same value goes to the
        // durable record and the wire: a heartbeat landing between the two reads
        // must not split the dispatch identity (the callback matches the durable
        // value, and the epoch sweep releases the slot against it).
        long beProcessEpoch = backend.getProcessEpoch();
        long deadlineMs = executeDeadlineMs(System.currentTimeMillis());
        long expectedDispatchRevision = job.getRevision() + 1;
        TLanceIndexJobDispatch dispatch;
        try {
            dispatch = buildDispatch(job, expectedDispatchRevision, invocationId, deadlineMs, beProcessEpoch,
                    resolveStorageOptions(job));
        } catch (Exception e) {
            // Not a trusted worker rejection and not an ambiguity either: nothing was
            // marked and nothing was sent, so the job simply waits for the next round.
            LOG.warn("failed to prepare the dispatch of lance index job {}; staying PENDING: {}",
                    job.getJobId(), e.getMessage());
            return false;
        }
        if (!jobManager.markRunning(job.getJobId(), job.getRevision(), backend.getId(),
                beProcessEpoch, invocationId, deadlineMs)) {
            // The compare-and-set lost: this attempt's dispatch identity is void and its
            // invocation id is discarded. A fresh identity is built from scratch next round.
            return false;
        }
        inflightByBackend.merge(backend.getId(), 1, Integer::sum);
        LanceIndexJob fresh = jobManager.getJob(job.getJobId());
        if (!Env.getCurrentEnv().isMaster() || fresh == null
                || fresh.getMutationState() != LanceIndexJobMutationState.RUNNING
                || fresh.getDispatchRevision() == null
                || fresh.getDispatchRevision() != expectedDispatchRevision
                || !invocationId.equals(fresh.getInvocationId())) {
            // The recheck failed right before the send: no send, and no resend either.
            // The job is durable RUNNING, so the deadline sweep or a matching callback
            // converges it.
            LOG.warn("lance index job {} did not survive the pre-send recheck; not sending", job.getJobId());
            return true;
        }
        TStatus status;
        try {
            status = sendExecuteRequest(backend, dispatch);
        } catch (PreInvocationSendException e) {
            // Proven never enqueued: converge through the no-enqueue channel, which
            // releases the possible-live slot this attempt took with markRunning in
            // the same durable transition.
            LOG.warn("dispatch of lance index job {} provably never enqueued: {}", job.getJobId(), e.getMessage());
            completePreInvocationRejected(jobManager, fresh, e.getMessage());
            return true;
        } catch (Exception e) {
            // The request may have reached the backend, so its outcome cannot be trusted.
            LOG.warn("dispatch send of lance index job {} failed: {}", job.getJobId(), e.getMessage());
            completeNoTrusted(jobManager, fresh, "dispatch send failed; the result cannot be trusted");
            return true;
        }
        if (status == null || status.getStatusCode() == null) {
            // Absence of a status is the absence of a trusted answer, not a clean
            // rejection; only a complete error status proves the dispatch was not
            // enqueued.
            LOG.warn("dispatch send of lance index job {} returned no status", job.getJobId());
            completeNoTrusted(jobManager, fresh, "dispatch send returned no status");
            return true;
        }
        if (status.getStatusCode() != TStatusCode.OK) {
            // A clean error status proves the backend did not enqueue the dispatch, so
            // this invocation is known never to have executed.
            LOG.warn("backend {} rejected the dispatch of lance index job {} before enqueueing",
                    backend.getId(), job.getJobId());
            completePreInvocationRejected(jobManager, fresh,
                    "backend returned a clean error status before enqueueing the dispatch");
        }
        // OK: enqueued exactly once. The result arrives through the report callback;
        // nothing more is done here, and the deadline sweep bounds the wait.
        return true;
    }

    /**
     * A send failure that proves the dispatch never reached a worker: the
     * client could not be borrowed, so no connection was ever established, or
     * an old backend answered UNKNOWN_METHOD for the new RPC during a rolling
     * upgrade, so it provably never enqueued the dispatch. Every other failure
     * after the invocation may have started (a broken write, a read timeout)
     * stays ambiguous and converges UNKNOWN instead.
     */
    public static class PreInvocationSendException extends Exception {
        public PreInvocationSendException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Sends one dispatch to the backend's thrift service and returns its status.
     * The connection is borrowed per send, returned only when the call completed,
     * and invalidated after a failed call. Two failure families are wrapped into
     * {@link PreInvocationSendException} because they prove the dispatch never
     * reached a worker: a borrow failure means no connection was ever
     * established, and an UNKNOWN_METHOD answer means an old backend (a rolling
     * upgrade not yet serving this RPC) provably never enqueued the dispatch —
     * with no BE capability bit to gate on, classifying that answer is what lets
     * a dispatch retry on another, already-upgraded backend. Everything thrown
     * later propagates unwrapped as ambiguous. Test seam: subclasses override
     * this method to record the request or inject faults without a live client
     * pool.
     */
    protected TStatus sendExecuteRequest(Backend backend, TLanceIndexJobDispatch dispatch) throws Exception {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
        BackendService.Client client = null;
        boolean callCompleted = false;
        try {
            try {
                client = ClientPool.backendPool.borrowObject(address);
            } catch (Exception e) {
                throw new PreInvocationSendException(
                        "no backend client could be borrowed; the dispatch was never sent", e);
            }
            TStatus status;
            try {
                status = client.submitLanceIndexJob(dispatch);
            } catch (TApplicationException e) {
                if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                    throw new PreInvocationSendException(
                            "backend does not serve submitLanceIndexJob (rolling upgrade); not enqueued", e);
                }
                throw e;
            }
            callCompleted = true;
            return status;
        } finally {
            if (client != null) {
                if (callCompleted) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
    }

    /**
     * Builds the wire request from the job record and the dispatch identity
     * that markRunning is about to make durable (the dispatch revision is the
     * pre-computed {@code job.revision + 1}; the pre-send recheck pins that the
     * durable record landed with exactly this identity). Definition fields a
     * DROP never carries travel as the empty string: the wire marks them
     * required, and the worker only reads them for CREATE and REPLACE.
     */
    private TLanceIndexJobDispatch buildDispatch(LanceIndexJob job, long dispatchRevision, String invocationId,
            long deadlineMs, long beProcessEpoch, Map<String, String> storageOptions) {
        TLanceIndexJobDispatch dispatch = new TLanceIndexJobDispatch();
        dispatch.setJobId(job.getJobId());
        dispatch.setDispatchRevision(dispatchRevision);
        dispatch.setInvocationId(invocationId);
        dispatch.setBeProcessEpoch(beProcessEpoch);
        dispatch.setDeadlineMs(deadlineMs);
        dispatch.setMutationType(TLanceIndexMutationType.valueOf(job.getMutationType().name()));
        dispatch.setIndexName(job.getDisplayIndexName());
        dispatch.setColumnName(job.getColumnName() == null ? "" : job.getColumnName());
        dispatch.setIndexType(job.getIndexType() == null ? "" : job.getIndexType());
        if (job.getPropertiesJson() != null) {
            dispatch.setPropertiesJson(job.getPropertiesJson());
        }
        dispatch.setIfNotExists(job.isIfNotExists());
        dispatch.setIfExists(job.isIfExists());
        dispatch.setDatasetUri(job.getNormalizedLocator());
        dispatch.setAdmittedDatasetVersion(job.getAdmittedDatasetVersion());
        dispatch.setSchemaContractJson(job.getSchemaContract() == null ? ""
                : GsonUtils.GSON.toJson(job.getSchemaContract()));
        if (!storageOptions.isEmpty()) {
            dispatch.setStorageOptions(storageOptions);
        }
        return dispatch;
    }

    /**
     * Resolves the storage options of one dataset at send time from the
     * catalog's current storage properties, in the vocabulary of the provider
     * the dataset URI routes to. The result is used for this dispatch only: it
     * is never persisted in the job record and never logged, so a rotated
     * credential takes effect on the next dispatch without any journal record.
     */
    private Map<String, String> resolveStorageOptions(LanceIndexJob job) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(job.getCatalogId());
        if (!(catalog instanceof LanceExternalCatalog)) {
            throw new IllegalStateException(
                    "catalog of lance index job " + job.getJobId() + " does not resolve to a Lance catalog");
        }
        return LanceStorageOptions.fromDorisStorageProperties(job.getNormalizedLocator(),
                ((LanceExternalCatalog) catalog).getCatalogProperty().getOrderedStoragePropertiesList());
    }

    private void completeNoTrusted(LanceIndexJobManager jobManager, LanceIndexJob job, String reason) {
        boolean completed = jobManager.completeWithResult(job.getJobId(),
                dispatchRevisionOf(job), job.getInvocationId(), job.getBeProcessEpoch(),
                new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                        LanceIndexJobCompletionReason.NONE, reason, false));
        if (!completed) {
            LOG.warn("no-trusted-result convergence skipped for lance index job {}: already converged by a callback"
                    + " or sweep", job.getJobId());
        }
    }

    private void completePreInvocationRejected(LanceIndexJobManager jobManager, LanceIndexJob job, String reason) {
        boolean completed = jobManager.completeProvenNoEnqueue(job.getJobId(),
                dispatchRevisionOf(job), job.getInvocationId(), job.getBeProcessEpoch(),
                new LanceIndexJobResult(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                        LanceIndexJobCompletionReason.NONE, reason, false));
        if (!completed) {
            LOG.warn("rejection convergence skipped for lance index job {}: already converged by a callback or sweep",
                    job.getJobId());
        }
    }

    /**
     * True for datasets on the local filesystem: a scheme-less absolute path or
     * a {@code file://} URI, matching the provider routing of the dataset URL.
     */
    private static boolean isLocalFileDataset(String normalizedLocator) {
        int separator = normalizedLocator.indexOf("://");
        if (separator < 0) {
            return true;
        }
        return "file".equals(normalizedLocator.substring(0, separator).toLowerCase(Locale.ROOT));
    }

    private static boolean isOnlyAliveBackend(SystemInfoService systemInfo, long backendId) {
        List<Long> aliveBackendIds = systemInfo.getAllBackendIds(true);
        return aliveBackendIds.size() == 1 && aliveBackendIds.get(0) == backendId;
    }

    private static long dispatchRevisionOf(LanceIndexJob job) {
        return job.getDispatchRevision() == null ? job.getRevision() : job.getDispatchRevision();
    }
}
