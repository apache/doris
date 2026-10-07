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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceIndexDatasetCheck;
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

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TApplicationException;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
 * is never reused. Each attempt also mints a random invocation secret, whose
 * role completes the dispatch identity rather than duplicating it: the
 * epoch/invocation pair proves a report is fresh (it matches the current
 * durable dispatch), while the secret proves its reporter is the BE this
 * dispatcher actually selected - every other identity field is readable from
 * SHOW LANCE INDEX JOB, and the FE thrift server cannot authenticate its
 * caller, so only the never-shown secret can reject a forged report. After a
 * successful markRunning there is exactly one send;
 * from that point a job converges only through a matching result callback, the
 * deadline sweep, or the epoch sweep, never through a resend. A failure that
 * still proves the dispatch was never enqueued (a clean pre-enqueue error
 * status, a client-pool borrow failure, or an UNKNOWN_METHOD answer from an
 * old backend) converges it NOT_COMMITTED through the no-enqueue channel,
 * which releases the possible-live slot in the same durable transition;
 * anything ambiguous after the invocation may have started converges UNKNOWN
 * with the slot retained. The blocking time of the send loop and of the
 * refresh loop is bounded per round (one backend RPC timeout each), because
 * this thread is also the only thread running the sweeps — see
 * {@link #dispatchPendingJobs()} and
 * {@link #driveRequiredRefreshes(LanceIndexJobManager, long)}.
 *
 * <p>The manager is resolved from the supplier once per round rather than
 * captured at construction: {@code Env.loadLanceIndexJobManager} replaces the
 * Env-owned manager with a brand-new object on every image load, so a cached
 * reference would keep scanning the abandoned pre-image manager after an FE
 * restart while replay, admission and SHOW all move on to the restored one.
 * Every phase of one round shares the single resolved instance.
 *
 * <p>The sleep between rounds is sliced at {@link #MAX_SLEEP_SLICE_MS} so a
 * shortened polling interval takes effect within one slice (see the field
 * javadoc), and {@link Config#lance_index_job_dispatcher_paused} suspends only
 * the dispatch phase (see {@link #dispatchPendingJobs}).
 */
public class LanceIndexJobDispatcher extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobDispatcher.class);

    /**
     * Upper bound of one sleep slice, equal to the shipped default interval. The
     * daemon never sleeps longer than this, so a shortened
     * {@link Config#lance_index_job_dispatch_interval_second} takes effect within
     * one slice instead of waiting out a previously adopted long sleep: the
     * elapsed check in {@link #runAfterCatalogReady} is re-evaluated against the
     * current config at every wake. Slices bound only the sleep; rounds still
     * honor the configured interval, because a wake whose configured interval
     * (longer than this bound) has not elapsed since the last round skips the
     * round. A lengthened interval takes effect at the next wake through the same
     * check, and an interval at or below this bound needs no check at all — every
     * wake runs a round, exactly one per configured period.
     */
    private static final long MAX_SLEEP_SLICE_MS = 10_000L;

    /**
     * Entropy of one dispatch secret: 16 bytes = 128 bits, well past guessability
     * for a token whose only threat model is a caller forging a report from
     * outside. Hex-encoded on the wire, so the token is 32 characters.
     */
    private static final int INVOCATION_SECRET_BYTES = 16;

    /** Source of the per-dispatch report secret; shared, since SecureRandom is thread-safe. */
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final Supplier<LanceIndexJobManager> jobManagerSupplier;

    // Daemon-owned FIFO: new obligations join behind existing ones, and each
    // considered job moves to the tail. The queue is rebuilt from durable jobs
    // after restart; scheduling order itself needs no journal record.
    private final Deque<Long> refreshQueue = new ArrayDeque<>();
    private final Set<Long> queuedRefreshJobIds = new HashSet<>();

    /** Wall time of the last executed round, or -1 before the first one. */
    private long lastRoundMs = -1L;

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
        long configuredMs = dispatchIntervalMs();
        setInterval(Math.min(configuredMs, MAX_SLEEP_SLICE_MS));
        if (configuredMs > MAX_SLEEP_SLICE_MS && lastRoundMs >= 0 && nowMs() - lastRoundMs < configuredMs) {
            // A wake inside a long configured interval: the slice elapsed, the
            // round period has not. Skipping is cheap and writes no journal record.
            return;
        }
        lastRoundMs = nowMs();
        try {
            runOneRound(jobManagerSupplier.get());
        } catch (Throwable t) {
            LOG.warn("Failed to process one round of the lance index job dispatcher", t);
        }
    }

    /** Clock seam for the round-period check; tests advance it instead of sleeping. */
    protected long nowMs() {
        return System.currentTimeMillis();
    }

    private void runOneRound(LanceIndexJobManager jobManager) {
        long nowMs = nowMs();
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
     * The clock the FAILED-refresh throttle measures from: the dedicated
     * refresh-failure timestamp, falling back to the generic update time only for
     * a legacy record replayed before the field existed. Measuring from the
     * generic time would let an unrelated transition — a CHILD_REAPED proof or an
     * epoch-gone release bumping it — postpone the next retry by a full interval
     * while the fence and quota stay held.
     */
    private static long refreshThrottledSinceMs(LanceIndexJob job) {
        return job.getRefreshFailureTimeMs() == null ? job.getUpdateTimeMs() : job.getRefreshFailureTimeMs();
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
     *
     * <p>The blocking time of this loop is bounded per round exactly like the
     * dispatch send loop ({@link #dispatchPendingJobs()}): one owed refresh can
     * initialize a Lance catalog and list remote databases or tables, so a slow
     * provider can consume a whole metadata timeout, and several owed jobs
     * could multiply that delay before the next deadline or epoch sweep and
     * before PENDING jobs dispatch. After spending one backend RPC timeout of
     * refresh work, no further attempt starts that round; an already-started
     * refresh can outlast that budget. The remaining owed jobs are logged and
     * deferred to the next round with their durable state untouched (REQUIRED
     * or FAILED, never a stranded RUNNING), so the FAILED throttle and the
     * markRefreshRunning semantics keep their meaning. The FIFO rotates both
     * REQUIRED jobs and FAILED retries, so a slow failing prefix cannot starve
     * untouched jobs, and new obligations cannot crowd out existing retries.
     */
    private void driveRequiredRefreshes(LanceIndexJobManager jobManager, long nowMs) {
        // fe.conf bypasses the validator, so a non-positive timeout is clamped to keep
        // at least one refresh attempt per round.
        long blockingBudgetMs = Math.max(1L, Config.backend_rpc_timeout_ms);
        Map<Long, LanceIndexJob> refreshJobs = new HashMap<>();
        for (LanceIndexJob job : jobManager.getJobsNeedingRefresh()) {
            refreshJobs.put(job.getJobId(), job);
            if (queuedRefreshJobIds.add(job.getJobId())) {
                refreshQueue.addLast(job.getJobId());
            }
        }
        // Consider each queued identity at most once this round, even when it
        // fails immediately and is ready to retry again.
        for (int remaining = refreshQueue.size(); remaining > 0; remaining--) {
            if (blockingBudgetMs <= 0) {
                LOG.info("lance index job dispatcher spent this round's blocking-refresh budget;"
                        + " deferring lance index job {} to a later round", refreshQueue.peekFirst());
                break;
            }
            long jobId = refreshQueue.removeFirst();
            LanceIndexJob job = refreshJobs.get(jobId);
            if (job == null) {
                queuedRefreshJobIds.remove(jobId);
                continue;
            }
            refreshQueue.addLast(jobId);
            try {
                if (job.getRefreshState() == LanceIndexJobRefreshState.RUNNING) {
                    // In flight elsewhere; the master-transfer sweep downgrades a stale
                    // RUNNING back to REQUIRED, so a lost driver cannot strand it.
                    continue;
                }
                if (job.getRefreshState() == LanceIndexJobRefreshState.FAILED
                        && nowMs - refreshThrottledSinceMs(job)
                                < Config.lance_index_job_refresh_retry_second * 1000L) {
                    continue;
                }
                long attemptStartMs = nowMs();
                try {
                    if (!jobManager.markRefreshRunning(job.getJobId(), job.getRevision())) {
                        // A concurrent driver won the compare-and-set; nothing to do here.
                        continue;
                    }
                    driveOneRefresh(jobManager, job);
                } finally {
                    blockingBudgetMs -= nowMs() - attemptStartMs;
                }
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
        // DONE is reserved for a refresh that verifiably did its work: a null db/table
        // lookup is NOT that evidence (a cold cache or a transient remote failure also
        // yields null), so the local resolution decides how completion is proven.
        DatabaseIf<? extends TableIf> db;
        TableIf table;
        try {
            db = catalog.getDbNullable(job.getDbName());
            table = db == null ? null : db.getTableNullable(job.getTableName());
        } catch (Throwable t) {
            LOG.warn("target of lance index job {} could not be resolved for its refresh; retrying later",
                    job.getJobId(), t);
            finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, false);
            return;
        }
        if (db == null || table == null) {
            // Positively verify the half-orphan through the namespace before DONE: only
            // a VERIFIED_ABSENT answer is "nothing is left to invalidate". PRESENT with
            // a cold local cache, or an UNRESOLVED check, marks the refresh FAILED and
            // retries — never DONE without evidence.
            if (catalog instanceof LanceExternalCatalog && ((LanceExternalCatalog) catalog).checkIndexJobDataset(
                    job.getDbName(), job.getTableName()).outcome
                    == LanceIndexDatasetCheck.Outcome.VERIFIED_ABSENT) {
                LOG.info("refresh of lance index job {} skipped: the target is verified absent", job.getJobId());
                finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, true);
                return;
            }
            LOG.warn("refresh of lance index job {} cannot be verified this round; retrying later", job.getJobId());
            finishRefreshTransition(jobManager, job.getJobId(), refreshRevision, false);
            return;
        }
        try {
            // The target resolves: this refresh actually invalidates it. It is addressed
            // by the persisted catalog id, never by the mutable name: a rename that
            // hands this catalog's old name to a different catalog between the two
            // resolutions must not refresh that one and bill the outcome to this job.
            // ignoreIfNotExists is deliberately false so the obligation survives a
            // re-resolution miss: RefreshManager resolves the names again on its own,
            // and with true a null db or table would silently return — DONE without
            // any refresh — exactly when the re-resolution failed (a catalog
            // initialization that could not list the namespace, a cold re-resolution,
            // or a genuine drop inside this window). With false the miss throws
            // DdlException instead, the catch below marks the refresh FAILED and the
            // job retries; a genuine drop converges next round through the
            // VERIFIED_ABSENT path above.
            Env.getCurrentEnv().getRefreshManager().handleRefreshTable(job.getCatalogId(),
                    job.getDbName(), job.getTableName(), false);
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
     *
     * <p>{@link Config#lance_index_job_dispatcher_paused} suspends this phase
     * only — the sweeps and the refresh driver keep running while it is set.
     * The switch is checked at the phase entry and again before every single
     * job attempt, which closes the admission race a test or operator cares
     * about: anyone who sets the switch <em>before</em> admitting a job is
     * guaranteed the job is never dispatched while paused. A round whose
     * snapshot was taken before the admission never sees the job at all, and
     * any round that can see it performs its per-job check after the
     * admission, hence after the switch was set, and skips it. A skipped job
     * never consumes the round's dispatch budget.
     *
     * <p>This daemon thread is also the only thread running the deadline sweep,
     * the epoch sweep, and the refresh driver, so the blocking time of the
     * dispatch attempts is bounded per round: at most one backend RPC timeout
     * of it may be spent before the remaining jobs are deferred to the next
     * round. A healthy round spends microseconds per attempt and never notices
     * the bound; a backend whose job RPC stalls while still heartbeating can
     * hold this thread for at most one in-flight timeout beyond the budget,
     * instead of the per-round cap times the timeout (minutes with the
     * defaults) — so the lifecycle work of the following rounds keeps its
     * cadence.
     */
    private void dispatchPendingJobs(LanceIndexJobManager jobManager) {
        if (Config.lance_index_job_dispatcher_paused) {
            return;
        }
        int maxPerRound = Math.max(1, Config.lance_index_job_max_dispatch_per_round);
        Map<Long, Integer> inflightByBackend = jobManager.countPossibleLiveSlotsByBackend();
        // fe.conf bypasses the validator, so a non-positive timeout is clamped to
        // keep at least one attempt per round.
        long blockingBudgetMs = Math.max(1L, Config.backend_rpc_timeout_ms);
        int dispatched = 0;
        for (LanceIndexJob job : jobManager.getJobsNeedingDispatch()) {
            if (Config.lance_index_job_dispatcher_paused) {
                // Flipped mid-round: stop without touching the budget.
                break;
            }
            if (dispatched >= maxPerRound) {
                break;
            }
            if (blockingBudgetMs <= 0) {
                LOG.info("lance index job dispatcher spent this round's blocking-dispatch budget;"
                        + " deferring lance index job {} to a later round", job.getJobId());
                break;
            }
            long attemptStartMs = nowMs();
            try {
                if (tryDispatch(jobManager, job, inflightByBackend)) {
                    dispatched++;
                }
            } catch (Throwable t) {
                LOG.warn("failed to dispatch lance index job " + job.getJobId(), t);
            }
            blockingBudgetMs -= nowMs() - attemptStartMs;
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
        // Every schedule-available worker — compute candidates plus the mix peers
        // filled in behind them: the first candidate with a free possible-live slot
        // takes the job, so a full backend defers this attempt only when every
        // selectable backend is at the cap, never just because the randomly picked
        // one is. (The policy shuffles its result; this loop scans every candidate,
        // so inclusion is what matters, not the order.) allowOnSameHost keeps
        // co-located backends visible (the policy default hides all but one per
        // host), and preferComputeNode lets compute-only clusters serve Lance
        // dispatch at all — the policy default filters every compute-role backend
        // out. The expected count is the registered backend count on purpose:
        // getCandidateBackends adds the compute candidates first and fills the
        // remainder of that count with mix nodes, while its default expectation of
        // zero returns only the compute candidates once any compute node exists —
        // so with one compute backend at the possible-live cap and an idle mix peer,
        // the default left this job PENDING every round despite the free backend.
        // The method-arg stays -1 (return as many candidates as possible); the
        // per-backend slot cap is applied by the local loop below, not by the policy.
        List<Long> backendIds = systemInfo.selectBackendIdsByPolicy(
                new BeSelectionPolicy.Builder().needScheduleAvailable().allowOnSameHost()
                        .preferComputeNode(true)
                        .assignExpectBeNum(systemInfo.getAllBackendIds(false).size()).build(), -1);
        int perBackendCap = Math.max(1, Config.lance_index_job_max_inflight_per_backend);
        Backend backend = null;
        for (Long backendId : backendIds) {
            Backend candidate = systemInfo.getBackend(backendId);
            if (candidate == null) {
                continue;
            }
            if (localDataset && !isOnlyRegisteredBackend(systemInfo, candidate.getId())) {
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
        // Generated independently of the invocation id (never derived from it) and never
        // logged: the invocation id travels in SHOW output, and a secret derivable from
        // a shown value would authorize reports exactly like the shown value does. It is
        // discarded together with the invocation id when the compare-and-set below loses.
        String invocationSecret = newInvocationSecret();
        // The process epoch is captured once, and the same value goes to the
        // durable record and the wire: a heartbeat landing between the two reads
        // must not split the dispatch identity (the callback matches the durable
        // value, and the epoch sweep releases the slot against it).
        long beProcessEpoch = backend.getProcessEpoch();
        long deadlineMs = executeDeadlineMs(System.currentTimeMillis());
        long expectedDispatchRevision = job.getRevision() + 1;
        TLanceIndexJobDispatch dispatch;
        try {
            dispatch = buildDispatch(job, expectedDispatchRevision, invocationId, invocationSecret, deadlineMs,
                    beProcessEpoch, resolveStorageOptions(job));
        } catch (Exception e) {
            // Not a trusted worker rejection and not an ambiguity either: nothing was
            // marked and nothing was sent, so the job simply waits for the next round.
            LOG.warn("failed to prepare the dispatch of lance index job {}; staying PENDING: {}",
                    job.getJobId(), e.getMessage());
            return false;
        }
        if (!jobManager.markRunning(job.getJobId(), job.getRevision(), backend.getId(),
                beProcessEpoch, invocationId, invocationSecret, deadlineMs)) {
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
            // the same durable transition — and hands this round's local capacity
            // straight back, so later jobs are not deferred behind a slot that no
            // longer exists (the shipped not-implemented stub hits this every time).
            LOG.warn("dispatch of lance index job {} provably never enqueued: {}", job.getJobId(), e.getMessage());
            if (completePreInvocationRejected(jobManager, fresh, e.getMessage())) {
                inflightByBackend.merge(backend.getId(), -1, Integer::sum);
            }
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
            // this invocation is known never to have executed. The bounded status-code
            // name travels into the log and the persisted reason, so SHOW can tell an
            // unavailable worker (NOT_IMPLEMENTED_ERROR) from a resource or policy
            // rejection; the free-form backend error message stays out unless sanitized.
            // The released slot is reclaimed for this round exactly like the exception
            // path above.
            LOG.warn("backend {} rejected the dispatch of lance index job {} before enqueueing: {}",
                    backend.getId(), job.getJobId(), status.getStatusCode());
            if (completePreInvocationRejected(jobManager, fresh,
                    "backend rejected the dispatch before enqueueing it: " + status.getStatusCode())) {
                inflightByBackend.merge(backend.getId(), -1, Integer::sum);
            }
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
     * One fresh dispatch secret, from an independent {@link SecureRandom} source and
     * hex-encoded: 128 bits of entropy, never derived from the UUID invocation id.
     * The value is only ever written into the markRunning journal record and the
     * dispatch request handed to the selected BE; no log line, SHOW column, or other
     * rendering may carry it.
     */
    private static String newInvocationSecret() {
        byte[] secret = new byte[INVOCATION_SECRET_BYTES];
        SECURE_RANDOM.nextBytes(secret);
        return Hex.encodeHexString(secret);
    }

    /**
     * Builds the wire request from the job record and the dispatch identity
     * that markRunning is about to make durable (the dispatch revision is the
     * pre-computed {@code job.revision + 1}; the pre-send recheck pins that the
     * durable record landed with exactly this identity). The invocation secret
     * completes that identity on the wire: the selected BE is the only party
     * that ever receives it, so its echo in the result report is what proves
     * the reporter is the dispatched BE. Definition fields a DROP never carries
     * travel as the empty string: the wire marks them required, and the worker
     * only reads them for CREATE and REPLACE.
     */
    private TLanceIndexJobDispatch buildDispatch(LanceIndexJob job, long dispatchRevision, String invocationId,
            String invocationSecret, long deadlineMs, long beProcessEpoch, Map<String, String> storageOptions) {
        TLanceIndexJobDispatch dispatch = new TLanceIndexJobDispatch();
        dispatch.setJobId(job.getJobId());
        dispatch.setDispatchRevision(dispatchRevision);
        dispatch.setInvocationId(invocationId);
        dispatch.setInvocationSecret(invocationSecret);
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

    /**
     * Converges a proven no-enqueue attempt. Returns whether the durable transition
     * landed: only then was the possible-live slot really released, and only then may
     * the caller reclaim this round's local capacity for that backend.
     */
    private boolean completePreInvocationRejected(LanceIndexJobManager jobManager, LanceIndexJob job,
            String reason) {
        boolean completed = jobManager.completeProvenNoEnqueue(job.getJobId(),
                dispatchRevisionOf(job), job.getInvocationId(), job.getBeProcessEpoch(),
                new LanceIndexJobResult(LanceIndexJobResultCode.PRE_INVOCATION_RESOURCE_REJECTED,
                        LanceIndexJobCompletionReason.NONE, reason, false));
        if (!completed) {
            LOG.warn("rejection convergence skipped for lance index job {}: already converged by a callback or sweep",
                    job.getJobId());
        }
        return completed;
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

    /**
     * True only when the deployment itself is single-BE: exactly one backend is
     * registered, and it is the candidate. Heartbeat loss on a multi-BE deployment
     * proves nothing about shared local-file identity — one FE and two registered
     * BEs is still a multi-node cluster while one of them is down — so the guard
     * reads the registered topology, not the alive one. (The candidate is alive by
     * construction: it passed the policy's schedule-available filter.)
     */
    private static boolean isOnlyRegisteredBackend(SystemInfoService systemInfo, long backendId) {
        List<Long> registeredBackendIds = systemInfo.getAllBackendIds(false);
        return registeredBackendIds.size() == 1 && registeredBackendIds.get(0) == backendId;
    }

    private static long dispatchRevisionOf(LanceIndexJob job) {
        return job.getDispatchRevision() == null ? job.getRevision() : job.getDispatchRevision();
    }
}
