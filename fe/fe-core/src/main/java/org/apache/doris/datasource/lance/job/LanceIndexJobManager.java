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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Master-owned manager of the durable Lance index job records, the same-name
 * fences, and the three-level unresolved-job quotas. It deliberately reuses
 * neither the generic JobManager scheduling framework nor the internal
 * IndexChangeJob machinery: the external one-shot CAS, no-redispatch rule,
 * same-name fence, and possible-live ownership required here are not provided
 * by either.
 *
 * <p>Every FE keeps the same in-memory image of {@link #jobs}: the master
 * writes each durable transition to the edit log and then applies the same
 * record locally; followers apply it from replay. All transitions share one
 * write-path shape:
 * <pre>
 * writeLock -&gt; validate (state, revision CAS, callback identity)
 *           -&gt; writeEditLog(updated copy)   // fails only by System.exit
 *           -&gt; applyToMemory(updated copy)  // verbatim swap + fence/quota accounting
 * </pre>
 * A published {@link LanceIndexJob} is never mutated; a transition stages a
 * copy, logs it, and swaps it in. {@link #replayUpsertJob(LanceIndexJob)} is a
 * verbatim replace with a monotonic-revision guard and performs no state
 * transformation at all: a follower tailing a live master must keep a fresh
 * RUNNING record RUNNING. The only place a durable RUNNING without a complete
 * matching terminal result becomes UNKNOWN is {@link #onTransferToMaster()},
 * the master-election sweep that runs after metadata replay and before any
 * dispatcher could start.
 *
 * <p>Fence and quota live and die together ({@link LanceIndexJob#isUnresolved()}):
 * released when a known terminal job's refresh is NOT_REQUIRED or DONE, or by
 * a durable FORCE_RELEASE; an unresolved UNKNOWN holds both across failover,
 * timeout, termination proof, and metadata observations.
 *
 * <p>The class starts no threads, so no checkpoint-thread guard is needed in
 * the constructor; the derived fence index and quota counters are rebuilt in
 * {@link #gsonPostProcess()} after image load.
 */
public class LanceIndexJobManager implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(LanceIndexJobManager.class);

    @SerializedName(value = "jobs")
    private ConcurrentMap<Long, LanceIndexJob> jobs = Maps.newConcurrentMap();

    /**
     * Derived: fence key -&gt; all job ids holding that fence. Legal admission creates
     * exactly one owner; retaining every owner for a corrupt collision keeps the
     * fence fail-closed when one of those jobs later settles. Identity-less corrupt
     * records are kept out of the books. Rebuilt after replay/image load.
     */
    private final Map<LanceIndexFenceKey, NavigableSet<Long>> fenceIndex = Maps.newHashMap();

    /** Derived: three-level unresolved counters, rebuilt from the unresolved jobs. */
    private final LanceIndexJobQuota quota = new LanceIndexJobQuota();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public LanceIndexJobManager() {
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * Edit-log seam: the only place this manager writes the journal. Tests
     * subclass and override to capture or swallow the record.
     */
    protected void writeEditLog(LanceIndexJob job) {
        Env.getCurrentEnv().getEditLog().logLanceIndexJob(job);
    }

    // ------------------------------------------------------------------
    // Master-only write paths
    // ------------------------------------------------------------------

    /**
     * Admit a new job: same-name fence CAS and unresolved-quota admission
     * happen inside the write lock, before anything is logged. A rejection
     * leaves no job, no fence, no quota charge, and no edit-log record.
     *
     * @throws DdlException on a fence conflict, a duplicate job id, or quota overload
     */
    public void createJob(LanceIndexJob job, long tableLimit, long catalogLimit, long globalLimit)
            throws DdlException {
        Objects.requireNonNull(job, "job");
        writeLock();
        try {
            try {
                job.validateForAdmission();
            } catch (IllegalArgumentException e) {
                throw new DdlException("invalid lance index job: " + e.getMessage(), e);
            }
            if (tableLimit <= 0 || catalogLimit <= 0 || globalLimit <= 0) {
                throw new DdlException("lance index job quota limits must all be positive");
            }
            if (jobs.containsKey(job.getJobId())) {
                throw new DdlException("lance index job id already exists: " + job.getJobId());
            }
            NavigableSet<Long> fencingJobIds = fenceIndex.get(job.fenceKey());
            if (fencingJobIds != null && !fencingJobIds.isEmpty()) {
                // Never disclose the locator in the rejection (the caller may lack target privilege).
                throw new DdlException("lance index '" + job.getDisplayIndexName()
                        + "' is fenced by unresolved job " + fencingJobIds.first()
                        + "; resolve that job (FORCE_RELEASE) before reusing the name");
            }
            // Pure admission check; the charge itself happens in applyToMemory together with the fence,
            // after the record is durable. An edit-log write failure exits the process, so no rollback exists.
            if (!quota.hasCapacity(job, tableLimit, catalogLimit, globalLimit)) {
                throw new DdlException("unresolved lance index job quota exceeded for index '"
                        + job.getDisplayIndexName() + "'; resolve or finish existing jobs first");
            }
            long now = System.currentTimeMillis();
            // Stage a private copy and reset every lifecycle field: the caller's object is
            // never published, and admission always starts from the same PENDING record no
            // matter what the caller left in the lifecycle fields.
            LanceIndexJob admitted = new LanceIndexJob(job);
            admitted.setMutationState(LanceIndexJobMutationState.PENDING);
            admitted.setRefreshState(LanceIndexJobRefreshState.NOT_REQUIRED);
            admitted.setRevision(0);
            admitted.setCreateTimeMs(now);
            admitted.setUpdateTimeMs(now);
            admitted.setResult(null);
            admitted.setBackendId(null);
            admitted.setBeProcessEpoch(null);
            admitted.setInvocationId(null);
            admitted.setDispatchRevision(null);
            admitted.setDeadlineMs(null);
            admitted.setPossibleLiveOwned(false);
            admitted.setTerminationProof(LanceIndexTerminationProof.NONE);
            admitted.setForceReleased(false);
            admitted.setForceActor(null);
            admitted.setForceTimeMs(null);
            admitted.setForceNote(null);
            admitted.setForceWarning(null);
            writeEditLog(admitted);
            applyToMemory(admitted);
        } finally {
            writeUnlock();
        }
    }

    /**
     * PENDING -&gt; RUNNING, the durable dispatch boundary. Compare-and-set on
     * (jobId, revision): only a PENDING job at the expected revision may be
     * dispatched, which is what makes redispatch after replay impossible.
     * Records the dispatch identity (backend, BE process epoch, immutable
     * invocation id, deadline) and takes the possible-live slot.
     *
     * @return false (with a warning) on any mismatch; the caller must not send
     */
    public boolean markRunning(long jobId, long expectedRevision, long backendId, long beProcessEpoch,
            String invocationId, long deadlineMs) {
        if (StringUtils.isBlank(invocationId)) {
            // A null/blank invocation identity would silently match a null field under
            // Objects.equals in completeWithResult and defeat the stale-callback guard.
            LOG.warn("reject markRunning for lance index job {}: invocation id is null or blank", jobId);
            return false;
        }
        writeLock();
        try {
            LanceIndexJob current = jobs.get(jobId);
            if (current == null || current.getRevision() != expectedRevision
                    || current.getMutationState() != LanceIndexJobMutationState.PENDING) {
                LOG.warn("reject markRunning for lance index job {}: expected revision {}, current {}",
                        jobId, expectedRevision, current);
                return false;
            }
            try {
                current.validateForAdmission();
            } catch (IllegalArgumentException e) {
                LOG.warn("reject markRunning for invalid lance index job {}: {}", jobId, e.getMessage());
                return false;
            }
            if (!hasFenceIdentity(current)) {
                LOG.warn("reject markRunning for lance index job {} without a valid fence identity", jobId);
                return false;
            }
            LanceIndexJob updated = new LanceIndexJob(current);
            updated.setMutationState(LanceIndexJobMutationState.RUNNING);
            updated.setBackendId(backendId);
            updated.setBeProcessEpoch(beProcessEpoch);
            updated.setInvocationId(invocationId);
            updated.setDeadlineMs(deadlineMs);
            updated.setPossibleLiveOwned(true);
            long dispatchRevision = current.getRevision() + 1;
            updated.setRevision(dispatchRevision);
            updated.setDispatchRevision(dispatchRevision);
            updated.setUpdateTimeMs(System.currentTimeMillis());
            writeEditLog(updated);
            applyToMemory(updated);
            return true;
        } finally {
            writeUnlock();
        }
    }

    /**
     * RUNNING -&gt; terminal, from a worker/supervisor result. A callback must
     * match the durable dispatch identity exactly (immutable dispatch revision, invocation
     * id, and BE process epoch); a stale callback only logs a warning and
     * changes nothing. The typed result is classified into (mutation state,
     * refresh obligation, completion reason); message text is never inspected.
     * A known terminal job whose refresh is NOT_REQUIRED releases fence and
     * quota immediately; an UNKNOWN keeps both until a durable FORCE_RELEASE.
     *
     * <p>This is also the channel for marking a job UNKNOWN on an ambiguous
     * result (result code NO_TRUSTED_RESULT), including the master-transfer
     * sweep; no separate markUnknown API exists.
     *
     * @return false (with a warning) when the callback is stale or the job is not RUNNING
     */
    public boolean completeWithResult(long jobId, long expectedDispatchRevision, String invocationId,
            Long beProcessEpoch,
            LanceIndexJobResult result) {
        Objects.requireNonNull(result, "result");
        writeLock();
        try {
            LanceIndexJob current = jobs.get(jobId);
            if (current == null || dispatchRevisionOf(current) != expectedDispatchRevision) {
                LOG.warn("reject stale lance index job callback for job {}: expected dispatch revision {}, current {}",
                        jobId, expectedDispatchRevision, current);
                return false;
            }
            if (current.getMutationState() != LanceIndexJobMutationState.RUNNING) {
                LOG.warn("reject lance index job callback for job {} in state {}: only RUNNING accepts a result",
                        jobId, current.getMutationState());
                return false;
            }
            if (!Objects.equals(current.getInvocationId(), invocationId)
                    || !Objects.equals(current.getBeProcessEpoch(), beProcessEpoch)) {
                LOG.warn("reject stale lance index job callback for job {}: invocation/epoch mismatch, current {}",
                        jobId, current);
                return false;
            }
            LanceIndexJobResultCode.Classification classification = LanceIndexJobResultCode.classify(
                    current.getMutationType(), result.getResultCode(), current.isIfExists(),
                    result.isExternalMetadataAdvanced());
            LanceIndexJob updated = new LanceIndexJob(current);
            if (updated.getDispatchRevision() == null) {
                // Backfill old RUNNING records before the global revision advances so
                // an independent termination proof can still identify this dispatch.
                updated.setDispatchRevision(expectedDispatchRevision);
            }
            updated.setMutationState(classification.getMutationState());
            updated.setRefreshState(classification.getRefreshState());
            updated.setResult(new LanceIndexJobResult(result.getResultCode(), classification.getCompletionReason(),
                    result.getSanitizedMessage(), result.isExternalMetadataAdvanced()));
            updated.setRevision(current.getRevision() + 1);
            updated.setUpdateTimeMs(System.currentTimeMillis());
            writeEditLog(updated);
            applyToMemory(updated);
            return true;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Refresh REQUIRED -&gt; RUNNING, or FAILED -&gt; RUNNING for a retry
     * through the idempotent external-table refresh path.
     */
    public boolean markRefreshRunning(long jobId, long expectedRevision) {
        return transitionRefresh(jobId, expectedRevision, LanceIndexJobRefreshState.RUNNING);
    }

    /**
     * Refresh RUNNING -&gt; DONE. On a known terminal job this releases the
     * same-name fence and the unresolved quota.
     */
    public boolean markRefreshDone(long jobId, long expectedRevision) {
        return transitionRefresh(jobId, expectedRevision, LanceIndexJobRefreshState.DONE);
    }

    /**
     * Refresh RUNNING -&gt; FAILED. The job keeps fence and quota; a later
     * {@link #markRefreshRunning} retries through the idempotent path.
     */
    public boolean markRefreshFailed(long jobId, long expectedRevision) {
        return transitionRefresh(jobId, expectedRevision, LanceIndexJobRefreshState.FAILED);
    }

    private boolean transitionRefresh(long jobId, long expectedRevision, LanceIndexJobRefreshState target) {
        writeLock();
        try {
            LanceIndexJob current = jobs.get(jobId);
            if (current == null || current.getRevision() != expectedRevision) {
                LOG.warn("reject refresh transition to {} for lance index job {}: expected revision {}, current {}",
                        target, jobId, expectedRevision, current);
                return false;
            }
            if (current.getMutationState() == null || !current.getMutationState().isTerminal()) {
                LOG.warn("reject refresh transition to {} for non-terminal lance index job {} in mutation state {}",
                        target, jobId, current.getMutationState());
                return false;
            }
            LanceIndexJobRefreshState from = current.getRefreshState();
            boolean legal = (target == LanceIndexJobRefreshState.RUNNING
                    && (from == LanceIndexJobRefreshState.REQUIRED || from == LanceIndexJobRefreshState.FAILED))
                    || (target == LanceIndexJobRefreshState.DONE && from == LanceIndexJobRefreshState.RUNNING)
                    || (target == LanceIndexJobRefreshState.FAILED && from == LanceIndexJobRefreshState.RUNNING);
            if (!legal) {
                LOG.warn("reject illegal refresh transition {} -> {} for lance index job {}",
                        from, target, jobId);
                return false;
            }
            LanceIndexJob updated = new LanceIndexJob(current);
            updated.setRefreshState(target);
            updated.setRevision(current.getRevision() + 1);
            updated.setUpdateTimeMs(System.currentTimeMillis());
            writeEditLog(updated);
            applyToMemory(updated);
            return true;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Record a matching termination proof for a job that still owns a
     * possible-live slot. Backend, BE process epoch, invocation id, and immutable
     * dispatch revision must all match. This releases only the slot: it never
     * changes the mutation state and never releases the fence or quota.
     */
    public boolean recordTerminationProof(long jobId, long expectedDispatchRevision, long backendId,
            long beProcessEpoch, String invocationId, LanceIndexTerminationProof proof) {
        Objects.requireNonNull(proof, "proof");
        writeLock();
        try {
            LanceIndexJob current = jobs.get(jobId);
            if (current == null || dispatchRevisionOf(current) != expectedDispatchRevision) {
                LOG.warn("reject termination proof for lance index job {}: expected dispatch revision {}, current {}",
                        jobId, expectedDispatchRevision, current);
                return false;
            }
            if (StringUtils.isBlank(invocationId)
                    || !Objects.equals(current.getBackendId(), backendId)
                    || !Objects.equals(current.getBeProcessEpoch(), beProcessEpoch)
                    || !Objects.equals(current.getInvocationId(), invocationId)) {
                LOG.warn("reject stale termination proof for lance index job {}:"
                        + " dispatch identity mismatch, current {}", jobId, current);
                return false;
            }
            if (proof == LanceIndexTerminationProof.NONE || !current.isPossibleLiveOwned()
                    || current.getTerminationProof() != LanceIndexTerminationProof.NONE) {
                LOG.warn("reject termination proof {} for lance index job {}: no possible-live slot owned, current {}",
                        proof, jobId, current);
                return false;
            }
            LanceIndexJob updated = new LanceIndexJob(current);
            if (updated.getDispatchRevision() == null) {
                updated.setDispatchRevision(expectedDispatchRevision);
            }
            updated.setTerminationProof(proof);
            updated.setPossibleLiveOwned(false);
            updated.setRevision(current.getRevision() + 1);
            updated.setUpdateTimeMs(System.currentTimeMillis());
            writeEditLog(updated);
            applyToMemory(updated);
            return true;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Master-election sweep, hooked from {@code Env.transferToMaster()} after
     * metadata replay and before any master-only dispatcher could start. A
     * durable RUNNING at this point means the terminal result may have been
     * lost with the old master: the job becomes UNKNOWN through the same
     * completeWithResult channel (result code NO_TRUSTED_RESULT, fence/quota/
     * possible-live ownership retained, never redispatched), and an in-flight
     * refresh is downgraded to REQUIRED so the idempotent refresh retries.
     * Both transitions are written to the edit log so followers converge.
     */
    public void onTransferToMaster() {
        List<LanceIndexJob> snapshot;
        readLock();
        try {
            snapshot = new ArrayList<>(jobs.values());
        } finally {
            readUnlock();
        }
        for (LanceIndexJob job : snapshot) {
            if (job.getMutationState() == LanceIndexJobMutationState.RUNNING) {
                boolean completed = completeWithResult(job.getJobId(), dispatchRevisionOf(job), job.getInvocationId(),
                        job.getBeProcessEpoch(),
                        new LanceIndexJobResult(LanceIndexJobResultCode.NO_TRUSTED_RESULT,
                                LanceIndexJobCompletionReason.NONE,
                                "FE master transferred while the job was RUNNING; the result is not trusted", false));
                if (completed) {
                    LOG.info("lance index job {} transitioned RUNNING -> UNKNOWN on master transfer",
                            job.getJobId());
                }
            }
            if (job.getRefreshState() == LanceIndexJobRefreshState.RUNNING) {
                downgradeRunningRefresh(job);
            }
        }
    }

    private void downgradeRunningRefresh(LanceIndexJob sweepCandidate) {
        writeLock();
        try {
            LanceIndexJob current = jobs.get(sweepCandidate.getJobId());
            if (current == null || current.getRefreshState() != LanceIndexJobRefreshState.RUNNING) {
                return;
            }
            LanceIndexJob updated = new LanceIndexJob(current);
            updated.setRefreshState(LanceIndexJobRefreshState.REQUIRED);
            updated.setRevision(current.getRevision() + 1);
            updated.setUpdateTimeMs(System.currentTimeMillis());
            writeEditLog(updated);
            applyToMemory(updated);
        } finally {
            writeUnlock();
        }
    }

    // ------------------------------------------------------------------
    // Replay (all FEs)
    // ------------------------------------------------------------------

    /**
     * Apply one durable record from the journal: a verbatim replace of the
     * in-memory record plus derived-index accounting, with a monotonic
     * revision guard so a stale record can never roll the state back. No state
     * transformation happens here on purpose: a follower tailing a live master
     * must keep a fresh RUNNING record RUNNING. Idempotent and tolerant of
     * missing fields (Gson defaults); never throws on behalf of record content.
     */
    public void replayUpsertJob(LanceIndexJob job) {
        if (job == null) {
            LOG.warn("ignore null lance index job record");
            return;
        }
        writeLock();
        try {
            LanceIndexJob replayed = new LanceIndexJob(job);
            LanceIndexJob existing = jobs.get(replayed.getJobId());
            if (existing != null && replayed.getRevision() < existing.getRevision()) {
                LOG.warn("ignore stale lance index job record for job {}: replayed revision {} < current {}",
                        replayed.getJobId(), replayed.getRevision(), existing.getRevision());
                return;
            }
            applyToMemory(replayed);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Swap a staged record into memory and settle fence/quota accounting for
     * the replaced record. Fence and quota always move together: a record
     * holds both while {@link LanceIndexJob#isUnresolved()}, provided it
     * carries fence identity (a corrupt identity-less record stays queryable
     * but out of the books on both the charge and the release side). A private
     * copy is always stored so neither a replay input nor an edit-log seam
     * reference can mutate the published record. Caller holds the write lock.
     */
    private void applyToMemory(LanceIndexJob job) {
        LanceIndexJob stored = new LanceIndexJob(job);
        LanceIndexJob old = jobs.put(stored.getJobId(), stored);
        // Release only what the identity guard below booked: an identity-less corrupt
        // record was stored without fence/quota accounting, so keying on it would throw.
        if (old != null && old.isUnresolved() && hasFenceIdentity(old)) {
            removeFenceOwner(old);
            quota.release(old);
        }
        if (stored.isUnresolved()) {
            if (hasFenceIdentity(stored)) {
                NavigableSet<Long> owners = fenceIndex.computeIfAbsent(stored.fenceKey(), ignored -> new TreeSet<>());
                if (!owners.isEmpty() && !owners.contains(stored.getJobId())) {
                    LOG.warn("fence key collision between unresolved lance index jobs {} and {};"
                                    + " keeping fence owner {}", owners.first(), stored.getJobId(),
                            Math.min(owners.first(), stored.getJobId()));
                }
                owners.add(stored.getJobId());
                quota.charge(stored);
            } else {
                // Corrupt record tolerance: keep it queryable but out of the fence/quota books.
                LOG.warn("lance index job {} lacks fence identity (provider/locator/name);"
                        + " stored without fence/quota accounting", stored.getJobId());
            }
        }
    }

    private void removeFenceOwner(LanceIndexJob job) {
        LanceIndexFenceKey fenceKey = job.fenceKey();
        NavigableSet<Long> owners = fenceIndex.get(fenceKey);
        if (owners == null) {
            return;
        }
        owners.remove(job.getJobId());
        if (owners.isEmpty()) {
            fenceIndex.remove(fenceKey);
        }
    }

    private static long dispatchRevisionOf(LanceIndexJob job) {
        return job.getDispatchRevision() == null ? job.getRevision() : job.getDispatchRevision();
    }

    private static boolean hasFenceIdentity(LanceIndexJob job) {
        return job.getProvider() != null && job.getNormalizedLocator() != null
                && job.getNormalizedIndexName() != null;
    }

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    public LanceIndexJob getJob(long jobId) {
        readLock();
        try {
            LanceIndexJob job = jobs.get(jobId);
            return job == null ? null : new LanceIndexJob(job);
        } finally {
            readUnlock();
        }
    }

    /**
     * All jobs still holding a fence and unresolved quota: PENDING/RUNNING,
     * unforced UNKNOWN, and known terminal jobs with unfinished refresh.
     */
    public List<LanceIndexJob> getUnresolvedJobs() {
        readLock();
        try {
            List<LanceIndexJob> result = new ArrayList<>();
            for (LanceIndexJob job : jobs.values()) {
                if (job != null && job.isUnresolved() && hasFenceIdentity(job)) {
                    result.add(new LanceIndexJob(job));
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    /**
     * Terminal jobs the refresh driver must pick up: refresh REQUIRED (waiting to
     * run, including jobs downgraded by the master-transfer sweep) or FAILED
     * (waiting for a retry). Both resume through the idempotent refresh path via
     * {@link #markRefreshRunning}; a FAILED job invisible here would hold its
     * fence forever with no retry channel.
     */
    public List<LanceIndexJob> getJobsNeedingRefresh() {
        readLock();
        try {
            List<LanceIndexJob> result = new ArrayList<>();
            for (LanceIndexJob job : jobs.values()) {
                if (job != null && job.getMutationState() != null && job.getMutationState().isTerminal()
                        && hasFenceIdentity(job)
                        && (job.getRefreshState() == LanceIndexJobRefreshState.REQUIRED
                                || job.getRefreshState() == LanceIndexJobRefreshState.FAILED)) {
                    result.add(new LanceIndexJob(job));
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public boolean isFenceHeld(LanceIndexFenceKey fenceKey) {
        readLock();
        try {
            return fenceIndex.containsKey(fenceKey);
        } finally {
            readUnlock();
        }
    }

    @VisibleForTesting
    LanceIndexJobQuota getQuota() {
        return quota;
    }

    @VisibleForTesting
    public int getJobCount() {
        readLock();
        try {
            return jobs.size();
        } finally {
            readUnlock();
        }
    }

    // ------------------------------------------------------------------
    // Image serialization (whole-object Gson, IndexPolicyMgr style)
    // ------------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        // Serialize against the declared base type: GsonUtils.BLOCK_INACCESSIBLE_JAVA refuses
        // reflection on non-public runtime classes, so a subclassed manager (the test seam)
        // would otherwise serialize as "null".
        Text.writeString(out, GsonUtils.GSON.toJson(this, LanceIndexJobManager.class));
    }

    public static LanceIndexJobManager read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), LanceIndexJobManager.class);
    }

    /**
     * Rebuild the derived fence index and quota counters from the durable
     * jobs after Gson image load. A fence-key collision between unresolved
     * jobs (only possible on a corrupt image) retains every owner so the fence
     * remains held until all colliding jobs settle; conflict reporting still
     * uses the smaller job id.
     */
    @Override
    public void gsonPostProcess() throws IOException {
        fenceIndex.clear();
        List<LanceIndexJob> unresolvedJobs = new ArrayList<>();
        if (jobs == null) {
            jobs = Maps.newConcurrentMap();
        }
        for (LanceIndexJob job : jobs.values()) {
            if (job == null || !job.isUnresolved()) {
                continue;
            }
            if (!hasFenceIdentity(job)) {
                LOG.warn("lance index job {} lacks fence identity (provider/locator/name);"
                        + " excluded from fence/quota rebuild", job.getJobId());
                continue;
            }
            unresolvedJobs.add(job);
            NavigableSet<Long> owners = fenceIndex.computeIfAbsent(job.fenceKey(), ignored -> new TreeSet<>());
            if (!owners.isEmpty() && !owners.contains(job.getJobId())) {
                LOG.warn("fence key collision between unresolved lance index jobs {} and {}; keeping fence owner {}",
                        owners.first(), job.getJobId(), Math.min(owners.first(), job.getJobId()));
            }
            owners.add(job.getJobId());
        }
        quota.rebuild(unresolvedJobs);
    }
}
