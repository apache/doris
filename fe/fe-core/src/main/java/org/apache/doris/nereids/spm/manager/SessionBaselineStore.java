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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.BaselineStatus;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * SessionBaselineStore - per-connection storage of SESSION-scope baselines.
 *
 * A {@code CREATE SESSION BASELINE PLAN} baseline never touches the shared
 * __internal_schema.spm_baselines table: it lives in this store, which hangs off the
 * ConnectContext that created it. Other sessions never see it, BaselineRefreshDaemon /
 * auto capture never touch it, and it is discarded together with the connection. The
 * rewrite matcher (SPMPlanner) consults the session store BEFORE the global
 * BaselineManager, so a session baseline can override a global one for this session only.
 *
 * The semantics mirror BaselineManager: a hashIndex for the Level 1 coarse filter,
 * exact digest matching, DISABLED filtering, the same priority ordering and the same
 * (hash, digest, planSql) dedup ("IF NOT EXISTS") on create.
 *
 * Ids come from the SESSION id range [2^62, 2^63) through one process-wide counter, so
 * a session id can never collide with any GLOBAL id (the shared generator allocates
 * from [1, 2^62), see BaselineScope#ofId): the scope of every id is self-describing and
 * SHOW / ALTER / DROP can address baselines of both scopes without ambiguity - and
 * without inventing cross-FE coordination for memory-only baselines. Numeric session
 * ids may repeat across FE processes, which is harmless: a session baseline never
 * leaves its FE/connection, so two sessions can never observe the same value in
 * conflicting stores.
 */
public class SessionBaselineStore {

    /**
     * Process-wide session id counter; the first session id is BaselineScope.SESSION_ID_BASE
     * (2^62). One counter per FE process keeps ids unique across the sessions of THIS FE
     * (nice for logs), while the disjoint range is what guarantees the no-collision
     * invariant against GLOBAL ids.
     */
    private static final AtomicLong SESSION_ID_GENERATOR =
            new AtomicLong(BaselineScope.SESSION_ID_BASE);

    /** id -> baseline. */
    private final Map<Long, BaselinePlan> baselines = new HashMap<>();

    /** bindSqlHash -> baseline id list (Level 1 coarse filter index). */
    private final Map<Long, List<Long>> hashIndex = new HashMap<>();

    /**
     * Creates a session baseline.
     *
     * Duplicate detection mirrors BaselineManager: identical (bindSqlHash,
     * bindSqlDigest, planSql) returns the existing id ("IF NOT EXISTS" semantics).
     *
     * @param plan the baseline (id is assigned here from the session id range)
     * @return the id of the created baseline (or the existing id when duplicated)
     */
    public synchronized long createBaseline(BaselinePlan plan) {
        if (plan.getBindSqlHash() != 0) {
            for (BaselinePlan existing : findByHash(plan.getBindSqlHash())) {
                if (existing.getBindSqlDigest().equals(plan.getBindSqlDigest())
                        && existing.getPlanSql().equals(plan.getPlanSql())) {
                    return existing.getId(); // exact duplicate -> skip
                }
            }
        }
        plan.setScope(BaselineScope.SESSION);
        long id = SESSION_ID_GENERATOR.getAndIncrement();
        // invariant: session ids live in [2^62, 2^63), so the scope derived from an id
        // (BaselineScope.ofId) is always exact
        Preconditions.checkState(id >= BaselineScope.SESSION_ID_BASE,
                "SPM session baseline id %s escaped the session id range", id);
        plan.setId(id);
        long now = System.currentTimeMillis();
        plan.setCreateTime(now);
        plan.setUpdateTime(now);
        baselines.put(id, plan);
        hashIndex.computeIfAbsent(plan.getBindSqlHash(), k -> new ArrayList<>()).add(id);
        return id;
    }

    /**
     * Drops a session baseline.
     *
     * @param id the baseline id
     * @return whether the id existed
     */
    public synchronized boolean dropBaseline(long id) {
        BaselinePlan removed = baselines.remove(id);
        if (removed == null) {
            return false;
        }
        List<Long> ids = hashIndex.get(removed.getBindSqlHash());
        if (ids != null) {
            ids.remove(id);
            if (ids.isEmpty()) {
                hashIndex.remove(removed.getBindSqlHash());
            }
        }
        return true;
    }

    /**
     * Updates the status of a session baseline (memory only, nothing to persist).
     *
     * @param id     the baseline id
     * @param status the target status
     * @return whether the id existed
     */
    public synchronized boolean updateStatus(long id, BaselineStatus status) {
        BaselinePlan plan = baselines.get(id);
        if (plan == null) {
            return false;
        }
        plan.setStatus(status);
        plan.setUpdateTime(System.currentTimeMillis());
        return true;
    }

    /**
     * Whether this connection-local store holds no baseline. Used by the rewrite fast path
     * together with BaselineManager#hasBaselines: when both stores are empty there is
     * nothing to match and the whole-tree digest rendering is skipped.
     *
     * @return whether the store is empty
     */
    public synchronized boolean isEmpty() {
        return baselines.isEmpty();
    }

    /**
     * Discards every session baseline. Called by ConnectContext#resetConnection (and any
     * equivalent session-reset path): COM_RESET_CONNECTION REUSES the same
     * ConnectContext, so without this a later logical session on a pooled connection
     * inherited the previous borrower's SESSION baselines - and because SPM consults the
     * session store BEFORE the global manager, a leftover session baseline could
     * silently rewrite the new session's query. The id counter is intentionally NOT
     * reset: ids stay unique across the process and a stale id from a discarded session
     * must never resolve to a different baseline.
     */
    public synchronized void clear() {
        baselines.clear();
        hashIndex.clear();
    }

    /**
     * Finds candidates matching the value-free digest / hash (same Level 1 hash +
     * Level 2 digest contract as BaselineManager).
     *
     * @param digest the parameterized digest of the user query
     * @param hash   the structural hash of the user query
     * @return the candidate baselines sorted by priority (possibly empty)
     */
    public synchronized List<BaselinePlan> findCandidateBaselines(String digest, long hash) {
        List<Long> candidateIds = hashIndex.get(hash);
        if (candidateIds == null || candidateIds.isEmpty()) {
            return List.of();
        }
        return candidateIds.stream()
                .map(baselines::get)
                .filter(p -> p != null)
                .filter(p -> p.getStatus().isActive())
                .filter(p -> p.getBindSqlDigest().equals(digest))
                .sorted(BaselineManager::compareCandidates)
                .collect(Collectors.toList());
    }

    /**
     * Returns a session baseline by id.
     *
     * @param id the baseline id
     * @return the baseline, or null when not found
     */
    public synchronized BaselinePlan getBaseline(long id) {
        return baselines.get(id);
    }

    /**
     * Returns all session baselines ordered by id (SHOW merges them with the globals).
     */
    public synchronized List<BaselinePlan> getAllBaselines() {
        return baselines.values().stream()
                .sorted(Comparator.comparingLong(BaselinePlan::getId))
                .collect(Collectors.toList());
    }

    private synchronized List<BaselinePlan> findByHash(long hash) {
        List<Long> ids = hashIndex.get(hash);
        if (ids == null) {
            return List.of();
        }
        return ids.stream().map(baselines::get).filter(p -> p != null).collect(Collectors.toList());
    }
}
