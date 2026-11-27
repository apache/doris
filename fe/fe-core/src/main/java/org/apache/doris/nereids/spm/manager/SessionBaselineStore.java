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
import org.apache.doris.nereids.spm.SPMForwardedSession;
import org.apache.doris.nereids.spm.SPMUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Connection-owned SESSION baseline storage. Rows are kept in memory and can be serialized into a forwarded
 * request context. Matching checks this store before GLOBAL candidates. The hash index is followed by exact digest
 * and active-status filtering, with the same candidate ordering as BaselineManager.
 *
 * Locally generated ids use a process-wide counter in the SESSION range. Imports can preserve ids from another FE;
 * numeric ids need only be unambiguous within the owning store. Synchronized methods protect the maps, but
 * returned BaselinePlan objects are not deep copies.
 */
public class SessionBaselineStore {

    /**
     * Process-wide counter for locally created SESSION ids. Imported rows may retain ids allocated by another FE.
     */
    private static final AtomicLong SESSION_ID_GENERATOR =
            new AtomicLong(BaselineScope.SESSION_ID_BASE);

    /** id -> baseline. */
    private final Map<Long, BaselinePlan> baselines = new HashMap<>();

    /** bindSqlHash -> baseline id list (Level 1 coarse filter index). */
    private final Map<Long, List<Long>> hashIndex = new HashMap<>();

    /**
     * Payload characters the CURRENT rows take when this connection's baselines are
     * forwarded (see SPMForwardedSession.payloadRowChars). The bound is enforced at
     * CREATION - a store that accepts more than the forwarded payload can carry forced
     * serialize to drop rows, and a dropped row still participates in LOCAL
     * matching: the same statement was rewritten here but planned without that baseline on
     * the master (a silently different rewrite context). Every accepted row (ENABLED or
     * not: a later ALTER ENABLE must stay forwardable) therefore always fits.
     */
    private long forwardedPayloadChars = 0;

    /**
     * Creates a session baseline.
     *
     * Duplicate detection mirrors BaselineManager: identical (bindSqlHash,
     * bindSqlDigest, planSql, schemaFingerprint) returns the existing id
     * ("IF NOT EXISTS" semantics). A row with the same bind key but an OLD fingerprint
     * (the referenced schema changed, e.g. ALTER TABLE after the baseline was created)
     * is NOT a duplicate: SPM matching skips it, so returning its id would leave the
     * session unable to recreate a usable baseline - it is retired instead.
     *
     * Parameter plan: the baseline (id is assigned here from the session id range)
     * Returns: the id of the created baseline (or the existing id when duplicated)
     */
    public synchronized long createBaseline(BaselinePlan plan) {
        long duplicate = retireStaleAndFindDuplicate(plan);
        if (duplicate > 0) {
            return duplicate;
        }
        return registerLocked(plan, allocateId());
    }

    /**
     * Imports a baseline carried over from the connection's own FE
     * (org.apache.doris.nereids.spm.SPMForwardedSession) under its ORIGINAL id.
     * The id is what the originating session uses for SHOW / ALTER / DROP and what a
     * forwarded EXPLAIN reports as its SPM hit; allocating a fresh id here (as a plain
     * create does) made the reported hit unmanageable from the user's session and
     * produced yet another id on every forwarded request.
     *
     * Parameter plan: the carried baseline (id from the source store)
     * Returns: the id of the imported baseline (or the existing id when duplicated)
     */
    public synchronized long importBaseline(BaselinePlan plan) {
        long duplicate = retireStaleAndFindDuplicate(plan);
        if (duplicate > 0) {
            return duplicate;
        }
        long id = plan.getId();
        if (id < BaselineScope.SESSION_ID_BASE) {
            // a payload without an id (older FE / hand-written): fall back to a fresh
            // session id instead of registering one the scope invariant rejects
            id = allocateId();
        }
        return registerLocked(plan, id);
    }

    /** Allocates an ID outside this store's imported ID set while holding the store monitor. */
    private long allocateId() {
        long id;
        do {
            id = SESSION_ID_GENERATOR.getAndIncrement();
        } while (baselines.containsKey(id));
        return id;
    }

    /**
     * Duplicate validation shared by createBaseline / importBaseline: an
     * identical (bindSqlHash, digest, planSql, fingerprint) row returns its id
     * ("IF NOT EXISTS" semantics), a same-key row under an OLD fingerprint is retired.
     *
     * Parameter plan: the baseline to check
     * Returns: the existing id, or 0 when the plan is new
     */
    private long retireStaleAndFindDuplicate(BaselinePlan plan) {
        if (plan.getBindSqlHash() != 0) {
            List<BaselinePlan> stale = new ArrayList<>();
            for (BaselinePlan existing : findByHash(plan.getBindSqlHash())) {
                if (!existing.getBindSqlDigest().equals(plan.getBindSqlDigest())
                        || !existing.getPlanSql().equals(plan.getPlanSql())) {
                    continue;
                }
                if (Objects.equals(existing.getSchemaFingerprint(),
                        plan.getSchemaFingerprint())) {
                    return existing.getId(); // exact duplicate -> skip
                }
                stale.add(existing);
            }
            for (BaselinePlan row : stale) {
                // retire the stale-fingerprint row (matching no longer accepts it)
                dropBaseline(row.getId());
            }
        }
        return 0;
    }

    /** Registers one row under the given id (scope / timestamps / both indexes). */
    private long registerLocked(BaselinePlan plan, long id) {
        plan.setScope(BaselineScope.SESSION);
        // invariant: session ids live in [2^62, 2^63), so the scope derived from an id
        // (BaselineScope.ofId) is always exact
        Preconditions.checkState(id >= BaselineScope.SESSION_ID_BASE,
                "SPM session baseline id %s escaped the session id range", id);
        plan.setId(id);
        int rowChars = SPMForwardedSession.payloadRowChars(plan);
        BaselinePlan replaced = baselines.get(id);
        long usedChars = forwardedPayloadChars
                - (replaced == null ? 0 : SPMForwardedSession.payloadRowChars(replaced));
        // Reserve the enclosure for the full management payload as well as the row charges.
        if (usedChars + rowChars + SPMForwardedSession.PAYLOAD_ENCLOSURE_CHARS
                > SPMForwardedSession.MAX_PAYLOAD_CHARS) {
            // reject the CREATION instead of dropping rows while forwarding (see
            // forwardedPayloadChars): the statement that fails here is an explicit user
            // action, while a dropped row would silently change a later statement's plan
            throw new IllegalStateException("SPM SESSION baseline rejected: this connection's"
                    + " forwarded baseline payload already uses " + usedChars + " of "
                    + SPMForwardedSession.MAX_PAYLOAD_CHARS + " characters; drop some SESSION"
                    + " baselines (DROP SESSION BASELINE PLAN <id>) first");
        }
        // every SPM stored instant is epoch MICROSECONDS (see SPMUtils.nowMicros): SHOW
        // renders a SESSION row with the same formatter as a GLOBAL one
        long now = SPMUtils.nowMicros();
        plan.setCreateTime(now);
        plan.setUpdateTime(now);
        if (replaced != null) {
            // an import reusing an existing id: drop the previous row's index entry (no
            // id may be indexed twice)
            List<Long> replacedIds = hashIndex.get(replaced.getBindSqlHash());
            if (replacedIds != null) {
                replacedIds.remove(id);
                if (replacedIds.isEmpty()) {
                    hashIndex.remove(replaced.getBindSqlHash());
                }
            }
        }
        baselines.put(id, plan);
        forwardedPayloadChars = usedChars + rowChars;
        hashIndex.computeIfAbsent(plan.getBindSqlHash(), k -> new ArrayList<>()).add(id);
        return id;
    }

    /**
     * Drops a session baseline.
     *
     * Parameter id: the baseline id
     * Returns: whether the id existed
     */
    public synchronized boolean dropBaseline(long id) {
        BaselinePlan removed = baselines.remove(id);
        if (removed == null) {
            return false;
        }
        forwardedPayloadChars -= SPMForwardedSession.payloadRowChars(removed);
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
     * Parameter id: the baseline id
     * Parameter status: the target status
     * Returns: whether the id existed
     */
    public synchronized boolean updateStatus(long id, BaselineStatus status) {
        BaselinePlan plan = baselines.get(id);
        if (plan == null) {
            return false;
        }
        plan.setStatus(status);
        plan.setUpdateTime(SPMUtils.nowMicros());
        return true;
    }

    /**
     * Whether this connection-local store holds no baseline. Used by the rewrite fast path
     * together with BaselineManager.hasBaselines: when both stores are empty there is
     * nothing to match and the whole-tree digest rendering is skipped.
     *
     * Returns: whether the store is empty
     */
    public synchronized boolean isEmpty() {
        return baselines.isEmpty();
    }

    /**
     * Clears the rows, hash index and forwarded-payload accounting when the connection resets. The process-wide id
     * counter is retained.
     */
    public synchronized void clear() {
        baselines.clear();
        hashIndex.clear();
        forwardedPayloadChars = 0;
    }

    /**
     * Finds candidates matching the value-free digest / hash (same Level 1 hash +
     * Level 2 digest contract as BaselineManager).
     *
     * Parameter digest: the parameterized digest of the user query
     * Parameter hash: the structural hash of the user query
     * Returns: the candidate baselines sorted by priority (possibly empty)
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
     * Parameter id: the baseline id
     * Returns: the baseline, or null when not found
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
