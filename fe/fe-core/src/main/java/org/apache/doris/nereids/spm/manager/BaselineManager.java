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

import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * BaselineManager - baseline storage, cache and index management (M3).
 *
 * Corresponds to design doc section 6.6. Manages the CRUD of baselines and maintains
 * two query structures:
 *
 * - hashIndex: {@code Map<Long, List<Long>>}, bindSqlHash -> baseline id list. Level 1
 *   coarse filtering with O(1) lookup.
 * - baselines: id -> BaselinePlan in-memory storage (Phase 1 MVP). Phase 2 persists it
 *   to the __internal_schema.spm_baselines internal table (see design doc 6.14).
 *
 * Candidate baseline lookup (the first two of the three-level filter):
 *
 * 1. Level 1: hashIndex.get(queryHash) -> candidate id list
 * 2. Level 2: exact digest.equals(baseline.bindSqlDigest) matching
 * 3. Sort by priority (see the comparator below)
 *
 * Id source (the GLOBAL id invariant): GLOBAL writes have a single writer (user DDL is
 * forwarded to the master, auto capture runs on the Leader), so "read the persistence
 * watermark, then allocate" is sufficient to keep the local generator from ever handing
 * out an id that is already used in the shared table: before EVERY allocation
 * createBaseline reads MAX(id) from the internal table (one light aggregation) and
 * advances idGenerator past it; when that read fails the CREATE fails visibly with a
 * retryable error and no id is allocated. This closes the failover lag hole (a new
 * master may start with a generator behind the table) and the load-failure hole (a
 * broken startup load leaves the generator at 1 while the table is full); it stays
 * correct even when a row was inserted out of contract (e.g. a manual table write). The
 * startup load / periodic refresh keep advancing the generator as a second safety net.
 * Nothing that does not allocate (ALTER / DROP / matching) reads the watermark, and
 * creates only come from the DDL / capture cycle, so the read is off the query hot path.
 * GLOBAL ids start at 1 and stay in [1, 2^62); SESSION ids live in [2^62, 2^63) (see
 * BaselineScope.ofId), so the scope of an id is always exact.
 *
 * Concurrency: the in-memory store (baselines / hashIndex / stateVersion / load state) is
 * guarded by one read-write lock. Lookups take the read lock and never serialize on each
 * other; create / drop / status / load / refresh take the write lock; internal-table I/O
 * runs outside the lock wherever correctness allows (see the individual methods).
 */
public class BaselineManager {

    private static final Logger LOG = LogManager.getLogger(BaselineManager.class);

    /** Singleton. */
    private static final BaselineManager INSTANCE = new BaselineManager();

    // ==================== internal-table persistence (__internal_schema.spm_baselines) ==========

    /** Fully qualified internal table (FeConstants.INTERNAL_DB_NAME == "__internal_schema"). */
    private static final String SPM_BASELINES_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_TBL_NAME;

    /** Column order follows InternalSchema.SPM_BASELINES_SCHEMA. */
    private static final String SELECT_ALL_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time` FROM " + SPM_BASELINES_TABLE;

    /** The persistence-layer id watermark (see the class javadoc "Id source"): read
     *  before every id allocation. MAX over an aggregate is a light single-row query. */
    private static final String SELECT_MAX_ID_SQL = "SELECT MAX(`id`) FROM " + SPM_BASELINES_TABLE;

    /**
     * Durable-key lookup used by the create-time dedup: the in-memory index can be stale
     * (a follower that loaded=true before becoming master missed rows written afterwards),
     * so the authoritative duplicate check reads the (bind_sql_digest, plan_sql) key back
     * from the table before a new row is inserted.
     */
    private static final String SELECT_BY_KEY_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time` FROM " + SPM_BASELINES_TABLE
            + " WHERE `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'";

    private static final String INSERT_SQL = "INSERT INTO " + SPM_BASELINES_TABLE
            + " VALUES (${id}, '${bindSql}', '${bindSqlDigest}', ${bindSqlHash},"
            + " '${planSql}', '${queryId}', ${cost}, ${queryTimeMs}, '${source}', '${status}',"
            + " '${createTime}', '${updateTime}')";

    /**
     * Deletes one row by id AND content key (bind_sql_digest + plan_sql): a delete
     * triggered by a stale id must never remove a row that happens to carry the same id
     * but different content (e.g. after the table was edited out of band, or the id was
     * rewound by an FE started with stale metadata).
     */
    private static final String DELETE_BY_IDENTITY_SQL = "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql` = '${planSql}'";

    /** Removes one baseline row by id + its previous status (status UPDATE support). */
    private static final String DELETE_BY_ID_AND_STATUS_SQL = "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `status` = '${status}'";

    /** DATETIME column format (internal table create_time / update_time). */
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ==================== priority ordering ====================

    /**
     * Candidate baseline priority ordering:
     *
     * 1. both have queryMs (>= 0) -> the one with shorter time wins
     * 2. neither has queryMs (-1) -> the one with lower cost wins
     * 3. only one has queryMs -> the one without time wins (manual baselines win over
     *    auto-captured ones)
     *
     * queryMs = -1 means no actual execution time (manually created); >= 0 means known
     * execution stats (0 is a valid measured sub-millisecond time). The comparison is a
     * total order: equal known times compare equal and a known time never compares -1 in
     * both directions.
     */
    private static final Comparator<BaselinePlan> comparator = (o1, o2) -> {
        // -1 is "unknown" (manual create); 0 is a VALID measured time, so "known" is
        // >= 0. compare(0, 0) must be 0 and 0 vs -1 must be ordered consistently, or
        // stream sorting can misorder candidates / throw a comparator-contract error.
        long time1 = o1.getQueryTimeMs();
        long time2 = o2.getQueryTimeMs();
        boolean known1 = time1 >= 0;
        boolean known2 = time2 >= 0;
        if (known1 && known2) {
            return Long.compare(time1, time2);
        } else if (!known1 && !known2) {
            return Double.compare(o1.getCost(), o2.getCost());
        } else {
            // the one with time is ordered last (the one without time wins)
            return known1 ? 1 : -1;
        }
    };

    /** Auto-increment id counter. */
    private final AtomicLong idGenerator = new AtomicLong(1);

    /** Whether the persisted baselines have been loaded from the internal table (or the
     *  internal schema db is disabled). Set by loadFromInternalTable() and
     *  clearForTest(). */
    private volatile boolean loaded = false;

    /** Whether CRUD writes to the internal table (disabled by clearForTest for tests). */
    private volatile boolean persistToTable = true;

    /**
     * Guards the in-memory store: {@code baselines} / {@code hashIndex} /
     * {@code stateVersion} and the {@code loaded} state machine. Only accesses to those
     * structures are critical sections - internal-table I/O and the CPU work derived
     * from a snapshot (digest filter, priority sort) run OUTSIDE the lock. Rewrite
     * lookups (hasBaselines + findCandidateBaselines on every query) take the read lock
     * and therefore never serialize on each other; create / drop / status / load /
     * refresh take the write lock for their in-memory validation / publication only.
     */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /**
     * Serializes WRITERS (create / drop / status) against each other for their
     * internal-table read-modify-write sequences. Deliberately separate from stateLock:
     * every SPM query takes stateLock.readLock() in hasBaselines / findCandidateBaselines
     * BEFORE its rewrite timeout can help, so ONE slow persistence statement (each
     * auto-capture candidate issues one) would stall planning for every such query on the
     * FE. The state lock only protects in-memory validation and publication - see the
     * two-phase structure of {@link #createBaseline} / {@link #dropBaseline} /
     * {@link #updateStatus}. Readers never touch this lock.
     */
    private final Object writerLock = new Object();

    /**
     * Monotonic version of the in-memory state, bumped by every local mutation (create /
     * drop / status change / load) and by every refresh that changed something. The periodic
     * refresh compares it to skip a table snapshot whose read overlapped a local mutation
     * (see refreshFromInternalTable). Guarded by stateLock.
     */
    private long stateVersion = 0;

    /** id -> BaselinePlan (Phase 1 in-memory storage). */
    private final Map<Long, BaselinePlan> baselines = new HashMap<>();

    /** bindSqlHash -> baseline id list (Level 1 coarse filter index). */
    private final Map<Long, List<Long>> hashIndex = new HashMap<>();

    private BaselineManager() {
    }

    public static BaselineManager getInstance() {
        return INSTANCE;
    }

    /**
     * Candidate baseline ordering shared with the SESSION-scope store: it applies the
     * exact same priority rules as this manager (see the comparator field).
     * Public so tests can verify the total-order contract directly.
     *
     * @param o1 first baseline
     * @param o2 second baseline
     * @return the comparison result
     */
    public static int compareCandidates(BaselinePlan o1, BaselinePlan o2) {
        return comparator.compare(o1, o2);
    }

    // ==================== CRUD ====================

    /**
     * Creates a baseline.
     *
     * Duplicate detection: identical (bindSqlHash, bindSqlDigest) with the exact same
     * planSql is skipped; a different planSql is allowed to coexist (one query can have
     * multiple plan baselines).
     *
     * @param plan the baseline (id is assigned here if unset)
     * @return the id of the created baseline (or the existing id when duplicated)
     */
    public long createBaseline(BaselinePlan plan) {
        ensureLoadedOrThrow();
        // Two-phase create. I/O (watermark read, durable-key dedup, repair deletes, INSERT)
        // runs under writerLock but NEVER under stateLock: the state lock only protects the
        // in-memory duplicate validation (phase 1) and the publication (phase 2). Holding
        // the state lock across the I/O would stall every SPM query's rewrite lookup.
        synchronized (writerLock) {
            // Id watermark first (see the class javadoc "Id source"): the generator must be
            // advanced past the persistence layer BEFORE an id is handed out. A create whose
            // watermark read fails fails visibly and allocates nothing, instead of silently
            // colliding with a row written by a newer master.
            final long watermark = readPersistedWatermark();
            // Phase 1: exact-duplicate validation against the in-memory index (read lock).
            stateLock.readLock().lock();
            try {
                if (plan.getBindSqlHash() != 0) {
                    for (BaselinePlan existing : findByHash(plan.getBindSqlHash())) {
                        if (existing.getBindSqlDigest().equals(plan.getBindSqlDigest())
                                && existing.getPlanSql().equals(plan.getPlanSql())) {
                            return existing.getId(); // exact duplicate -> skip
                        }
                    }
                }
            } finally {
                stateLock.readLock().unlock();
            }
            // Durable-key check: the in-memory index can be stale (e.g. a follower that
            // loaded=true before becoming master missed rows written after its last
            // refresh). Without this check the INSERT below would REPLACE a durable
            // baseline - changing its id and, on an INSERT failure, losing the old row.
            // A durable duplicate returns its id and is adopted into memory instead;
            // extra same-key rows (partial-state survivors) are repaired away idempotently.
            // writerLock keeps another writer's INSERT/DELETE pair out of this window.
            if (persistenceEnabled() && plan.getBindSqlDigest() != null) {
                List<BaselinePlan> durable =
                        readPersistedByKey(plan.getBindSqlDigest(), plan.getPlanSql());
                if (!durable.isEmpty()) {
                    BaselinePlan winner = durable.get(0);
                    for (int i = 1; i < durable.size(); i++) {
                        winner = pickDurableWinner(winner, durable.get(i));
                    }
                    for (BaselinePlan row : durable) {
                        if (row.getId() != winner.getId()) {
                            try {
                                persistDeleteByIdentity(row);
                            } catch (RuntimeException e) {
                                // best-effort repair: the key state is deterministic either
                                // way (the winner above), the leftover row is warned below
                                LOG.warn("SPM failed to repair a duplicate baseline row (id={}): {}",
                                        row.getId(), e.getMessage());
                            }
                        }
                    }
                    publishBaseline(winner);
                    LOG.info("SPM baseline create deduplicated against the durable key: id={}",
                            winner.getId());
                    return winner.getId();
                }
            }
            // every baseline owned by the global manager is GLOBAL-scope (same value the
            // rows loaded from the internal table and the auto capturer get); the id stays
            // in the GLOBAL range [1, 2^62), so BaselineScope.ofId(id) is exact
            plan.setScope(BaselineScope.GLOBAL);
            if (watermark >= idGenerator.get()) {
                // watermark + 1 is an id the table has never seen; the generator is
                // monotonic and the watermark was read at the top of this create, so
                // concurrent local creates can only jump the generator further up, never
                // below a watermark any create has observed
                idGenerator.set(watermark + 1);
            }
            long id = idGenerator.getAndIncrement();
            plan.setId(id);
            long now = System.currentTimeMillis();
            plan.setCreateTime(now);
            plan.setUpdateTime(now);
            // persist first so a persist failure leaves the in-memory state untouched and
            // fails the DDL visibly; no same-key row can exist here (the durable-key check
            // above returned any), so the INSERT cannot overwrite an existing baseline
            persistInsert(plan);
            // Phase 2: publish (the only state-lock section of a create).
            publishBaseline(plan);
            return id;
        }
    }

    /**
     * Phase 2 of a writer: publishes a baseline into the in-memory store. No I/O - only
     * the hash index / map / version are touched, under the write lock. Replaces a row a
     * concurrent refresh may have loaded for the same id (its index entry is dropped
     * first, so no id is indexed twice).
     */
    private void publishBaseline(BaselinePlan plan) {
        stateLock.writeLock().lock();
        try {
            BaselinePlan replaced = baselines.put(plan.getId(), plan);
            if (replaced != null) {
                removeFromHashIndex(replaced);
            }
            addToHashIndex(plan);
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Drops a baseline.
     *
     * @param id the baseline id
     * @return whether the drop succeeded
     */
    public boolean dropBaseline(long id) {
        ensureLoadedOrThrow();
        // Two-phase drop: the DELETE I/O runs under writerLock but NOT under stateLock;
        // the state lock only removes the in-memory entry afterwards.
        synchronized (writerLock) {
            BaselinePlan removed;
            stateLock.readLock().lock();
            try {
                removed = baselines.get(id);
            } finally {
                stateLock.readLock().unlock();
            }
            if (removed == null) {
                return false;
            }
            // persist first so a failure keeps both the in-memory state and the table row;
            // the delete is keyed by id + content, so a stale id can never remove an
            // unrelated row that reused the id
            persistDeleteByIdentity(removed);
            stateLock.writeLock().lock();
            try {
                BaselinePlan gone = baselines.remove(id);
                if (gone == null) {
                    // raced with a concurrent drop / refresh: the row is gone either way
                    return true;
                }
                removeFromHashIndex(gone);
                stateVersion++;
                return true;
            } finally {
                stateLock.writeLock().unlock();
            }
        }
    }

    /**
     * Updates the status of a baseline (ENABLE / DISABLE).
     *
     * @param id     the baseline id
     * @param status the target status
     * @return whether the update succeeded
     */
    public boolean updateStatus(long id, BaselineStatus status) {
        ensureLoadedOrThrow();
        // Two-phase status change: the INSERT(new) + DELETE(old) pair is internal-table I/O
        // and runs under writerLock, never under stateLock. Matching tolerates a status
        // flip racing with a lookup exactly like an ALTER landing right after the lookup
        // (see findCandidateBaselines); on failure the in-memory flip is reverted.
        synchronized (writerLock) {
            BaselinePlan plan;
            stateLock.readLock().lock();
            try {
                plan = baselines.get(id);
            } finally {
                stateLock.readLock().unlock();
            }
            if (plan == null) {
                return false;
            }
            BaselineStatus previousStatus = plan.getStatus();
            if (previousStatus == status) {
                // ALTER to the already-set status: nothing to persist. (Inserting + "deleting
                // the old row by its status" would delete the freshly inserted row as well,
                // because both rows carry the same status.)
                return true;
            }
            // The internal table is a DUPLICATE-key table on which UPDATE is not supported, so a
            // status change is persisted as INSERT (new status) + DELETE (old status). The INSERT
            // runs FIRST so the durable new row exists before any delete: a failure can never
            // leave the in-memory state "old" while the only table row was already removed (the
            // delete-then-insert gap, where the next refresh / restart silently dropped the
            // baseline). Deleting by the PREVIOUS status can never touch the freshly inserted
            // row (the statuses differ).
            long previousUpdateTime = plan.getUpdateTime();
            plan.setStatus(status);
            plan.setUpdateTime(System.currentTimeMillis());
            try {
                persistInsert(plan);
                persistDeleteByIdAndStatus(id, previousStatus);
            } catch (RuntimeException e) {
                // repair: delete the freshly inserted row by its (new) status - the old row
                // was not touched yet, so the durable state is the old row again - then
                // revert memory so memory and the table agree
                try {
                    persistDeleteByIdAndStatus(id, status);
                } catch (RuntimeException repairFailure) {
                    LOG.error("SPM failed to roll back baseline {} after a failed status update",
                            id, repairFailure);
                }
                plan.setStatus(previousStatus);
                plan.setUpdateTime(previousUpdateTime);
                throw e;
            }
            stateLock.writeLock().lock();
            try {
                stateVersion++;
            } finally {
                stateLock.writeLock().unlock();
            }
            return true;
        }
    }

    // ==================== query matching (Level 1 + Level 2 + ordering) ====================

    /**
     * Finds candidate baselines (the first two of the three-level filter plus priority
     * ordering).
     *
     * @param digest the parameterized digest of the user query
     * @param hash   the structural hash of the user query
     * @return the candidate baselines sorted by priority (possibly empty)
     */
    public List<BaselinePlan> findCandidateBaselines(String digest, long hash) {
        ensureLoaded();
        List<BaselinePlan> candidates;
        stateLock.readLock().lock();
        try {
            List<Long> candidateIds = hashIndex.get(hash);
            if (candidateIds == null || candidateIds.isEmpty()) {
                return List.of();
            }
            // snapshot the candidate objects while holding the lock: the map itself must
            // never be read concurrently with a writer
            candidates = candidateIds.stream()
                    .map(baselines::get)
                    .filter(p -> p != null)
                    .collect(Collectors.toList());
        } finally {
            stateLock.readLock().unlock();
        }
        // digest filter + priority ordering happen OUTSIDE the lock: the baseline objects
        // are immutable for matching purposes except a status flip, which races as
        // harmlessly as an ALTER landing right after the lock was released
        return candidates.stream()
                .filter(p -> p.getStatus().isActive())
                .filter(p -> p.getBindSqlDigest().equals(digest)) // Level 2 exact digest match
                .sorted(comparator)
                .collect(Collectors.toList());
    }

    /**
     * Finds baselines by hash (internal helper). Only reads the maps and the index, so a
     * READ lock is enough; {@link #createBaseline} calls it in its phase-1 validation.
     */
    private List<BaselinePlan> findByHash(long hash) {
        List<Long> ids = hashIndex.get(hash);
        if (ids == null) {
            return List.of();
        }
        return ids.stream().map(baselines::get).filter(p -> p != null).collect(Collectors.toList());
    }

    /**
     * Whether any global baseline exists. Used by the rewrite path as a fast path: with
     * no baseline anywhere there is nothing to match, so the whole-tree digest rendering
     * can be skipped entirely.
     *
     * @return whether the global storage currently holds at least one baseline
     */
    public boolean hasBaselines() {
        ensureLoaded();
        stateLock.readLock().lock();
        try {
            return !baselines.isEmpty();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Returns a baseline by id.
     *
     * @param id the baseline id
     * @return the baseline, or null when not found
     */
    public BaselinePlan getBaseline(long id) {
        ensureLoaded();
        stateLock.readLock().lock();
        try {
            return baselines.get(id);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Returns all baselines (for SHOW / tests).
     */
    public List<BaselinePlan> getAllBaselines() {
        ensureLoaded();
        List<BaselinePlan> all;
        stateLock.readLock().lock();
        try {
            all = new ArrayList<>(baselines.values());
        } finally {
            stateLock.readLock().unlock();
        }
        all.sort(Comparator.comparingLong(BaselinePlan::getId));
        return all;
    }

    // ==================== Phase 2: auto-capture helpers ====================

    /**
     * Whether a baseline with the given bindSqlDigest already exists (Phase 2 auto
     * capture dedup; design doc 7.2.5).
     *
     * @param bindSqlDigest the parameterized digest
     * @return true when at least one baseline with this digest exists
     */
    public boolean existsBaselineByDigest(String bindSqlDigest) {
        ensureLoaded();
        if (bindSqlDigest == null) {
            return false;
        }
        stateLock.readLock().lock();
        try {
            return baselines.values().stream()
                    .anyMatch(p -> bindSqlDigest.equals(p.getBindSqlDigest()));
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Whether an identical (digest, planSql) baseline already exists (Phase 2 auto
     * capture dedup; design doc 7.2.5 level 2).
     *
     * @param bindSqlDigest the parameterized digest
     * @param planSql       the frozen plan SQL
     * @return true when an identical baseline already exists
     */
    public boolean existsBaseline(String bindSqlDigest, String planSql) {
        ensureLoaded();
        if (bindSqlDigest == null) {
            return false;
        }
        stateLock.readLock().lock();
        try {
            return baselines.values().stream()
                    .anyMatch(p -> bindSqlDigest.equals(p.getBindSqlDigest())
                            && (planSql == null ? p.getPlanSql() == null
                                    : planSql.equals(p.getPlanSql())));
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * For tests: clears the storage.
     */
    public void clearForTest() {
        stateLock.writeLock().lock();
        try {
            loaded = true; // tests manage the in-memory storage directly; never touch the table
            persistToTable = false; // and never write the table from a unit test
            baselines.clear();
            hashIndex.clear();
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    // ==================== internal table persistence (__internal_schema.spm_baselines) ========

    /** Whether the internal schema db / table is enabled (false in FE unit tests). */
    private static boolean persistenceEnabled() {
        return FeConstants.enableInternalSchemaDb && INSTANCE.persistToTable;
    }

    /**
     * Loads all baselines from the internal table and rebuilds the in-memory hash index.
     * Idempotent. The transient parameterized bind and plan trees of each row are rebuilt on
     * the fly from the stored bindSql / planSql text (pure AST parsing + placeholder
     * parameterization, needs no catalog). After loading, the id generator continues after
     * the largest persisted id.
     */
    public void loadFromInternalTable() {
        if (!persistenceEnabled()) {
            loaded = true;
            return;
        }
        if (loaded) {
            return;
        }
        // Read the table OUTSIDE the lock: an internal query can be slow and must not
        // block rewrite lookups. Until the load completes no local mutation can run
        // (every mutator calls ensureLoaded first), so a concurrent second load simply
        // reads again and loses the write section's re-check below.
        final Map<Long, BaselinePlan> snapshot;
        try {
            snapshot = readPersistedSnapshot();
        } catch (Throwable t) {
            // keep loaded=false so the next access retries lazily (e.g. BE / tablet not
            // ready yet right after an FE restart)
            LOG.warn("SPM load baselines from internal table failed (will retry lazily): {}",
                    t.getMessage());
            return;
        }
        stateLock.writeLock().lock();
        try {
            if (loaded) {
                return; // a concurrent load won the race
            }
            doLoadFromTable(snapshot);
            loaded = true;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Loads the persisted baselines on first use (called at the head of every public
     * CRUD / query entry point). Cheap once loaded (a volatile read); retries while the
     * internal table is not ready yet.
     */
    public void ensureLoaded() {
        if (loaded) {
            return;
        }
        loadFromInternalTable();
    }

    /**
     * Strict variant for management operations (CREATE / ALTER / DROP): when the first
     * load has not succeeded (internal table / BE unavailable), fail with a retryable
     * error instead of silently operating on an empty map - DROP ... IF EXISTS would
     * otherwise report success without deleting the durable row, which then reappears on
     * the next refresh. Query-side callers keep ensureLoaded()'s degradation (no match
     * while the store is unavailable).
     */
    private void ensureLoadedOrThrow() {
        ensureLoaded();
        if (!loaded) {
            throw new IllegalStateException("SPM baseline store is not ready yet"
                    + " (the baseline table has not been loaded); please retry later");
        }
    }

    /** Replaces the in-memory store with the read snapshot (caller holds the write lock). */
    private void doLoadFromTable(Map<Long, BaselinePlan> loadedPlans) {
        Map<Long, List<Long>> loadedIndex = new HashMap<>();
        long maxId = 0;
        for (BaselinePlan p : loadedPlans.values()) {
            loadedIndex.computeIfAbsent(p.getBindSqlHash(), k -> new ArrayList<>()).add(p.getId());
            maxId = Math.max(maxId, p.getId());
        }
        baselines.clear();
        hashIndex.clear();
        baselines.putAll(loadedPlans);
        hashIndex.putAll(loadedIndex);
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        stateVersion++;
        LOG.info("SPM loaded {} baselines from internal table", baselines.size());
    }

    // ==================== periodic refresh (cross-FE visibility) ====================

    /**
     * Merges the persisted baselines into the in-memory cache (incremental diff). Called
     * periodically by BaselineRefreshDaemon on EVERY FE: baselines can be created / dropped /
     * altered on any FE (user DDL runs on the FE that receives it, auto capture runs on the
     * Leader), so without this refresh a baseline created on FE A would stay invisible on
     * FE B and whether a query hits a baseline would depend on which FE serves the client.
     *
     * Pure read: never writes the internal table. The table is read OUTSIDE the manager lock
     * (an internal query can take a while and must not block matching queries); the diff is
     * applied under the lock only when no local mutation / load happened in between
     * (stateVersion). A skipped snapshot is simply retried with a fresh read on the next
     * cycle - local mutations persist before they touch the in-memory state, so the skipped
     * snapshot may only be stale, never the other way around.
     */
    public void refreshFromInternalTable() {
        if (!persistenceEnabled()) {
            return; // internal schema db disabled (or a unit test): nothing to merge
        }
        // Serialize refresh with the WRITERS (writerLock, never stateLock): a local
        // mutation publishes its durable write and its in-memory state under this lock,
        // so the read-outside-the-lock + version-guarded apply below cannot interleave
        // with it. Without the serialization updateStatus can persist the new row, a
        // refresh that started BEFORE that write applies the OLD row while the version is
        // still unchanged, and the version bump that follows only records the mutation - it
        // does not republish the status, so this FE keeps matching the stale in-memory
        // row until another refresh. Matching queries are unaffected: they never take
        // writerLock (only stateLock read).
        synchronized (writerLock) {
            if (!loaded) {
                // The first load doubles as the first refresh (downloads the whole table).
                loadFromInternalTable();
                return;
            }
            long versionAtRead;
            stateLock.readLock().lock();
            try {
                if (!loaded) {
                    return; // raced with a concurrent first load: retry next cycle
                }
                versionAtRead = stateVersion; // no writer publishes while writerLock is held
            } finally {
                stateLock.readLock().unlock();
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                LOG.warn("SPM baseline refresh read failed (will retry next cycle): {}", t.getMessage());
                return;
            }
            if (!applyRefreshedSnapshotIfUnchanged(versionAtRead, snapshot)) {
                LOG.debug("SPM baseline refresh skipped: local state changed while reading");
            }
        }
    }

    /**
     * Applies a snapshot read by a refresh IF no local mutation published since the read
     * started (the version guard). The guard is what makes a stale snapshot harmless:
     * updateStatus publishes its in-memory flip and its version bump before a refresh can
     * reach this point, so the older row read before that update is rejected instead of
     * overwriting the newer status. Public for unit tests; production callers use
     * {@link #refreshFromInternalTable}, which additionally serializes with the writers.
     *
     * @param versionAtRead the state version observed when the snapshot read started
     * @param persisted     the snapshot to apply
     * @return whether the snapshot was applied
     */
    public boolean applyRefreshedSnapshotIfUnchanged(long versionAtRead,
            Map<Long, BaselinePlan> persisted) {
        stateLock.writeLock().lock();
        try {
            if (stateVersion != versionAtRead) {
                return false;
            }
            applyRefreshedBaselinesLocked(persisted);
            return true;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * The current in-memory state version: bumped by every local mutation that changes
     * the published store. Public for unit tests (the stale-snapshot guard).
     *
     * @return the state version
     */
    public long getStateVersion() {
        stateLock.readLock().lock();
        try {
            return stateVersion;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Applies a refreshed persisted snapshot (incremental diff):
     *
     * - ids only present in the snapshot are added (created on another FE);
     * - ids only present in memory are removed (dropped on another FE);
     * - ids present in both with changed persisted content (e.g. status changed by an ALTER
     *   on another FE) are replaced; unchanged rows keep their current object, so instance
     *   identity and the transient parameterized trees stay intact;
     * - the id generator is advanced past the largest persisted id.
     *
     * The snapshot is authoritative; callers must ensure it was read while no local mutation
     * was in flight (see refreshFromInternalTable). Public for unit tests.
     */
    public void applyRefreshedBaselines(Map<Long, BaselinePlan> persisted) {
        stateLock.writeLock().lock();
        try {
            applyRefreshedBaselinesLocked(persisted);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Applies a snapshot; the caller must hold the write lock (see refreshFromInternalTable). */
    private void applyRefreshedBaselinesLocked(Map<Long, BaselinePlan> persisted) {
        long maxId = 0;
        int added = 0;
        int updated = 0;
        for (BaselinePlan row : persisted.values()) {
            maxId = Math.max(maxId, row.getId());
            BaselinePlan current = baselines.get(row.getId());
            if (current == null) {
                baselines.put(row.getId(), row);
                addToHashIndex(row);
                added++;
            } else if (persistedContentChanged(current, row)) {
                removeFromHashIndex(current);
                baselines.put(row.getId(), row);
                addToHashIndex(row);
                updated++;
            }
        }
        List<Long> vanished = new ArrayList<>();
        for (Long id : baselines.keySet()) {
            if (!persisted.containsKey(id)) {
                vanished.add(id);
            }
        }
        for (Long id : vanished) {
            removeFromHashIndex(baselines.remove(id));
        }
        int removed = vanished.size();
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        if (added > 0 || removed > 0 || updated > 0) {
            stateVersion++;
            LOG.info("SPM baseline refresh applied: added={}, removed={}, updated={}, total={}",
                    added, removed, updated, baselines.size());
        }
    }

    /**
     * Reads the persistence-layer id watermark (MAX(id)) - the id-source invariant described
     * in the class javadoc. Returns 0 when persistence is disabled (unit tests / internal
     * schema db off). A failed read is rethrown as a retryable error: createBaseline must
     * never allocate an id while the watermark is unknown.
     */
    private static long readPersistedWatermark() {
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows =
                    StatisticsUtil.executeQuery(SELECT_MAX_ID_SQL, Collections.emptyMap());
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            // an empty table yields one row with a NULL MAX(id)
            String maxId = rows.get(0).getWithDefault(0, "");
            return maxId.isEmpty() ? 0 : Long.parseLong(maxId.trim());
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM baseline id watermark read failed (retry the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * Builds the persisted snapshot from the internal table: one BaselinePlan per row with
     * the transient trees rebuilt exactly like the startup load does. Invalid rows are
     * skipped with a warning (the next cycle retries).
     */
    private static Map<Long, BaselinePlan> readPersistedSnapshot() throws Exception {
        List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_ALL_SQL, Collections.emptyMap());
        Map<Long, BaselinePlan> snapshot = new HashMap<>();
        for (ResultRow row : rows) {
            try {
                BaselinePlan p = parsePersistedRow(row);
                BaselinePlan previous = snapshot.put(p.getId(), p);
                if (previous != null) {
                    // Two rows carry the same id (e.g. an ALTER status update whose
                    // compensating delete failed, or an out-of-contract manual write):
                    // SELECT_ALL_SQL has no ordering, so "last read wins" would make
                    // refresh / restart decide the status NONDETERMINISTICALLY - a failed
                    // DISABLE could be silently re-enabled. Pick a deterministic winner
                    // (see pickDurableWinner) so every FE / restart converges on it.
                    BaselinePlan winner = pickDurableWinner(previous, p);
                    snapshot.put(p.getId(), winner);
                    LOG.warn("SPM persisted baseline id {} appears in more than one row"
                                    + " (statuses {} / {}, update times {} / {});"
                                    + " deterministically keeping the {} row",
                            p.getId(), previous.getStatus(), p.getStatus(),
                            previous.getUpdateTime(), p.getUpdateTime(), winner.getStatus());
                }
            } catch (Throwable t) {
                LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
            }
        }
        return snapshot;
    }

    /**
     * Rebuilds one persisted row exactly like the startup load does: one BaselinePlan
     * with its transient (parameterized) trees rebuilt from the stored bindSql / planSql.
     *
     * @param row the internal-table row
     * @return the parsed row
     * @throws Exception when the row's bindSql cannot be parsed
     */
    private static BaselinePlan parsePersistedRow(ResultRow row) throws Exception {
        BaselinePlan p = fromRow(row);
        String planSql = p.getPlanSql();
        boolean frozen = planSql != null
                && (planSql.contains(SPMFrozenTreeReplacer.CONST_VAR_FUNC)
                        || planSql.contains(SPMFrozenTreeReplacer.CONST_LIST_FUNC));
        // Rebuild the transient trees with ONE shared builder over both texts in
        // the CREATE order (bind first, then plan), so the placeholder ids of the
        // two trees stay aligned and a value extracted from the bind tree can
        // never be substituted into a literal slot of the other tree. Frozen
        // (placeholder-carrying) planSql is replayed as text - no plan tree.
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                p.getBindSql(), frozen ? null : planSql);
        if (trees.first == null) {
            throw new RuntimeException("SPM baseline " + p.getId()
                    + " bindSql cannot be parsed");
        }
        p.setParameterizedBindPlan(trees.first);
        if (!frozen) {
            p.setParameterizedPlanPlan(trees.second);
        }
        return p;
    }

    /**
     * For tests: rebuilds one persisted row exactly like the startup load / periodic
     * refresh does (including the hint-bearing bindSql path, which must be parsed WITHOUT a
     * session and without executing the hint's SET_VAR side effects).
     *
     * @param row the internal-table row
     * @return the parsed row
     * @throws Exception when the row's bindSql cannot be parsed
     */
    @VisibleForTesting
    public static BaselinePlan parsePersistedRowForTest(ResultRow row) throws Exception {
        return parsePersistedRow(row);
    }

    /**
     * Reads every durable row with the given (bind_sql_digest, plan_sql) key. A read
     * failure is rethrown as a retryable error: createBaseline must not fall through to
     * an INSERT while the durable duplicate state is unknown.
     *
     * @param bindSqlDigest the parameterized digest
     * @param planSql       the frozen plan SQL
     * @return the parsed rows (possibly empty)
     */
    private static List<BaselinePlan> readPersistedByKey(String bindSqlDigest, String planSql) {
        Map<String, String> params = new HashMap<>();
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSql", StatisticsUtil.escapeSQL(planSql));
        try {
            List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_BY_KEY_SQL, params);
            List<BaselinePlan> result = new ArrayList<>();
            for (ResultRow row : rows) {
                try {
                    result.add(parsePersistedRow(row));
                } catch (Throwable t) {
                    LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM durable-key read failed (retry the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * Deterministic winner of duplicate rows carrying one id (the internal table is a
     * DUPLICATE-key table: the ALTER-status protocol INSERTs the new-status row before
     * deleting the old-status one, so a failure of BOTH the old-row delete and the
     * compensating delete leaves both rows behind).
     *
     * Rule: the LATER updateTime wins - it is the user's latest intent and, in the
     * failure case above, the newly inserted row. When the timestamps tie (DATETIME has
     * second precision), prefer DISABLED: silently re-enabling a baseline the user tried
     * to disable is the harmful direction (it would start rewriting query plans again),
     * while a failed ENABLE left disabled only means no rewrite - and a retried ENABLE
     * converges. The selection is total and order-independent, so refresh / restart /
     * every FE agree on the same row.
     *
     * @param first  one of the rows
     * @param second the other row
     * @return the row to keep in memory
     */
    @VisibleForTesting
    static BaselinePlan pickDurableWinner(BaselinePlan first, BaselinePlan second) {
        if (first.getUpdateTime() != second.getUpdateTime()) {
            return first.getUpdateTime() > second.getUpdateTime() ? first : second;
        }
        boolean firstDisabled = !first.getStatus().isActive();
        boolean secondDisabled = !second.getStatus().isActive();
        if (firstDisabled != secondDisabled) {
            return firstDisabled ? first : second;
        }
        return first;
    }

    /**
     * Forces an authoritative reload of the internal table (master acquisition): the
     * in-memory cache may have been loaded long BEFORE this FE became master, so it can
     * miss every row the previous master wrote after that load - the create-time key
     * dedup would then miss an existing durable baseline. {@code loaded} is cleared
     * first, so a failed read keeps the lazy-retry state machine intact (the next access
     * retries; mutators fail visibly via ensureLoadedOrThrow until the read succeeds).
     */
    public void forceReloadFromInternalTable() {
        if (!persistenceEnabled()) {
            return;
        }
        invalidatePublishedStore();
        loadFromInternalTable();
    }

    /**
     * Invalidates the published store for an authoritative reload: clears the maps
     * together with {@code loaded}. Clearing ONLY {@code loaded} left the OLD maps visible:
     * if the internal-table read failed, hasBaselines / findCandidateBaselines would retry
     * the load and then still read the populated maps - a newly promoted FE could apply a
     * baseline the previous master had already disabled or dropped. Matching now sees an
     * EMPTY store until a fresh snapshot is atomically published.
     */
    private void invalidatePublishedStore() {
        stateLock.writeLock().lock();
        try {
            loaded = false;
            baselines.clear();
            hashIndex.clear();
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * For tests: the invalidation half of {@link #forceReloadFromInternalTable} (the
     * production caller follows it with the reload).
     */
    @VisibleForTesting
    public void invalidatePublishedStoreForTest() {
        invalidatePublishedStore();
    }

    /**
     * Whether a persisted row differs from the in-memory baseline. Timestamps are
     * intentionally ignored: the internal table stores DATETIME (second precision) while
     * memory keeps epoch millis, so comparing them would always differ and needlessly
     * replace locally created objects every cycle.
     */
    private static boolean persistedContentChanged(BaselinePlan memory, BaselinePlan row) {
        return !Objects.equals(memory.getBindSql(), row.getBindSql())
                || !Objects.equals(memory.getBindSqlDigest(), row.getBindSqlDigest())
                || memory.getBindSqlHash() != row.getBindSqlHash()
                || !Objects.equals(memory.getPlanSql(), row.getPlanSql())
                || !Objects.equals(memory.getQueryId(), row.getQueryId())
                || Double.compare(memory.getCost(), row.getCost()) != 0
                || memory.getQueryTimeMs() != row.getQueryTimeMs()
                || memory.getSource() != row.getSource()
                || memory.getStatus() != row.getStatus();
    }

    private void addToHashIndex(BaselinePlan p) {
        hashIndex.computeIfAbsent(p.getBindSqlHash(), k -> new ArrayList<>()).add(p.getId());
    }

    private void removeFromHashIndex(BaselinePlan p) {
        if (p == null) {
            return;
        }
        List<Long> ids = hashIndex.get(p.getBindSqlHash());
        if (ids != null) {
            ids.remove(p.getId());
            if (ids.isEmpty()) {
                hashIndex.remove(p.getBindSqlHash());
            }
        }
    }

    /** Rebuilds a BaselinePlan from one internal-table row (column order = schema). */
    private static BaselinePlan fromRow(ResultRow row) throws Exception {
        BaselinePlan p = new BaselinePlan();
        p.setId(Long.parseLong(row.get(0)));
        p.setBindSql(row.get(1));
        p.setBindSqlDigest(row.get(2));
        p.setBindSqlHash(Long.parseLong(row.get(3)));
        p.setPlanSql(row.get(4));
        p.setQueryId(row.get(5));
        p.setCost(Double.parseDouble(row.get(6)));
        p.setQueryTimeMs(Long.parseLong(row.get(7)));
        p.setSource(BaselineSource.fromString(row.get(8)));
        p.setStatus(BaselineStatus.fromString(row.get(9)));
        p.setCreateTime(fromTs(row.get(10)));
        p.setUpdateTime(fromTs(row.get(11)));
        return p;
    }

    private static void persistInsert(BaselinePlan p) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(p.getId()));
        params.put("bindSql", StatisticsUtil.escapeSQL(p.getBindSql()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("bindSqlHash", String.valueOf(p.getBindSqlHash()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        params.put("queryId", StatisticsUtil.escapeSQL(p.getQueryId() == null ? "" : p.getQueryId()));
        params.put("cost", String.valueOf(p.getCost()));
        params.put("queryTimeMs", String.valueOf(p.getQueryTimeMs()));
        params.put("source", p.getSource().name());
        params.put("status", p.getStatus().name());
        params.put("createTime", toTs(p.getCreateTime()));
        params.put("updateTime", toTs(p.getUpdateTime()));
        try {
            StatisticsUtil.execUpdate(INSERT_SQL, params);
        } catch (Exception e) {
            throw new RuntimeException("SPM persist (insert) failed: " + e.getMessage(), e);
        }
    }

    private static void persistDeleteByIdentity(BaselinePlan p) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(p.getId()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        try {
            StatisticsUtil.execUpdate(DELETE_BY_IDENTITY_SQL, params);
        } catch (Exception e) {
            throw new RuntimeException("SPM persist (delete) failed: " + e.getMessage(), e);
        }
    }

    /** Removes the row(s) with the given id whose status matches the previous status. */
    private static void persistDeleteByIdAndStatus(long id, BaselineStatus status) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        params.put("status", status.name());
        try {
            StatisticsUtil.execUpdate(DELETE_BY_ID_AND_STATUS_SQL, params);
        } catch (Exception e) {
            throw new RuntimeException("SPM persist (delete by status) failed: " + e.getMessage(), e);
        }
    }

    /** Epoch millis -> internal-table DATETIME literal ('yyyy-MM-dd HH:mm:ss'). */
    private static String toTs(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault())
                .format(TS_FORMAT);
    }

    /** Internal-table DATETIME literal -> epoch millis. */
    private static long fromTs(String ts) {
        return LocalDateTime.parse(ts, TS_FORMAT)
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
