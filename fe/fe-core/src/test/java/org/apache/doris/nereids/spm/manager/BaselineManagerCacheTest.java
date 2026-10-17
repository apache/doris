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
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The matching-path row cache (round-59 refactor):
 *
 * - memory holds the FULL bind_sql_hash set (knownHashes) plus, per hash, its complete
 *   row set only AFTER that hash was matched (or published locally);
 * - in the durable mode a cache miss reads the hash's rows from the internal table and
 *   caches them atomically (all rows of that hash, never a partial set);
 * - the cache is an LRU bounded by spm_baseline_cache_size, so the rows of one hash are
 *   evicted together;
 * - a failed read degrades that single match attempt and is never cached.
 */
public class BaselineManagerCacheTest {

    private final BaselineTestDoubles.TestBaselineStore store =
            new BaselineTestDoubles.TestBaselineStore();
    private final BaselineTestDoubles.TestClusterGateway gateway =
            new BaselineTestDoubles.TestClusterGateway();
    private BaselineManager manager;
    private int originalCacheSize;

    @BeforeEach
    public void setUp() {
        originalCacheSize = VariableMgr.getDefaultSessionVariable().getSpmBaselineCacheSize();
        manager = new BaselineManager(store, gateway, System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
    }

    @AfterEach
    public void tearDown() {
        BaselineManagerTestAccess.clear(manager);
        VariableMgr.getDefaultSessionVariable().setSpmBaselineCacheSize(originalCacheSize);
    }

    private static BaselinePlan row(long id, String digest, long hash, BaselineStatus status) {
        BaselinePlan p = new BaselinePlan();
        p.setId(id);
        p.setBindSql("SELECT 1 FROM t WHERE k = ?");
        p.setBindSqlDigest(digest);
        p.setBindSqlHash(hash);
        p.setPlanSql("SELECT 1 FROM t WHERE k = _spm_const_var(1)");
        p.setCost(1.0D);
        p.setQueryTimeMs(-1L);
        p.setStatus(status);
        return p;
    }

    /**
     * The load path keeps hashes only; the rows of a hash enter the cache exactly when
     * that hash is matched, and the LRU evicts whole hash entries.
     */
    @Test
    public void testOnlyMatchedRowsAreCachedAndLruEvictsWholeHashes() {
        store.durableOverride = true;
        VariableMgr.getDefaultSessionVariable().setSpmBaselineCacheSize(2);

        manager.applyRefreshedBaselines(Map.of(
                101L, row(101L, "digest_a", 0xA1L, BaselineStatus.ENABLED),
                102L, row(102L, "digest_b", 0xB2L, BaselineStatus.ENABLED),
                103L, row(103L, "digest_c", 0xC3L, BaselineStatus.ENABLED)));
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager),
                "the loaded state keeps the hash set only, no rows");

        Map<Long, AtomicInteger> reads = new HashMap<>();
        store.byHashReader = hash -> {
            reads.computeIfAbsent(hash, h -> new AtomicInteger()).incrementAndGet();
            switch ((int) hash) {
                case 0xA1:
                    return List.of(row(101L, "digest_a", 0xA1L, BaselineStatus.ENABLED));
                case 0xB2:
                    return List.of(row(102L, "digest_b", 0xB2L, BaselineStatus.ENABLED));
                default:
                    return List.of(row(103L, "digest_c", 0xC3L, BaselineStatus.ENABLED));
            }
        };

        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCount(manager),
                "only the matched hash left a cache entry");

        // a second match of the SAME hash is served from the cache: no re-read
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
        Assertions.assertEquals(1, reads.get(0xA1L).get());

        // a second distinct hash fills the second slot, a third pushes the least recently
        // used entry (0xA1) out
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_b", 0xB2L).size());
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_c", 0xC3L).size());
        Assertions.assertEquals(2, BaselineManagerTestAccess.cachedHashCount(manager),
                "the capacity of 2 holds exactly two hash entries");
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size(),
                "the evicted hash is read again");
        Assertions.assertEquals(2, reads.get(0xA1L).get(),
                "0xA1 was evicted: its rows had to be read a second time");
        Assertions.assertEquals(1, reads.get(0xB2L).get(),
                "0xB2 was not touched after its own match: still cached");
        Assertions.assertEquals(2, BaselineManagerTestAccess.cachedHashCount(manager),
                "re-reading 0xA1 evicted the least recently used entry (0xB2)");
    }

    /**
     * One hash entry carries EVERY row of that bind_sql (the reviewer requirement: the
     * cache of one bindSql must be complete): a second id of the same hash is served
     * from the same entry without another read - even when it does not participate in
     * the match (the disabled twin).
     */
    @Test
    public void testOneHashIsCachedCompletely() {
        store.durableOverride = true;
        BaselinePlan enabled = row(201L, "digest_x", 0xA1L, BaselineStatus.ENABLED);
        BaselinePlan disabled = row(202L, "digest_x", 0xA1L, BaselineStatus.DISABLED);
        disabled.setPlanSql("SELECT 1 FROM t WHERE k = _spm_const_var(2)");
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            return List.of(enabled, disabled);
        };
        manager.applyRefreshedBaselines(Map.of(201L, enabled, 202L, disabled));

        List<BaselinePlan> matched = manager.findCandidateBaselines("digest_x", 0xA1L);
        Assertions.assertEquals(1, matched.size(), "only the enabled twin is a candidate");
        Assertions.assertEquals(201L, matched.get(0).getId());
        Assertions.assertEquals(1, reads.get());

        Assertions.assertEquals(202L, manager.getBaseline(202L).getId(),
                "the disabled twin of the SAME hash was cached with the match");
        Assertions.assertEquals(1, reads.get(),
                "no second read: the entry holds the complete row set of the hash");
    }

    /**
     * Round-65 review: a handoff can leave the SAME baseline key at two ids (40 enabled /
     * 41 disabled). Snapshot publication hides the older allocation
     * (collapseDuplicateKeyRows), but the RAW by-hash read of a cache miss only chose one
     * winner per id - BOTH ids survived, the active-status filter then dropped the newer
     * DISABLED id 41 and the superseded ENABLED id 40 was replayed although
     * ALTER BASELINE PLAN 41 DISABLE had completed. The matching path must apply the
     * snapshot's cross-id collapse to its own read.
     */
    @Test
    public void testCrossIdSameKeyTwinsCollapseOnCacheMiss() {
        store.durableOverride = true;
        // the local state knows only the newer allocation (id 41, disabled)
        BaselinePlan disabled41 = row(41L, "digest_h", 0xD4L, BaselineStatus.DISABLED);
        manager.applyRefreshedBaselines(Map.of(41L, disabled41));
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager),
                "precondition: a durable load keeps the hash only, no rows");

        // the raw read still serves the two allocations of the SAME key (identical
        // digest / planSql / fingerprint): only the id and the status differ
        BaselinePlan enabled40 = row(40L, "digest_h", 0xD4L, BaselineStatus.ENABLED);
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            return List.of(enabled40, disabled41);
        };

        Assertions.assertTrue(manager.findCandidateBaselines("digest_h", 0xD4L).isEmpty(),
                "the superseded ENABLED twin must not be replayed after id 41 was"
                        + " disabled");
        Assertions.assertEquals(1, reads.get(), "the read itself must have happened");
    }

    /**
     * A failing by-hash read degrades that single attempt (empty result) and must never
     * leave a partial / empty cache entry behind: the next match reads again.
     */
    @Test
    public void testFailedReadIsNeverCached() {
        store.durableOverride = true;
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            throw new RuntimeException("table unavailable");
        };
        manager.applyRefreshedBaselines(Map.of(
                301L, row(301L, "digest_e", 0xE1L, BaselineStatus.ENABLED)));

        Assertions.assertTrue(manager.findCandidateBaselines("digest_e", 0xE1L).isEmpty());
        Assertions.assertEquals(1, reads.get());
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager),
                "a failed read leaves no cache entry");

        Assertions.assertTrue(manager.findCandidateBaselines("digest_e", 0xE1L).isEmpty());
        Assertions.assertEquals(2, reads.get(), "the miss was not negative-cached");
    }

    /**
     * The in-memory (non-durable) mode - the one FE unit tests and the seam-driven paths
     * run in - keeps whole rows in the cache, one complete entry per published hash.
     */
    @Test
    public void testInMemoryModeCachesPublishedHashesCompletely() {
        // the twins must differ in their stored plan: identical (digest, planSql) rows of
        // one key are merged by the duplicate-key collapse, not by the cache
        BaselinePlan active = row(401L, "digest_f", 0xF1L, BaselineStatus.ENABLED);
        BaselinePlan disabled = row(402L, "digest_f", 0xF1L, BaselineStatus.DISABLED);
        disabled.setPlanSql("SELECT 1 FROM t WHERE k = _spm_const_var(2)");
        manager.applyRefreshedBaselines(Map.of(401L, active, 402L, disabled));

        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCount(manager));
        Assertions.assertEquals(402L, manager.getBaseline(402L).getId());
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_f", 0xF1L).size());
        Assertions.assertEquals(2, manager.getAllBaselines().size());
    }

    // ==================== shallow snapshot decode + lazy trees ====================

    /**
     * The snapshot decode (the load / refresh loop, see
     * BaselineManager#collectSnapshotRows) is SHALLOW: it reads the persisted SCALARS
     * only, because the hash index, the mutation-fence settlement, the drop-tombstone
     * filter, the duplicate-key collapse and the id watermark need no parsed SQL - a
     * durable-mode load keeps the HASHES and never the rows, so rebuilding every row's
     * trees was pure load overhead. The row is MARKED instead, and its trees are rebuilt
     * when it enters the match cache (see the tests below).
     */
    @Test
    public void testSnapshotRowsAreDecodedShallowly() throws Exception {
        BaselinePlan decoded = decodeSnapshotRow(persistedRow(101L, "digest_a", 0xA1L,
                "select k from db.t1", "select k from db.t1 where k = 1",
                BaselineStatus.ENABLED));

        Assertions.assertTrue(decoded.isTreesPending(),
                "the snapshot decode marks the row: its transient trees are rebuilt lazily");
        Assertions.assertNull(decoded.getParameterizedBindPlan(),
                "the snapshot decode must not parse the bindSql");
        Assertions.assertNull(decoded.getParameterizedPlanPlan(),
                "the snapshot decode must not parse the planSql");
        // every scalar the load-time invariants read is present
        Assertions.assertEquals(101L, decoded.getId());
        Assertions.assertEquals("digest_a", decoded.getBindSqlDigest());
        Assertions.assertEquals(0xA1L, decoded.getBindSqlHash());
        Assertions.assertEquals(BaselineStatus.ENABLED, decoded.getStatus());
        Assertions.assertEquals("select k from db.t1", decoded.getBindSql());
        Assertions.assertEquals("select k from db.t1 where k = 1", decoded.getPlanSql());
        Assertions.assertEquals(1767225600000L, decoded.getUpdateTime(),
                "the stored second is decoded for the fence / winner rules");
    }

    /**
     * A snapshot row that ENTERS a cached entry is deepened exactly there: the merged row
     * can serve the Level 3 match / the placeholder substitution, while the rows of hashes
     * that were never matched stay unparsed.
     */
    @Test
    public void testRowEnteringTheCacheIsDeepenedLazily() throws Exception {
        store.durableOverride = true;
        BaselinePlan seeded = row(101L, "digest_a", 0xA1L, BaselineStatus.ENABLED);
        // the load publishes the hash index; the durable mode keeps NO rows
        manager.applyRefreshedBaselines(Map.of(101L, seeded));
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager),
                "precondition: the load kept the hash only");

        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            return List.of(seeded);
        };
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCount(manager));

        // the refresh hands the SAME hash a second row, decoded shallowly like the snapshot
        // loop does
        BaselinePlan shallow = decodeSnapshotRow(persistedRow(102L, "digest_b", 0xA1L,
                "select k from db.t2", "select k from db.t2 where k = 1",
                BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(101L, seeded, 102L, shallow));

        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCount(manager),
                "the hash keeps its entry");
        Assertions.assertFalse(shallow.isTreesPending(),
                "the row entering the entry is deepened");
        Assertions.assertNotNull(shallow.getParameterizedBindPlan(),
                "a cached row must be able to serve the Level 3 match");
        Assertions.assertNotNull(shallow.getParameterizedPlanPlan(),
                "and the plan tree the placeholder substitution rewrites");
        Assertions.assertSame(seeded, manager.getBaseline(101L),
                "the unchanged row keeps its cached object");
        Assertions.assertEquals(1, reads.get(),
                "no by-hash re-read: the merge served the entry");
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_b", 0xA1L).size(),
                "the deepened row is a candidate now");
    }

    /**
     * A snapshot row whose trees cannot be rebuilt must never enter the cache: it cannot
     * serve a match (the substitution would run against a null tree), so it is skipped
     * exactly like the former eager snapshot parse skipped unparsable rows.
     */
    @Test
    public void testUnusableSnapshotRowNeverEntersTheCache() throws Exception {
        store.durableOverride = true;
        BaselinePlan seeded = row(101L, "digest_a", 0xA1L, BaselineStatus.ENABLED);
        // the load publishes the hash index; the durable mode keeps NO rows
        manager.applyRefreshedBaselines(Map.of(101L, seeded));
        store.byHashReader = hash -> List.of(seeded);
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());

        BaselinePlan broken = decodeSnapshotRow(persistedRow(103L, "digest_bad", 0xA1L,
                "select k from", "select k from db.t3 where k = 1",
                BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(101L, seeded, 103L, broken));

        Assertions.assertTrue(broken.isTreesPending(),
                "the trees of the unusable row stay unbuilt");
        Assertions.assertNull(broken.getParameterizedBindPlan());
        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCount(manager),
                "the hash entry survives the unusable row");
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size(),
                "the entry still serves the usable row");
        Assertions.assertTrue(manager.findCandidateBaselines("digest_bad", 0xA1L).isEmpty(),
                "the unusable row must never become a candidate");
    }

    /**
     * One spm_baselines row in the SNAPSHOT column order (see BaselineManager#SNAPSHOT_COLUMNS):
     * the TIMESTAMPTZ columns are read as epoch millis, the mode / provenance columns carry
     * the values a CREATE persists.
     */
    private static ResultRow persistedRow(long id, String digest, long hash, String bindSql,
            String planSql, BaselineStatus status) {
        return new ResultRow(List.of(
                String.valueOf(id),                            // id
                bindSql,                                       // bind_sql
                digest,                                        // bind_sql_digest
                String.valueOf(hash),                          // bind_sql_hash
                planSql,                                       // plan_sql
                "qid",                                         // query_id
                "1.0",                                         // cost
                "-1",                                          // query_time_ms
                "USER",                                        // source
                status.name(),                                 // status
                "1767225600000",                               // create_time
                "1767225600000",                               // update_time
                String.valueOf(SqlModeHelper.MODE_DEFAULT),    // sql_mode
                String.valueOf(SqlModeHelper.MODE_DEFAULT),    // plan_sql_mode
                "false",                                       // plan_frozen
                ""));                                           // schema_fingerprint
    }

    /** Decodes one persisted row through the production snapshot loop (shallow). */
    private static BaselinePlan decodeSnapshotRow(ResultRow persisted) throws Exception {
        Map<Long, List<BaselinePlan>> rowsById = BaselineManagerTestAccess.collectSnapshotRows(
                (pageStart, offset) -> pageStart == null ? List.of(persisted) : List.of(),
                1, new AtomicLong());
        return rowsById.values().iterator().next().get(0);
    }
}
