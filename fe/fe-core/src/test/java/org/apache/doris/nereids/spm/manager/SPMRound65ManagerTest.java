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
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Round-65 review fixes on the manager:
 *
 * - #4 a DROP must also retire the KEY's superseded same-key twins: leader repair is
 *   best-effort (a follower cannot repair at all), so dropping the collapsed winner 41
 *   used to leave enabled 40 durable - the next cold lookup replayed the baseline that
 *   was just removed;
 * - #10 an UNKNOWN row probe must NOT condemn a CREATE reservation: after the 5-minute
 *   idle window, one failing by-id read durably tombstoned a healthy identity; only a
 *   successful ABSENT read may reach the expiry handling, UNKNOWN fails retryably;
 * - #11 an UNCACHED DROP must advance the hash's mutation epoch: a by-hash load that read
 *   the row before the DROP pauses between its reconciliation and the publication lock;
 *   the unchanged epoch let it publish and cache the dropped ENABLED row, so later cache
 *   hits served a dropped baseline until the next refresh;
 * - #12 an EMPTY schema fingerprint (a TVF-only baseline) cannot be matched by
 *   IFNULL(schema_fingerprint,'')='' - DeleteFromCommand rejects it - so the DELETE
 *   predicates use the supported `IS NULL` / `= ''` forms; the by-status flip deletes
 *   BOTH stored shapes.
 */
public class SPMRound65ManagerTest {

    // ==================== #11 / #4: a substituted durable table ====================

    /**
     * A minimal durable table for the DROP-side scenarios: by-id and raw by-hash reads
     * (the same seam the live TablePersistence implements) plus the append-only
     * tombstone log. The by-hash read is scripted so a test can pause a load inside it.
     */
    private static final class Round65Store implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** Raw by-hash read; the default reads every durable row of the hash. */
        java.util.function.Function<Long, List<BaselinePlan>> hashReads = hash -> {
            List<BaselinePlan> found = new ArrayList<>();
            for (BaselinePlan row : rows.values()) {
                if (row.getBindSqlHash() == hash) {
                    found.add(row);
                }
            }
            return found;
        };
        /** One id whose identity DELETE fails (the best-effort leader repair case). */
        long failDeletesForId = -1L;

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public boolean hashReadsAvailable() {
            return true;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash) {
            return hashReads.apply(hash);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (plan.getId() == failDeletesForId) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            rows.remove(plan.getId());
        }

        @Override
        public boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            tombstones.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
            return true;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            Set<String> found = new java.util.HashSet<>();
            for (String tombstone : tombstones) {
                int cut = tombstone.indexOf('|');
                if (ids.contains(Long.parseLong(tombstone.substring(0, cut)))) {
                    found.add(tombstone);
                }
            }
            return found;
        }

        @Override
        public long tableWatermark() {
            long max = 0;
            for (BaselinePlan row : rows.values()) {
                max = Math.max(max, row.getId());
            }
            return max;
        }
    }

    private static BaselinePlan row(long id, String digest, long hash, BaselineStatus status,
            long updateTime) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(hash);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(status);
        plan.setSchemaFingerprint("t1|100|k:int|NN");
        plan.setCreateTime(updateTime);
        plan.setUpdateTime(updateTime);
        return plan;
    }

    private static BaselineManager newManager(BaselinePersistence store) {
        BaselineManager created = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(created);
        return created;
    }

    /**
     * Hash H is known but its only row is EVICTED. A query misses H, reads its ENABLED
     * row and pauses inside the read. The DROP of that (uncached) row completes -
     * deletes it, appends its tombstone and returns. The paused load must then DISCARD
     * its stale rows: the DROP advanced H's mutation epoch under the state lock, so the
     * publication gate fails. Before the fix the load published and CACHED the dropped
     * row, and every later cache hit replayed the dropped baseline until the next
     * refresh.
     */
    @Test
    public void testUncachedDropDiscardsAnInFlightHashLoad() throws Exception {
        Round65Store store = new Round65Store();
        BaselineManager manager = newManager(store);
        BaselinePlan enabled = row(10L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        store.rows.put(10L, enabled);
        manager.applyRefreshedBaselines(Map.of(10L, enabled));
        // force the CACHE-MISS DROP branch deterministically (no epoch change here)
        BaselineManagerTestAccess.evictRowFromCacheOnly(manager, 10L);
        Assertions.assertNull(BaselineManagerTestAccess.rawCacheGet(manager, 10L),
                "the row must be evicted for the uncached DROP path to run");

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch readerRelease = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        store.hashReads = hash -> {
            if (reads.incrementAndGet() == 1) {
                readerEntered.countDown();
                try {
                    readerRelease.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // the row as it was READ, before the DROP lands
                return List.of(row(10L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L));
            }
            return store.rows.containsKey(10L)
                    ? List.of(store.rows.get(10L)) : List.of();
        };

        AtomicReference<List<BaselinePlan>> loadResult = new AtomicReference<>();
        Thread loader = new Thread(() ->
                loadResult.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        loader.start();
        Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "the by-hash load must be in flight");

        // the UNCACHED DROP: deletes the durable row, confirms its tombstone, returns
        Assertions.assertTrue(manager.dropBaseline(10L),
                "the uncached DROP must delete the durable row");
        Assertions.assertFalse(store.rows.containsKey(10L));

        readerRelease.countDown();
        loader.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertTrue(loadResult.get().isEmpty(),
                "the paused load must discard its pre-DROP rows (the DROP advanced the"
                        + " hash epoch while it waited for the publication lock)");
        Assertions.assertEquals(0,
                BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                "the dropped row must not be CACHED by the discarded load");

        // the next lookup re-reads the table (no stale cache hit) and finds nothing
        int readsBeforeSecondLookup = reads.get();
        Assertions.assertTrue(
                manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                "later statements must not be served the dropped baseline from the cache");
        Assertions.assertTrue(reads.get() > readsBeforeSecondLookup,
                "the second lookup must read the table instead of a stale cached bucket");
    }

    // ==================== #4: the superseded same-key twin ====================

    /**
     * A handoff leaves the same baseline key enabled at ids 40 and 41 (matching collapses
     * them to 41). Dropping the winner 41 before a (best-effort) repair must not leave 40
     * durable: the cache only ever carries the COLLAPSED winner, so the cleanup reads the
     * raw durable by-hash rows and retires the twin with its own tombstone.
     */
    @Test
    public void testDropRetiresTheSupersededSameKeyTwin() {
        Round65Store store = new Round65Store();
        BaselineManager manager = newManager(store);
        BaselinePlan twin = row(40L, "digest_t", 0x71L, BaselineStatus.ENABLED, 1_000L);
        BaselinePlan winner = row(41L, "digest_t", 0x71L, BaselineStatus.ENABLED, 2_000L);
        store.rows.put(40L, twin);
        store.rows.put(41L, winner);
        // The leader repair of the refresh is BEST-EFFORT: make it fail so the twin stays
        // durable, exactly the state a DROP can run in before a successful repair.
        store.failDeletesForId = 40L;
        manager.applyRefreshedBaselines(Map.of(40L, twin, 41L, winner));
        store.failDeletesForId = -1L;
        Assertions.assertTrue(store.rows.containsKey(40L),
                "precondition: the best-effort repair could not retire the twin");

        List<BaselinePlan> matched = manager.findCandidateBaselines("digest_t", 0x71L);
        Assertions.assertEquals(1, matched.size(),
                "precondition: the same-key twins collapse to one candidate");
        Assertions.assertEquals(41L, matched.get(0).getId(),
                "precondition: the deterministic winner is the newer id");

        Assertions.assertTrue(manager.dropBaseline(41L));
        Assertions.assertFalse(store.rows.containsKey(41L));
        Assertions.assertFalse(store.rows.containsKey(40L),
                "the superseded same-key twin must be retired durably with the winner: a"
                        + " cold lookup must not replay the baseline that was just dropped");
        Assertions.assertTrue(store.tombstones.stream().anyMatch(t -> t.startsWith("40|digest_t|")),
                "the twin's identity gets its own tombstone so an in-flight write cannot"
                        + " revive it");
        Assertions.assertTrue(manager.findCandidateBaselines("digest_t", 0x71L).isEmpty(),
                "the next cold lookup sees no row of the dropped key");
    }

    // ==================== #10: UNKNOWN must not condemn a reservation ====================

    /** In-memory identity store modeling the durable tables #10 touches. */
    private static final class IdentityStore implements BaselineTestDoubles.IdAllocatorStore {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        /** The latest sequence-table record per key: {id, atMillis, unconfirmed, dropped}. */
        final Map<String, long[]> records = new ConcurrentHashMap<>();
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** A by-id read fails (an unavailable tablet): probeRow answers UNKNOWN. */
        boolean failReads;
        long reservedHighWater;

        private static String key(String digest, long planSqlHash) {
            return digest + '\u0001' + planSqlHash;
        }

        @Override
        public long watermark() {
            long max = 0;
            for (BaselinePlan row : rows.values()) {
                max = Math.max(max, row.getId());
            }
            return max;
        }

        @Override
        public long seqWatermark() {
            return reservedHighWater;
        }

        @Override
        public void reserveId(long id) {
            reservedHighWater = Math.max(reservedHighWater, id);
        }

        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMs) {
            reserveId(id);
            records.put(key(bindSqlDigest, planSqlHash),
                    new long[] {id, reserveTimeMs, 0, 0});
        }

        @Override
        public BaselinePersistence.SeqReservation pendingSeqReservation(
                String bindSqlDigest, long planSqlHash) {
            long[] entry = records.get(key(bindSqlDigest, planSqlHash));
            return entry == null ? null : new BaselinePersistence.SeqReservation(
                    entry[0], entry[1], entry[2] == 1, entry[3] == 1);
        }

        @Override
        public void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            tombstones.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
        }

        @Override
        public List<String> droppedMarkers() {
            return new ArrayList<>(tombstones);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (failReads) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            rows.remove(plan.getId());
        }

        /** Ages every reservation record: the durable fence runs on reserve_time. */
        void ageAllRecords(long millis) {
            long aged = System.currentTimeMillis() - millis;
            for (Map.Entry<String, long[]> entry : records.entrySet()) {
                long[] record = entry.getValue();
                entry.setValue(new long[] {record[0], aged, record[2], record[3]});
            }
        }
    }

    private static BaselinePlan baseline(String digest, String planSql) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(7);
        plan.setPlanSql(planSql);
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        return plan;
    }

    /**
     * Every successful CREATE leaves a (plain) reservation; once it is idle for more than
     * five minutes, an identical CREATE probes the reserved id. When that read FAILS the
     * outcome is UNPROVEN - the expiry handling used to condemn the healthy identity (a
     * durable tombstone for the previous row) and allocate a SECOND id, even though the
     * retry never proved anything absent. UNKNOWN now fails the statement retryably and
     * changes nothing; the retry (with a working read) must find the row PRESENT and
     * ADOPT the reserved id instead of reallocating.
     */
    @Test
    public void testUnknownProbeDoesNotCondemnTheReservation() {
        BaselineTestDoubles.TestBaselineStore store =
                new BaselineTestDoubles.TestBaselineStore();
        IdentityStore identity = new IdentityStore();
        store.idAllocatorStore = identity;
        BaselineManager manager = newManager(store);

        BaselinePlan plan = baseline("digest_p", "plan_p");
        long firstId = manager.createBaseline(plan);
        Assertions.assertEquals(1L, firstId, "precondition: the first CREATE succeeds");
        Assertions.assertEquals(1, identity.rows.size());
        // MORE than the 5-minute idle window
        identity.ageAllRecords(TimeUnit.MINUTES.toMillis(10));

        // the by-id read now fails: the probe answers UNKNOWN
        identity.failReads = true;
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.createBaseline(baseline("digest_p", "plan_p")));
        Assertions.assertTrue(failure.getMessage().contains("UNKNOWN"),
                "the failure must name the unprovable probe: " + failure.getMessage());
        Assertions.assertTrue(identity.tombstones.isEmpty(),
                "no tombstone may be appended for an UNPROVEN absence");
        Assertions.assertEquals(1, identity.rows.size(),
                "no replacement row may be written");
        Assertions.assertEquals(1L, identity.rows.get(1L).getId());
        Assertions.assertEquals(0, identity.records.values().iterator().next()[3],
                "the reservation must not be marked dropped");

        // the retry reads again and finds the reserved row PRESENT: it ADOPTS the id
        identity.failReads = false;
        long adoptedId = manager.createBaseline(baseline("digest_p", "plan_p"));
        Assertions.assertEquals(1L, adoptedId,
                "the retry must adopt the reserved identity instead of allocating a second id");
        Assertions.assertEquals(1, identity.rows.size());
        Assertions.assertTrue(identity.tombstones.isEmpty());
    }

    // ==================== #12: supported DELETE predicates for an empty fingerprint ====================

    /**
     * A TVF-only baseline stores an EMPTY schema fingerprint, and
     * IFNULL(schema_fingerprint,'')='' is rejected by DeleteFromCommand (a function on the
     * comparison's left side / the rewritten OR); the fallback supports UNIQUE KEY tables
     * only. The predicates use the supported simple forms instead: separate IS NULL /
     * = '' deletes, and the by-status flip deletes BOTH stored shapes.
     */
    @Test
    public void testEmptyFingerprintDeletesUseSupportedPredicates() throws Exception {
        for (String name : List.of("DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT",
                "DELETE_BY_ID_AND_STATUS_SQL_NULL_FINGERPRINT")) {
            Field field = BaselineManager.class.getDeclaredField(name);
            field.setAccessible(true);
            String sql = (String) field.get(null);
            Assertions.assertTrue(sql.contains("`schema_fingerprint` IS NULL"),
                    name + " must match the NULL shape with IS NULL: " + sql);
            Assertions.assertFalse(sql.contains("IFNULL"),
                    name + " must not use a function left side: " + sql);
        }
        for (String name : List.of("DELETE_BY_IDENTITY_SQL_EMPTY_FINGERPRINT",
                "DELETE_BY_ID_AND_STATUS_SQL_EMPTY_FINGERPRINT")) {
            Field field = BaselineManager.class.getDeclaredField(name);
            field.setAccessible(true);
            String sql = (String) field.get(null);
            Assertions.assertTrue(sql.contains("`schema_fingerprint` = ''"),
                    name + " must match the empty-string shape with a plain equality: " + sql);
            Assertions.assertFalse(sql.contains("IFNULL"),
                    name + " must not use a function left side: " + sql);
        }

        Method factory = BaselineManager.class.getDeclaredMethod("deleteByIdAndStatusSql",
                long.class, String.class);
        factory.setAccessible(true);
        List<?> emptyFingerprint = (List<?>) factory.invoke(null, 0L, "");
        Assertions.assertEquals(2, emptyFingerprint.size(),
                "an empty fingerprint needs BOTH simple predicates (NULL and '')");
        @SuppressWarnings("unchecked")
        List<String> versions = (List<String>) factory.invoke(null, 1_700_000_000_000L, "");
        Assertions.assertEquals(2, versions.size());
        Assertions.assertTrue(((String) versions.get(0)).contains("`update_time` = '"),
                "the version predicate must bound both variants: " + versions.get(0));
        List<?> normal = (List<?>) factory.invoke(null, 0L, "t|100|k:int|NN");
        Assertions.assertEquals(1, normal.size(),
                "a non-empty fingerprint keeps the single predicate");
        Assertions.assertTrue(((String) normal.get(0)).contains("`schema_fingerprint`"));
    }
}
