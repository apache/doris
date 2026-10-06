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
import org.apache.doris.nereids.spm.SPMUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Round-44 review fixes of the durable create / drop fences:
 *
 * - #3: the forwarded CREATE's outcome expectation is identified by the STATEMENT QUERY
 *   ID (the master persists the DECOMPILED plan text, so the submitted planSql only
 *   matches the raw-fallback rows);
 * - #4: a failed DROP writes NO deletion marker before absence is proven - the DELETE may
 *   have failed before commit and the live row must not be hidden;
 * - #5: the plain pre-INSERT reservation of every attempt is an IDENTITY record: while
 *   its row is unreadable and the record young it fences a cross-FE retry even when the
 *   explicit unconfirmed-marker write failed / lagged;
 * - #6: an EXPIRED reservation is condemned with an append-only tombstone BEFORE a fresh
 *   id is allocated, so a late publication of the abandoned write is repaired away
 *   instead of becoming a second ENABLED row.
 */
public class BaselineManagerRound44Test {

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

    /** In-memory identity store modeling the durable tables these fixes touch. */
    private static final class Store implements BaselineManager.IdAllocatorStoreForTest {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        /** The LATEST sequence-table record per key: {id, atMillis, unconfirmed, dropped}. */
        final Map<String, long[]> records = new ConcurrentHashMap<>();
        /** Every appended tombstone as {@code id|digest|planSqlHash} (append-only). */
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** Every INSERT fails BEFORE committing (an ambiguous write). */
        boolean failInsert;
        /**
         * The UNCONFIRMED-marker write fails / lags (round-44 #5's gap): only the plain
         * pre-INSERT reservation is left as the identity record.
         */
        boolean suppressMarkers;
        /** The DELETE reports an error WITHOUT removing the row (failed before commit). */
        boolean failDeleteKeepingRow;
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

        /** The plain pre-INSERT reservation (round-44 #5): an identity record. */
        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMs) {
            reserveId(id);
            records.compute(key(bindSqlDigest, planSqlHash), (k, current) ->
                    current != null && current[1] > reserveTimeMs
                            ? current : new long[] {id, reserveTimeMs, 0, 0});
        }

        @Override
        public void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
            if (suppressMarkers) {
                return; // the marker write failed / lagged - the reviewer's round-44 #5 gap
            }
            records.put(key(bindSqlDigest, planSqlHash), new long[] {id, atMillis, 1, 0});
        }

        @Override
        public BaselineManager.SeqReservation pendingSeqReservation(String bindSqlDigest,
                long planSqlHash) {
            long[] entry = records.get(key(bindSqlDigest, planSqlHash));
            return entry == null ? null : new BaselineManager.SeqReservation(
                    entry[0], entry[1], entry[2] == 1, entry[3] == 1);
        }

        @Override
        public void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            records.put(key(bindSqlDigest, planSqlHash), new long[] {id, atMillis, 0, 1});
            tombstones.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
        }

        @Override
        public List<String> droppedMarkers() {
            return new ArrayList<>(tombstones);
        }

        @Override
        public void insert(BaselinePlan plan) {
            if (failInsert) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            rows.put(plan.getId(), plan);
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (failDeleteKeepingRow) {
                throw new RuntimeException("delete reported an error (statement timed out)");
            }
            rows.remove(plan.getId());
        }

        /** Ages the key's identity record (the durable fence runs on reserve_time). */
        void ageRecord(String digest, String planSql, long millis) {
            records.compute(key(digest, SPMUtils.hashOf(planSql)), (k, current) ->
                    current == null ? null : new long[] {current[0], current[1] - millis,
                            current[2], current[3]});
        }
    }

    // ==================== #5: the reservation itself fences ====================

    /**
     * The reviewer's gap: the baseline INSERT commits as id N and stays unreadable, but
     * the separate UNCONFIRMED-marker write fails / lags. After a leader handoff the
     * retry saw neither N nor a readable marker, allocated N+1, and BOTH rows later
     * published ENABLED. The pre-INSERT reservation carries the same identity, so it must
     * fence the retry on its own.
     */
    @Test
    public void testPlainReservationFencesACrossFeRetryWithoutTheMarker() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        Store store = new Store();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            store.failInsert = true;
            store.suppressMarkers = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-p5", "p-p5")));
            long reserved = store.reservedHighWater;
            Assertions.assertTrue(reserved > 0, "the attempt reserved an id");
            BaselineManager.SeqReservation record = store.pendingSeqReservation(
                    "d-p5", SPMUtils.hashOf("p-p5"));
            Assertions.assertNotNull(record,
                    "the plain reservation IS the durable identity record");
            Assertions.assertFalse(record.unconfirmed,
                    "no marker was written - the plain reservation carries the fence");

            // a NEW leader: its in-memory registry is empty, the shared table is not
            manager.clearForTest();
            BaselineManager.idAllocatorStoreForTest = store;
            IllegalStateException deferred = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-p5", "p-p5")));
            Assertions.assertTrue(deferred.getMessage().contains("awaiting publication"),
                    "the plain reservation must defer the retry: " + deferred.getMessage());
            Assertions.assertEquals(reserved, store.reservedHighWater,
                    "the retry must NOT allocate a second id while the write is unresolved");

            // the committed row publishes: the retry adopts the reserved id
            BaselinePlan committed = baseline("d-p5", "p-p5");
            committed.setId(reserved);
            store.rows.put(reserved, committed);
            store.failInsert = false;
            long adopted = manager.createBaseline(baseline("d-p5", "p-p5"));
            Assertions.assertEquals(reserved, adopted,
                    "the reserved id must survive as the single row");
            Assertions.assertEquals(1, store.rows.size(),
                    "exactly one row exists - a second id was never consumed");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== #6: expire, condemn, then reallocate ====================

    /**
     * The reviewer's case: the first leader commits id N (or its transaction lingers
     * unreadable), the row stays unreadable past the fence, a new leader creates the same
     * key as N+1, and BOTH ENABLED rows appear when the first publication recovers.
     * Elapsed time is not a terminal outcome: the abandoned identity is CONDEMNED with an
     * append-only tombstone before the fresh id is allocated, so the late publication is
     * treated as deleted (and repaired away).
     */
    @Test
    public void testExpiredReservationIsCondemnedBeforeAFreshIdIsAllocated() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        Store store = new Store();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            store.failInsert = true;
            store.suppressMarkers = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-p6", "p-p6")));
            long abandoned = store.reservedHighWater;
            Assertions.assertTrue(abandoned > 0);

            // the fence elapsed (beyond the durable bound): a NEW leader (empty in-memory
            // registry) must condemn the abandoned identity and only THEN allocate a
            // fresh id
            manager.clearForTest();
            BaselineManager.idAllocatorStoreForTest = store;
            store.ageRecord("d-p6", "p-p6", 6 * 60 * 1000L);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-p6", "p-p6")));
            long fresh = store.reservedHighWater;
            Assertions.assertTrue(fresh > abandoned,
                    "a fresh id is allocated after the fence expired: " + fresh);
            String condemned = abandoned + "|d-p6|" + SPMUtils.hashOf("p-p6");
            Assertions.assertTrue(store.tombstones.contains(condemned),
                    "the abandoned identity must be condemned before the reallocation: "
                            + store.tombstones);

            // the abandoned write publishes LATE: the tombstone makes every LOAD treat it
            // as deleted (and repair it away), so it can never become a second row
            BaselinePlan late = baseline("d-p6", "p-p6");
            late.setId(abandoned);
            store.rows.put(abandoned, late);
            BaselineManager.snapshotReaderForTest = () -> Map.of(abandoned, late);
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertTrue(store.rows.isEmpty(),
                    "the late publication must be repaired away: " + store.rows.keySet());
            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "no dual ENABLED rows may exist for one key");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== #4: no tombstone before absence is proven ====================

    /**
     * The reviewer's case: the DELETE fails BEFORE commit, the baseline row stays
     * readable, and the old code still appended a durable dropped marker - the next load
     * treated that live row as deleted, hid it and issued a repair DELETE, silently
     * completing a DROP that had reported failure. The tombstone is withheld until a
     * snapshot proves the row gone.
     */
    @Test
    public void testFailedDeleteKeepsTheLiveRowAndTombstonesOnlyAfterAbsence() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        Store store = new Store();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long id = manager.createBaseline(baseline("d-p4", "p-p4"));
            Assertions.assertEquals(1, store.rows.size(), "precondition: the row is durable");

            store.failDeleteKeepingRow = true;
            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(id));
            Assertions.assertEquals(1, store.rows.size(),
                    "the failed DELETE left the row readable: " + store.rows.keySet());
            Assertions.assertTrue(store.tombstones.isEmpty(),
                    "NO deletion marker before absence is proven: " + store.tombstones);
            Assertions.assertFalse(manager.getAllBaselines().stream()
                            .anyMatch(row -> row.getId() == id),
                    "the possibly-deleted row must stop matching immediately");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the outcome stays fenced until a snapshot proves it");

            // the delete turns out to have committed: a snapshot proving ABSENCE resolves
            // the fence and completes the drop's tombstone (the delayed-commit guard)
            store.failDeleteKeepingRow = false;
            store.rows.clear();
            manager.applyRefreshedBaselines(Map.of());
            String tombstone = id + "|d-p4|" + SPMUtils.hashOf("p-p4");
            Assertions.assertTrue(store.tombstones.contains(tombstone),
                    "the confirmed absence completes the drop's tombstone: " + store.tombstones);
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
                    "the resolved fence releases the id");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== #3: the forwarded CREATE's statement query id ================

    /**
     * The reviewer's case: the master persists SPMPlan2SQLBuilder's DECOMPILED planSql
     * for an ordinary supported GLOBAL CREATE, while the follower's expectation carried
     * the raw submitted text - every follower snapshot failed the comparison, the
     * callback invalidated its cache and reported an error after bounded retries. The
     * statement's QUERY ID survives the freezing: the forward carried it to the master
     * (whose context adopts it) and the CREATE stored it on the row.
     */
    @Test
    public void testForwardedCreateExpectationMatchesTheStatementQueryId() {
        BaselineManager.ForwardedDdlExpectation created =
                BaselineManager.ForwardedDdlExpectation.created(
                        "select k from t1", "SELECT k FROM t1", "qid-create-7");

        // the stored plan text is the DECOMPILED rendering: only the query id matches
        BaselinePlan decompiled = baseline("d-qid", "DECOMPILED FROZEN TEXT");
        decompiled.setId(42L);
        decompiled.setQueryId("qid-create-7");
        Assertions.assertTrue(created.isSatisfiedBy(Map.of(42L, decompiled)),
                "the statement's query id identifies the committed row");

        // another statement's row matches neither arm
        BaselinePlan other = baseline("d-qid", "DECOMPILED FROZEN TEXT");
        other.setId(43L);
        other.setQueryId("qid-other");
        Assertions.assertFalse(created.isSatisfiedBy(Map.of(43L, other)),
                "another statement's row must not satisfy the expectation");

        // the TEXT arm keeps the raw-fallback CREATE working
        BaselinePlan raw = baseline("d-qid", "SELECT k FROM t1");
        raw.setId(44L);
        raw.setQueryId("whatever");
        Assertions.assertTrue(created.isSatisfiedBy(Map.of(44L, raw)),
                "a row carrying the submitted plan text still satisfies the expectation");

        // a context without a query id ("" / "NaN") must fall back to the text match
        BaselineManager.ForwardedDdlExpectation noId =
                BaselineManager.ForwardedDdlExpectation.created(
                        "select k from t1", "SUBMITTED", "");
        BaselinePlan nan = baseline("d-qid", "SOMETHING ELSE");
        nan.setId(45L);
        nan.setQueryId("NaN");
        Assertions.assertFalse(noId.isSatisfiedBy(Map.of(45L, nan)),
                "an unusable query id must not match every row the master stored as NaN");
    }
}
