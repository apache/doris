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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Round-72 review fixes on the manager:
 *
 * <ul>
 *   <li>#1 a DROP TOMBSTONE is keyed by the id of the DROP, so a late INSERT of the SAME
 *       baseline under an OLDER id (a demoted master's in-flight create that was still
 *       unreadable when the new master created the key at another id and then dropped it)
 *       matched no marker and re-matched the dropped baseline. The identity's NEWEST
 *       sequence state now condemns every row of a dropped identity, and the leader-side
 *       twin repair appends the LOSER's own tombstone with its delete;</li>
 *   <li>#2 the create's ownership proof used to be skipped whenever a readback VALUE
 *       answered zero (`sequenceWatermark() &lt;= 0`) and an ABSENT identity record was
 *       trusted unconditionally - a lagging readback then allocated a second id next to an
 *       unreadable reservation. The proof is gated on the store CAPABILITY and the absent
 *       record must be covered by a sequence watermark that reaches the durable table;</li>
 *   <li>#3/#4 the retired same-key twins of a DROP were accumulated in METHOD-LOCAL state:
 *       a later twin's delete throwing lost the already-retired rows' tombstone
 *       obligations (the retry finds them ABSENT and never re-appends their markers) and
 *       left them warm in the cache (completeRowsForHash serves a warm bucket before any
 *       reconciliation). The accumulator is caller-owned now and the failure paths evict
 *       it.</li>
 * </ul>
 */
public class SPMRound72ManagerTest {

    /** Durable store that models the production tombstone / reservation read shapes. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        /** Tombstones keyed exactly like production: {@code id|digest|hash}. */
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        /** One id whose identity DELETE fails (the partial-cleanup scenario). */
        long failDeletesForId = -1L;
        /** null = durable (the store models the durable side). */
        Boolean durableOverride;
        /** The identity's newest sequence state (see readLatestReservation). */
        SeqReservation reservation;
        /** Whether the store models the reservation reads at all. */
        boolean modelReservationReads;
        long sequenceWatermark;
        long tableWatermark;

        @Override
        public boolean isDurable() {
            return durableOverride == null || durableOverride;
        }

        @Override
        public boolean hashReadsAvailable() {
            return isDurable();
        }

        @Override
        public boolean reservationReadsAvailable() {
            return modelReservationReads;
        }

        @Override
        public long sequenceWatermark() {
            return sequenceWatermark;
        }

        @Override
        public long tableWatermark() {
            return tableWatermark;
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            return reservation;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash) {
            return readRowsByHash(hash, 0);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            List<BaselinePlan> found = new ArrayList<>();
            for (BaselinePlan row : rows.values()) {
                if (row.getBindSqlHash() == hash) {
                    found.add(row);
                }
            }
            return found;
        }

        @Override
        public Map<Long, BaselinePlan> readSnapshot() {
            return new ConcurrentHashMap<>(rows);
        }

        @Override
        public DurablePresence probeRow(long id, String bindSqlDigest, String planSql,
                BaselineStatus status, String schemaFingerprint, Long requiredUpdateTimeMs) {
            BaselinePlan row = rows.get(id);
            if (row == null) {
                return DurablePresence.ABSENT;
            }
            return row.getBindSqlDigest().equals(bindSqlDigest)
                    && row.getPlanSql().equals(planSql)
                    ? DurablePresence.PRESENT : DurablePresence.ABSENT;
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
            Set<String> found = new HashSet<>();
            for (String tombstone : tombstones) {
                int cut = tombstone.indexOf('|');
                if (ids.contains(Long.parseLong(tombstone.substring(0, cut)))) {
                    found.add(tombstone);
                }
            }
            return found;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids, int timeoutSeconds) {
            return readDroppedIdentities(ids);
        }
    }

    private static BaselinePlan row(long id, BaselineStatus status) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1 where k = ?");
        plan.setBindSqlDigest("digest_a");
        plan.setBindSqlHash(0xA1L);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(status);
        plan.setCreateTime(1_000L);
        plan.setUpdateTime(1_000L);
        return plan;
    }

    private static BaselineManager manager(TestStore store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    // ==================== #1: a late row under an OLDER id ====================

    /**
     * The DROP of id 41 wrote its tombstones under 41. A demoted master's in-flight INSERT
     * of the SAME baseline under id 40 was still unreadable at that moment; when it
     * publishes, its id matches no tombstone - and the identity's newest sequence state
     * (41, dropped = 1) must condemn it, or the dropped baseline keeps matching.
     */
    @Test
    public void testLateRowOfADroppedIdentityDoesNotMatch() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        // the hash index knows the key (the CREATE/DROP churn touched it)
        manager.applyRefreshedBaselines(Map.of(41L, row(41L, BaselineStatus.ENABLED)));
        // ... the drop of 41 completed: the identity's newest state is a tombstone
        store.reservation = new BaselinePersistence.SeqReservation(
                41L, 1_000L, false, true);
        // the late row of the OLDER id publishes afterwards
        store.rows.put(40L, row(40L, BaselineStatus.ENABLED));

        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                "a row of a DROPPED identity must not match, whatever id it carries");
    }

    /** The leader-side twin repair appends the LOSER's tombstone (a delayed revival). */
    @Test
    public void testTwinRepairTombstonesTheLoser() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(40L, row(40L, BaselineStatus.ENABLED));
        store.rows.put(41L, row(41L, BaselineStatus.ENABLED));
        // the collapse keeps the larger id; the loser is retired durably
        Map<Long, BaselinePlan> snapshot = Map.of(40L, row(40L, BaselineStatus.ENABLED),
                41L, row(41L, BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(snapshot);

        Assertions.assertTrue(store.tombstones.contains(
                        40L + "|" + "digest_a" + "|" + planSqlHash(row(40L, null))),
                "the retired twin's own tombstone must be appended: " + store.tombstones);
        Assertions.assertNull(manager.getBaseline(40L),
                "the superseded twin is hidden");
    }

    private static long planSqlHash(BaselinePlan plan) {
        try {
            java.lang.reflect.Method method = BaselineManager.class
                    .getDeclaredMethod("identityPlanSqlHash", BaselinePlan.class);
            method.setAccessible(true);
            return (Long) method.invoke(null, plan);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    // ==================== #2: the two zero-reservation readback bypasses ====================

    /**
     * A reservation whose stored second cannot be read (age 0 / unknown) must NOT be
     * condemned: the fence decision needs the age, and condemning a possibly fresh
     * identity lets a second id publish beside its still-unreadable row.
     */
    @Test
    public void testUnknownReservationAgeDefersInsteadOfCondemning() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.modelReservationReads = true;
        store.sequenceWatermark = 7L;
        store.tableWatermark = 7L;
        store.reservation = new BaselinePersistence.SeqReservation(7L, 0L, false, false);

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.createBaseline(row(8L, BaselineStatus.ENABLED)));
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("cannot be proven"),
                String.valueOf(failure.getMessage()));
        Assertions.assertTrue(store.tombstones.isEmpty(),
                "an unknown age must not tombstone the identity: " + store.tombstones);
    }

    // ==================== #3/#4: partial twin cleanup ====================

    /**
     * The DROP retires twin 40 (deleted, tombstone obligation recorded) and then the
     * delete of twin 42 throws. The rows retired BEFORE the failure are already GONE
     * durably: they must leave the WARM cache (#4), and their confirmation obligation must
     * survive the throw so the retry (which finds the rows absent) can settle it (#3).
     */
    @Test
    public void testPartialTwinCleanupKeepsObligationsAndEvictsTheWarmTwin() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.durableOverride = false;
        // warm the cache with the twin ONLY: the dropped id 41 was created on ANOTHER FE
        // and is not cached here at all (the round-68 scenario)
        manager.applyRefreshedBaselines(Map.of(40L, row(40L, BaselineStatus.ENABLED)));
        Assertions.assertNotNull(BaselineManagerTestAccess.rawCacheGet(manager, 40L));
        store.durableOverride = true;
        store.rows.put(40L, row(40L, BaselineStatus.ENABLED));
        store.rows.put(41L, row(41L, BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, BaselineStatus.ENABLED));
        store.failDeletesForId = 42L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));

        Assertions.assertNull(BaselineManagerTestAccess.rawCacheGet(manager, 40L),
                "the twin retired before the failure must leave the warm cache");
        Assertions.assertTrue(BaselineManagerTestAccess.owesDropTombstoneConfirmation(41L),
                "the retired twin's tombstone obligation must survive the throw");

        // the retry finds the rows ABSENT and settles the retained obligation
        store.failDeletesForId = -1L;
        Assertions.assertFalse(manager.dropBaseline(41L), "the rows are already gone");
        Assertions.assertFalse(BaselineManagerTestAccess.owesDropTombstoneConfirmation(41L),
                "a confirmed retry releases the obligation");
        Assertions.assertTrue(store.tombstones.contains(
                        40L + "|" + "digest_a" + "|" + planSqlHash(row(40L, null))),
                "the twin's marker is re-appended and readable: " + store.tombstones);
    }

    // ==================== #5: the prune visibility proof covers both shapes ====================

    /**
     * An EMPTY schema fingerprint stores two shapes (NULL and the empty string) and the
     * prune issues one DELETE per shape. The visibility proof must count the same shapes:
     * counting only the empty-string rows reported "0 reclaimed versions" while the
     * NULL-fingerprint DELETE was still unreadable, and the shared mutation window was
     * closed over a mixed snapshot.
     */
    @Test
    public void testPruneProofCountsBothFingerprintShapes() {
        List<String> emptyShapes = BaselineManagerTestAccess.countVersionsBeforeSqlTexts("");
        Assertions.assertEquals(2, emptyShapes.size(),
                "an empty fingerprint stores two shapes: " + emptyShapes);
        Assertions.assertTrue(emptyShapes.get(0).contains("`schema_fingerprint` IS NULL"),
                emptyShapes.get(0));
        Assertions.assertTrue(emptyShapes.get(1).contains("`schema_fingerprint` = ''"),
                emptyShapes.get(1));
        List<String> realShape = BaselineManagerTestAccess.countVersionsBeforeSqlTexts(
                "t1|100|k:int|NN");
        Assertions.assertEquals(1, realShape.size(),
                "a real fingerprint stores one shape: " + realShape);
    }
}
