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
import org.apache.doris.nereids.spm.manager.BaselinePersistence.SeqReservation;

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
 * Round-88 review fixes on the manager.
 *
 * <ul>
 *   <li>#2 the DROP tombstone is published by TWO writes - the strict append-only HISTORY row and
 *       the best-effort COMPACT identity slot every reader consults first. Confirming only the
 *       history let a DROP report success while the compact slot still carried the identity's
 *       previous plain reservation, so the identity still looked alive and a delayed INSERT of an
 *       older id of the same key was republished as an active baseline.</li>
 *   <li>#3 {@code isNewerReservation} compared the reserve INSTANT first, although the monotonic
 *       {@code baseline_id} is the incarnation order: under FE clock skew a newer id could be
 *       discarded in favour of an older tombstone.</li>
 * </ul>
 */
public class SPMRound88ManagerTest {

    /**
     * A DROP whose compact slot still answers "live" must NOT report success: the history row is
     * readable, but the slot a successor reads first is exactly the window that republished a
     * delayed row of an older id.
     */
    @Test
    public void testDropFailsWhileTheCompactSlotStillCarriesTheLiveReservation() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a"));

        // the history append lands, the compact publish does not: the identity still reads "live"
        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, false);
        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(41L),
                "a DROP that cannot publish the compact tombstone must stay retryable");
        Assertions.assertTrue(store.tombstones.stream().anyMatch(t -> t.startsWith("41|")),
                "the history marker itself was appended: " + store.tombstones);

        // once the compact slot reports the tombstone the retry settles the obligation
        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, true);
        Assertions.assertFalse(manager.dropBaseline(41L), "the row of 41 is already gone");
        Assertions.assertNull(store.rows.get(41L), "the row stays deleted");
    }

    /** A healthy DROP (both writes visible) succeeds on the first attempt. */
    @Test
    public void testDropSucceedsWhenBothTombstonesAreVisible() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(51L, row(51L, "digest_b"));
        store.batchedState = new SeqReservation(51L, 1_700_000_000_000_000L, false, true);

        Assertions.assertTrue(manager.dropBaseline(51L), "the healthy path drops the row");
        Assertions.assertNull(store.rows.get(51L));
    }

    /**
     * The monotonic id decides which record is the identity's later state; the reserve instant
     * only breaks ties (it comes from the writing FE's clock, so it can go backwards).
     */
    @Test
    public void testNewerReservationComparesTheIdFirst() {
        // a NEWER id with an EARLIER timestamp (clock skew) is still the later state
        Assertions.assertTrue(BaselineManager.isNewerReservation(
                new SeqReservation(42L, 1_000L, false, false),
                new SeqReservation(41L, 2_000L, false, true)),
                "the higher baseline id is the newer incarnation, whatever the clocks say");
        Assertions.assertFalse(BaselineManager.isNewerReservation(
                new SeqReservation(41L, 2_000L, false, true),
                new SeqReservation(42L, 1_000L, false, false)),
                "an OLDER id must never replace a newer committed reservation");
    }

    /** Same id: the newer instant wins, and at equal instants the further lifecycle state wins. */
    @Test
    public void testNewerReservationTieBreakers() {
        Assertions.assertTrue(BaselineManager.isNewerReservation(
                new SeqReservation(41L, 2_000L, false, false),
                new SeqReservation(41L, 1_000L, false, false)),
                "the same incarnation's later write wins");
        Assertions.assertTrue(BaselineManager.isNewerReservation(
                new SeqReservation(41L, 1_000L, false, true),
                new SeqReservation(41L, 1_000L, false, false)),
                "a DROP tombstone is later than the live record of the same incarnation");
        Assertions.assertTrue(BaselineManager.isNewerReservation(
                new SeqReservation(41L, 1_000L, false, false),
                new SeqReservation(41L, 1_000L, true, false)),
                "a committed record is later than the unconfirmed reservation of that id");
    }

    // ==================== helpers ====================

    /** A durable store that models the identity slots as one shared state. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        /** The compact state the batched read answers for every slot. */
        SeqReservation batchedState;

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
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
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

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            return batchedState;
        }

        @Override
        public Map<Long, SeqReservation> readLatestReservations(Collection<Long> compactIds,
                int timeoutSeconds) {
            if (batchedState == null) {
                return Map.of();
            }
            Map<Long, SeqReservation> states = new java.util.HashMap<>();
            for (Long compactId : compactIds) {
                states.put(compactId, batchedState);
            }
            return states;
        }
    }

    private static BaselinePlan row(long id, String digest) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1 where k = ?");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(0xB2L);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setCreateTime(1_000L);
        plan.setUpdateTime(1_000L);
        return plan;
    }

    private static BaselineManager manager(BaselinePersistence store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), () -> 1_700_000_000_000L);
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }
}
