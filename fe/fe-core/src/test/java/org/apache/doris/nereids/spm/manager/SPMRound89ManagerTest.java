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
 * Round-89 review fix: a NON-dropped compact answer must be reconciled with the identity-filtered
 * history before it is used to filter / publish rows.
 *
 * <p>The compact identity slot and the append-only history are SEPARATE writes, and the compact
 * copy of a DROP is the best-effort one. A reader that trusted a non-dropped compact answer
 * therefore published a delayed INSERT of the same digest / plan hash under an OLDER baseline id
 * that the newer DROP tombstone had already condemned.
 */
public class SPMRound89ManagerTest {

    /**
     * The compact slot still answers "alive" for the NEWER incarnation while the history already
     * carries its DROP tombstone: the older incarnation about to be filtered must be suppressed.
     */
    @Test
    public void testNonDroppedCompactAnswerIsReconciledWithTheHistory() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan delayedOlder = row(40L, "digest_x");

        // the slot names incarnation 41 and calls it alive; the history says 41 was DROPPED
        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, false);
        store.historyState = new SeqReservation(41L, 1_700_000_000_000_000L, false, true);

        Set<String> markers = manager.withIdentityDrops(Set.of(), List.of(delayedOlder));
        Assertions.assertTrue(markers.stream().anyMatch(marker -> marker.startsWith("40|")),
                "the delayed older incarnation of a dropped identity must be suppressed: "
                        + markers);
    }

    /** The same shape with a live history record stays publishable. */
    @Test
    public void testReconciledLiveIdentityKeepsTheRow() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan delayedOlder = row(40L, "digest_x");

        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, false);
        store.historyState = new SeqReservation(41L, 1_700_000_000_000_000L, false, false);

        Set<String> markers = manager.withIdentityDrops(Set.of(), List.of(delayedOlder));
        Assertions.assertFalse(markers.stream().anyMatch(marker -> marker.startsWith("40|")),
                "a live identity keeps its rows: " + markers);
    }

    /**
     * The healthy steady state - the compact answer names the row's OWN incarnation - needs no
     * history read at all, which is what keeps the batched snapshot path bounded.
     */
    @Test
    public void testSelfNamedCompactAnswerNeedsNoHistoryRead() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan live = row(41L, "digest_x");

        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, false);
        // would suppress the row if it were consulted
        store.historyState = new SeqReservation(41L, 1_700_000_000_000_000L, false, true);

        Set<String> markers = manager.withIdentityDrops(Set.of(), List.of(live));
        Assertions.assertFalse(markers.stream().anyMatch(marker -> marker.startsWith("41|")),
                "the row's own live state decides: " + markers);
        Assertions.assertEquals(0, store.historyReads,
                "the batched answer already names the row's incarnation, so no history read");
    }

    /** A compact TOMBSTONE still suppresses without asking the history (unchanged behaviour). */
    @Test
    public void testCompactTombstoneSuppressesTheIdentity() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan row = row(41L, "digest_x");
        store.batchedState = new SeqReservation(41L, 1_700_000_000_000_000L, false, true);

        Set<String> markers = manager.withIdentityDrops(Set.of(), List.of(row));
        Assertions.assertTrue(markers.stream().anyMatch(marker -> marker.startsWith("41|")),
                "a compact tombstone suppresses its identity: " + markers);
    }

    // ==================== helpers ====================

    /** A durable store whose batched (compact) and single (history) answers differ on purpose. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        /** What the BATCHED compact read answers for every slot. */
        SeqReservation batchedState;
        /** What the reconciling single read answers (it replays the identity history). */
        SeqReservation historyState;
        int historyReads;

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
            historyReads++;
            // the live read reconciles the compact slot with the identity history (see
            // readPersistedSeqReservation), so it answers the history when the test models one
            return historyState != null ? historyState : batchedState;
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
