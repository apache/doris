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
 * Round-79 review fixes on the manager.
 *
 * <ul>
 *   <li>#1 a REAL status flip must pass the same unresolved-intent gate as a no-op one: a
 *       committed-but-unreadable no-op DISABLE leaves a DISABLED fence behind, and an immediate
 *       ENABLE read the OLD row, overwrote that fence and wrote ENABLED at the very stored second
 *       the hidden DISABLED row carries - so the DISABLED-first tie-break of the durable pick
 *       reversed the successful ENABLE on the next refresh.</li>
 * </ul>
 */
public class SPMRound79ManagerTest {

    /** A durable store modelling rows, tombstones and the mutation clock. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        final List<Long> pendingTicks = new java.util.concurrent.CopyOnWriteArrayList<>();
        long newestStoredMicros;
        long idWatermark;
        /**
         * When set, a reported-successful INSERT stays UNREADABLE: the row is durable (the
         * store wrote it) but no read shows it yet - the publication lag the ALTER treats as
         * "committed, unresolved".
         */
        boolean writesUnreadable;

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            return !writesUnreadable;
        }

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
        public long newestStoredUpdateMicros(long id) {
            return newestStoredMicros;
        }

        @Override
        public long readHwmRecord() {
            return idWatermark;
        }

        @Override
        public long readSequenceTail(long floor) {
            return 0L;
        }

        // ==================== mutation clock ====================

        /**
         * The SHARED clock stays QUIET (the default {@code clock()} token): this test isolates the
         * LOCAL unresolved-intent gate, and a noisy clock would fail the ALTER for the other
         * reason (both are retryable, but only one of them keeps a committed write from being
         * reversed). The window itself must still open and become visible, or the ALTER fails
         * retryably before it reaches the gate.
         */
        @Override
        public long[] slotState() {
            long maxTick = 0;
            for (Long tick : pendingTicks) {
                maxTick = Math.max(maxTick, tick);
            }
            return new long[] {0L, maxTick};
        }

        @Override
        public void appendHwmRecord(long lastId, long tick, long pending) {
            if (pending != 0) {
                pendingTicks.add(pending);
            }
        }

        @Override
        public void closeMutationWindow(long windowTick) {
        }

        @Override
        public long countVisiblePending(long tick) {
            return pendingTicks.contains(tick) ? 1L : 0L;
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

    private static BaselinePlan row(long id, String digest, BaselineStatus status) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1 where k = ?");
        plan.setBindSqlDigest(digest);
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
                new BaselineTestDoubles.TestClusterGateway(),
                () -> 1_700_000_000_000L);
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    /**
     * A no-op DISABLE whose durable write is still UNPUBLISHED keeps a DISABLED fence. The
     * immediate ENABLE reads the OLD row (it takes the DIFFERS path, which used to skip the
     * gate), so it must fail retryably instead of writing ENABLED at the stored second the
     * hidden DISABLED row carries.
     */
    @Test
    public void testRealFlipIsRefusedWhileAnOppositeIntentIsUnresolved() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        // the durable row already carries DISABLED while the cache still believes ENABLED
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.DISABLED));
        manager.applyRefreshedBaselines(Map.of(41L, row(41L, "digest_a", BaselineStatus.ENABLED)));
        // the no-op DISABLE's intent COMMITS but stays unreadable: the ALTER reports success and
        // keeps the confirmed DISABLED fence of that unresolved write
        store.writesUnreadable = true;
        // the VISIBLE row's stored instant is ahead of wall time (a burst of flips pushed it
        // there), so the intent it hides carries the very instant the opposite flip would write:
        // the flip cannot order itself above the intent and must be refused. The margin must
        // outlive the no-op's own visibility budget (its ~1 s of read-back retries run before
        // the flip, and the stored instant has to stay AHEAD of wall time for the whole of it);
        // 30 s is still well inside MAX_STORED_UPDATE_LEAD_MICROS.
        store.newestStoredMicros = System.currentTimeMillis() * 1000L + 30_000_000L;

        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.DISABLED),
                "the no-op repairs the cache and leaves its DISABLED fence behind");
        store.writesUnreadable = false;

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.updateStatus(41L, BaselineStatus.ENABLED),
                "a REAL flip must not overwrite the fence of the unresolved DISABLE");
        Assertions.assertTrue(error.getMessage().contains("still unresolved"),
                "the failure names the unresolved earlier write: " + error.getMessage());
    }

    /** Once the fence is satisfied by a snapshot, the very same flip succeeds. */
    @Test
    public void testRealFlipProceedsOnceTheIntentIsPublished() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.DISABLED));
        manager.applyRefreshedBaselines(Map.of(41L, row(41L, "digest_a", BaselineStatus.ENABLED)));
        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.DISABLED));

        // the hidden write PUBLISHED: the snapshot now carries the DISABLED row the fence expects
        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));

        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.ENABLED),
                "the gate is transient: it only holds while the earlier write is unresolved");
    }
}
