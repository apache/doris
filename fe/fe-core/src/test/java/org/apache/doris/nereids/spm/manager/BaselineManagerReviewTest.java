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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class BaselineManagerReviewTest {
    private static final long HASH = 0xC7L;
    private ReviewStore store;
    private BaselineManager manager;

    @BeforeEach
    void setUp() {
        store = new ReviewStore();
        manager = new BaselineManager(store, new BaselineClusterGateway() { }, System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
    }

    @AfterEach
    void tearDown() {
        BaselineManagerTestAccess.clear(manager);
    }

    private static BaselinePlan row(long id, String queryId) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("SELECT k FROM t");
        plan.setBindSqlDigest("review_digest");
        plan.setBindSqlHash(HASH);
        plan.setPlanSql("SELECT k FROM t");
        plan.setQueryId(queryId);
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setQueryTimeMs(-1);
        plan.setCreateTime(1_700_000_000_000_000L);
        plan.setUpdateTime(1_700_000_000_000_000L);
        return plan;
    }

    private void publish(BaselinePlan row) {
        store.put(row);
        manager.applyRefreshedBaselines(Map.of(row.getId(), row.copyPersistedScalars()));
    }

    @Test
    void coldReadSharesDeadlineWithTombstoneAndIdentityReads() {
        publish(row(7L, "cold"));
        long deadline = System.currentTimeMillis() + 1500;

        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH, deadline).size());
        Assertions.assertTrue(store.compactTimeout > 0 && store.compactTimeout <= 2);
        Assertions.assertEquals(deadline, store.tombstoneDeadline);
        Assertions.assertEquals(deadline, store.identityDeadline);
        Assertions.assertThrows(IllegalStateException.class, () -> BaselineManager.remainingReadTimeout(0));
    }

    @Test
    void newerIncarnationSatisfiesAnOldDropConfirmation() {
        publish(row(7L, "old"));
        store.identityState = new BaselinePersistence.SeqReservation(7L, 10L, false, false);

        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(7L));
        Assertions.assertTrue(BaselineManagerTestAccess.owesDropTombstoneConfirmation(7L));

        store.put(row(8L, "new"));
        store.identityState = new BaselinePersistence.SeqReservation(8L, 11L, false, false);
        Assertions.assertFalse(manager.dropBaseline(7L));
        Assertions.assertFalse(BaselineManagerTestAccess.owesDropTombstoneConfirmation(7L));
        Assertions.assertTrue(store.rows.containsKey(8L));
    }

    @Test
    void hashCollisionCannotPublishACacheReadFromBeforeRefresh() {
        Assertions.assertEquals("Aa".hashCode(), "BB".hashCode());
        publish(row(7L, "Aa"));
        store.beforeHashReadReturns = () -> publish(row(7L, "BB"));

        Assertions.assertTrue(manager.findCandidateBaselines("review_digest", HASH).isEmpty(),
                "a read overlapping a snapshot publication must be discarded even when field hashes collide");
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager));
        Assertions.assertEquals("BB", manager.findCandidateBaselines("review_digest", HASH).get(0).getQueryId());
    }

    @Test
    void collidingRefreshStillFencesAnOlderSnapshot() {
        publish(row(7L, "Aa"));
        long version = manager.getStateVersion();
        publish(row(7L, "BB"));

        Assertions.assertFalse(manager.applyRefreshedSnapshotIfUnchanged(version, Map.of(7L, row(7L, "Aa"))),
                "hash equality cannot let an older refresh overwrite the accepted snapshot");
        Assertions.assertEquals("BB", manager.findCandidateBaselines("review_digest", HASH).get(0).getQueryId());
    }

    @Test
    void unchangedSnapshotKeepsHashEpochButFencesOverlappingColdReads() {
        publish(row(7L, "same"));
        long epoch = BaselineManagerTestAccess.knownHashEpoch(manager, HASH);
        store.beforeHashReadReturns = () -> publish(row(7L, "same"));

        Assertions.assertTrue(manager.findCandidateBaselines("review_digest", HASH).isEmpty());
        Assertions.assertEquals(epoch, BaselineManagerTestAccess.knownHashEpoch(manager, HASH));
        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
    }

    @Test
    void cachedDuplicateWithUnknownDurablePresenceDoesNotAllocateOrWrite() {
        publish(row(7L, "cached"));
        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
        store.unknownProbe = true;

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.createBaseline(row(0L, "retry")));
        Assertions.assertTrue(failure.getMessage().contains("UNKNOWN"), failure.getMessage());
        Assertions.assertEquals(0, store.inserts);
        Assertions.assertEquals(0, store.reservations);
        Assertions.assertEquals(7L, manager.getBaseline(7L).getId());
    }

    @Test
    void cachedDuplicateUsesTheDurableStatusInsteadOfCreatingAnEnabledTwin() {
        publish(row(7L, "cached"));
        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
        BaselinePlan disabled = row(7L, "durable");
        disabled.setStatus(BaselineStatus.DISABLED);
        store.put(disabled);

        Assertions.assertEquals(7L, manager.createBaseline(row(0L, "retry")));
        Assertions.assertEquals(0, store.inserts);
        Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(7L).getStatus());
        Assertions.assertTrue(manager.findCandidateBaselines("review_digest", HASH).isEmpty());
    }

    @Test
    void coldCreateDedupReadsDurableRowsWithoutReservationMetadata() {
        publish(row(7L, "existing"));
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager));

        Assertions.assertEquals(7L, manager.createBaseline(row(0L, "retry")));
        Assertions.assertEquals(0, store.inserts);
        Assertions.assertEquals(0, store.reservations);
    }

    @Test
    void oldIdentityTombstoneDoesNotCoverALaterAllocation() {
        store.identityState = new BaselinePersistence.SeqReservation(7L, 1_700_000_000_000_000L, false, true);
        Set<String> markers = manager.withIdentityDrops(Set.of(),
                List.of(row(6L, "old"), row(7L, "dropped"), row(8L, "new")));

        Assertions.assertTrue(markers.stream().anyMatch(marker -> marker.startsWith("6|")));
        Assertions.assertTrue(markers.stream().anyMatch(marker -> marker.startsWith("7|")));
        Assertions.assertFalse(markers.stream().anyMatch(marker -> marker.startsWith("8|")),
                "a tombstone for allocation 7 cannot condemn allocation 8");
    }

    @Test
    void coldReadPreservesRecreationWhenIdentityStateLags() {
        publish(row(8L, "new"));
        store.identityState = new BaselinePersistence.SeqReservation(7L, 1_700_000_000_000_000L, false, true);

        Assertions.assertEquals(8L, manager.findCandidateBaselines("review_digest", HASH).get(0).getId());
        Assertions.assertTrue(store.rows.containsKey(8L), "reading stale metadata must not delete the new row");
    }

    @Test
    void dropDoesNotRetireAnAllocationCreatedAfterItsWatermark() {
        publish(row(7L, "old"));
        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
        store.afterDelete = () -> store.put(row(8L, "new"));

        Assertions.assertTrue(manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.containsKey(8L),
                "the normal DROP path must use the same allocation bound as resumed cleanup");
        manager.applyRefreshedBaselines(Map.of(8L, row(8L, "new")));
        Assertions.assertEquals(8L, manager.findCandidateBaselines("review_digest", HASH).get(0).getId());
    }

    @Test
    void coldDropUsesItsAllocationBoundToo() {
        publish(row(7L, "old"));
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCount(manager));
        store.afterDelete = () -> store.put(row(8L, "new"));

        Assertions.assertTrue(manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.containsKey(8L));
        manager.applyRefreshedBaselines(Map.of(8L, row(8L, "new")));
        Assertions.assertEquals(8L, manager.findCandidateBaselines("review_digest", HASH).get(0).getId());
    }

    @Test
    void exactCacheChangesAdvanceHashEpochEvenWhenSignaturesCollide() {
        publish(row(7L, "Aa"));
        Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
        long epoch = BaselineManagerTestAccess.knownHashEpoch(manager, HASH);

        publish(row(7L, "BB"));

        Assertions.assertNotEquals(epoch, BaselineManagerTestAccess.knownHashEpoch(manager, HASH));
        Assertions.assertEquals("BB", manager.findCandidateBaselines("review_digest", HASH).get(0).getQueryId());
    }

    @Test
    void absentDropRetryRetainsEveryIdentityOfACollidingId() {
        putCollidingIdentitiesWithTwins(false);
        store.failHashRead = true;

        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(7L));
        Assertions.assertFalse(store.rows.containsKey(7L));
        Assertions.assertTrue(store.extraIncarnations.isEmpty());
        Assertions.assertTrue(store.rows.containsKey(5L));
        Assertions.assertTrue(store.rows.containsKey(6L));

        store.failHashRead = false;
        Assertions.assertFalse(manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.isEmpty(), "retry must retire the twins of both removed identities");
    }

    @Test
    void partialIdentityDeleteRetryRetiresPreviouslyRemovedIdentityTwins() {
        putCollidingIdentitiesWithTwins(false);
        store.failDeleteDigest = "other_digest";

        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(7L));
        Assertions.assertEquals("injected identity delete failure", failure.getCause().getMessage());
        Assertions.assertFalse(store.rows.containsKey(7L));
        Assertions.assertEquals(1, store.extraIncarnations.size());
        Assertions.assertTrue(store.tombstones.stream().anyMatch(marker -> marker.startsWith("7|review_digest|")));
        Assertions.assertFalse(store.tombstones.stream().anyMatch(marker -> marker.startsWith("7|other_digest|")));

        store.failDeleteDigest = null;
        Assertions.assertTrue(manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.isEmpty(), "the first attempt's removed identity still needs cleanup");
        Assertions.assertTrue(store.extraIncarnations.isEmpty());
    }

    @Test
    void cachedDropRetiresTwinsOfEveryCollidingIdentity() {
        putCollidingIdentitiesWithTwins(true);

        Assertions.assertTrue(manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.isEmpty());
        Assertions.assertTrue(store.extraIncarnations.isEmpty());
    }

    @Test
    void absentRetryMustConfirmEveryIdentitySharingOneId() {
        putCollidingIdentitiesWithTwins(false);
        store.blockedMarkerPrefixes.add("7|other_digest|");

        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(7L));
        Assertions.assertTrue(store.rows.isEmpty());
        Assertions.assertTrue(store.extraIncarnations.isEmpty());
        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(7L),
                "a readable marker for the first identity cannot confirm the second identity's missing marker");

        store.blockedMarkerPrefixes.clear();
        Assertions.assertFalse(manager.dropBaseline(7L));
        Assertions.assertTrue(store.tombstones.stream().anyMatch(marker -> marker.startsWith("7|other_digest|")));
        Assertions.assertFalse(BaselineManagerTestAccess.owesDropTombstoneConfirmation(7L));
    }

    @Test
    void localMarkerRetriesKeepEveryScopeOfOneId() {
        putCollidingIdentitiesWithTwins(false);
        store.blockedMarkerPrefixes.add("7|review_digest|");
        store.blockedMarkerPrefixes.add("7|other_digest|");
        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropBaseline(7L));

        Set<String> masked = BaselineManagerTestAccess.droppedIdentities(manager, List.of(7L), false);
        Assertions.assertTrue(masked.stream().anyMatch(marker -> marker.startsWith("7|review_digest|")));
        Assertions.assertTrue(masked.stream().anyMatch(marker -> marker.startsWith("7|other_digest|")));

        store.blockedMarkerPrefixes.remove("7|review_digest|");
        BaselineManagerTestAccess.droppedIdentities(manager, List.of(7L), false);
        Assertions.assertTrue(store.tombstones.stream().anyMatch(marker -> marker.startsWith("7|review_digest|")),
                "making another scope readable must not discard this scope's pending retry");
        Assertions.assertFalse(store.tombstones.stream().anyMatch(marker -> marker.startsWith("7|other_digest|")));
        Assertions.assertTrue(BaselineManagerTestAccess.droppedIdentities(manager, List.of(7L), false).stream()
                .anyMatch(marker -> marker.startsWith("7|other_digest|")));
    }

    private void putCollidingIdentitiesWithTwins(boolean warm) {
        BaselinePlan first = row(7L, "first");
        if (warm) {
            publish(first);
            Assertions.assertEquals(1, manager.findCandidateBaselines("review_digest", HASH).size());
        } else {
            store.put(first);
        }
        BaselinePlan second = row(7L, "second");
        second.setBindSqlDigest("other_digest");
        second.setPlanSql("SELECT v FROM t");
        store.extraIncarnations.add(second);
        BaselinePlan firstTwin = first.copyPersistedScalars();
        firstTwin.setId(5L);
        store.put(firstTwin);
        BaselinePlan secondTwin = second.copyPersistedScalars();
        secondTwin.setId(6L);
        store.put(secondTwin);
    }

    private static final class ReviewStore implements BaselinePersistence {
        private final Map<Long, BaselinePlan> rows = new HashMap<>();
        private final List<BaselinePlan> extraIncarnations = new ArrayList<>();
        private final Set<String> tombstones = new HashSet<>();
        private final Set<String> blockedMarkerPrefixes = new HashSet<>();
        private Runnable beforeHashReadReturns;
        private Runnable afterDelete;
        private SeqReservation identityState;
        private int compactTimeout;
        private long tombstoneDeadline;
        private long identityDeadline;
        private boolean unknownProbe;
        private boolean failHashRead;
        private String failDeleteDigest;
        private long watermark;
        private int inserts;
        private int reservations;

        void put(BaselinePlan row) {
            rows.put(row.getId(), row.copyPersistedScalars());
            watermark = Math.max(watermark, row.getId());
        }

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            List<BaselinePlan> result = new ArrayList<>();
            BaselinePlan row = rows.get(id);
            if (row != null) {
                result.add(row.copyPersistedScalars());
            }
            extraIncarnations.stream().filter(extra -> extra.getId() == id)
                    .forEach(extra -> result.add(extra.copyPersistedScalars()));
            return result;
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            if (failHashRead) {
                throw new IllegalStateException("injected key cleanup read failure");
            }
            List<BaselinePlan> result = new ArrayList<>();
            rows.values().stream().filter(row -> row.getBindSqlHash() == hash)
                    .forEach(row -> result.add(row.copyPersistedScalars()));
            extraIncarnations.stream().filter(row -> row.getBindSqlHash() == hash)
                    .forEach(row -> result.add(row.copyPersistedScalars()));
            Runnable callback = beforeHashReadReturns;
            beforeHashReadReturns = null;
            if (callback != null) {
                callback.run();
            }
            return result;
        }

        @Override
        public DurablePresence probeRow(long id, String digest, String planSql, BaselineStatus status,
                String fingerprint, Long updateTime) {
            if (unknownProbe) {
                return DurablePresence.UNKNOWN;
            }
            return readRowsById(id, false).stream().anyMatch(row -> Objects.equals(row.getBindSqlDigest(), digest)
                    && Objects.equals(row.getPlanSql(), planSql) && (status == null || row.getStatus() == status)
                    && (fingerprint == null || Objects.equals(row.getSchemaFingerprint(), fingerprint))
                    && (updateTime == null || row.getUpdateTime() == updateTime))
                    ? DurablePresence.PRESENT : DurablePresence.ABSENT;
        }

        @Override
        public boolean isRowGone(BaselinePlan identity) {
            return readRowsById(identity.getId(), false).stream().noneMatch(row ->
                    Objects.equals(row.getBindSqlDigest(), identity.getBindSqlDigest())
                            && Objects.equals(row.getPlanSql(), identity.getPlanSql())
                            && Objects.equals(row.getSchemaFingerprint(), identity.getSchemaFingerprint()));
        }

        @Override
        public void insert(BaselinePlan row) {
            inserts++;
            put(row);
        }

        @Override
        public void reserveId(long id, String digest, long planHash, long reserveTime) {
            reservations++;
            watermark = Math.max(watermark, id);
        }

        @Override
        public long tableWatermark() {
            return rows.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        }

        @Override
        public long sequenceWatermark() {
            return watermark;
        }

        @Override
        public long readHwmRecord() {
            return watermark;
        }

        @Override
        public Map<Long, SeqReservation> readLatestReservations(Collection<Long> ids, int timeoutSeconds) {
            compactTimeout = timeoutSeconds;
            return Map.of();
        }

        @Override
        public SeqReservation readLatestReservation(String digest, long planHash, long deadlineMillis) {
            identityDeadline = deadlineMillis;
            return BaselinePersistence.super.readLatestReservation(digest, planHash, deadlineMillis);
        }

        @Override
        public SeqReservation readLatestReservation(String digest, long planHash) {
            return identityState;
        }

        @Override
        public Set<String> readDroppedIdentitiesBefore(Collection<Long> ids, long deadlineMillis) {
            tombstoneDeadline = deadlineMillis;
            return BaselinePersistence.super.readDroppedIdentitiesBefore(ids, deadlineMillis);
        }

        @Override
        public boolean appendDroppedMarker(long id, String digest, long planHash, long atMillis) {
            String marker = id + "|" + digest + "|" + planHash;
            if (blockedMarkerPrefixes.stream().anyMatch(marker::startsWith)) {
                return false;
            }
            tombstones.add(marker);
            return true;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            return new HashSet<>(tombstones);
        }

        @Override
        public void deleteByIdentity(BaselinePlan row) {
            if (Objects.equals(row.getBindSqlDigest(), failDeleteDigest)) {
                throw new IllegalStateException("injected identity delete failure");
            }
            rows.computeIfPresent(row.getId(), (id, current) ->
                    Objects.equals(current.getBindSqlDigest(), row.getBindSqlDigest())
                            && Objects.equals(current.getPlanSql(), row.getPlanSql()) ? null : current);
            extraIncarnations.removeIf(extra -> extra.getId() == row.getId()
                    && Objects.equals(extra.getBindSqlDigest(), row.getBindSqlDigest())
                    && Objects.equals(extra.getPlanSql(), row.getPlanSql()));
            Runnable callback = afterDelete;
            afterDelete = null;
            if (callback != null) {
                callback.run();
            }
        }
    }
}
