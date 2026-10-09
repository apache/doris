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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.cache.MetaCacheBudgetManager.AdmissionReservation;
import org.apache.doris.connector.cache.MetaCacheBudgetManager.EntryBudget;
import org.apache.doris.connector.cache.MetaCacheBudgetManager.ReservationReplacement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Budget arithmetic of {@link MetaCacheBudgetManager} without a cache on top: the global, catalog and
 * entry-group limits, the reservation hand-off between two generations of one key, release on close,
 * and which peers a reclamation request may ask.
 */
class MetaCacheBudgetManagerTest {
    private static final OptionalLong NONE = OptionalLong.empty();

    @Test
    void reservationsRespectGlobalCatalogAndEntryLimits() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget file = manager.createEntryBudget(
                1L, "hive", "file", "file", OptionalLong.of(80L), OptionalLong.of(60L));
        EntryBudget partitionView = manager.createEntryBudget(
                1L, "hive", "partition_view", "partition_view", OptionalLong.of(80L), OptionalLong.of(50L));

        AdmissionReservation fileReservation = file.tryReserve(60L).get();
        Assertions.assertFalse(partitionView.tryReserve(30L).isPresent(),
                "the catalog limit of 80 is already 60 used");
        AdmissionReservation viewReservation = partitionView.tryReserve(20L).get();
        Assertions.assertEquals(80L, manager.getGlobalUsedWeight());
        Assertions.assertFalse(partitionView.tryReplace(viewReservation, 30L).isPresent());

        fileReservation.release();
        ReservationReplacement grown = partitionView.tryReplace(viewReservation, 30L).get();
        grown.commit();
        Assertions.assertEquals(30L, manager.getGlobalUsedWeight());
        Assertions.assertEquals(30L, partitionView.getUsedWeight());

        grown.current().release();
        Assertions.assertFalse(partitionView.tryReserve(51L).isPresent(),
                "the entry limit of 50 applies even when the catalog and global budgets have room");
        file.close();
        partitionView.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void concurrentReservationsNeverExceedTheGlobalLimit() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget budget = manager.createEntryBudget(1L, "iceberg", "manifest", "manifest", NONE, NONE);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<AdmissionReservation> reservations = Collections.synchronizedList(new ArrayList<>());
        try {
            for (int i = 0; i < 200; i++) {
                executor.submit(() -> {
                    await(start);
                    budget.tryReserve(1L).ifPresent(reservations::add);
                });
            }
            start.countDown();
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
            Assertions.assertEquals(100, reservations.size());
            Assertions.assertEquals(100L, manager.getGlobalUsedWeight());
        } finally {
            executor.shutdownNow();
            reservations.forEach(AdmissionReservation::release);
            budget.close();
        }
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void entryBudgetIsClampedToTheSmallestEnclosingLimit() {
        // A catalog created on an FE with a larger global quota replays here with limits above ours.
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget replayed = manager.createEntryBudget(
                1L, "iceberg", "table", "table", OptionalLong.of(400L), OptionalLong.of(300L));
        Assertions.assertEquals(100L, replayed.getEffectiveMaxWeight());
        AdmissionReservation reservation = replayed.tryReserve(100L).get();
        Assertions.assertFalse(replayed.tryReserve(1L).isPresent());
        reservation.release();
        replayed.close();

        EntryBudget entryAboveCatalog = manager.createEntryBudget(
                2L, "iceberg", "table", "table", OptionalLong.of(40L), OptionalLong.of(60L));
        Assertions.assertEquals(40L, entryAboveCatalog.getEffectiveMaxWeight());
        Assertions.assertFalse(entryAboveCatalog.tryReserve(41L).isPresent());
        entryAboveCatalog.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void weightParsingAcceptsBinaryUnitsAndHeapPercentages() {
        long heap = 4L * 1024L * 1024L * 1024L;
        String key = "external_meta_cache_max_weight";
        Assertions.assertEquals(1L, CacheSpec.parseWeight("1", key, true, heap));
        Assertions.assertEquals(2L * 1024L * 1024L, CacheSpec.parseWeight(" 2 mb ", key, true, heap));
        Assertions.assertEquals(heap / 4L, CacheSpec.parseWeight("25%", key, true, heap));
        Assertions.assertEquals(heap / 8L, CacheSpec.parseWeight("12.5%", key, true, heap));
        // 0 disables the global quota; rejecting 0% is the caller's job (ExternalMetaCacheMgr).
        Assertions.assertEquals(0L, CacheSpec.parseWeight("0", key, true, heap));
        Assertions.assertEquals(0L, CacheSpec.parseWeight("0%", key, true, heap));

        for (String invalid : new String[] {"", "1.5GB", "101%", "-1%", "-1", "8EB", "1PB000",
                "8192PB", "999999999999999999PB"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> CacheSpec.parseWeight(invalid, key, true, heap), invalid);
        }
        IllegalArgumentException catalogPercentage = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CacheSpec.parseWeight("10%", "meta.cache.max-weight", false, heap));
        Assertions.assertEquals("Invalid cache weight for 'meta.cache.max-weight': 10%",
                catalogPercentage.getMessage());
    }

    @Test
    void releasingAReservationTwiceReleasesItOnce() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget first = manager.createEntryBudget(1L, "hive", "partition_view", "partition_view", NONE, NONE);
        EntryBudget second = manager.createEntryBudget(1L, "hive", "file", "file", NONE, NONE);
        AdmissionReservation released = first.tryReserve(40L).get();
        AdmissionReservation kept = second.tryReserve(30L).get();

        released.release();
        released.release();

        Assertions.assertEquals(30L, manager.getGlobalUsedWeight(),
                "a repeated release must not take bytes from another reservation");
        Assertions.assertEquals(0L, released.getBytes());
        kept.release();
        first.close();
        second.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void replacementHoldsTheLargerGenerationUntilCommitOrRollback() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget budget = manager.createEntryBudget(1L, "iceberg", "table", "table", NONE, NONE);
        AdmissionReservation first = budget.tryReserve(30L).get();

        ReservationReplacement grow = budget.tryReplace(first, 50L).get();
        Assertions.assertEquals(50L, manager.getGlobalUsedWeight(), "max(old, new) is held, never old + new");
        Assertions.assertFalse(budget.tryReplace(first, 10L).isPresent(),
                "a generation that is being replaced can not be replaced again");
        grow.commit();
        AdmissionReservation second = grow.current();
        Assertions.assertEquals(50L, second.getBytes());

        ReservationReplacement shrink = budget.tryReplace(second, 10L).get();
        Assertions.assertEquals(50L, manager.getGlobalUsedWeight(),
                "the old generation stays charged until the new one is published");
        shrink.commit();
        Assertions.assertEquals(10L, manager.getGlobalUsedWeight());

        AdmissionReservation third = shrink.current();
        ReservationReplacement abandoned = budget.tryReplace(third, 40L).get();
        Assertions.assertEquals(40L, manager.getGlobalUsedWeight());
        abandoned.rollback();
        Assertions.assertEquals(10L, manager.getGlobalUsedWeight());
        Assertions.assertThrows(IllegalStateException.class, abandoned::commit);
        third.release();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight(), "rollback reactivates the previous generation");
        budget.close();
    }

    @Test
    void closedBudgetRejectsNewReservationsAndReplacements() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget stale = manager.createEntryBudget(1L, "hive", "partition_view", "partition_view", NONE, NONE);
        AdmissionReservation zeroBytes = stale.tryReserve(0L).get();

        stale.close();
        stale.close();

        Assertions.assertFalse(stale.tryReserve(1L).isPresent());
        Assertions.assertFalse(stale.tryReplace(zeroBytes, 1L).isPresent());
        zeroBytes.release();

        EntryBudget replacement = manager.createEntryBudget(
                1L, "hive", "partition_view", "partition_view", NONE, NONE);
        AdmissionReservation full = replacement.tryReserve(100L).get();
        Assertions.assertEquals(100L, manager.getGlobalUsedWeight());
        full.release();
        replacement.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void closeForceReleasesOutstandingReservations() {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget closing = manager.createEntryBudget(1L, "hive", "partition_view", "partition_view", NONE, NONE);
        EntryBudget sibling = manager.createEntryBudget(1L, "hive", "file", "file", NONE, NONE);
        AdmissionReservation leaked = closing.tryReserve(40L).get();
        AdmissionReservation live = sibling.tryReserve(30L).get();

        closing.close();
        Assertions.assertEquals(30L, manager.getGlobalUsedWeight());
        leaked.release();
        Assertions.assertEquals(30L, manager.getGlobalUsedWeight(),
                "a late release from a closed owner must not be subtracted twice");
        Assertions.assertEquals(30L, sibling.getUsedWeight());

        EntryBudget reopened = manager.createEntryBudget(1L, "hive", "partition_view", "partition_view", NONE, NONE);
        AdmissionReservation reReserved = reopened.tryReserve(70L).get();
        Assertions.assertEquals(100L, manager.getGlobalUsedWeight());
        reReserved.release();
        live.release();
        reopened.close();
        sibling.close();
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    void peerReclaimCoalescesQueuedRequestsToTheLargestAdmission() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(100L));
        EntryBudget owner = manager.createEntryBudget(1L, "iceberg", "partition", "partition", NONE, NONE);
        EntryBudget requester = manager.createEntryBudget(2L, "hive", "partition_view", "partition_view", NONE, NONE);
        AdmissionReservation reservation = owner.tryReserve(100L).get();
        CountDownLatch firstReclaimStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstReclaim = new CountDownLatch(1);
        CountDownLatch secondReclaimFinished = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        List<Long> targets = Collections.synchronizedList(new ArrayList<>());
        owner.setReclaimer(target -> {
            targets.add(target);
            if (invocations.getAndIncrement() == 0) {
                firstReclaimStarted.countDown();
                await(releaseFirstReclaim);
            } else {
                secondReclaimFinished.countDown();
            }
            return 0L;
        });
        try {
            requester.requestPeerReclaim(10L);
            Assertions.assertTrue(firstReclaimStarted.await(10L, TimeUnit.SECONDS));

            requester.requestPeerReclaim(10L);
            requester.requestPeerReclaim(20L);
            requester.requestPeerReclaim(15L);
            releaseFirstReclaim.countDown();

            Assertions.assertTrue(secondReclaimFinished.await(10L, TimeUnit.SECONDS));
            Assertions.assertEquals(Arrays.asList(10L, 20L), new ArrayList<>(targets),
                    "misses queued behind a running reclamation collapse into one request for the largest");
        } finally {
            releaseFirstReclaim.countDown();
            reservation.release();
            owner.close();
            requester.close();
        }
    }

    @Test
    void catalogDeficitNeverAsksAnotherCatalog() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(200L));
        EntryBudget sibling = manager.createEntryBudget(
                1L, "iceberg", "partition", "partition", OptionalLong.of(100L), NONE);
        EntryBudget requester = manager.createEntryBudget(
                1L, "iceberg", "manifest", "manifest", OptionalLong.of(100L), NONE);
        EntryBudget otherCatalog = manager.createEntryBudget(
                2L, "paimon", "partition_view", "partition_view", OptionalLong.of(100L), NONE);
        AdmissionReservation siblingReservation = sibling.tryReserve(100L).get();
        // 150 of the global 200 is used, so the request below is short only at the catalog level.
        AdmissionReservation otherReservation = otherCatalog.tryReserve(50L).get();
        CountDownLatch siblingAskedTwice = new CountDownLatch(2);
        List<Long> siblingTargets = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger otherCatalogAsks = new AtomicInteger();
        sibling.setReclaimer(target -> {
            siblingTargets.add(target);
            siblingAskedTwice.countDown();
            return 0L;
        });
        otherCatalog.setReclaimer(target -> {
            otherCatalogAsks.incrementAndGet();
            return 0L;
        });
        try {
            requester.requestPeerReclaim(20L);
            awaitCount(siblingTargets, 1);
            // The reclaim executor runs one drain at a time, so the second request is served only after
            // the first drain has walked every candidate.
            requester.requestPeerReclaim(20L);

            Assertions.assertTrue(siblingAskedTwice.await(10L, TimeUnit.SECONDS));
            Assertions.assertEquals(Long.valueOf(20L), siblingTargets.get(0));
            Assertions.assertEquals(0, otherCatalogAsks.get(),
                    "only this catalog's caches can relieve a catalog-level deficit");
        } finally {
            siblingReservation.release();
            otherReservation.release();
            sibling.close();
            requester.close();
            otherCatalog.close();
        }
    }

    @Test
    void groupDeficitOnlyAsksPhysicalCachesOfTheSameGroup() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(NONE);
        EntryBudget groupPeer = manager.createEntryBudget(
                1L, "iceberg", "mvcc-partition-view", "partition_view", NONE, OptionalLong.of(100L));
        EntryBudget requester = manager.createEntryBudget(
                1L, "iceberg", "list-partitions-view", "partition_view", NONE, OptionalLong.of(100L));
        EntryBudget otherGroup = manager.createEntryBudget(
                1L, "iceberg", "manifest", "manifest", NONE, OptionalLong.of(500L));
        AdmissionReservation peerReservation = groupPeer.tryReserve(100L).get();
        AdmissionReservation otherReservation = otherGroup.tryReserve(400L).get();
        CountDownLatch peerAskedTwice = new CountDownLatch(2);
        List<Long> peerTargets = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger otherGroupAsks = new AtomicInteger();
        groupPeer.setReclaimer(target -> {
            peerTargets.add(target);
            peerAskedTwice.countDown();
            return 0L;
        });
        otherGroup.setReclaimer(target -> {
            otherGroupAsks.incrementAndGet();
            return 0L;
        });
        try {
            requester.requestPeerReclaim(20L);
            awaitCount(peerTargets, 1);
            requester.requestPeerReclaim(20L);

            Assertions.assertTrue(peerAskedTwice.await(10L, TimeUnit.SECONDS));
            Assertions.assertEquals(Long.valueOf(20L), peerTargets.get(0));
            Assertions.assertEquals(0, otherGroupAsks.get(),
                    "the larger cache of another group can not relieve this group's shared limit");
        } finally {
            peerReservation.release();
            otherReservation.release();
            groupPeer.close();
            requester.close();
            otherGroup.close();
        }
    }

    @Test
    void catalogAndGroupBucketsLiveExactlyAsLongAsTheirEntries() throws Exception {
        MetaCacheBudgetManager manager = new MetaCacheBudgetManager(OptionalLong.of(1L << 20));
        List<EntryBudget> budgets = new ArrayList<>();
        for (long catalogId = 1L; catalogId <= 3L; catalogId++) {
            for (String entry : new String[] {"paimon-table", "partition_view"}) {
                budgets.add(manager.createEntryBudget(catalogId, "paimon", entry, entry, NONE, NONE));
            }
        }
        Assertions.assertEquals(3, privateMap(manager, "catalogBuckets").size());
        Assertions.assertEquals(6, privateMap(manager, "entryGroupBuckets").size());

        // Closing one of a catalog's entries keeps its bucket; closing the last one removes it, and
        // unrelated catalogs keep reserving while that happens.
        budgets.get(0).close();
        Assertions.assertEquals(3, privateMap(manager, "catalogBuckets").size());
        AdmissionReservation unrelated = budgets.get(2).tryReserve(64L).get();
        budgets.get(1).close();
        Assertions.assertEquals(2, privateMap(manager, "catalogBuckets").size());
        Assertions.assertEquals(4, privateMap(manager, "entryGroupBuckets").size());
        unrelated.release();

        // A re-created catalog starts from a fresh bucket and can reserve again.
        EntryBudget recreated = manager.createEntryBudget(1L, "paimon", "paimon-table", "paimon-table", NONE, NONE);
        Assertions.assertEquals(3, privateMap(manager, "catalogBuckets").size());
        recreated.tryReserve(128L).get().release();
        recreated.close();
        for (int i = 2; i < budgets.size(); i++) {
            budgets.get(i).close();
        }
        Assertions.assertTrue(privateMap(manager, "catalogBuckets").isEmpty());
        Assertions.assertTrue(privateMap(manager, "entryGroupBuckets").isEmpty());
        Assertions.assertTrue(privateMap(manager, "entryBudgets").isEmpty());
        Assertions.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    private static Map<?, ?> privateMap(MetaCacheBudgetManager manager, String fieldName)
            throws ReflectiveOperationException {
        Field field = MetaCacheBudgetManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<?, ?>) field.get(manager);
    }

    private static void awaitCount(List<?> observed, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (observed.size() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assertions.assertTrue(observed.size() >= expected, "reclaimer was not asked in time");
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
