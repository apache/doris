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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.AdmissionReservation;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager.EntryBudget;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalMetaCacheBudgetManagerTest {

    @Test
    public void testGlobalCatalogAndEntryLimits() {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget first = manager.createEntryBudget(
                1L, "hive", "file", OptionalLong.of(80L), OptionalLong.of(60L));
        EntryBudget second = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.of(80L), OptionalLong.of(50L));

        AdmissionReservation firstReservation = first.tryReserve(60L).get();
        Assert.assertFalse(second.tryReserve(30L).isPresent());
        AdmissionReservation secondReservation = second.tryReserve(20L).get();
        Assert.assertEquals(80L, manager.getGlobalUsedWeight());
        Assert.assertFalse(secondReservation.tryResize(30L));

        firstReservation.release();
        Assert.assertTrue(secondReservation.tryResize(30L));
        Assert.assertEquals(30L, manager.getGlobalUsedWeight());

        secondReservation.release();
        first.close();
        second.close();
        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    public void testConcurrentReservationNeverExceedsGlobalLimit() throws Exception {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget budget = manager.createEntryBudget(
                1L, "iceberg", "manifest", OptionalLong.empty(), OptionalLong.empty());
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<AdmissionReservation> reservations = Collections.synchronizedList(new ArrayList<>());
        try {
            for (int i = 0; i < 200; i++) {
                executor.submit(() -> {
                    await(start);
                    Optional<AdmissionReservation> reservation = budget.tryReserve(1L);
                    reservation.ifPresent(reservations::add);
                });
            }
            start.countDown();
            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
            Assert.assertEquals(100, reservations.size());
            Assert.assertEquals(100L, manager.getGlobalUsedWeight());
        } finally {
            executor.shutdownNow();
            reservations.forEach(AdmissionReservation::release);
            budget.close();
        }
        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    public void testRejectChildLargerThanParent() {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createEntryBudget(
                1L, "hive", "file", OptionalLong.of(80L), OptionalLong.of(90L)));

        Map<String, String> properties = new HashMap<>();
        properties.put(ExternalMetaCacheBudgetManager.CATALOG_MAX_WEIGHT_PROPERTY, "120");
        Assert.assertEquals(120L, manager.parseCatalogMaxWeight(properties).getAsLong());
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.validateCatalogMaxWeight(properties));
    }

    @Test
    public void testRuntimeBudgetClampsReplayedCatalogToLocalGlobalLimit() {
        ExternalMetaCacheBudgetManager observerManager = manager(100L);

        EntryBudget budget = observerManager.createEntryBudget(
                1L, "iceberg", "table", OptionalLong.of(400L), OptionalLong.of(300L));

        Assert.assertEquals(100L, budget.getEffectiveMaxWeight());
        Assert.assertEquals(100L, budget.getCatalogMaxWeight());
        AdmissionReservation reservation = budget.tryReserve(100L).get();
        Assert.assertFalse(budget.tryReserve(1L).isPresent());
        reservation.release();
        budget.close();
    }

    @Test
    public void testGlobalConfigSupportsPercentageAndDisabledZero() {
        String original = Config.external_meta_cache_max_weight;
        try {
            Config.external_meta_cache_max_weight = "25%";
            ExternalMetaCacheBudgetManager percentageManager = ExternalMetaCacheBudgetManager.fromConfig();
            Assert.assertEquals(Runtime.getRuntime().maxMemory() / 4L,
                    percentageManager.getGlobalMaxWeight().getAsLong());

            Config.external_meta_cache_max_weight = "0";
            Assert.assertFalse(ExternalMetaCacheBudgetManager.fromConfig().getGlobalMaxWeight().isPresent());

            Config.external_meta_cache_max_weight = "0%";
            Assert.assertThrows(IllegalArgumentException.class, ExternalMetaCacheBudgetManager::fromConfig);
        } finally {
            Config.external_meta_cache_max_weight = original;
        }
    }

    @Test
    public void testReservationReleaseIsIdempotent() {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget budget = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        AdmissionReservation reservation = budget.tryReserve(40L).get();

        reservation.release();
        reservation.release();

        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        budget.close();
    }

    @Test
    public void testClosedBudgetRejectsStaleHandleAndReservationResize() {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget staleBudget = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        AdmissionReservation zeroByteReservation = staleBudget.tryReserve(0L).get();

        staleBudget.close();
        staleBudget.close();

        Assert.assertFalse(staleBudget.tryReserve(1L).isPresent());
        Assert.assertFalse(zeroByteReservation.tryResize(1L));
        Assert.assertEquals(0L, staleBudget.getRejectedCount());
        Assert.assertEquals(0L, manager.getGlobalRejectedCount());
        zeroByteReservation.release();

        EntryBudget replacement = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        AdmissionReservation replacementReservation = replacement.tryReserve(100L).get();
        Assert.assertEquals(100L, manager.getGlobalUsedWeight());
        replacementReservation.release();
        replacement.close();
        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    @Test
    public void testCloseForceReleasesOutstandingAccounting() {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget staleBudget = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        AdmissionReservation staleReservation = staleBudget.tryReserve(40L).get();

        staleBudget.close();

        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
        staleReservation.release();
        Assert.assertFalse(staleReservation.isActive());
        EntryBudget replacement = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        replacement.close();
    }

    @Test
    public void testPeerReclaimCoalescesConcurrentMissesToLargestAdmission() throws Exception {
        ExternalMetaCacheBudgetManager manager = manager(100L);
        EntryBudget owner = manager.createEntryBudget(
                1L, "iceberg", "snapshot", OptionalLong.empty(), OptionalLong.empty());
        EntryBudget requester = manager.createEntryBudget(
                2L, "hive", "partition_values", OptionalLong.empty(), OptionalLong.empty());
        AdmissionReservation reservation = owner.tryReserve(100L).get();
        CountDownLatch firstReclaimStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstReclaim = new CountDownLatch(1);
        CountDownLatch secondReclaimFinished = new CountDownLatch(1);
        AtomicInteger invocation = new AtomicInteger();
        List<Long> targets = Collections.synchronizedList(new ArrayList<>());
        owner.setReclaimer(target -> {
            targets.add(target);
            if (invocation.getAndIncrement() == 0) {
                firstReclaimStarted.countDown();
                await(releaseFirstReclaim);
            } else {
                secondReclaimFinished.countDown();
            }
            return 0L;
        });
        try {
            requester.requestPeerReclaim(10L);
            Assert.assertTrue(firstReclaimStarted.await(3L, TimeUnit.SECONDS));

            requester.requestPeerReclaim(10L);
            requester.requestPeerReclaim(20L);
            requester.requestPeerReclaim(15L);
            releaseFirstReclaim.countDown();

            Assert.assertTrue(secondReclaimFinished.await(3L, TimeUnit.SECONDS));
            Assert.assertEquals(2, targets.size());
            Assert.assertEquals(Long.valueOf(10L), targets.get(0));
            Assert.assertEquals(Long.valueOf(20L), targets.get(1));
        } finally {
            releaseFirstReclaim.countDown();
            reservation.release();
            owner.close();
            requester.close();
        }
    }

    @Test
    public void testCatalogOnlyDeficitReclaimsSiblingWithoutTouchingOtherCatalog() throws Exception {
        ExternalMetaCacheBudgetManager manager = manager(200L);
        EntryBudget sibling = manager.createEntryBudget(
                1L, "iceberg", "snapshot", OptionalLong.of(100L), OptionalLong.empty());
        EntryBudget requester = manager.createEntryBudget(
                1L, "hive", "partition_values", OptionalLong.of(100L), OptionalLong.empty());
        EntryBudget otherCatalog = manager.createEntryBudget(
                2L, "paimon", "snapshot", OptionalLong.of(100L), OptionalLong.empty());
        AdmissionReservation siblingReservation = sibling.tryReserve(100L).get();
        AdmissionReservation otherReservation = otherCatalog.tryReserve(50L).get();
        CountDownLatch siblingReclaimed = new CountDownLatch(1);
        AtomicInteger otherCatalogReclaims = new AtomicInteger();
        sibling.setReclaimer(target -> {
            siblingReservation.release();
            siblingReclaimed.countDown();
            return 100L;
        });
        otherCatalog.setReclaimer(target -> {
            otherCatalogReclaims.incrementAndGet();
            return 0L;
        });
        try {
            requester.requestPeerReclaim(20L);

            Assert.assertTrue(siblingReclaimed.await(3L, TimeUnit.SECONDS));
            Assert.assertEquals(0, otherCatalogReclaims.get());
            Assert.assertEquals(0L, sibling.getUsedWeight());
            Assert.assertEquals(50L, manager.getGlobalUsedWeight());
        } finally {
            siblingReservation.release();
            otherReservation.release();
            sibling.close();
            requester.close();
            otherCatalog.close();
        }
    }

    private static ExternalMetaCacheBudgetManager manager(long maxWeight) {
        return new ExternalMetaCacheBudgetManager(OptionalLong.of(maxWeight));
    }

    private static void await(CountDownLatch latch) {
        try {
            Assert.assertTrue(latch.await(3L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCatalogBucketLifecycleTracksLiveEntriesWithoutScanning() throws Exception {
        ExternalMetaCacheBudgetManager manager = new ExternalMetaCacheBudgetManager(OptionalLong.of(1L << 20));
        java.util.List<ExternalMetaCacheBudgetManager.EntryBudget> budgets = new java.util.ArrayList<>();
        for (long catalogId = 1L; catalogId <= 3L; catalogId++) {
            for (String entry : new String[] {"table", "snapshot"}) {
                budgets.add(manager.createEntryBudget(
                        catalogId, "paimon", entry, OptionalLong.empty(), OptionalLong.empty()));
            }
        }
        java.util.Map<?, ?> catalogBuckets = readCatalogBuckets(manager);
        Assert.assertEquals(3, catalogBuckets.size());

        // Closing one of a catalog's entries keeps its bucket; closing the last removes it, and
        // unrelated catalogs keep reserving while the retirement is in progress.
        budgets.get(0).close();
        Assert.assertEquals(3, readCatalogBuckets(manager).size());
        AdmissionReservation unrelated = budgets.get(2).tryReserve(64L).get();
        budgets.get(1).close();
        Assert.assertEquals(2, readCatalogBuckets(manager).size());
        unrelated.release();

        // A re-created catalog starts from a fresh bucket and can reserve again.
        ExternalMetaCacheBudgetManager.EntryBudget recreated = manager.createEntryBudget(
                1L, "paimon", "table", OptionalLong.empty(), OptionalLong.empty());
        Assert.assertEquals(3, readCatalogBuckets(manager).size());
        AdmissionReservation recreatedReservation = recreated.tryReserve(128L).get();
        recreatedReservation.release();
        recreated.close();
        for (int i = 2; i < budgets.size(); i++) {
            budgets.get(i).close();
        }
        Assert.assertEquals(0, readCatalogBuckets(manager).size());
        Assert.assertEquals(0L, manager.getGlobalUsedWeight());
    }

    private static java.util.Map<?, ?> readCatalogBuckets(ExternalMetaCacheBudgetManager manager)
            throws Exception {
        java.lang.reflect.Field field = ExternalMetaCacheBudgetManager.class.getDeclaredField("catalogBuckets");
        field.setAccessible(true);
        return (java.util.Map<?, ?>) field.get(manager);
    }
}
