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
}
