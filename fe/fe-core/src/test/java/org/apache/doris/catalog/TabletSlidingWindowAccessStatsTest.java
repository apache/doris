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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TActiveTabletStat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TabletSlidingWindowAccessStatsTest {
    private boolean originalEnabled;
    private TabletSlidingWindowAccessStats stats;

    @BeforeEach
    public void setUp() {
        originalEnabled = Config.enable_active_tablet_sliding_window_access_stats;
        Config.enable_active_tablet_sliding_window_access_stats = true;
        stats = new TabletSlidingWindowAccessStats();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_active_tablet_sliding_window_access_stats = originalEnabled;
    }

    @Test
    public void testUpdateFromReportProvidesAccessInfo() {
        stats.updateFromReport(1L,
                Collections.singletonList(queryStat(10L, 12L, 100L, 60_000L)),
                Collections.singletonList(loadStat(10L, 3L, 200L, 60_000L)));

        TabletSlidingWindowAccessStats.AccessStatsResult result = stats.getAccessInfo(10L);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(15L, result.accessCount);
        Assertions.assertEquals(200L, result.lastAccessTime);
        Assertions.assertEquals(12.0, result.scanRate);
        Assertions.assertEquals(3.0, result.loadRate);
    }

    // A report carries only the tablets that saw traffic since the previous one. Dropping the
    // rest would shrink "active" to a single report interval, which in a cluster reporting
    // every second means a tablet is active for one second after it stops being queried.
    @Test
    public void testTabletsMissingFromTheNextReportStayActive() {
        long now = System.currentTimeMillis();
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 12L, now, 60_000L)),
                Collections.emptyList());
        // Next report: tablet 10 saw no traffic, tablet 20 did.
        stats.updateFromReport(1L, Collections.singletonList(queryStat(20L, 7L, now, 60_000L)),
                Collections.emptyList());

        Assertions.assertEquals(12L, stats.getAccessInfo(10L).accessCount);
        Assertions.assertEquals(7L, stats.getAccessInfo(20L).accessCount);
        Assertions.assertEquals(2L, stats.getActiveIdsInWindow());
        Assertions.assertEquals(19L, stats.getRecentAccessCountInWindow());
    }

    // A repeated tablet takes the fresh values, it is not accumulated across reports.
    @Test
    public void testRepeatedTabletTakesTheLatestReport() {
        long now = System.currentTimeMillis();
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 12L, now - 1_000L, 60_000L)),
                Collections.emptyList());
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 7L, now, 60_000L)),
                Collections.emptyList());

        Assertions.assertEquals(7L, stats.getAccessInfo(10L).accessCount);
        Assertions.assertEquals(now, stats.getAccessInfo(10L).lastAccessTime);
        Assertions.assertEquals(1L, stats.getActiveIdsInWindow());
    }

    // Equal rates fall through to the tie-break, and the tie-break runs in the same direction
    // as the rate: most recently touched wins.
    @Test
    public void testEqualRatesPreferTheMostRecentlyTouched() {
        long now = System.currentTimeMillis();
        stats.updateFromReport(1L, List.of(
                queryStat(10L, 10L, now - 1_000L, 60_000L),
                queryStat(20L, 10L, now, 60_000L)), Collections.emptyList());

        List<TabletSlidingWindowAccessStats.AccessStatsResult> top = stats.getTopNActive(1);
        Assertions.assertEquals(1, top.size());
        Assertions.assertEquals(20L, top.get(0).id);
    }

    // The half-and-half budget is a floor, not a cap: whichever dimension has fewer candidates
    // hands its unused share to the other, and when both are short everything is returned.
    @Test
    public void testTopNQuotaSpillsToTheOtherDimension() {
        long now = System.currentTimeMillis();

        // Only queries: the load half is handed over, so all 8 query tablets fit in topN=8.
        TabletSlidingWindowAccessStats queryOnly = new TabletSlidingWindowAccessStats();
        queryOnly.updateFromReport(1L, queryStats(8, now), Collections.emptyList());
        Assertions.assertEquals(8, queryOnly.getTopNActive(8).size());

        // Only loads: mirror image.
        TabletSlidingWindowAccessStats loadOnly = new TabletSlidingWindowAccessStats();
        loadOnly.updateFromReport(1L, Collections.emptyList(), loadStats(8, now));
        Assertions.assertEquals(8, loadOnly.getTopNActive(8).size());

        // Both below topN/2: nothing to spill, every candidate is returned and topN is not met.
        TabletSlidingWindowAccessStats bothShort = new TabletSlidingWindowAccessStats();
        bothShort.updateFromReport(1L, queryStats(2, now), loadStats(3, now));
        Assertions.assertEquals(5, bothShort.getTopNActive(20).size());
    }

    // Retention ends at active_tablet_sliding_window_time_window_second.
    @Test
    public void testEntriesAgeOutOfTheWindow() {
        long windowMs = Config.active_tablet_sliding_window_time_window_second * 1000L;
        long now = System.currentTimeMillis();
        stats.updateFromReport(1L,
                Collections.singletonList(queryStat(10L, 12L, now - windowMs - 60_000L, 60_000L)),
                Collections.emptyList());
        stats.updateFromReport(1L, Collections.singletonList(queryStat(20L, 7L, now, 60_000L)),
                Collections.emptyList());

        Assertions.assertNull(stats.getAccessInfo(10L));
        Assertions.assertEquals(1L, stats.getActiveIdsInWindow());
    }

    // Retention is bounded, so a backend with a churning hot set cannot grow FE memory for a
    // whole window. The oldest entries go first.
    @Test
    public void testRetentionIsCappedByTopnKeepingTheNewest() {
        int originalTopn = Config.cloud_active_partition_scheduling_topn;
        Config.cloud_active_partition_scheduling_topn = 2;
        try {
            long now = System.currentTimeMillis();
            stats.updateFromReport(1L, List.of(
                    queryStat(1L, 1L, now - 3_000L, 60_000L),
                    queryStat(2L, 1L, now - 2_000L, 60_000L)), Collections.emptyList());
            stats.updateFromReport(1L,
                    Collections.singletonList(queryStat(3L, 1L, now, 60_000L)), Collections.emptyList());

            Assertions.assertEquals(2L, stats.getActiveIdsInWindow());
            Assertions.assertNull(stats.getAccessInfo(1L));
            Assertions.assertNotNull(stats.getAccessInfo(2L));
            Assertions.assertNotNull(stats.getAccessInfo(3L));
        } finally {
            Config.cloud_active_partition_scheduling_topn = originalTopn;
        }
    }

    @Test
    public void testTopNReservesCapacityForLoadTablets() {
        stats.updateFromReport(1L, List.of(
                queryStat(1L, 1_000L, 1L, 60_000L),
                queryStat(2L, 900L, 2L, 60_000L),
                queryStat(3L, 800L, 3L, 60_000L),
                queryStat(4L, 700L, 4L, 60_000L)), List.of(
                loadStat(101L, 10L, 101L, 60_000L),
                loadStat(102L, 9L, 102L, 60_000L),
                loadStat(103L, 8L, 103L, 60_000L),
                loadStat(104L, 7L, 104L, 60_000L)));

        Set<Long> ids = stats.getTopNActive(4).stream().map(r -> r.id).collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(1L, 2L, 101L, 102L), ids);
    }

    @Test
    public void testTopNSortsByRateInsteadOfRawDeltaAcrossBackends() {
        stats.updateFromReport(1L, Collections.singletonList(queryStat(1L, 100L, 1L, 120_000L)),
                Collections.emptyList());
        stats.updateFromReport(2L, Collections.singletonList(queryStat(2L, 75L, 2L, 60_000L)),
                Collections.emptyList());

        List<TabletSlidingWindowAccessStats.AccessStatsResult> results = stats.getTopNActive(2);
        Assertions.assertEquals(2L, results.get(0).id);
        Assertions.assertEquals(1L, results.get(1).id);
    }

    @Test
    public void testReplicaStatsAreMergedByMax() {
        stats.updateFromReport(1L,
                Collections.singletonList(queryStat(10L, 10L, 100L, 60_000L)),
                Collections.singletonList(loadStat(10L, 2L, 300L, 60_000L)));
        stats.updateFromReport(2L,
                Collections.singletonList(queryStat(10L, 20L, 200L, 60_000L)),
                Collections.singletonList(loadStat(10L, 1L, 400L, 60_000L)));

        TabletSlidingWindowAccessStats.AccessStatsResult result = stats.getTopNActive(2).get(0);
        Assertions.assertEquals(21L, result.accessCount);
        Assertions.assertEquals(400L, result.lastAccessTime);
        Assertions.assertEquals(20.0, result.scanRate);
        Assertions.assertEquals(2.0, result.loadRate);
        Assertions.assertEquals(21L, stats.getRecentAccessCountInWindow());
        Assertions.assertEquals(1L, stats.getActiveIdsInWindow());
    }

    @Test
    public void testRemoveBackendRemovesItsSnapshot() {
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 1L, 100L, 60_000L)),
                Collections.emptyList());

        stats.removeBackend(1L);

        Assertions.assertNull(stats.getAccessInfo(10L));
        Assertions.assertTrue(stats.getTopNActive(10).isEmpty());
    }

    @Test
    public void testDisabledStatsReturnEmptyValues() {
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 1L, 100L, 60_000L)),
                Collections.emptyList());
        Config.enable_active_tablet_sliding_window_access_stats = false;

        Assertions.assertNull(stats.getAccessInfo(10L));
        Assertions.assertTrue(stats.getTopNActive(10).isEmpty());
        Assertions.assertEquals(0L, stats.getRecentAccessCountInWindow());
        Assertions.assertEquals(0L, stats.getActiveIdsInWindow());
        Assertions.assertEquals(0L, stats.getTotalAccessCount());
        Assertions.assertEquals("Active tablet sliding window access stats is disabled", stats.getStatsSummary());
    }

    private static List<TActiveTabletStat> queryStats(int count, long lastAccessTime) {
        List<TActiveTabletStat> stats = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            stats.add(queryStat(1_000L + i, 10L + i, lastAccessTime, 60_000L));
        }
        return stats;
    }

    private static List<TActiveTabletStat> loadStats(int count, long lastAccessTime) {
        List<TActiveTabletStat> stats = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            stats.add(loadStat(2_000L + i, 10L + i, lastAccessTime, 60_000L));
        }
        return stats;
    }

    private static TActiveTabletStat queryStat(long tabletId, long delta, long lastAccessTime, long windowMs) {
        TActiveTabletStat stat = new TActiveTabletStat();
        stat.setTabletId(tabletId);
        stat.setScanCountDelta(delta);
        stat.setLastQueryTimeMs(lastAccessTime);
        stat.setDeltaWindowMs(windowMs);
        return stat;
    }

    private static TActiveTabletStat loadStat(long tabletId, long delta, long lastAccessTime, long windowMs) {
        TActiveTabletStat stat = new TActiveTabletStat();
        stat.setTabletId(tabletId);
        stat.setLoadCountDelta(delta);
        stat.setLastLoadTimeMs(lastAccessTime);
        stat.setDeltaWindowMs(windowMs);
        return stat;
    }
}
