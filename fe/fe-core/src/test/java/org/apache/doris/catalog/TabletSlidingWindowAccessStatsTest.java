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

    @Test
    public void testReportReplacesPreviousBackendSnapshot() {
        stats.updateFromReport(1L, Collections.singletonList(queryStat(10L, 12L, 100L, 60_000L)),
                Collections.emptyList());
        stats.updateFromReport(1L, Collections.singletonList(queryStat(20L, 7L, 200L, 60_000L)),
                Collections.emptyList());

        Assertions.assertNull(stats.getAccessInfo(10L));
        Assertions.assertEquals(7L, stats.getAccessInfo(20L).accessCount);
        Assertions.assertEquals(1L, stats.getActiveIdsInWindow());
        Assertions.assertEquals(7L, stats.getRecentAccessCountInWindow());
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
