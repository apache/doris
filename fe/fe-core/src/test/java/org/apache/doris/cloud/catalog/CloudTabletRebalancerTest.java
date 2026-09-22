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

package org.apache.doris.cloud.catalog;

import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CloudTabletRebalancerTest {

    private boolean oldEnableActiveScheduling;

    @BeforeEach
    public void setUp() {
        oldEnableActiveScheduling = Config.enable_cloud_active_tablet_priority_scheduling;
        Config.enable_cloud_active_tablet_priority_scheduling = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_cloud_active_tablet_priority_scheduling = oldEnableActiveScheduling;
    }

    private static class TestRebalancer extends CloudTabletRebalancer {
        private final Set<Long> internalDbIds = new HashSet<>();

        TestRebalancer() {
            super(null);
        }

        void setInternalDbIds(Set<Long> ids) {
            internalDbIds.clear();
            internalDbIds.addAll(ids);
        }

        @Override
        protected boolean isInternalDbId(Long dbId) {
            return dbId != null && internalDbIds.contains(dbId);
        }
    }

    private static void setField(Object obj, String name, Object value) throws Exception {
        Field f = CloudTabletRebalancer.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private static Object getField(Object obj, String name) throws Exception {
        Field f = CloudTabletRebalancer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(obj);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object obj, String method, Class<?>[] types, Object[] args) throws Exception {
        Method m = CloudTabletRebalancer.class.getDeclaredMethod(method, types);
        m.setAccessible(true);
        return (T) m.invoke(obj, args);
    }

    @Test
    public void testInfightTaskPreservesSelectedBoxedTabletId() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long tabletId = Long.valueOf(50_001L);
        Class<?> taskClass = Class.forName(CloudTabletRebalancer.class.getName() + "$InfightTask");
        Constructor<?> constructor = taskClass.getDeclaredConstructor(CloudTabletRebalancer.class);
        constructor.setAccessible(true);
        Object task = constructor.newInstance(rebalancer);
        Field tabletIdField = taskClass.getDeclaredField("pickedTabletId");
        tabletIdField.setAccessible(true);

        tabletIdField.set(task, tabletId);

        Assertions.assertSame(tabletId, tabletIdField.get(task));
    }

    @Test
    public void testWarmupTaskPreservesSelectedBoxedTabletId() throws Exception {
        Long tabletId = Long.valueOf(50_001L);
        Class<?> taskClass = Class.forName(CloudTabletRebalancer.class.getName() + "$WarmupTabletTask");
        Constructor<?> constructor = taskClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);

        Object task = constructor.newInstance(tabletId, 1L, 2L, "cluster-a");
        Field tabletIdField = taskClass.getDeclaredField("pickedTabletId");
        tabletIdField.setAccessible(true);

        Assertions.assertSame(tabletId, tabletIdField.get(task));
    }

    @Test
    public void testPickTabletPreferCold_picksColdWhenAvailable() throws Exception {
        TestRebalancer r = new TestRebalancer();
        setField(r, "rand", new Random(1));

        Set<Long> tabletIds = new HashSet<>();
        tabletIds.add(100L); // hot
        tabletIds.add(200L); // cold

        Set<Long> activeIds = new HashSet<>();
        activeIds.add(100L);

        Set<Long> picked = new HashSet<>();

        Long pickedTabletId = invokePrivate(r, "pickTabletPreferCold",
                new Class<?>[] {long.class, Set.class, Set.class, Set.class},
                new Object[] {1L, tabletIds, activeIds, picked});

        Assertions.assertNotNull(pickedTabletId);
        Assertions.assertEquals(200L, pickedTabletId.longValue(), "Should prefer cold tablet when available");
    }

    @Test
    public void testPickTabletPreferCold_fallbackRandomWhenStatsUnavailable() throws Exception {
        TestRebalancer r = new TestRebalancer();
        setField(r, "rand", new Random(1));

        Set<Long> tabletIds = new HashSet<>();
        tabletIds.add(300L);

        // active stats unavailable -> activeIds empty or cache null
        Set<Long> activeIds = new HashSet<>();
        Set<Long> picked = new HashSet<>();

        Long pickedTabletId = invokePrivate(r, "pickTabletPreferCold",
                new Class<?>[] {long.class, Set.class, Set.class, Set.class},
                new Object[] {1L, tabletIds, activeIds, picked});

        Assertions.assertNotNull(pickedTabletId);
        Assertions.assertEquals(300L, pickedTabletId.longValue());
    }

    @Test
    public void testTableEntryComparator_ordersByDbActiveThenTableActiveThenIdDesc() throws Exception {
        TestRebalancer r = new TestRebalancer();
        r.setInternalDbIds(Collections.emptySet()); // no internal db

        // tableId -> dbId
        Map<Long, Long> tableToDb = new HashMap<>();
        tableToDb.put(10L, 1L);
        tableToDb.put(11L, 1L);
        tableToDb.put(20L, 2L);
        setField(r, "tableIdToDbId", new ConcurrentHashMap<>(tableToDb));

        // db active
        Map<Long, Long> dbActive = new HashMap<>();
        dbActive.put(1L, 5L);
        dbActive.put(2L, 1L);
        setField(r, "dbIdToActiveCount", new ConcurrentHashMap<>(dbActive));

        // table active
        Map<Long, Long> tableActive = new HashMap<>();
        tableActive.put(10L, 2L);
        tableActive.put(11L, 2L);
        tableActive.put(20L, 100L); // should still lose because dbActive(2)=1 < dbActive(1)=5
        setField(r, "tableIdToActiveCount", new ConcurrentHashMap<>(tableActive));

        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Long>>>> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Long>>>> list = new ArrayList<>();
        list.add(new AbstractMap.SimpleEntry<>(10L, new ConcurrentHashMap<>()));
        list.add(new AbstractMap.SimpleEntry<>(11L, new ConcurrentHashMap<>()));
        list.add(new AbstractMap.SimpleEntry<>(20L, new ConcurrentHashMap<>()));

        list.sort(cmp);

        // dbId=1 entries first, and for tableId tie-breaker is desc (11 before 10)
        Assertions.assertEquals(11L, list.get(0).getKey());
        Assertions.assertEquals(10L, list.get(1).getKey());
        Assertions.assertEquals(20L, list.get(2).getKey());
    }

    @Test
    public void testTableEntryComparator_internalDbLast() throws Exception {
        TestRebalancer r = new TestRebalancer();
        r.setInternalDbIds(Collections.singleton(1L)); // dbId=1 is internal

        Map<Long, Long> tableToDb = new HashMap<>();
        tableToDb.put(10L, 1L);
        tableToDb.put(20L, 2L);
        setField(r, "tableIdToDbId", new ConcurrentHashMap<>(tableToDb));
        setField(r, "dbIdToActiveCount", new ConcurrentHashMap<>());
        setField(r, "tableIdToActiveCount", new ConcurrentHashMap<>());

        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Long>>>> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Long>>>> list = new ArrayList<>();
        list.add(new AbstractMap.SimpleEntry<>(10L, new ConcurrentHashMap<>()));
        list.add(new AbstractMap.SimpleEntry<>(20L, new ConcurrentHashMap<>()));
        list.sort(cmp);

        Assertions.assertEquals(20L, list.get(0).getKey());
        Assertions.assertEquals(10L, list.get(1).getKey(), "Internal db table should be scheduled last");
    }

    @Test
    public void testPartitionEntryComparator_internalDbLastAndIdDescTieBreak() throws Exception {
        TestRebalancer r = new TestRebalancer();
        r.setInternalDbIds(Collections.singleton(1L)); // dbId=1 is internal

        Map<Long, Long> partToDb = new HashMap<>();
        partToDb.put(100L, 1L); // internal
        partToDb.put(200L, 2L); // normal
        partToDb.put(201L, 2L); // normal
        setField(r, "partitionIdToDbId", new ConcurrentHashMap<>(partToDb));

        Map<Long, Long> dbActive = new HashMap<>();
        dbActive.put(1L, 100L);
        dbActive.put(2L, 100L);
        setField(r, "dbIdToActiveCount", new ConcurrentHashMap<>(dbActive));

        Map<Long, Long> partActive = new HashMap<>();
        partActive.put(200L, 1L);
        partActive.put(201L, 1L);
        setField(r, "partitionIdToActiveCount", new ConcurrentHashMap<>(partActive));

        @SuppressWarnings("unchecked")
        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>> cmp =
                invokePrivate(r, "partitionEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>> list = new ArrayList<>();
        list.add(new AbstractMap.SimpleEntry<>(100L, new ConcurrentHashMap<>()));
        list.add(new AbstractMap.SimpleEntry<>(200L, new ConcurrentHashMap<>()));
        list.add(new AbstractMap.SimpleEntry<>(201L, new ConcurrentHashMap<>()));
        list.sort(cmp);

        // normal db first; for 200 vs 201 (same dbActive, same partActive) tie-breaker is id desc => 201 first
        Assertions.assertEquals(201L, list.get(0).getKey());
        Assertions.assertEquals(200L, list.get(1).getKey());
        Assertions.assertEquals(100L, list.get(2).getKey(), "Internal db partition should be scheduled last");
    }



    @Test
    public void testMigrateTabletsForSmoothUpgrade_emptyQueueDoesNotThrow() throws Exception {
        TestRebalancer r = new TestRebalancer();
        // migrateTabletsForSmoothUpgrade returns void; just verify it completes without error on empty queue
        Assertions.assertDoesNotThrow(() ->
                invokePrivate(r, "migrateTabletsForSmoothUpgrade", new Class<?>[] {}, new Object[] {}));
    }

    @Test
    public void testResetCloudBalanceMetric_clearsMetricForAllClusters() throws Exception {
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        TestRebalancer r = new TestRebalancer();
        setField(r, "cloudSystemInfoService", systemInfoService);

        Map<String, List<Long>> clusterToBes = new HashMap<>();
        clusterToBes.put("cluster-a", Collections.singletonList(1L));
        clusterToBes.put("cluster-b", Collections.singletonList(2L));
        setField(r, "clusterToBes", clusterToBes);

        Mockito.when(systemInfoService.getClusterNameByClusterId("cluster-a")).thenReturn("compute_cluster_a");
        Mockito.when(systemInfoService.getClusterNameByClusterId("cluster-b")).thenReturn("compute_cluster_b");

        try (MockedStatic<MetricRepo> metricRepo = Mockito.mockStatic(MetricRepo.class)) {
            invokePrivate(r, "resetCloudBalanceMetric",
                    new Class<?>[] {CloudTabletRebalancer.StatType.class},
                    new Object[] {CloudTabletRebalancer.StatType.PARTITION});

            metricRepo.verify(() -> MetricRepo.updateClusterCloudBalanceNum(
                    "compute_cluster_a", "cluster-a", CloudTabletRebalancer.StatType.PARTITION, 0L));
            metricRepo.verify(() -> MetricRepo.updateClusterCloudBalanceNum(
                    "compute_cluster_b", "cluster-b", CloudTabletRebalancer.StatType.PARTITION, 0L));
        }
    }

    @Test
    public void testStaleRouteSweepPreservedAcrossEmptyTopology() {
        boolean saved = Config.enable_cloud_replica_stale_route_clean;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            TestRebalancer rebalancer = new TestRebalancer();

            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(Set.of(1L)));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(Set.of(1L)));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(Set.of()));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(Set.of()));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(Set.of()));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(Set.of(2L)));
        } finally {
            Config.enable_cloud_replica_stale_route_clean = saved;
        }
    }

    @Test
    public void testIsWithinWindow() {
        // normal (non-wrapping) window
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(12, 0), LocalTime.of(10, 0), LocalTime.of(14, 0)));
        Assertions.assertFalse(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(9, 59), LocalTime.of(10, 0), LocalTime.of(14, 0)));
        Assertions.assertFalse(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(14, 1), LocalTime.of(10, 0), LocalTime.of(14, 0)));
        // boundaries are inclusive
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(10, 0), LocalTime.of(10, 0), LocalTime.of(14, 0)));
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(14, 0), LocalTime.of(10, 0), LocalTime.of(14, 0)));

        // window across midnight, e.g. 23:00 - 06:00
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(23, 30), LocalTime.of(23, 0), LocalTime.of(6, 0)));
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(1, 0), LocalTime.of(23, 0), LocalTime.of(6, 0)));
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(23, 0), LocalTime.of(23, 0), LocalTime.of(6, 0)));
        Assertions.assertTrue(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(6, 0), LocalTime.of(23, 0), LocalTime.of(6, 0)));
        Assertions.assertFalse(CloudTabletRebalancer.isWithinWindow(
                LocalTime.of(12, 0), LocalTime.of(23, 0), LocalTime.of(6, 0)));
    }

    @Test
    public void testIsStaleRouteCleanTimeAllowedDefaultsToUnrestricted() {
        String savedStart = Config.cloud_tablet_rebalancer_stale_route_clean_start_time;
        String savedEnd = Config.cloud_tablet_rebalancer_stale_route_clean_end_time;
        try {
            TestRebalancer rebalancer = new TestRebalancer();

            // the actual default
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = "00:00";
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = "00:00";
            Assertions.assertTrue(rebalancer.isStaleRouteCleanTimeAllowed());

            // any equal start/end means unrestricted, not just the default value
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = "09:30";
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = "09:30";
            Assertions.assertTrue(rebalancer.isStaleRouteCleanTimeAllowed());

            // an unparseable configuration falls back to unrestricted rather than disabling cleanup
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = "not-a-time";
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = "18:00";
            Assertions.assertTrue(rebalancer.isStaleRouteCleanTimeAllowed());
        } finally {
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = savedStart;
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = savedEnd;
        }
    }

    @Test
    public void testStaleRouteSweepGateRespectsCleanWindow() {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        String savedStart = Config.cloud_tablet_rebalancer_stale_route_clean_start_time;
        String savedEnd = Config.cloud_tablet_rebalancer_stale_route_clean_end_time;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = "00:00";
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = "00:00";
            TestRebalancer rebalancer = new TestRebalancer();
            Set<Long> bes = new HashSet<>(Arrays.asList(1L, 2L));
            // establish a clean baseline with no rounds pending
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(bes));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(bes));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(bes));

            // close the window (a 1-2 hour offset from "now" is never in it), then a backend disappears
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm");
            LocalTime now = LocalTime.now(TimeUtils.getDorisZoneId());
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = now.plusHours(1).format(fmt);
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = now.plusHours(2).format(fmt);
            Set<Long> shrunk = new HashSet<>(Arrays.asList(1L));
            // the drop is detected (baseline advances) but the window blocks the sweep -- and the two
            // rounds it owes must not be drained by rounds that never actually ran
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(shrunk));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(shrunk));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(shrunk));

            // window opens: the two rounds owed from the earlier drop are still there, untouched
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = "00:00";
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = "00:00";
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(shrunk));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(shrunk));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(shrunk));
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            Config.cloud_tablet_rebalancer_stale_route_clean_start_time = savedStart;
            Config.cloud_tablet_rebalancer_stale_route_clean_end_time = savedEnd;
        }
    }
}
