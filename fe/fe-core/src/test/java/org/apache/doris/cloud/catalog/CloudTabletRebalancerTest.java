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

import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
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
    public void testPickTabletPreferCold_picksColdWhenAvailable() throws Exception {
        TestRebalancer r = new TestRebalancer();
        setField(r, "rand", new Random(1));

        Tablet hot = Mockito.mock(Tablet.class);
        Mockito.when(hot.getId()).thenReturn(100L);
        Tablet cold = Mockito.mock(Tablet.class);
        Mockito.when(cold.getId()).thenReturn(200L);

        Set<Tablet> tablets = new HashSet<>();
        tablets.add(hot);
        tablets.add(cold);

        Set<Long> activeIds = new HashSet<>();
        activeIds.add(100L);

        Set<Long> picked = new HashSet<>();

        Tablet pickedTablet = invokePrivate(r, "pickTabletPreferCold",
                new Class<?>[] {long.class, Set.class, Set.class, Set.class},
                new Object[] {1L, tablets, activeIds, picked});

        Assertions.assertNotNull(pickedTablet);
        Assertions.assertEquals(200L, pickedTablet.getId(), "Should prefer cold tablet when available");
    }

    @Test
    public void testPickTabletPreferCold_fallbackRandomWhenStatsUnavailable() throws Exception {
        TestRebalancer r = new TestRebalancer();
        setField(r, "rand", new Random(1));

        Tablet only = Mockito.mock(Tablet.class);
        Mockito.when(only.getId()).thenReturn(300L);
        Set<Tablet> tablets = new HashSet<>();
        tablets.add(only);

        // active stats unavailable -> activeIds empty or cache null
        Set<Long> activeIds = new HashSet<>();
        Set<Long> picked = new HashSet<>();

        Tablet pickedTablet = invokePrivate(r, "pickTabletPreferCold",
                new Class<?>[] {long.class, Set.class, Set.class, Set.class},
                new Object[] {1L, tablets, activeIds,  picked});

        Assertions.assertNotNull(pickedTablet);
        Assertions.assertEquals(300L, pickedTablet.getId());
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

        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> list = new ArrayList<>();
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

        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, Set<Tablet>>>> list = new ArrayList<>();
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
        Comparator<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> cmp =
                invokePrivate(r, "partitionEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Map.Entry<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Tablet>>>>> list = new ArrayList<>();
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
}


