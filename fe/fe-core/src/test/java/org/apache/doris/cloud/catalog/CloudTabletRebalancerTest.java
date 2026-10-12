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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractList;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class CloudTabletRebalancerTest {

    private boolean oldEnableActiveScheduling;
    private long oldActiveTabletIdsRefreshIntervalSecond;
    private int oldForceInactiveAfterRounds;
    private int oldWarmupBatchSize;

    @BeforeEach
    public void setUp() {
        oldEnableActiveScheduling = Config.enable_cloud_active_tablet_priority_scheduling;
        oldActiveTabletIdsRefreshIntervalSecond = Config.cloud_active_tablet_ids_refresh_interval_second;
        oldForceInactiveAfterRounds = Config.cloud_active_unbalanced_force_inactive_after_rounds;
        oldWarmupBatchSize = Config.cloud_warm_up_batch_size;
        Config.enable_cloud_active_tablet_priority_scheduling = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_cloud_active_tablet_priority_scheduling = oldEnableActiveScheduling;
        Config.cloud_active_tablet_ids_refresh_interval_second = oldActiveTabletIdsRefreshIntervalSecond;
        Config.cloud_active_unbalanced_force_inactive_after_rounds = oldForceInactiveAfterRounds;
        Config.cloud_warm_up_batch_size = oldWarmupBatchSize;
    }

    private static class TestRebalancer extends CloudTabletRebalancer {
        private final Set<Long> internalDbIds = new HashSet<>();

        TestRebalancer() {
            this(null);
        }

        TestRebalancer(CloudSystemInfoService cloudSystemInfoService) {
            super(cloudSystemInfoService);
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

    private static class CapacityTrackingRebalancer extends TestRebalancer {
        private final List<Integer> globalTabletSetInitialCapacities = new ArrayList<>();
        private final List<Integer> routeInfoListInitialCapacities = new ArrayList<>();

        @Override
        protected Set<Long> newGlobalTabletSet(int initialCapacity) {
            globalTabletSetInitialCapacities.add(initialCapacity);
            return ConcurrentHashMap.newKeySet();
        }

        @Override
        protected <T> List<T> newRouteInfoList(int initialCapacity) {
            routeInfoListInitialCapacities.add(initialCapacity);
            return super.newRouteInfoList(initialCapacity);
        }
    }

    private static class SnapshotCapacityTrackingRebalancer extends TestRebalancer {
        private final List<Integer> snapshotTabletSetInitialCapacities = new ArrayList<>();

        @Override
        protected Set<Long> newSnapshotTabletSet(int expectedSize) {
            snapshotTabletSetInitialCapacities.add(expectedSize);
            return super.newSnapshotTabletSet(expectedSize);
        }
    }

    private static class CountingConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
        private int computeIfAbsentCalls;
        private int getCalls;
        private int putIfAbsentCalls;

        @Override
        public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
            computeIfAbsentCalls++;
            return super.computeIfAbsent(key, mappingFunction);
        }

        @Override
        public V get(Object key) {
            getCalls++;
            return super.get(key);
        }

        @Override
        public V putIfAbsent(K key, V value) {
            putIfAbsentCalls++;
            return super.putIfAbsent(key, value);
        }
    }

    private static class IteratorRejectingList<E> extends AbstractList<E> {
        private final E element;

        IteratorRejectingList(E element) {
            this.element = element;
        }

        @Override
        public E get(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
            return element;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public java.util.Iterator<E> iterator() {
            throw new AssertionError("replica traversal must not allocate an iterator");
        }
    }

    private static class EmptyLookupRejectingMap<K, V> extends ConcurrentHashMap<K, V> {
        @Override
        public V get(Object key) {
            throw new AssertionError("an empty inflight map must not allocate and probe a composite key");
        }
    }

    private static void setField(Object obj, String name, Object value) throws Exception {
        Field f = CloudTabletRebalancer.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object obj, String name) throws Exception {
        Field f = CloudTabletRebalancer.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(obj);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object obj, String method, Class<?>[] types, Object[] args) throws Exception {
        Method m = CloudTabletRebalancer.class.getDeclaredMethod(method, types);
        m.setAccessible(true);
        return (T) m.invoke(obj, args);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object obj, String method, int parameterCount, Object[] args)
            throws Exception {
        Method target = null;
        for (Method candidate : CloudTabletRebalancer.class.getDeclaredMethods()) {
            if (candidate.getName().equals(method) && candidate.getParameterCount() == parameterCount) {
                target = candidate;
                break;
            }
        }
        Assertions.assertNotNull(target, "Cannot find method " + method);
        target.setAccessible(true);
        return (T) target.invoke(obj, args);
    }

    private static class RouteMaps {
        private final ConcurrentHashMap<Long, Set<Long>> global = new ConcurrentHashMap<>();
    }

    @Test
    public void testPartitionAndTableRouteIndexesAreNotRetained() {
        Set<String> fieldNames = new HashSet<>();
        for (Field field : CloudTabletRebalancer.class.getDeclaredFields()) {
            fieldNames.add(field.getName());
        }
        Assertions.assertFalse(fieldNames.contains("partitionToTablets"));
        Assertions.assertFalse(fieldNames.contains("futurePartitionToTablets"));
        Assertions.assertFalse(fieldNames.contains("beToTabletsInTable"));
        Assertions.assertFalse(fieldNames.contains("futureBeToTabletsInTable"));
    }

    @Test
    public void testInfightTabletIsStaticAndPreservesHashCode() throws Exception {
        long tabletId = 50_001L;
        String clusterId = "cluster-a";
        Class<?> infightTabletClass = null;
        for (Class<?> nestedClass : CloudTabletRebalancer.class.getDeclaredClasses()) {
            if (nestedClass.getSimpleName().equals("InfightTablet")) {
                infightTabletClass = nestedClass;
                break;
            }
        }
        Assertions.assertNotNull(infightTabletClass);
        Assertions.assertTrue(Modifier.isStatic(infightTabletClass.getModifiers()));
        Constructor<?> constructor = infightTabletClass.getDeclaredConstructor(long.class, String.class);
        constructor.setAccessible(true);
        Object infightTablet = constructor.newInstance(tabletId, clusterId);

        int expectedHashCode = 31 * (31 + Long.hashCode(tabletId)) + clusterId.hashCode();
        Assertions.assertEquals(expectedHashCode, infightTablet.hashCode());
    }

    @Test
    public void testCloudReplicaReturnsStoredBoxedPrimaryBackendId() throws Exception {
        CloudReplica replica = new CloudReplica();
        String clusterId = "cluster-a";
        long backendId = 60_001L;
        replica.updateClusterToPrimaryBe(clusterId, backendId);

        Method method = CloudReplica.class.getDeclaredMethod(
                "getNonColocatedPrimaryBackendId", String.class);
        method.setAccessible(true);
        Long first = (Long) method.invoke(replica, clusterId);
        Long second = (Long) method.invoke(replica, clusterId);

        Assertions.assertEquals(backendId, first);
        Assertions.assertSame(first, second);
    }

    @Test
    public void testSystemInfoServiceLooksUpBackendWithBoxedId() throws Exception {
        SystemInfoService systemInfoService = new SystemInfoService();
        Long backendId = Long.valueOf(60_001L);
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        systemInfoService.addBackend(backend);

        Method method = SystemInfoService.class.getDeclaredMethod("getBackendByIdWithBoxedId", Long.class);
        Backend actual = (Backend) method.invoke(systemInfoService, backendId);

        Assertions.assertSame(backend, actual);
    }

    @Test
    public void testRouteRebuildDoesNotUsePrimitivePrimaryBackendPath() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";
        Tablet tablet = mockTablet(tabletId);
        CloudReplica replica = (CloudReplica) tablet.getReplicas().get(0);
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tablet, clusterId, beId)) {
            boolean completed = invokePrivate(rebalancer, "completeRouteInfo",
                    new Class<?>[] {}, new Object[] {});
            Assertions.assertTrue(completed);
            rebalancer.statRouteInfo();
        }

        Mockito.verify(replica, Mockito.never()).getPrimaryBackend(clusterId, false);
    }

    @Test
    public void testStatRouteInfoTraversesReplicasWithoutIterator() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";
        Tablet tablet = mockTablet(tabletId);
        CloudReplica replica = (CloudReplica) tablet.getReplicas().get(0);
        Mockito.when(tablet.getReplicas()).thenReturn(new IteratorRejectingList<Replica>(replica));
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tablet, clusterId, beId)) {
            rebalancer.statRouteInfo();
        }
    }

    @Test
    public void testStatRouteInfoSkipsCompositeKeyForEmptyInflightMap() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";
        Tablet tablet = mockTablet(tabletId);
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));
        setField(rebalancer, "tabletToInfightTask", new EmptyLookupRejectingMap<>());

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tablet, clusterId, beId)) {
            rebalancer.statRouteInfo();
        }
    }

    @Test
    public void testCompleteRouteInfoPresizesRouteListsFromIndexTablets() throws Exception {
        CapacityTrackingRebalancer rebalancer = new CapacityTrackingRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, beId, 3)) {
            boolean completed = invokePrivate(rebalancer, "completeRouteInfo",
                    new Class<?>[] {}, new Object[] {});

            Assertions.assertTrue(completed);
            Assertions.assertEquals(List.of(3, 3), rebalancer.routeInfoListInitialCapacities);
        }
    }

    @Test
    public void testCompleteRouteInfoDoesNotPresizeUnusedColocateRouteLists() throws Exception {
        CapacityTrackingRebalancer rebalancer = new CapacityTrackingRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, beId, 3, true)) {
            boolean completed = invokePrivate(rebalancer, "completeRouteInfo",
                    new Class<?>[] {}, new Object[] {});

            Assertions.assertTrue(completed);
            Assertions.assertEquals(List.of(0, 0), rebalancer.routeInfoListInitialCapacities);
        }
    }

    @Test
    public void testSnapshotTabletSetsUseOnePresizedResultPerRequest() throws Exception {
        SnapshotCapacityTrackingRebalancer rebalancer = new SnapshotCapacityTrackingRebalancer();
        Long beId = 60_001L;
        ConcurrentHashMap<Long, Set<Long>> primary = new ConcurrentHashMap<>();
        primary.put(beId, Set.of(50_001L, 50_002L));
        ConcurrentHashMap<Long, Set<Long>> colocate = new ConcurrentHashMap<>();
        colocate.put(beId, Set.of(50_002L, 50_003L));
        ConcurrentHashMap<Long, Set<Long>> secondary = new ConcurrentHashMap<>();
        secondary.put(beId, Set.of(50_003L, 50_004L));
        setField(rebalancer, "beToTabletsGlobal", primary);
        setField(rebalancer, "beToColocateTabletsGlobal", colocate);
        setField(rebalancer, "beToTabletsGlobalInSecondary", secondary);

        Assertions.assertEquals(Set.of(50_001L, 50_002L, 50_003L),
                rebalancer.getSnapshotTabletsInPrimaryByBeId(beId));
        Assertions.assertEquals(Set.of(50_003L, 50_004L),
                rebalancer.getSnapshotTabletsInSecondaryByBeId(beId));
        Assertions.assertEquals(Set.of(50_001L, 50_002L, 50_003L, 50_004L),
                rebalancer.getSnapshotTabletsInPrimaryAndSecondaryByBeId(beId));

        Assertions.assertEquals(List.of(4, 2, 6),
                rebalancer.snapshotTabletSetInitialCapacities);
    }

    @Test
    public void testSnapshotTabletSetsRemainEmptyForUnknownBackend() {
        SnapshotCapacityTrackingRebalancer rebalancer = new SnapshotCapacityTrackingRebalancer();
        Long unknownBeId = 60_001L;

        Assertions.assertTrue(rebalancer.getSnapshotTabletsInPrimaryByBeId(unknownBeId).isEmpty());
        Assertions.assertTrue(rebalancer.getSnapshotTabletsInSecondaryByBeId(unknownBeId).isEmpty());
        Assertions.assertTrue(rebalancer.getSnapshotTabletsInPrimaryAndSecondaryByBeId(unknownBeId).isEmpty());
        Assertions.assertEquals(List.of(0, 0, 0),
                rebalancer.snapshotTabletSetInitialCapacities);
    }

    @Test
    public void testFillGlobalBeToTabletsReusesBoxedIds() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long beId = 10_001L;
        Long tabletId = 50_001L;

        ConcurrentHashMap<Long, Set<Long>> currentGlobal = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Set<Long>> futureGlobal = new ConcurrentHashMap<>();

        invokePrivate(rebalancer, "fillGlobalBeToTablets", 4,
                new Object[] {beId, tabletId, (Function<Long, Set<Long>>) ignored -> new HashSet<>(),
                        currentGlobal});
        invokePrivate(rebalancer, "fillGlobalBeToTablets", 4,
                new Object[] {beId, tabletId, (Function<Long, Set<Long>>) ignored -> new HashSet<>(),
                        futureGlobal});

        assertSameStoredId(beId, currentGlobal);
        assertSameStoredId(beId, futureGlobal);
        assertSameStoredId(tabletId, currentGlobal.get(beId));
        assertSameStoredId(tabletId, futureGlobal.get(beId));
    }

    @Test
    public void testTransferTabletReusesSelectedBoxedIdsAcrossGlobalRoutes() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tabletId);

        try (MockedStatic<Env> ignored = mockTabletMeta(tabletId, tableId, partitionId, indexId)) {
            boolean moved = invokePrivate(rebalancer, "transferTablet", 6,
                    new Object[] {tabletId, srcBe, destBe, "cluster-a",
                            CloudTabletRebalancer.BalanceType.GLOBAL, new ArrayList<UpdateCloudReplicaInfo>()});

            Assertions.assertTrue(moved);
            assertSameRouteIds(destBe, tabletId, current);
            assertSameRouteIds(destBe, tabletId, future);
        }
    }

    @Test
    public void testPreheatTabletReusesSelectedBoxedIdsInFutureGlobalRoute() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        Config.cloud_warm_up_batch_size = 10;

        try (MockedStatic<Env> ignored = mockTabletMeta(tabletId, tableId, partitionId, indexId)) {
            boolean moved = invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, "cluster-a", CloudTabletRebalancer.BalanceType.GLOBAL});

            Assertions.assertTrue(moved);
            assertSameRouteIds(destBe, tabletId, future);
        }
    }

    @Test
    public void testWarmupRollbackRestoresSelectedBoxedIdsInFutureGlobalRoute() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        Config.cloud_warm_up_batch_size = 10;

        try (MockedStatic<Env> ignored = mockTabletMeta(tabletId, tableId, partitionId, indexId)) {
            boolean moved = invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, "cluster-a", CloudTabletRebalancer.BalanceType.GLOBAL});
            Assertions.assertTrue(moved);

            Map<?, ?> warmupBatches = getField(rebalancer, "warmupBatches");
            Object batch = warmupBatches.values().iterator().next();
            Field tasksField = batch.getClass().getDeclaredField("tasks");
            tasksField.setAccessible(true);
            Object task = ((List<?>) tasksField.get(batch)).get(0);
            invokePrivate(rebalancer, "revertWarmupState", new Class<?>[] {task.getClass()}, new Object[] {task});

            assertSameRouteIds(srcBe, tabletId, future);
        }
    }

    @Test
    public void testTemporaryPartitionAndTableViewsFollowWarmupAndRollback() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long dbId = 15_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        String clusterId = "cluster-a";
        initializeRouteMaps(rebalancer, new RouteMaps(), new RouteMaps(), srcBe, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(srcBe, destBe)));
        setField(rebalancer, "allBes", Set.of(srcBe, destBe));
        Config.cloud_warm_up_batch_size = 10;

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            rebalancer.statRouteInfo();
            Map<Long, Map<Long, Set<Long>>> before = invokePrivate(rebalancer, "buildPartitionViews",
                    new Class<?>[] {Long.class, String.class}, new Object[] {partitionId, clusterId});
            Assertions.assertEquals(Set.of(tabletId), before.get(indexId).get(srcBe));

            boolean moved = invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, clusterId,
                            CloudTabletRebalancer.BalanceType.GLOBAL});
            Assertions.assertTrue(moved);
            Map<Long, Map<Long, Set<Long>>> warmup = invokePrivate(rebalancer, "buildPartitionViews",
                    new Class<?>[] {Long.class, String.class}, new Object[] {partitionId, clusterId});
            Assertions.assertEquals(Set.of(tabletId), warmup.get(indexId).get(destBe));
            Map<Long, Set<Long>> tableView = invokePrivate(rebalancer, "buildTableView",
                    new Class<?>[] {Long.class, String.class}, new Object[] {tableId, clusterId});
            Assertions.assertEquals(Set.of(tabletId), tableView.get(destBe));

            Object batch = ((Map<?, ?>) getField(rebalancer, "warmupBatches")).values().iterator().next();
            Field tasksField = batch.getClass().getDeclaredField("tasks");
            tasksField.setAccessible(true);
            Object task = ((List<?>) tasksField.get(batch)).get(0);
            invokePrivate(rebalancer, "revertWarmupState", new Class<?>[] {task.getClass()}, new Object[] {task});

            Map<Long, Map<Long, Set<Long>>> rollback = invokePrivate(rebalancer, "buildPartitionViews",
                    new Class<?>[] {Long.class, String.class}, new Object[] {partitionId, clusterId});
            Assertions.assertEquals(Set.of(tabletId), rollback.get(indexId).get(srcBe));
        }
    }

    @Test
    public void testLiveConflictCountsTrackWarmupAndRollback() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long dbId = 15_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        String clusterId = "cluster-a";
        initializeRouteMaps(rebalancer, new RouteMaps(), new RouteMaps(), srcBe, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        setField(rebalancer, "allBes", Set.of(srcBe, destBe));
        setField(rebalancer, "tableCountClusterId", clusterId);
        Config.cloud_warm_up_batch_size = 10;
        Class<?> variantClass = Class.forName(CloudTabletRebalancer.class.getName() + "$RouteVariant");
        Object current = variantClass.getEnumConstants()[0];
        Object future = variantClass.getEnumConstants()[1];

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            Object[] currentArgs = {srcBe, destBe, tabletId,
                    CloudTabletRebalancer.BalanceType.GLOBAL, clusterId, current};
            Object[] futureArgs = {srcBe, destBe, tabletId,
                    CloudTabletRebalancer.BalanceType.GLOBAL, clusterId, future};
            Object[] tableFutureArgs = {srcBe, destBe, tabletId,
                    CloudTabletRebalancer.BalanceType.TABLE, clusterId, future};
            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "isConflict", 6, currentArgs));
            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "isConflict", 6, futureArgs));
            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "isConflict", 6, tableFutureArgs));

            setField(rebalancer, "tableCountClusterId", "other-cluster");
            Exception mismatch = Assertions.assertThrows(Exception.class,
                    () -> invokePrivate(rebalancer, "isConflict", 6, futureArgs));
            Assertions.assertInstanceOf(IllegalStateException.class, mismatch.getCause());
            setField(rebalancer, "tableCountClusterId", clusterId);

            Assertions.assertTrue((Boolean) invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, clusterId,
                            CloudTabletRebalancer.BalanceType.GLOBAL}));
            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "isConflict", 6, currentArgs));
            Assertions.assertTrue((Boolean) invokePrivate(rebalancer, "isConflict", 6, futureArgs));
            Assertions.assertTrue((Boolean) invokePrivate(rebalancer, "isConflict", 6, tableFutureArgs));
            Map<Long, Map<Long, Integer>> futureCounts = getField(rebalancer, "futureTableCounts");
            Assertions.assertEquals(0, futureCounts.get(tableId).get(srcBe));
            Assertions.assertEquals(1, futureCounts.get(tableId).get(destBe));

            Object batch = ((Map<?, ?>) getField(rebalancer, "warmupBatches")).values().iterator().next();
            Field tasksField = batch.getClass().getDeclaredField("tasks");
            tasksField.setAccessible(true);
            Object task = ((List<?>) tasksField.get(batch)).get(0);
            invokePrivate(rebalancer, "revertWarmupState", new Class<?>[] {task.getClass()}, new Object[] {task});

            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "isConflict", 6, futureArgs));
            Assertions.assertEquals(1, futureCounts.get(tableId).get(srcBe));
            Assertions.assertEquals(0, futureCounts.get(tableId).get(destBe));
        }
    }

    @Test
    public void testMoveInViewCanMoveSameTabletTwice() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Map<Long, Set<Long>> view = new HashMap<>();
        view.put(1L, new HashSet<>(Set.of(4L)));
        invokePrivate(rebalancer, "moveInView", new Class<?>[] {Map.class, Long.class, Long.class, Long.class},
                new Object[] {view, 4L, 1L, 2L});
        invokePrivate(rebalancer, "moveInView", new Class<?>[] {Map.class, Long.class, Long.class, Long.class},
                new Object[] {view, 4L, 2L, 3L});
        Assertions.assertEquals(Set.of(), view.get(1L));
        Assertions.assertEquals(Set.of(), view.get(2L));
        Assertions.assertEquals(Set.of(4L), view.get(3L));
    }

    @Test
    public void testRandomRoutesConvergeWithoutFurtherMigrations() throws Exception {
        assertRandomRoutesConverge(BalanceTypeEnum.WITHOUT_WARMUP);
    }

    @Test
    public void testRandomRoutesConvergeAfterAsyncWarmup() throws Exception {
        assertRandomRoutesConverge(BalanceTypeEnum.ASYNC_WARMUP);
    }

    private void assertRandomRoutesConverge(BalanceTypeEnum mode) throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Config.enable_cloud_active_tablet_priority_scheduling = false;
        Config.cloud_warm_up_batch_size = Integer.MAX_VALUE;
        setField(rebalancer, "globalBalanceTypeEnum", mode);
        setField(rebalancer, "rand", new Random(7));
        Map<String, List<Long>> clusters = Map.of(
                "cluster-a", List.of(10_001L, 10_002L),
                "cluster-b", List.of(20_001L, 20_002L, 20_003L),
                "cluster-c", List.of(30_001L, 30_002L, 30_003L, 30_004L));
        Map<Long, Backend> backends = new HashMap<>();
        for (List<Long> bes : clusters.values()) {
            for (Long beId : bes) {
                Backend backend = Mockito.mock(Backend.class);
                Mockito.when(backend.isAlive()).thenReturn(true);
                backends.put(beId, backend);
            }
        }
        CloudSystemInfoService cloudService = Mockito.mock(CloudSystemInfoService.class);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        for (Map.Entry<Long, Backend> entry : backends.entrySet()) {
            Mockito.when(cloudService.getBackend(entry.getKey())).thenReturn(entry.getValue());
            Mockito.when(systemInfo.getBackend(entry.getKey())).thenReturn(entry.getValue());
            Mockito.when(systemInfo.getBackendByIdWithBoxedId(entry.getKey())).thenReturn(entry.getValue());
        }
        setField(rebalancer, "cloudSystemInfoService", cloudService);
        setField(rebalancer, "clusterToBes", clusters);
        setField(rebalancer, "allBes", backends.keySet());

        Long dbId = 15_001L;
        Long tableId = 25_001L;
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getTabletInvertedIndex()).thenReturn(invertedIndex);
        Mockito.when(catalog.getDbIds()).thenReturn(List.of(dbId));
        Mockito.when(catalog.getDbNullable(dbId)).thenReturn(db);
        Mockito.when(db.getTables()).thenReturn(List.of(table));
        Mockito.when(db.getTableNullable(tableId)).thenReturn(table);
        Mockito.when(db.getId()).thenReturn(dbId);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(colocateIndex.isColocateTable(tableId)).thenReturn(false);

        Random random = new Random(42);
        AtomicInteger migrations = new AtomicInteger();
        List<Partition> partitions = new ArrayList<>();
        Map<Long, List<CloudReplica>> replicasByPartition = new HashMap<>();
        for (int p = 0; p < 20; p++) {
            Long partitionId = 35_001L + p;
            Long indexId = 45_001L + p;
            Partition partition = Mockito.mock(Partition.class);
            MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
            List<Tablet> tablets = new ArrayList<>();
            List<CloudReplica> replicas = new ArrayList<>();
            Mockito.when(partition.getId()).thenReturn(partitionId);
            Mockito.when(partition.getIndex(indexId)).thenReturn(index);
            Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                    .thenReturn(List.of(index));
            Mockito.when(index.getId()).thenReturn(indexId);
            Mockito.when(table.getPartition(partitionId)).thenReturn(partition);
            for (int t = 0; t < 12; t++) {
                Long tabletId = 55_001L + p * 12 + t;
                TabletMeta meta = Mockito.mock(TabletMeta.class);
                Mockito.when(meta.getDbId()).thenReturn(dbId);
                Mockito.when(meta.getTableId()).thenReturn(tableId);
                Mockito.when(meta.getPartitionId()).thenReturn(partitionId);
                Mockito.when(meta.getIndexId()).thenReturn(indexId);
                Mockito.when(invertedIndex.getTabletMeta(tabletId)).thenReturn(meta);
                CloudTablet tablet = Mockito.mock(CloudTablet.class);
                CloudReplica replica = Mockito.mock(CloudReplica.class);
                Map<String, Long> routes = new HashMap<>();
                for (Map.Entry<String, List<Long>> cluster : clusters.entrySet()) {
                    List<Long> bes = cluster.getValue();
                    routes.put(cluster.getKey(), bes.get(random.nextInt(4) == 0 ? random.nextInt(bes.size()) : 0));
                }
                Mockito.when(tablet.getId()).thenReturn(tabletId);
                Mockito.when(tablet.getReplicas()).thenReturn(List.of(replica));
                Mockito.when(tablet.getCloudReplica()).thenReturn(replica);
                Mockito.when(index.getTablet(tabletId)).thenReturn(tablet);
                Mockito.when(replica.getNonColocatedPrimaryBackendId(Mockito.anyString()))
                        .thenAnswer(invocation -> routes.get(invocation.getArgument(0)));
                Mockito.doAnswer(invocation -> {
                    routes.put(invocation.getArgument(0), invocation.getArgument(1));
                    migrations.incrementAndGet();
                    return null;
                }).when(replica).updateClusterToPrimaryBe(Mockito.anyString(), Mockito.anyLong());
                Mockito.when(replica.discardRouteIfBackendVanished(Mockito.anyLong())).thenReturn(true);
                tablets.add(tablet);
                replicas.add(replica);
            }
            Mockito.when(index.getTablets()).thenReturn(tablets);
            partitions.add(partition);
            replicasByPartition.put(partitionId, replicas);
        }
        Mockito.when(table.getAllPartitions()).thenReturn(partitions);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            mockedEnv.when(Env::getCurrentColocateIndex).thenReturn(colocateIndex);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            int initialExcess = 0;
            for (Map.Entry<String, List<Long>> cluster : clusters.entrySet()) {
                for (List<CloudReplica> replicas : replicasByPartition.values()) {
                    Map<Long, Integer> counts = new HashMap<>();
                    for (CloudReplica replica : replicas) {
                        counts.merge(replica.getNonColocatedPrimaryBackendId(cluster.getKey()), 1, Integer::sum);
                    }
                    for (Long be : cluster.getValue()) {
                        initialExcess += Math.max(0, counts.getOrDefault(be, 0)
                                - replicas.size() / cluster.getValue().size());
                    }
                }
            }
            int previousMoves;
            int rounds = 0;
            do {
                previousMoves = migrations.get();
                rebalancer.statRouteInfo();
                invokePrivate(rebalancer, "performBalancing", new Class<?>[] {}, new Object[] {});
                completeWarmups(rebalancer, mode);
                rounds++;
            } while (migrations.get() != previousMoves && rounds < 30);
            Assertions.assertTrue(rounds < 30, "routes did not converge");
            Assertions.assertTrue(migrations.get() > 0, "test must exercise migrations");
            Assertions.assertTrue(migrations.get() <= initialExcess * 2,
                    "migrations " + migrations.get() + " exceed twice initial excess " + initialExcess);
            for (Map.Entry<String, List<Long>> cluster : clusters.entrySet()) {
                for (List<CloudReplica> replicas : replicasByPartition.values()) {
                    assertBalancedRoutes(rebalancer, replicas, cluster.getKey(), cluster.getValue());
                }
                List<CloudReplica> allReplicas = new ArrayList<>();
                replicasByPartition.values().forEach(allReplicas::addAll);
                assertBalancedRoutes(rebalancer, allReplicas, cluster.getKey(), cluster.getValue());
            }
            int convergedMoves = migrations.get();
            for (int i = 0; i < 5; i++) {
                rebalancer.statRouteInfo();
                invokePrivate(rebalancer, "performBalancing", new Class<?>[] {}, new Object[] {});
                completeWarmups(rebalancer, mode);
                Assertions.assertEquals(convergedMoves, migrations.get(), "migration after convergence in round " + i);
            }
        }
    }

    private static void completeWarmups(TestRebalancer rebalancer, BalanceTypeEnum mode) throws Exception {
        if (mode != BalanceTypeEnum.ASYNC_WARMUP) {
            return;
        }
        Map<?, ?> tasks = getField(rebalancer, "tabletToInfightTask");
        for (Map.Entry<?, ?> entry : new ArrayList<>(tasks.entrySet())) {
            Object key = entry.getKey();
            Field tabletId = key.getClass().getDeclaredField("tabletId");
            Field clusterId = key.getClass().getDeclaredField("clusterId");
            tabletId.setAccessible(true);
            clusterId.setAccessible(true);
            invokePrivate(rebalancer, "handleWarmupCompletion", 5,
                    new Object[] {entry.getValue(), clusterId.get(key), true, tabletId.getLong(key), new ArrayList<>()});
        }
    }

    private static void assertBalancedRoutes(TestRebalancer rebalancer, List<CloudReplica> replicas,
            String clusterId, List<Long> bes) throws Exception {
        Map<Long, Integer> counts = new HashMap<>();
        for (CloudReplica replica : replicas) {
            counts.merge(replica.getNonColocatedPrimaryBackendId(clusterId), 1, Integer::sum);
        }
        Long srcBe = bes.stream().max(Comparator.comparingInt(be -> counts.getOrDefault(be, 0))).orElseThrow();
        int max = counts.getOrDefault(srcBe, 0);
        int min = bes.stream().mapToInt(be -> counts.getOrDefault(be, 0)).min().orElseThrow();
        boolean transferValid = invokePrivate(rebalancer, "isTransferValid",
                new Class<?>[] {long.class, long.class, long.class, long.class},
                new Object[] {srcBe, (long) min, (long) max, (long) replicas.size() / bes.size()});
        Assertions.assertFalse(transferValid, clusterId + " route counts: " + counts);
    }

    @Test
    public void testWarmupRollbackReusesInflightBoxedTabletIdAfterRouteRebuild() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long dbId = 15_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        String clusterId = "cluster-a";
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(srcBe, destBe)));
        setField(rebalancer, "allBes", Set.of(srcBe, destBe));
        Config.cloud_warm_up_batch_size = 10;

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            boolean moved = invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, clusterId,
                            CloudTabletRebalancer.BalanceType.GLOBAL});
            Assertions.assertTrue(moved);

            Map<?, ?> warmupBatches = getField(rebalancer, "warmupBatches");
            Object batch = warmupBatches.values().iterator().next();
            Field tasksField = batch.getClass().getDeclaredField("tasks");
            tasksField.setAccessible(true);
            Object task = ((List<?>) tasksField.get(batch)).get(0);

            rebalancer.statRouteInfo();
            invokePrivate(rebalancer, "handleWarmupBatchFailure",
                    new Class<?>[] {List.class, Exception.class},
                    new Object[] {Collections.singletonList(task), null});
            boolean rolledBack = invokePrivate(rebalancer, "processFailedWarmupTasks",
                    new Class<?>[] {}, new Object[] {});
            Assertions.assertTrue(rolledBack);
            Assertions.assertFalse((Boolean) invokePrivate(rebalancer, "processFailedWarmupTasks",
                    new Class<?>[] {}, new Object[] {}));

            ConcurrentHashMap<Long, Set<Long>> rebuiltCurrentGlobal = getField(rebalancer, "beToTabletsGlobal");
            ConcurrentHashMap<Long, Set<Long>> rebuiltFutureGlobal = getField(
                    rebalancer, "futureBeToTabletsGlobal");
            Long currentTabletId = getStoredId(tabletId, rebuiltCurrentGlobal.get(srcBe));
            Long futureTabletId = getStoredId(tabletId, rebuiltFutureGlobal.get(srcBe));
            Assertions.assertSame(currentTabletId, futureTabletId);
            Map<Long, Map<Long, Set<Long>>> partitionViews = invokePrivate(rebalancer,
                    "buildPartitionViews", new Class<?>[] {Long.class, String.class},
                    new Object[] {partitionId, clusterId});
            Assertions.assertEquals(Set.of(tabletId), partitionViews.get(indexId).get(srcBe));
        }
    }

    @Test
    public void testStatRouteInfoPresizesGlobalTabletSetsFromPreviousRoute() throws Exception {
        CapacityTrackingRebalancer rebalancer = new CapacityTrackingRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";

        ConcurrentHashMap<Long, Set<Long>> previousCurrent = new ConcurrentHashMap<>();
        previousCurrent.put(beId, Set.of(1L, 2L, 3L));
        ConcurrentHashMap<Long, Set<Long>> previousFuture = new ConcurrentHashMap<>();
        previousFuture.put(beId, Set.of(1L, 2L, 3L, 4L, 5L));
        setField(rebalancer, "beToTabletsGlobal", previousCurrent);
        setField(rebalancer, "futureBeToTabletsGlobal", previousFuture);
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, beId)) {
            rebalancer.statRouteInfo();
        }

        Assertions.assertEquals(List.of(3, 5), rebalancer.globalTabletSetInitialCapacities);
        ConcurrentHashMap<Long, Set<Long>> current = getField(rebalancer, "beToTabletsGlobal");
        ConcurrentHashMap<Long, Set<Long>> future = getField(rebalancer, "futureBeToTabletsGlobal");
        Assertions.assertEquals(Set.of(tabletId), current.get(beId));
        Assertions.assertEquals(Set.of(tabletId), future.get(beId));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStatRouteInfoBoundsStaleGlobalTabletSetCapacity() throws Exception {
        CapacityTrackingRebalancer rebalancer = new CapacityTrackingRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";

        Set<Long> stalePreviousTablets = Mockito.mock(Set.class);
        Mockito.when(stalePreviousTablets.size()).thenReturn(2_000_000);
        ConcurrentHashMap<Long, Set<Long>> previousCurrent = new ConcurrentHashMap<>();
        previousCurrent.put(beId, stalePreviousTablets);
        ConcurrentHashMap<Long, Set<Long>> previousFuture = new ConcurrentHashMap<>();
        previousFuture.put(beId, stalePreviousTablets);
        setField(rebalancer, "beToTabletsGlobal", previousCurrent);
        setField(rebalancer, "futureBeToTabletsGlobal", previousFuture);
        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, beId)) {
            rebalancer.statRouteInfo();
        }

        Assertions.assertEquals(List.of(65_536, 65_536),
                rebalancer.globalTabletSetInitialCapacities);
        ConcurrentHashMap<Long, Set<Long>> current = getField(rebalancer, "beToTabletsGlobal");
        ConcurrentHashMap<Long, Set<Long>> future = getField(rebalancer, "futureBeToTabletsGlobal");
        Assertions.assertEquals(Set.of(tabletId), current.get(beId));
        Assertions.assertEquals(Set.of(tabletId), future.get(beId));
    }

    @Test
    public void testStatRouteInfoUsesZeroCapacityForNewBackend() throws Exception {
        CapacityTrackingRebalancer rebalancer = new CapacityTrackingRebalancer();
        Long dbId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        Long beId = 60_001L;
        String clusterId = "cluster-a";

        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(beId)));
        setField(rebalancer, "allBes", Set.of(beId));

        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, beId)) {
            rebalancer.statRouteInfo();
        }

        Assertions.assertEquals(List.of(0, 0), rebalancer.globalTabletSetInitialCapacities);
    }

    @Test
    public void testReleaseSchedulingIndexesKeepsGlobalRoutesAndAllowsNextRebuild() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long dbId = 15_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        String clusterId = "cluster-a";
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tabletId);

        invokePrivate(rebalancer, "releaseSchedulingIndexes", new Class<?>[] {}, new Object[] {});

        ConcurrentHashMap<Long, Set<Long>> currentGlobal = getField(rebalancer, "beToTabletsGlobal");
        ConcurrentHashMap<Long, Set<Long>> futureGlobal = getField(rebalancer, "futureBeToTabletsGlobal");
        Assertions.assertSame(current.global, currentGlobal);
        Assertions.assertSame(future.global, futureGlobal);
        Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "partitionIdToTableId")).isEmpty());
        Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "currentTableCounts")).isEmpty());
        Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "futureTableCounts")).isEmpty());

        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(srcBe)));
        setField(rebalancer, "allBes", Set.of(srcBe));
        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            rebalancer.statRouteInfo();
        }

        Map<Long, Long> rebuiltPartitions = getField(rebalancer, "partitionIdToTableId");
        Assertions.assertEquals(tableId, rebuiltPartitions.get(partitionId));
        ConcurrentHashMap<Long, Set<Long>> rebuiltCurrentGlobal = getField(rebalancer, "beToTabletsGlobal");
        ConcurrentHashMap<Long, Set<Long>> rebuiltFutureGlobal = getField(rebalancer, "futureBeToTabletsGlobal");
        Assertions.assertEquals(Set.of(tabletId), rebuiltCurrentGlobal.get(srcBe));
        Assertions.assertEquals(Set.of(tabletId), rebuiltFutureGlobal.get(srcBe));
    }

    @Test
    public void testRunAfterCatalogReadyReleasesSchedulingIndexesWhenMigrationFails() throws Exception {
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long dbId = 15_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        String clusterId = "cluster-a";
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Backend srcBackend = Mockito.mock(Backend.class);
        Mockito.when(systemInfoService.getAllBackendIds()).thenReturn(List.of(srcBe));
        Mockito.when(systemInfoService.getBackend(srcBe)).thenReturn(srcBackend);
        Mockito.when(srcBackend.getCloudClusterId()).thenReturn(clusterId);
        TestRebalancer rebalancer = new TestRebalancer(systemInfoService);
        rebalancer.addTabletMigrationTask(srcBe, destBe);

        boolean oldEnableCloudMultiReplica = Config.enable_cloud_multi_replica;
        Config.enable_cloud_multi_replica = false;
        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            TabletInvertedIndex invertedIndex = Env.getCurrentEnv().getTabletInvertedIndex();
            Mockito.when(invertedIndex.getTabletMeta(tabletId))
                    .thenThrow(new RuntimeException("injected migration failure"));

            RuntimeException exception = Assertions.assertThrows(
                    RuntimeException.class, rebalancer::runAfterCatalogReady);

            Assertions.assertEquals("injected migration failure", exception.getMessage());
            ConcurrentHashMap<Long, Set<Long>> currentGlobal = getField(rebalancer, "beToTabletsGlobal");
            Assertions.assertEquals(Set.of(tabletId), currentGlobal.get(srcBe));
            Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "partitionIdToTableId")).isEmpty());
            Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "currentTableCounts")).isEmpty());
            Assertions.assertTrue(((Map<?, ?>) getField(rebalancer, "futureTableCounts")).isEmpty());
        } finally {
            Config.enable_cloud_multi_replica = oldEnableCloudMultiReplica;
        }
    }

    private static void initializeRouteMaps(TestRebalancer rebalancer, RouteMaps current, RouteMaps future,
            Long srcBe, Long tabletId) throws Exception {
        current.global.computeIfAbsent(srcBe, ignored -> new HashSet<>()).add(tabletId);
        future.global.computeIfAbsent(srcBe, ignored -> new HashSet<>()).add(tabletId);
        setField(rebalancer, "beToTabletsGlobal", current.global);
        setField(rebalancer, "futureBeToTabletsGlobal", future.global);
    }

    private static MockedStatic<Env> mockTabletMeta(Long tabletId, Long tableId, Long partitionId, Long indexId) {
        Env env = Mockito.mock(Env.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        TabletMeta tabletMeta = Mockito.mock(TabletMeta.class);
        CloudInternalCatalog catalog = Mockito.spy(new CloudInternalCatalog());
        Mockito.when(env.getTabletInvertedIndex()).thenReturn(invertedIndex);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(invertedIndex.getTabletMeta(tabletId)).thenReturn(tabletMeta);
        Mockito.when(tabletMeta.getTableId()).thenReturn(tableId);
        Mockito.when(tabletMeta.getPartitionId()).thenReturn(partitionId);
        Mockito.when(tabletMeta.getIndexId()).thenReturn(indexId);
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return mockedEnv;
    }

    private static MockedStatic<Env> mockRouteEnvironment(Long dbId, Long tableId, Long partitionId,
            Long indexId, Long tabletId, String clusterId, Long srcBe) {
        return mockRouteEnvironment(dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe, 1, false);
    }

    private static MockedStatic<Env> mockRouteEnvironment(Long dbId, Long tableId, Long partitionId,
            Long indexId, Long tabletId, String clusterId, Long srcBe, int tabletCount) {
        return mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe, tabletCount, false);
    }

    private static MockedStatic<Env> mockRouteEnvironment(Long dbId, Long tableId, Long partitionId,
            Long indexId, Long tabletId, String clusterId, Long srcBe, int tabletCount, boolean colocated) {
        return mockRouteEnvironment(dbId, tableId, partitionId, indexId, mockTablet(tabletId),
                clusterId, srcBe, tabletCount, colocated);
    }

    private static Tablet mockTablet(long tabletId) {
        Tablet tablet = Mockito.mock(Tablet.class);
        CloudReplica replica = Mockito.mock(CloudReplica.class);
        Mockito.when(tablet.getId()).thenReturn(tabletId);
        Mockito.when(tablet.getReplicas()).thenReturn(Collections.singletonList(replica));
        return tablet;
    }

    private static MockedStatic<Env> mockRouteEnvironment(Long dbId, Long tableId, Long partitionId,
            Long indexId, Tablet tablet, String clusterId, Long srcBe) {
        return mockRouteEnvironment(dbId, tableId, partitionId, indexId, tablet, clusterId, srcBe, 1, false);
    }

    private static MockedStatic<Env> mockRouteEnvironment(Long dbId, Long tableId, Long partitionId,
            Long indexId, Tablet tablet, String clusterId, Long srcBe, int tabletCount, boolean colocated) {
        Env env = Mockito.mock(Env.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        TabletMeta tabletMeta = Mockito.mock(TabletMeta.class);
        CloudInternalCatalog catalog = Mockito.spy(new CloudInternalCatalog());
        ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        CloudReplica replica = (CloudReplica) tablet.getReplicas().get(0);
        Backend primaryBackend = Mockito.mock(Backend.class);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Long tabletId = tablet.getId();

        Mockito.when(env.getTabletInvertedIndex()).thenReturn(invertedIndex);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getClusterInfo()).thenReturn(systemInfoService);
        Mockito.when(invertedIndex.getTabletMeta(tabletId)).thenReturn(tabletMeta);
        Mockito.when(tabletMeta.getTableId()).thenReturn(tableId);
        Mockito.when(tabletMeta.getDbId()).thenReturn(dbId);
        Mockito.when(tabletMeta.getPartitionId()).thenReturn(partitionId);
        Mockito.when(tabletMeta.getIndexId()).thenReturn(indexId);
        Mockito.when(catalog.getDbIds()).thenReturn(Collections.singletonList(dbId));
        Mockito.when(catalog.getDbNullable(dbId)).thenReturn(database);
        Mockito.when(database.getTables()).thenReturn(Collections.singletonList(table));
        Mockito.when(database.getId()).thenReturn(dbId);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(colocateTableIndex.isColocateTable(tableId)).thenReturn(colocated);
        Mockito.when(table.getAllPartitions()).thenReturn(Collections.singletonList(partition));
        Mockito.when(database.getTableNullable(tableId)).thenReturn(table);
        Mockito.when(table.getPartition(partitionId)).thenReturn(partition);
        Mockito.when(partition.getId()).thenReturn(partitionId);
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(Collections.singletonList(index));
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(Collections.singletonList(index));
        Mockito.when(index.getId()).thenReturn(indexId);
        Mockito.when(index.getTablets()).thenReturn(Collections.nCopies(tabletCount, tablet));
        Mockito.when(replica.getNonColocatedPrimaryBackendId(clusterId)).thenReturn(srcBe);
        Mockito.when(systemInfoService.getBackendByIdWithBoxedId(srcBe)).thenReturn(primaryBackend);
        Mockito.when(replica.getPrimaryBackend(clusterId, false)).thenReturn(primaryBackend);
        Mockito.when(primaryBackend.getId()).thenReturn(srcBe);
        Mockito.when(primaryBackend.isQueryAvailable()).thenReturn(true);

        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnv.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        return mockedEnv;
    }

    private static CloudSystemInfoService mockBackendService(Long srcBe, Long destBe) {
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfoService.getBackend(srcBe)).thenReturn(Mockito.mock(Backend.class));
        Mockito.when(systemInfoService.getBackend(destBe)).thenReturn(Mockito.mock(Backend.class));
        return systemInfoService;
    }

    private static void assertSameRouteIds(Long beId, Long tabletId, RouteMaps routeMaps) {
        assertSameStoredId(beId, routeMaps.global);
        assertSameStoredId(tabletId, routeMaps.global.get(beId));
    }

    private static <V> void assertSameStoredId(Long expected, Map<Long, V> map) {
        Long stored = map.keySet().stream().filter(expected::equals).findFirst().orElseThrow();
        Assertions.assertSame(expected, stored);
    }

    private static void assertSameStoredId(Long expected, Set<Long> ids) {
        Assertions.assertSame(expected, getStoredId(expected, ids));
    }

    private static Long getStoredId(Long expected, Set<Long> ids) {
        return ids.stream().filter(expected::equals).findFirst().orElseThrow();
    }

    @Test
    public void testFillGlobalBeToTabletsUsesComputedContainer() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long beId = 1L;
        CountingConcurrentHashMap<Long, Set<Long>> global = new CountingConcurrentHashMap<>();
        Function<Long, Set<Long>> factory = ignored -> new HashSet<>();

        invokePrivate(rebalancer, "fillGlobalBeToTablets", 4, new Object[] {beId, 5L, factory, global});
        invokePrivate(rebalancer, "fillGlobalBeToTablets", 4, new Object[] {beId, 6L, factory, global});

        assertComputedContainerUsed(global);
        Assertions.assertEquals(Set.of(5L, 6L), global.get(beId));
    }

    private static void assertComputedContainerUsed(CountingConcurrentHashMap<?, ?> map) {
        Assertions.assertEquals(2, map.computeIfAbsentCalls);
        Assertions.assertEquals(0, map.putIfAbsentCalls);
        Assertions.assertEquals(0, map.getCalls);
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

        Comparator<Long> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Long> list = new ArrayList<>();
        list.add(10L);
        list.add(11L);
        list.add(20L);

        list.sort(cmp);

        // dbId=1 entries first, and for tableId tie-breaker is desc (11 before 10)
        Assertions.assertEquals(11L, list.get(0));
        Assertions.assertEquals(10L, list.get(1));
        Assertions.assertEquals(20L, list.get(2));
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

        Comparator<Long> cmp =
                invokePrivate(r, "tableEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Long> list = new ArrayList<>();
        list.add(10L);
        list.add(20L);
        list.sort(cmp);

        Assertions.assertEquals(20L, list.get(0));
        Assertions.assertEquals(10L, list.get(1), "Internal db table should be scheduled last");
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
        Comparator<Long> cmp =
                invokePrivate(r, "partitionEntryComparator", new Class<?>[] {}, new Object[] {});

        List<Long> list = new ArrayList<>();
        list.add(100L);
        list.add(200L);
        list.add(201L);
        list.sort(cmp);

        // normal db first; for 200 vs 201 (same dbActive, same partActive) tie-breaker is id desc => 201 first
        Assertions.assertEquals(201L, list.get(0));
        Assertions.assertEquals(200L, list.get(1));
        Assertions.assertEquals(100L, list.get(2), "Internal db partition should be scheduled last");
    }

    @Test
    public void testShouldForceInactivePhase_afterConsecutiveUnbalancedRounds() throws Exception {
        TestRebalancer r = new TestRebalancer();
        Config.cloud_active_unbalanced_force_inactive_after_rounds = 3;

        boolean forceRound1 = invokePrivate(r, "shouldForceInactivePhase",
                new Class<?>[] {boolean.class}, new Object[] {false});
        boolean forceRound2 = invokePrivate(r, "shouldForceInactivePhase",
                new Class<?>[] {boolean.class}, new Object[] {false});
        boolean forceRound3 = invokePrivate(r, "shouldForceInactivePhase",
                new Class<?>[] {boolean.class}, new Object[] {false});

        Assertions.assertFalse(forceRound1);
        Assertions.assertFalse(forceRound2);
        Assertions.assertTrue(forceRound3);
        Assertions.assertEquals(0, (int) getField(r, "consecutiveActiveUnbalancedRounds"));

        boolean forceAfterBalanced = invokePrivate(r, "shouldForceInactivePhase",
                new Class<?>[] {boolean.class}, new Object[] {true});
        Assertions.assertFalse(forceAfterBalanced);
        Assertions.assertEquals(0, (int) getField(r, "consecutiveActiveUnbalancedRounds"));
    }

    @Test
    public void testShouldRefreshActiveTabletIds_respectsIntervalAndClamp() throws Exception {
        TestRebalancer r = new TestRebalancer();

        Config.cloud_active_tablet_ids_refresh_interval_second = 60L;
        setField(r, "lastActiveTabletIdsRefreshMs", 0L);
        boolean firstRound = invokePrivate(r, "shouldRefreshActiveTabletIds",
                new Class<?>[] {long.class}, new Object[] {1000L});
        Assertions.assertTrue(firstRound);

        setField(r, "lastActiveTabletIdsRefreshMs", 1000L);
        boolean beforeInterval = invokePrivate(r, "shouldRefreshActiveTabletIds",
                new Class<?>[] {long.class}, new Object[] {60000L});
        boolean atInterval = invokePrivate(r, "shouldRefreshActiveTabletIds",
                new Class<?>[] {long.class}, new Object[] {61000L});
        Assertions.assertFalse(beforeInterval);
        Assertions.assertTrue(atInterval);

        Config.cloud_active_tablet_ids_refresh_interval_second = 0L; // clamp to 1s
        setField(r, "lastActiveTabletIdsRefreshMs", 1000L);
        boolean beforeClampInterval = invokePrivate(r, "shouldRefreshActiveTabletIds",
                new Class<?>[] {long.class}, new Object[] {1500L});
        boolean atClampInterval = invokePrivate(r, "shouldRefreshActiveTabletIds",
                new Class<?>[] {long.class}, new Object[] {2000L});
        Assertions.assertFalse(beforeClampInterval);
        Assertions.assertTrue(atClampInterval);
    }

    @Test
    public void testMigrateTabletsForSmoothUpgrade_emptyQueueReturnsFalse() throws Exception {
        TestRebalancer r = new TestRebalancer();
        boolean migrated = invokePrivate(r, "migrateTabletsForSmoothUpgrade", new Class<?>[] {}, new Object[] {});
        Assertions.assertFalse(migrated);
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
    public void testStaleRouteSweepGate() {
        boolean saved = Config.enable_cloud_replica_stale_route_clean;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            TestRebalancer rebalancer = new TestRebalancer();
            Set<Long> bes = new HashSet<>(Arrays.asList(1L, 2L));

            // no baseline yet, so sweep -- twice, to catch route writers in flight during the first pass
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(bes));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(bes));
            // unchanged topology: nothing can have gone stale
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(bes));

            // a new backend strands nothing, so it does not trigger a sweep on its own
            Set<Long> grown = new HashSet<>(Arrays.asList(1L, 2L, 3L));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(grown));
            // ... but it must have entered the baseline, so dropping it again is still seen as a removal
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L, 2L))));
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L, 2L))));
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L, 2L))));

            // a backend that disappeared triggers a sweep
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L))));

            // Cleanup must also run after the last compute group disappears.
            TestRebalancer lastGroupRebalancer = new TestRebalancer();
            Assertions.assertTrue(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L))));
            Assertions.assertTrue(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(1L))));
            Assertions.assertTrue(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>()));
            Assertions.assertTrue(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>()));
            Assertions.assertFalse(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>()));
            Assertions.assertFalse(lastGroupRebalancer.staleRouteSweepNeeded(new HashSet<>(Arrays.asList(2L))));

            // turning the switch off drops the baseline so turning it back on sweeps again
            Config.enable_cloud_replica_stale_route_clean = false;
            Assertions.assertFalse(rebalancer.staleRouteSweepNeeded(bes));
            Config.enable_cloud_replica_stale_route_clean = true;
            Assertions.assertTrue(rebalancer.staleRouteSweepNeeded(bes));
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
