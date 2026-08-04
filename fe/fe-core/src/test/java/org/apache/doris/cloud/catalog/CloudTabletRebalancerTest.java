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
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
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
import java.util.AbstractList;
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
        private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> byTable =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                byPartition = new ConcurrentHashMap<>();
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
    public void testFillBeToTabletsReusesBoxedIdsAcrossIndexes() {
        TestRebalancer rebalancer = new TestRebalancer();
        Long beId = 10_001L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;

        ConcurrentHashMap<Long, Set<Long>> currentGlobal = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> currentByTable =
                new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>> currentByPartition =
                new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Set<Long>> futureGlobal = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> futureByTable =
                new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>> futureByPartition =
                new ConcurrentHashMap<>();

        rebalancer.fillBeToTablets(beId, tableId, partitionId, indexId, tabletId,
                currentGlobal, currentByTable, currentByPartition);
        rebalancer.fillBeToTablets(beId, tableId, partitionId, indexId, tabletId,
                futureGlobal, futureByTable, futureByPartition);

        assertSameStoredId(beId, currentGlobal);
        assertSameStoredId(beId, currentByTable.get(tableId));
        assertSameStoredId(beId, currentByPartition.get(partitionId).get(indexId));
        assertSameStoredId(beId, futureGlobal);
        assertSameStoredId(beId, futureByTable.get(tableId));
        assertSameStoredId(beId, futureByPartition.get(partitionId).get(indexId));
        assertSameStoredId(tableId, currentByTable);
        assertSameStoredId(tableId, futureByTable);
        assertSameStoredId(partitionId, currentByPartition);
        assertSameStoredId(partitionId, futureByPartition);
        assertSameStoredId(indexId, currentByPartition.get(partitionId));
        assertSameStoredId(indexId, futureByPartition.get(partitionId));
        assertSameStoredId(tabletId, currentGlobal.get(beId));
        assertSameStoredId(tabletId, currentByTable.get(tableId).get(beId));
        assertSameStoredId(tabletId, currentByPartition.get(partitionId).get(indexId).get(beId));
        assertSameStoredId(tabletId, futureGlobal.get(beId));
        assertSameStoredId(tabletId, futureByTable.get(tableId).get(beId));
        assertSameStoredId(tabletId, futureByPartition.get(partitionId).get(indexId).get(beId));
    }

    @Test
    public void testTransferTabletReusesSelectedBoxedIdsAcrossCurrentAndFutureIndexes() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tableId, partitionId, indexId, tabletId);

        try (MockedStatic<Env> ignored = mockTabletMeta(tabletId, tableId, partitionId, indexId)) {
            boolean moved = invokePrivate(rebalancer, "transferTablet", 6,
                    new Object[] {tabletId, srcBe, destBe, "cluster-a",
                            CloudTabletRebalancer.BalanceType.GLOBAL, new ArrayList<UpdateCloudReplicaInfo>()});

            Assertions.assertTrue(moved);
            assertSameRouteIds(destBe, tableId, partitionId, indexId, tabletId, current);
            assertSameRouteIds(destBe, tableId, partitionId, indexId, tabletId, future);
        }
    }

    @Test
    public void testPreheatTabletReusesSelectedBoxedIdsInFutureIndexes() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tableId, partitionId, indexId, tabletId);
        setField(rebalancer, "cloudSystemInfoService", mockBackendService(srcBe, destBe));
        Config.cloud_warm_up_batch_size = 10;

        try (MockedStatic<Env> ignored = mockTabletMeta(tabletId, tableId, partitionId, indexId)) {
            boolean moved = invokePrivate(rebalancer, "preheatAndUpdateTablet", 5,
                    new Object[] {tabletId, srcBe, destBe, "cluster-a", CloudTabletRebalancer.BalanceType.GLOBAL});

            Assertions.assertTrue(moved);
            assertSameRouteIds(destBe, tableId, partitionId, indexId, tabletId, future);
        }
    }

    @Test
    public void testWarmupRollbackRestoresSelectedBoxedIdsInFutureIndexes() throws Exception {
        TestRebalancer rebalancer = new TestRebalancer();
        Long srcBe = 10_001L;
        Long destBe = 10_002L;
        Long tableId = 20_001L;
        Long partitionId = 30_001L;
        Long indexId = 40_001L;
        Long tabletId = 50_001L;
        RouteMaps current = new RouteMaps();
        RouteMaps future = new RouteMaps();
        initializeRouteMaps(rebalancer, current, future, srcBe, tableId, partitionId, indexId, tabletId);
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

            assertSameRouteIds(srcBe, tableId, partitionId, indexId, tabletId, future);
        }
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
        initializeRouteMaps(rebalancer, current, future, srcBe, tableId, partitionId, indexId, tabletId);
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
            invokePrivate(rebalancer, "processFailedWarmupTasks", new Class<?>[] {}, new Object[] {});

            ConcurrentHashMap<Long, Set<Long>> rebuiltCurrentGlobal = getField(rebalancer, "beToTabletsGlobal");
            ConcurrentHashMap<Long, Set<Long>> rebuiltFutureGlobal = getField(
                    rebalancer, "futureBeToTabletsGlobal");
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> rebuiltCurrentByTable = getField(
                    rebalancer, "beToTabletsInTable");
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> rebuiltFutureByTable = getField(
                    rebalancer, "futureBeToTabletsInTable");
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                    rebuiltCurrentByPartition = getField(rebalancer, "partitionToTablets");
            ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                    rebuiltFutureByPartition = getField(rebalancer, "futurePartitionToTablets");
            Long currentTabletId = getStoredId(tabletId, rebuiltCurrentGlobal.get(srcBe));
            Long futureTabletId = getStoredId(tabletId, rebuiltFutureGlobal.get(srcBe));
            Assertions.assertSame(currentTabletId, futureTabletId);
            assertSameStoredId(currentTabletId, rebuiltCurrentByTable.get(tableId).get(srcBe));
            assertSameStoredId(currentTabletId, rebuiltFutureByTable.get(tableId).get(srcBe));
            assertSameStoredId(currentTabletId,
                    rebuiltCurrentByPartition.get(partitionId).get(indexId).get(srcBe));
            assertSameStoredId(currentTabletId,
                    rebuiltFutureByPartition.get(partitionId).get(indexId).get(srcBe));
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
        initializeRouteMaps(rebalancer, current, future, srcBe, tableId, partitionId, indexId, tabletId);

        invokePrivate(rebalancer, "releaseSchedulingIndexes", new Class<?>[] {}, new Object[] {});

        ConcurrentHashMap<Long, Set<Long>> currentGlobal = getField(rebalancer, "beToTabletsGlobal");
        ConcurrentHashMap<Long, Set<Long>> futureGlobal = getField(rebalancer, "futureBeToTabletsGlobal");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> releasedCurrentByTable =
                getField(rebalancer, "beToTabletsInTable");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> releasedFutureByTable =
                getField(rebalancer, "futureBeToTabletsInTable");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                releasedCurrentByPartition = getField(rebalancer, "partitionToTablets");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                releasedFutureByPartition = getField(rebalancer, "futurePartitionToTablets");
        Assertions.assertSame(current.global, currentGlobal);
        Assertions.assertSame(future.global, futureGlobal);
        Assertions.assertNotSame(current.byTable, releasedCurrentByTable);
        Assertions.assertNotSame(future.byTable, releasedFutureByTable);
        Assertions.assertNotSame(current.byPartition, releasedCurrentByPartition);
        Assertions.assertNotSame(future.byPartition, releasedFutureByPartition);
        Assertions.assertTrue(releasedCurrentByTable.isEmpty());
        Assertions.assertTrue(releasedFutureByTable.isEmpty());
        Assertions.assertTrue(releasedCurrentByPartition.isEmpty());
        Assertions.assertTrue(releasedFutureByPartition.isEmpty());

        setField(rebalancer, "clusterToBes", Collections.singletonMap(clusterId, List.of(srcBe)));
        setField(rebalancer, "allBes", Set.of(srcBe));
        try (MockedStatic<Env> ignored = mockRouteEnvironment(
                dbId, tableId, partitionId, indexId, tabletId, clusterId, srcBe)) {
            rebalancer.statRouteInfo();
        }

        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> rebuiltCurrentByTable =
                getField(rebalancer, "beToTabletsInTable");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> rebuiltFutureByTable =
                getField(rebalancer, "futureBeToTabletsInTable");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                rebuiltCurrentByPartition = getField(rebalancer, "partitionToTablets");
        ConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                rebuiltFutureByPartition = getField(rebalancer, "futurePartitionToTablets");
        Assertions.assertEquals(Set.of(tabletId), rebuiltCurrentByTable.get(tableId).get(srcBe));
        Assertions.assertEquals(Set.of(tabletId), rebuiltFutureByTable.get(tableId).get(srcBe));
        Assertions.assertEquals(Set.of(tabletId),
                rebuiltCurrentByPartition.get(partitionId).get(indexId).get(srcBe));
        Assertions.assertEquals(Set.of(tabletId),
                rebuiltFutureByPartition.get(partitionId).get(indexId).get(srcBe));
    }

    private static void initializeRouteMaps(TestRebalancer rebalancer, RouteMaps current, RouteMaps future,
            Long srcBe, Long tableId, Long partitionId, Long indexId, Long tabletId) throws Exception {
        rebalancer.fillBeToTablets(srcBe, tableId, partitionId, indexId, tabletId,
                current.global, current.byTable, current.byPartition);
        rebalancer.fillBeToTablets(srcBe, tableId, partitionId, indexId, tabletId,
                future.global, future.byTable, future.byPartition);
        setField(rebalancer, "beToTabletsGlobal", current.global);
        setField(rebalancer, "beToTabletsInTable", current.byTable);
        setField(rebalancer, "partitionToTablets", current.byPartition);
        setField(rebalancer, "futureBeToTabletsGlobal", future.global);
        setField(rebalancer, "futureBeToTabletsInTable", future.byTable);
        setField(rebalancer, "futurePartitionToTablets", future.byPartition);
    }

    private static MockedStatic<Env> mockTabletMeta(Long tabletId, Long tableId, Long partitionId, Long indexId) {
        Env env = Mockito.mock(Env.class);
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        TabletMeta tabletMeta = Mockito.mock(TabletMeta.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getTabletInvertedIndex()).thenReturn(invertedIndex);
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
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
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
        Mockito.when(invertedIndex.getTabletMeta(tabletId)).thenReturn(tabletMeta);
        Mockito.when(tabletMeta.getTableId()).thenReturn(tableId);
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

    private static void assertSameRouteIds(Long beId, Long tableId, Long partitionId, Long indexId,
            Long tabletId, RouteMaps routeMaps) {
        assertSameStoredId(beId, routeMaps.global);
        assertSameStoredId(beId, routeMaps.byTable.get(tableId));
        assertSameStoredId(beId, routeMaps.byPartition.get(partitionId).get(indexId));
        assertSameStoredId(tableId, routeMaps.byTable);
        assertSameStoredId(partitionId, routeMaps.byPartition);
        assertSameStoredId(indexId, routeMaps.byPartition.get(partitionId));
        assertSameStoredId(tabletId, routeMaps.global.get(beId));
        assertSameStoredId(tabletId, routeMaps.byTable.get(tableId).get(beId));
        assertSameStoredId(tabletId, routeMaps.byPartition.get(partitionId).get(indexId).get(beId));
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
    public void testFillBeToTabletsUsesComputedContainers() {
        TestRebalancer rebalancer = new TestRebalancer();
        long beId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;

        CountingConcurrentHashMap<Long, Set<Long>> globalBeToTablets = new CountingConcurrentHashMap<>();
        CountingConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> beToTabletsInTable =
                new CountingConcurrentHashMap<>();
        CountingConcurrentHashMap<Long, Set<Long>> beToTabletsOfTable = new CountingConcurrentHashMap<>();
        beToTabletsInTable.put(tableId, beToTabletsOfTable);

        CountingConcurrentHashMap<Long, ConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>>>
                partToTablets = new CountingConcurrentHashMap<>();
        CountingConcurrentHashMap<Long, ConcurrentHashMap<Long, Set<Long>>> indexToTablets =
                new CountingConcurrentHashMap<>();
        CountingConcurrentHashMap<Long, Set<Long>> beToTabletsOfIndex = new CountingConcurrentHashMap<>();
        partToTablets.put(partitionId, indexToTablets);
        indexToTablets.put(indexId, beToTabletsOfIndex);

        rebalancer.fillBeToTablets(beId, tableId, partitionId, indexId, 5L,
                globalBeToTablets, beToTabletsInTable, partToTablets);
        rebalancer.fillBeToTablets(beId, tableId, partitionId, indexId, 6L,
                globalBeToTablets, beToTabletsInTable, partToTablets);

        assertComputedContainerUsed(globalBeToTablets);
        assertComputedContainerUsed(beToTabletsInTable);
        assertComputedContainerUsed(beToTabletsOfTable);
        assertComputedContainerUsed(partToTablets);
        assertComputedContainerUsed(indexToTablets);
        assertComputedContainerUsed(beToTabletsOfIndex);
        Assertions.assertEquals(Set.of(5L, 6L), globalBeToTablets.get(beId));
        Assertions.assertEquals(Set.of(5L, 6L), beToTabletsOfTable.get(beId));
        Assertions.assertEquals(Set.of(5L, 6L), beToTabletsOfIndex.get(beId));
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
}
