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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class LoadBalanceScanWorkerSelectorBackendSelectionTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    void testLoadSelectionOrdersNereidsReplicaCandidates() {
        String oldCloudUniqueId = Config.cloud_unique_id;
        String oldDeployMode = Config.deploy_mode;
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        try {
            Backend first = backend(1L);
            Backend preferred = backend(2L);
            DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
            Mockito.when(workerManager.getWorker(InternalCatalog.INTERNAL_CATALOG_ID, 1L))
                    .thenReturn(new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, first));
            Mockito.when(workerManager.getWorker(InternalCatalog.INTERNAL_CATALOG_ID, 2L))
                    .thenReturn(new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, preferred));

            ConnectContext context = new ConnectContext();
            context.recordLoadBackendSelectionDecision(new BackendSelection.SelectionHint(
                    "preferred", BackendSelection.Mode.PREFER, "test"));
            BackendSelectionProvider policy = new BackendSelectionProvider() {
                @Override
                public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint hint) {
                    return true;
                }

                @Override
                public List<Backend> orderLoadCandidates(BackendSelection.SelectionHint hint,
                        List<Backend> candidates) {
                    return Arrays.asList(preferred, first);
                }
            };

            BackendSelectionManager.setProviderForTest(policy);
            LoadBalanceScanWorkerSelector selector =
                        new LoadBalanceScanWorkerSelector(workerManager, context, true);
            List<TScanRangeLocation> ordered = selector.orderLoadReplicas(
                        Arrays.asList(location(1L), location(2L)), InternalCatalog.INTERNAL_CATALOG_ID);

            Assertions.assertEquals(2L, ordered.get(0).getBackendId());
            Assertions.assertEquals(1L, ordered.get(1).getBackendId());
        } finally {
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.deploy_mode = oldDeployMode;
        }
    }

    @Test
    void testLoadSelectionUsesPreferredTierBeforeMinWorkload() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        DistributedPlanWorkerManager workerManager = workerManager(fallback, preferred);
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.PREFER, "test");
        context.recordLoadBackendSelectionDecision(hint);
        BackendSelectionManager.setProviderForTest(querySelectionProvider(hint));
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(workerManager, context, true);

        select(selector, locations(location(2L)), 100L);
        WorkerScanRanges selected = select(selector, locations(location(1L), location(2L)), 100L);

        Assertions.assertEquals(2L, selected.worker.id());
    }

    @Test
    void testQueryJobDoesNotApplyLoadSelection() {
        DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(
                workerManager, new ConnectContext(), false);
        List<TScanRangeLocation> locations = Collections.singletonList(location(1L));

        Assertions.assertSame(locations,
                selector.orderLoadReplicas(locations, InternalCatalog.INTERNAL_CATALOG_ID));
        Mockito.verifyNoInteractions(workerManager);
    }

    @Test
    void testQuerySelectionUsesPreferredTierBeforeMinWorkload() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        DistributedPlanWorkerManager workerManager = workerManager(fallback, preferred);
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.PREFER, "test");
        BackendSelectionManager.setProviderForTest(querySelectionProvider(hint));
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(workerManager, context, false);

        select(selector, locations(location(2L)), 100L);
        WorkerScanRanges selected = select(selector, locations(location(1L), location(2L)), 100L);

        Assertions.assertEquals(2L, selected.worker.id());
    }

    @Test
    void testQuerySelectionWithoutHintKeepsMinWorkloadBehavior() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        DistributedPlanWorkerManager workerManager = workerManager(fallback, preferred);
        ConnectContext context = new ConnectContext();
        BackendSelectionManager.setProviderForTest(new BackendSelectionProvider() {
        });
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(workerManager, context, false);

        select(selector, locations(location(2L)), 100L);
        WorkerScanRanges selected = select(selector, locations(location(1L), location(2L)), 100L);

        Assertions.assertEquals(1L, selected.worker.id());
    }

    @Test
    void testQuerySelectionFallsBackWhenPreferredWorkersAreUnavailable() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        preferred.setAlive(false);
        DistributedPlanWorkerManager workerManager = workerManager(fallback, preferred);
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.PREFER, "test");
        BackendSelectionManager.setProviderForTest(querySelectionProvider(hint));
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(workerManager, context, false);

        WorkerScanRanges selected = select(selector, locations(location(2L), location(1L)), 100L);

        Assertions.assertEquals(1L, selected.worker.id());
    }

    @Test
    void testSingleReplicaSelectionRejectsUnavailableWorker() {
        Backend unavailable = backend(1L, "preferred");
        unavailable.setAlive(false);
        DistributedPlanWorkerManager workerManager = workerManager(unavailable);
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(
                workerManager, new ConnectContext(), false);

        Assertions.assertThrows(AnalysisException.class,
                () -> select(selector, locations(location(1L)), 100L));
    }

    @Test
    void testSingleReplicaSelectionRejectsQueryDisabledWorker() {
        Backend queryDisabled = backend(1L, "preferred");
        queryDisabled.setQueryDisabled(true);
        DistributedPlanWorkerManager workerManager = workerManager(queryDisabled);
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(
                workerManager, new ConnectContext(), false);

        Assertions.assertThrows(AnalysisException.class,
                () -> select(selector, locations(location(1L)), 100L));
    }

    @Test
    void testSingleReplicaSelectionKeepsAvailableWorker() throws Exception {
        Backend available = backend(1L, "preferred");
        DistributedPlanWorkerManager workerManager = workerManager(available);
        LoadBalanceScanWorkerSelector selector = new LoadBalanceScanWorkerSelector(
                workerManager, new ConnectContext(), false);

        WorkerScanRanges selected = select(selector, locations(location(1L)), 100L);

        Assertions.assertEquals(1L, selected.worker.id());
    }

    @Test
    void testLoadCoordinatorSelectionHonorsPreferenceAndRequire() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint prefer = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.PREFER, "test");
        BackendSelectionProvider provider = querySelectionProvider(prefer);
        BackendSelectionManager.setProviderForTest(provider);
        context.recordLoadBackendSelectionDecision(prefer);

        Assertions.assertSame(preferred, BackendSelectionManager.chooseFirstPreferredLoadBackend(
                context, Arrays.asList(fallback, preferred), Backend::isQueryAvailable));

        preferred.setAlive(false);
        Assertions.assertSame(fallback, BackendSelectionManager.chooseFirstPreferredLoadBackend(
                context, Arrays.asList(fallback, preferred), Backend::isQueryAvailable));

        BackendSelection.SelectionHint require = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.REQUIRE, "test");
        BackendSelectionManager.setProviderForTest(querySelectionProvider(require));
        context.recordLoadBackendSelectionDecision(require);
        Assertions.assertThrows(UserException.class, () -> BackendSelectionManager.chooseFirstPreferredLoadBackend(
                context, Arrays.asList(fallback, preferred), Backend::isQueryAvailable));
    }

    @Test
    void testNereidsLoadWorkerManagerUsesPreferenceAndRecordsCoordinator() throws Exception {
        Backend fallback = backend(1L, "fallback");
        Backend preferred = backend(2L, "preferred");
        ConnectContext context = new ConnectContext();
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "preferred", BackendSelection.Mode.PREFER, "test");
        context.recordLoadBackendSelectionDecision(hint);
        BackendSelectionManager.setProviderForTest(querySelectionProvider(hint));

        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getClusterInfo()).thenReturn(systemInfo);
        Mockito.when(catalog.getId()).thenReturn(InternalCatalog.INTERNAL_CATALOG_ID);
        Mockito.when(systemInfo.getBackendsByCurrentCluster()).thenReturn(
                com.google.common.collect.ImmutableMap.of(fallback.getId(), fallback, preferred.getId(), preferred));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);

            BackendDistributedPlanWorkerManager workerManager =
                    new BackendDistributedPlanWorkerManager(context, false, true);
            Assertions.assertEquals(preferred.getId(),
                    workerManager.randomAvailableWorker(InternalCatalog.INTERNAL_CATALOG_ID).id());
            Assertions.assertTrue(context.getBackendSelectionProfile().getLoadSummary().contains(
                    "coordinator_backend=" + preferred.getId()));

            preferred.setAlive(false);
            Assertions.assertEquals(fallback.getId(),
                    workerManager.randomAvailableWorker(InternalCatalog.INTERNAL_CATALOG_ID).id());

            BackendSelection.SelectionHint require = new BackendSelection.SelectionHint(
                    "preferred", BackendSelection.Mode.REQUIRE, "test");
            context.recordLoadBackendSelectionDecision(require);
            Assertions.assertThrows(NereidsException.class,
                    () -> workerManager.randomAvailableWorker(InternalCatalog.INTERNAL_CATALOG_ID));
        }
    }

    private DistributedPlanWorkerManager workerManager(Backend... backends) {
        DistributedPlanWorkerManager workerManager = Mockito.mock(DistributedPlanWorkerManager.class);
        for (Backend backend : backends) {
            Mockito.when(workerManager.getWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend.getId()))
                    .thenReturn(new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend));
        }
        return workerManager;
    }

    private WorkerScanRanges select(LoadBalanceScanWorkerSelector selector,
            List<TScanRangeLocation> locations, long bytes) throws Exception {
        TScanRangeLocations tablet = new TScanRangeLocations();
        tablet.setLocations(locations);
        Method method = LoadBalanceScanWorkerSelector.class.getDeclaredMethod(
                "selectScanReplicaAndMinWorkloadWorker", TScanRangeLocations.class,
                long.class, boolean.class, long.class);
        method.setAccessible(true);
        try {
            return (WorkerScanRanges) method.invoke(
                    selector, tablet, bytes, false, InternalCatalog.INTERNAL_CATALOG_ID);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private BackendSelectionProvider querySelectionProvider(BackendSelection.SelectionHint hint) {
        return new BackendSelectionProvider() {
            @Override
            public BackendSelection.SelectionHint getQuerySelectionHint(ConnectContext context) {
                return hint;
            }

            @Override
            public boolean hasQuerySelectionPreference(BackendSelection.SelectionHint selectionHint) {
                return true;
            }

            @Override
            public <T> BackendSelection.CandidateSelection<T> partitionPreferredQueryCandidates(
                    BackendSelection.SelectionHint selectionHint, List<T> candidates,
                    java.util.function.Function<T, Tag> tagOf) {
                List<T> preferred = candidates.stream()
                        .filter(candidate -> "preferred".equals(tagOf.apply(candidate).value))
                        .collect(java.util.stream.Collectors.toList());
                List<T> fallback = candidates.stream()
                        .filter(candidate -> !preferred.contains(candidate))
                        .collect(java.util.stream.Collectors.toList());
                return new BackendSelection.CandidateSelection<>(preferred, fallback);
            }

            @Override
            public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint selectionHint) {
                return true;
            }

            @Override
            public List<Backend> orderLoadCandidates(BackendSelection.SelectionHint selectionHint,
                    List<Backend> candidates) {
                return candidates.stream()
                        .sorted((left, right) -> Boolean.compare(
                                "preferred".equals(right.getLocationTag().value),
                                "preferred".equals(left.getLocationTag().value)))
                        .collect(java.util.stream.Collectors.toList());
            }

            @Override
            public BackendSelection.CandidateSelection<Backend> partitionRequiredLoadCandidates(
                    BackendSelection.SelectionHint selectionHint, List<Backend> candidates) {
                Backend preferred = preferred(candidates);
                return new BackendSelection.CandidateSelection<>(
                        preferred == null ? Collections.emptyList() : Collections.singletonList(preferred),
                        candidates.stream().filter(candidate -> candidate != preferred)
                                .collect(java.util.stream.Collectors.toList()));
            }

            @Override
            public BackendSelection.CandidateSelection<Backend> partitionPreferredLoadCandidates(
                    BackendSelection.SelectionHint selectionHint, List<Backend> candidates) {
                Backend preferred = preferred(candidates);
                return new BackendSelection.CandidateSelection<>(
                        preferred == null ? Collections.emptyList() : Collections.singletonList(preferred),
                        candidates.stream().filter(candidate -> candidate != preferred)
                                .collect(java.util.stream.Collectors.toList()));
            }

            private Backend preferred(List<Backend> candidates) {
                return candidates.stream().filter(candidate -> "preferred".equals(
                        candidate.getLocationTag().value)).findFirst().orElse(null);
            }

            private Backend fallback(List<Backend> candidates) {
                return candidates.stream().filter(candidate -> !"preferred".equals(
                        candidate.getLocationTag().value)).findFirst().orElse(null);
            }
        };
    }

    private static Backend backend(long id) {
        return backend(id, "preferred");
    }

    private static Backend backend(long id, String tag) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        backend.setAlive(true);
        backend.setTagMap(Collections.singletonMap(Tag.TYPE_LOCATION, tag));
        return backend;
    }

    private static TScanRangeLocation location(long backendId) {
        TScanRangeLocation location = new TScanRangeLocation(new TNetworkAddress("127.0.0.1", 9060));
        location.setBackendId(backendId);
        return location;
    }

    private static List<TScanRangeLocation> locations(TScanRangeLocation... locations) {
        return Arrays.asList(locations);
    }
}
