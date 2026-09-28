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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.Planner;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class CoordinatorBackendSelectionTest {

    @AfterEach
    void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    void testCoordinatorQuerySelectionUsesCoordinatorContext() throws Exception {
        ConnectContext context = context(3L);
        Coordinator coordinator = coordinator(context);
        Backend first = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(first.getId(), first, preferred.getId(), preferred);
        TScanRangeLocations locations = locations(first, preferred);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", true);

        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        BackendSelectionManager.setProviderForTest(policy);
        try {
            Reference<Long> backendId = new Reference<>();

            selectBackendsByRoundRobin(coordinator, locations, backendId);

            Assertions.assertEquals(preferred.getId(), backendId.getRef());
            Assertions.assertSame(context, policy.context);
            Assertions.assertEquals(
                    "preferred=tag_b, mode=PREFER, selected_preferred_scan_ranges=1, "
                            + "selected_non_preferred_scan_ranges=0",
                    context.getBackendSelectionProfile().getQuerySummary());
        } finally {
            restoreContext(previous);
        }
    }

    @Test
    void testCoordinatorPreferSelectionDoesNotUseFallbackForLowerLoad() throws Exception {
        ConnectContext context = context(6L);
        Coordinator coordinator = coordinator(context);
        Backend remote = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(remote.getId(), remote, preferred.getId(), preferred);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", true);

        Map<TNetworkAddress, Long> assignedBytes = new HashMap<>();
        assignedBytes.put(new TNetworkAddress(preferred.getHost(), preferred.getBePort()), 10L);
        Map<TNetworkAddress, Long> replicaNum = new HashMap<>();
        replicaNum.put(new TNetworkAddress(remote.getHost(), remote.getBePort()), 1L);
        replicaNum.put(new TNetworkAddress(preferred.getHost(), preferred.getBePort()), 1L);

        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        BackendSelectionManager.setProviderForTest(policy);
        try {
            Reference<Long> backendId = new Reference<>();

            selectBackendsByRoundRobin(coordinator, locations(remote, preferred), backendId,
                    assignedBytes, replicaNum);

            Assertions.assertEquals(preferred.getId(), backendId.getRef());
        } finally {
            restoreContext(previous);
        }
    }

    @Test
    void testCoordinatorPreferSelectionFallsBackWhenPreferredUnavailable() throws Exception {
        ConnectContext context = context(9L);
        Coordinator coordinator = coordinator(context);
        Backend fallback = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        preferred.setAlive(false);
        coordinator.idToBackend = ImmutableMap.of(fallback.getId(), fallback, preferred.getId(), preferred);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", true);

        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        BackendSelectionManager.setProviderForTest(policy);
        try {
            Reference<Long> backendId = new Reference<>();

            selectBackendsByRoundRobin(coordinator, locations(fallback, preferred), backendId);

            Assertions.assertEquals(fallback.getId(), backendId.getRef());
        } finally {
            restoreContext(previous);
        }
    }

    @Test
    void testCoordinatorPreferSelectionConfinesToAvailablePreferredTier() throws Exception {
        ConnectContext context = context(10L);
        Coordinator coordinator = coordinator(context);
        Backend fallback = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(fallback.getId(), fallback, preferred.getId(), preferred);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", true);

        BackendSelectionManager.setProviderForTest(policy);
        Reference<Long> backendId = new Reference<>();

        selectBackendsByRoundRobin(coordinator, locations(fallback, preferred), backendId);

        Assertions.assertEquals(preferred.getId(), backendId.getRef());
    }

    @Test
    void testCoordinatorRequiredSelectionErrorsWhenPreferredUnavailable() throws Exception {
        ConnectContext context = context(11L);
        Coordinator coordinator = coordinator(context);
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        preferred.setAlive(false);
        coordinator.idToBackend = ImmutableMap.of(preferred.getId(), preferred);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", true,
                BackendSelection.Mode.REQUIRE);
        BackendSelectionManager.setProviderForTest(policy);

        Reference<Long> backendId = new Reference<>();

        InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                () -> selectBackendsByRoundRobin(coordinator, locations(preferred), backendId));
        Assertions.assertTrue(error.getCause() instanceof UserException);
    }

    @Test
    void testCoordinatorSkipsQuerySelectionWithoutPreference() throws Exception {
        ConnectContext context = context(4L);
        Coordinator coordinator = coordinator(context);
        Backend first = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend second = backend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(first.getId(), first, second.getId(), second);
        ContextCapturingPolicy policy = new ContextCapturingPolicy("tag_b", false);

        BackendSelectionManager.setProviderForTest(policy);
        Reference<Long> backendId = new Reference<>();

        selectBackendsByRoundRobin(coordinator, locations(first, second), backendId);

        Assertions.assertEquals(first.getId(), backendId.getRef());
        Assertions.assertFalse(policy.orderCalled);
    }

    @Test
    void testLoadSelectionWithoutPreferenceFallsBackToSimpleScheduler() throws Exception {
        ConnectContext context = context(5L);
        context.recordLoadBackendSelectionDecision(BackendSelection.SelectionHint.noSelection());
        Coordinator coordinator = coordinator(context);
        coordinator.setQueryType(TQueryType.LOAD);
        Backend first = backend(10001L, "127.0.0.1", 9060, "tag_a");
        ImmutableMap<Long, Backend> backends = ImmutableMap.of(first.getId(), first);
        coordinator.idToBackend = backends;
        Map<TNetworkAddress, Long> addressToBackendId = Collections.singletonMap(
                new TNetworkAddress(first.getHost(), first.getBePort()), first.getId());
        TNetworkAddress hostFallback = new TNetworkAddress("127.0.0.10", 9070);
        TNetworkAddress currentFallback = new TNetworkAddress("127.0.0.11", 9071);
        Reference<Long> backendId = new Reference<>();
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);

        BackendSelectionManager.setProviderForTest(policy);
        try (MockedStatic<SimpleScheduler> scheduler =
                        Mockito.mockStatic(SimpleScheduler.class, Mockito.CALLS_REAL_METHODS)) {
            scheduler.when(() -> SimpleScheduler.getHost(backends, backendId)).thenReturn(hostFallback);
            scheduler.when(() -> SimpleScheduler.getHostByCurrentBackend(addressToBackendId))
                    .thenReturn(currentFallback);

            Assertions.assertSame(hostFallback, chooseHost(coordinator, backends, backendId));
            Assertions.assertSame(currentFallback, chooseCurrentHost(coordinator, addressToBackendId));
        }
    }

    @Test
    void testRequiredLoadSelectionDoesNotFallBackToSimpleScheduler() throws Exception {
        ConnectContext context = context(6L);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "tag_a", BackendSelection.Mode.REQUIRE, "test");
        context.recordLoadBackendSelectionDecision(hint);
        Coordinator coordinator = coordinator(context);
        coordinator.setQueryType(TQueryType.LOAD);
        Backend unavailable = backend(10001L, "127.0.0.1", 9060, "tag_a");
        unavailable.setAlive(false);
        ImmutableMap<Long, Backend> backends = ImmutableMap.of(unavailable.getId(), unavailable);
        coordinator.idToBackend = backends;
        Map<TNetworkAddress, Long> addressToBackendId = Collections.singletonMap(
                new TNetworkAddress(unavailable.getHost(), unavailable.getBePort()), unavailable.getId());
        BackendSelectionProvider policy = Mockito.mock(BackendSelectionProvider.class);
        Mockito.when(policy.partitionRequiredLoadCandidates(hint, Collections.singletonList(unavailable)))
                .thenReturn(new BackendSelection.CandidateSelection<>(
                        Collections.singletonList(unavailable), Collections.emptyList()));
        Reference<Long> backendId = new Reference<>();

        BackendSelectionManager.setProviderForTest(policy);
        try (MockedStatic<SimpleScheduler> scheduler =
                        Mockito.mockStatic(SimpleScheduler.class, Mockito.CALLS_REAL_METHODS)) {
            InvocationTargetException hostError = Assertions.assertThrows(
                    InvocationTargetException.class, () -> chooseHost(coordinator, backends, backendId));
            Assertions.assertTrue(hostError.getCause() instanceof UserException);
            InvocationTargetException currentError = Assertions.assertThrows(
                    InvocationTargetException.class, () -> chooseCurrentHost(coordinator, addressToBackendId));
            Assertions.assertTrue(currentError.getCause() instanceof UserException);
            scheduler.verify(() -> SimpleScheduler.getHost(backends, backendId), Mockito.never());
            scheduler.verify(() -> SimpleScheduler.getHostByCurrentBackend(addressToBackendId), Mockito.never());
        }
    }

    @Test
    void testLoadCoordinatorSelectionIsRecorded() throws Exception {
        ConnectContext context = context(7L);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "tag_b", BackendSelection.Mode.PREFER, "test");
        context.recordLoadBackendSelectionDecision(hint);
        Coordinator coordinator = coordinator(context);
        coordinator.setQueryType(TQueryType.LOAD);
        Backend first = backend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        ImmutableMap<Long, Backend> backends = ImmutableMap.of(first.getId(), first, preferred.getId(), preferred);
        coordinator.idToBackend = backends;
        Reference<Long> backendId = new Reference<>();
        BackendSelectionProvider policy = new ContextCapturingPolicy("tag_b", true) {
            @Override
            public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint selectionHint) {
                return true;
            }

            @Override
            public List<Backend> orderLoadCandidates(BackendSelection.SelectionHint selectionHint,
                    List<Backend> candidates) {
                return candidates.stream()
                        .sorted((left, right) -> Boolean.compare(
                                preferred.getId() == right.getId(), preferred.getId() == left.getId()))
                        .collect(Collectors.toList());
            }
        };

        BackendSelectionManager.setProviderForTest(policy);

        Assertions.assertEquals(new TNetworkAddress(preferred.getHost(), preferred.getBePort()),
                    chooseHost(coordinator, backends, backendId));
        Assertions.assertEquals(
                    "preferred=tag_b, mode=PREFER, coordinator_backend=10002, coordinator_group=tag_b",
                    context.getBackendSelectionProfile().getLoadSummary());
    }

    @Test
    void testLoadSinkCoordinatorSelectionIsRecordedFromAssignedFragment() throws Exception {
        ConnectContext context = context(8L);
        BackendSelection.SelectionHint hint = new BackendSelection.SelectionHint(
                "tag_b", BackendSelection.Mode.REQUIRE, "test");
        context.recordLoadBackendSelectionDecision(hint);
        PlanFragmentId fragmentId = new PlanFragmentId(0);
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(fragmentId);
        Mockito.when(fragment.getSink()).thenReturn(Mockito.mock(OlapTableSink.class));
        Coordinator coordinator = coordinator(context, Collections.singletonList(fragment));
        coordinator.setQueryType(TQueryType.LOAD);
        Backend preferred = backend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(preferred.getId(), preferred);
        TNetworkAddress host = new TNetworkAddress(preferred.getHost(), preferred.getBePort());
        privateMap(coordinator, "addressToBackendID").put(host, preferred.getId());
        Coordinator.FragmentExecParams params = coordinator.new FragmentExecParams(fragment);
        params.instanceExecParams.add(new Coordinator.FInstanceExecParam(null, host, params));
        privateMap(coordinator, "fragmentExecParamsMap").put(fragmentId, params);

        Method method = Coordinator.class.getDeclaredMethod("recordLoadSinkCoordinator");
        method.setAccessible(true);
        method.invoke(coordinator);

        Assertions.assertEquals(
                "preferred=tag_b, mode=REQUIRE, coordinator_backend=10002, coordinator_group=tag_b",
                context.getBackendSelectionProfile().getLoadSummary());
    }

    private Coordinator coordinator(ConnectContext context) {
        return coordinator(context, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> privateMap(Coordinator coordinator, String fieldName) throws Exception {
        Field field = Coordinator.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<K, V>) field.get(coordinator);
    }

    private Coordinator coordinator(ConnectContext context, List<PlanFragment> fragments) {
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getFragments()).thenReturn(fragments);
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getDescTable()).thenReturn(new DescriptorTable());
        return new Coordinator(context, planner);
    }

    private ConnectContext context(long id) {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(id, id));
        return context;
    }

    private Backend backend(long id, String host, int bePort, String tag) throws Exception {
        Backend backend = new Backend(id, host, bePort + 1000);
        backend.setAlive(true);
        backend.setBePort(bePort);
        backend.setTagMap(Tag.create(Tag.TYPE_LOCATION, tag).toMap());
        return backend;
    }

    private TScanRangeLocations locations(Backend... backends) {
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setLocations(new ArrayList<>());
        for (Backend backend : backends) {
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(backend.getId());
            location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
            locations.addToLocations(location);
        }
        return locations;
    }

    private void selectBackendsByRoundRobin(Coordinator coordinator, TScanRangeLocations locations,
            Reference<Long> backendId) throws Exception {
        Map<TNetworkAddress, Long> replicaNum = locations.getLocations().stream()
                .collect(Collectors.toMap(location -> location.server, location -> 1L));
        selectBackendsByRoundRobin(coordinator, locations, backendId, new HashMap<>(), replicaNum);
    }

    private void selectBackendsByRoundRobin(Coordinator coordinator, TScanRangeLocations locations,
            Reference<Long> backendId, Map<TNetworkAddress, Long> assignedBytes,
            Map<TNetworkAddress, Long> replicaNum) throws Exception {
        Method method = Coordinator.class.getDeclaredMethod("selectBackendsByRoundRobin",
                TScanRangeLocations.class, Map.class, Map.class, Reference.class, boolean.class, boolean.class);
        method.setAccessible(true);
        method.invoke(coordinator, locations, assignedBytes, replicaNum, backendId, false, true);
    }

    private TNetworkAddress chooseHost(Coordinator coordinator, ImmutableMap<Long, Backend> backends,
            Reference<Long> backendId) throws Exception {
        Method method = Coordinator.class.getDeclaredMethod(
                "chooseHostWithSelection", ImmutableMap.class, Reference.class);
        method.setAccessible(true);
        return (TNetworkAddress) method.invoke(coordinator, backends, backendId);
    }

    private TNetworkAddress chooseCurrentHost(Coordinator coordinator,
            Map<TNetworkAddress, Long> addressToBackendId) throws Exception {
        Method method = Coordinator.class.getDeclaredMethod("chooseHostByCurrentBackendSelection", Map.class);
        method.setAccessible(true);
        return (TNetworkAddress) method.invoke(coordinator, addressToBackendId);
    }

    private void restoreContext(ConnectContext previous) {
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    private static class ContextCapturingPolicy implements BackendSelectionProvider {
        private final String preferredTag;
        private final boolean hasPreference;
        private final BackendSelection.Mode mode;
        private ConnectContext context;
        private boolean orderCalled;

        ContextCapturingPolicy(String preferredTag, boolean hasPreference) {
            this(preferredTag, hasPreference, BackendSelection.Mode.PREFER);
        }

        ContextCapturingPolicy(String preferredTag, boolean hasPreference, BackendSelection.Mode mode) {
            this.preferredTag = preferredTag;
            this.hasPreference = hasPreference;
            this.mode = mode;
        }

        @Override
        public BackendSelection.SelectionHint getQuerySelectionHint(ConnectContext context) {
            this.context = context;
            return new BackendSelection.SelectionHint(preferredTag, mode, "test");
        }

        @Override
        public boolean hasQuerySelectionPreference(BackendSelection.SelectionHint hint) {
            return hasPreference;
        }

        @Override
        public <T> BackendSelection.QuerySelectionResult classifyQuerySelection(
                BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> tagOf) {
            return preferredTag.equals(tagOf.apply(candidates.get(0)).value)
                    ? BackendSelection.QuerySelectionResult.PREFERRED_HIT
                    : BackendSelection.QuerySelectionResult.FALLBACK_PREFERRED_UNAVAILABLE;
        }

        @Override
        public <T> List<T> orderQueryCandidates(BackendSelection.SelectionHint hint, List<T> candidates,
                Function<T, Tag> tagOf) {
            orderCalled = true;
            return candidates.stream()
                    .sorted((left, right) -> Boolean.compare(
                            preferredTag.equals(tagOf.apply(right).value),
                            preferredTag.equals(tagOf.apply(left).value)))
                    .collect(Collectors.toList());
        }

        @Override
        public <T> BackendSelection.CandidateSelection<T> partitionPreferredQueryCandidates(
                BackendSelection.SelectionHint hint, List<T> candidates, Function<T, Tag> tagOf) {
            List<T> preferred = candidates.stream()
                    .filter(candidate -> preferredTag.equals(tagOf.apply(candidate).value))
                    .collect(Collectors.toList());
            List<T> fallback = candidates.stream()
                    .filter(candidate -> !preferredTag.equals(tagOf.apply(candidate).value))
                    .collect(Collectors.toList());
            return new BackendSelection.CandidateSelection<>(preferred, fallback);
        }
    }
}
