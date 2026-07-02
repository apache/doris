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
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.resource.ResourceGroupAffinity;
import org.apache.doris.resource.ResourceGroupAffinityPolicy;
import org.apache.doris.resource.ResourceGroupAffinityPolicyFactory;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTopnFilterDesc;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CoordinatorTest extends TestWithFeService {

    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table tbl(id int, v int) distributed by hash(id)"
                + " buckets 3 properties('replication_num' = '1');");
    }

    /**
     * Verifies that FragmentExecParams.toThrift() pre-computes topnFilterDescs once and shares
     * the same list object across all TPipelineInstanceParams when instanceExecParams.size() > 1.
     *
     * Before the fix, a new List was created per instance inside the loop.
     * After the fix, one List is created outside the loop and shared by all instances.
     */
    @Test
    public void testTopnFilterDescsSharedAmongInstances() throws Exception {
        NereidsPlanner planner = plan("select * from test.tbl order by id limit 5");
        List<TopnFilter> topnFilters = planner.getTopnFilters();

        Assumptions.assumeTrue(!topnFilters.isEmpty(),
                "Query did not generate topn filters; test skipped");

        Coordinator coordinator = (Coordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);

        // prepare() populates fragmentExecParamsMap; it is protected and accessible
        // from this package.
        coordinator.prepare();

        // Access private fragmentExecParamsMap via reflection.
        Field mapField = Coordinator.class.getDeclaredField("fragmentExecParamsMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PlanFragmentId, Coordinator.FragmentExecParams> fragMap =
                (Map<PlanFragmentId, Coordinator.FragmentExecParams>) mapField.get(coordinator);

        Assertions.assertFalse(fragMap.isEmpty());

        // Pick any fragment and inject 3 fake instances on the same host so that
        // toThrift() groups them into one TPipelineFragmentParams with 3 local entries.
        Coordinator.FragmentExecParams fragParams = fragMap.values().iterator().next();
        fragParams.instanceExecParams.clear();

        TNetworkAddress host = new TNetworkAddress("127.0.0.1", 9060);
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 1L), host, fragParams));
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 2L), host, fragParams));
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 3L), host, fragParams));

        // toThrift() is package-private and accessible from this package.
        Map<TNetworkAddress, TPipelineFragmentParams> result = fragParams.toThrift(0);

        TPipelineFragmentParams pipelineParams = result.get(host);
        Assertions.assertNotNull(pipelineParams);

        List<TPipelineInstanceParams> localParamsList = pipelineParams.getLocalParams();
        Assertions.assertEquals(3, localParamsList.size());

        // Every instance must have topn_filter_descs set and non-empty.
        for (TPipelineInstanceParams lp : localParamsList) {
            Assertions.assertNotNull(lp.getTopnFilterDescs());
            Assertions.assertFalse(lp.getTopnFilterDescs().isEmpty());
        }

        // All instances must reference the SAME list object (not merely equal copies).
        // This is the invariant introduced by the commit: the list is built once outside
        // the instance loop and shared across all instances.
        List<TTopnFilterDesc> shared = localParamsList.get(0).getTopnFilterDescs();
        for (int i = 1; i < localParamsList.size(); i++) {
            Assertions.assertSame(shared, localParamsList.get(i).getTopnFilterDescs(),
                    "instance " + i + " must share the same topnFilterDescs list object");
        }
    }

    @Test
    public void testFragmentExecParamsMarksNonOlapTopnFilterSource() {
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        SortNode sortNode = Mockito.mock(SortNode.class);
        Mockito.when(scanNode.getTopnFilterSortNodes()).thenReturn(Collections.singletonList(sortNode));

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(0));
        Mockito.when(fragment.toThrift()).thenReturn(new TPlanFragment());
        Mockito.when(fragment.isTransferQueryStatisticsWithEveryBatch()).thenReturn(false);

        Coordinator.FragmentExecParams fragParams = new Coordinator(0L, new TUniqueId(1L, 1L),
                new DescriptorTable(), Collections.singletonList(fragment), Collections.singletonList(scanNode),
                "UTC", false, false).new FragmentExecParams(fragment);
        TNetworkAddress host = new TNetworkAddress("127.0.0.1", 9060);
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(2L, 2L), host, fragParams));

        fragParams.toThrift(0);

        Mockito.verify(sortNode).setHasRuntimePredicate();
    }

    @Test
    public void testCoordinatorQueryAffinityUsesCoordinatorContext() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(3L, 3L));
        Coordinator coordinator = new Coordinator(context, mockPlanner());

        Backend tagABackend = createAvailableBackend(10001L, "127.0.0.1", 9060, "tag_a");
        Backend tagBBackend = createAvailableBackend(10002L, "127.0.0.2", 9061, "tag_b");
        coordinator.idToBackend = ImmutableMap.of(tagABackend.getId(), tagABackend, tagBBackend.getId(), tagBBackend);

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.setLocations(new ArrayList<>());
        scanRangeLocations.addToLocations(createScanRangeLocation(tagABackend));
        scanRangeLocations.addToLocations(createScanRangeLocation(tagBBackend));

        ContextCapturingQueryAffinityPolicy policy = new ContextCapturingQueryAffinityPolicy("tag_b");
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext.remove();
        try (MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get).thenReturn(policy);

            Reference<Long> backendIdRef = new Reference<>();
            selectBackendsByRoundRobinWithAffinity(coordinator, scanRangeLocations, new HashMap<>(),
                    initialReplicaNumPerHost(scanRangeLocations), backendIdRef);

            Assertions.assertEquals(tagBBackend.getId(), backendIdRef.getRef());
            Assertions.assertSame(context, policy.getDecideContext());
        } finally {
            restoreThreadLocalContext(previousContext);
        }
    }

    private NereidsPlanner plan(String sql) throws IOException {
        connectContext.getSessionVariable().setDisableNereidsRules(
                "PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        connectContext.getSessionVariable().topNLazyMaterializationThreshold = -1;
        // The test table is empty (0 rows). The topn-filter condition is:
        //   Math.max(rowCount, 1) * topnFilterRatio > limit
        // With default ratio=0.5: Math.max(0,1)*0.5=0.5 which is NOT > 5, so no filter.
        // Set ratio > 5 so that even an empty table (rowCount=0) passes the check.
        connectContext.getSessionVariable().topnFilterRatio = 10.0;
        connectContext.setThreadLocalInfo();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(
                new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        return PlanChecker.from(connectContext).plan(sql);
    }

    private Planner mockPlanner() {
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getFragments()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getDescTable()).thenReturn(new DescriptorTable());
        Mockito.when(planner.getRuntimeFilters()).thenReturn(Collections.<RuntimeFilter>emptyList());
        Mockito.when(planner.handleQueryInFe(Mockito.any())).thenReturn(Optional.empty());
        Mockito.when(planner.getTopnFilters()).thenReturn(Collections.emptyList());
        return planner;
    }

    private Backend createAvailableBackend(long backendId, String host, int bePort, String locationTag)
            throws Exception {
        Backend backend = new Backend(backendId, host, bePort + 1000);
        backend.setAlive(true);
        backend.setBePort(bePort);
        backend.setTagMap(Tag.create(Tag.TYPE_LOCATION, locationTag).toMap());
        return backend;
    }

    private TScanRangeLocation createScanRangeLocation(Backend backend) {
        TScanRangeLocation location = new TScanRangeLocation();
        location.setBackendId(backend.getId());
        location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
        return location;
    }

    private Map<TNetworkAddress, Long> initialReplicaNumPerHost(TScanRangeLocations scanRangeLocations) {
        return scanRangeLocations.getLocations().stream()
                .collect(Collectors.toMap(location -> location.server, location -> 1L));
    }

    private void restoreThreadLocalContext(ConnectContext previousContext) {
        if (previousContext == null) {
            ConnectContext.remove();
            return;
        }
        previousContext.setThreadLocalInfo();
    }

    private void selectBackendsByRoundRobinWithAffinity(Coordinator coordinator,
            TScanRangeLocations scanRangeLocations, Map<TNetworkAddress, Long> assignedBytesPerHost,
            Map<TNetworkAddress, Long> replicaNumPerHost, Reference<Long> backendIdRef) throws Exception {
        Method method = Coordinator.class.getDeclaredMethod("selectBackendsByRoundRobin",
                TScanRangeLocations.class, Map.class, Map.class, Reference.class, boolean.class, boolean.class);
        method.setAccessible(true);
        method.invoke(coordinator, scanRangeLocations, assignedBytesPerHost, replicaNumPerHost, backendIdRef,
                false, true);
    }

    private static final class ContextCapturingQueryAffinityPolicy implements ResourceGroupAffinityPolicy {
        private final String preferredTag;
        private ConnectContext decideContext;

        private ContextCapturingQueryAffinityPolicy(String preferredTag) {
            this.preferredTag = preferredTag;
        }

        private ConnectContext getDecideContext() {
            return decideContext;
        }

        @Override
        public ResourceGroupAffinity.AffinityDecision decideForQuery(ConnectContext context) {
            decideContext = context;
            return new ResourceGroupAffinity.AffinityDecision(preferredTag,
                    ResourceGroupAffinity.Policy.PREFER_LOCAL, "test");
        }

        @Override
        public <T> List<T> applyQueryAffinity(ResourceGroupAffinity.AffinityDecision decision, List<T> candidates,
                Function<T, Tag> beTagOf) throws UserException {
            if (!preferredTag.equals(decision.getEffectivePreferredGroup())) {
                return candidates;
            }
            return candidates.stream()
                    .sorted((left, right) -> Boolean.compare(
                            preferredTag.equals(beTagOf.apply(right).value),
                            preferredTag.equals(beTagOf.apply(left).value)))
                    .collect(Collectors.toList());
        }
    }
}
