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

package org.apache.doris.qe.runtime;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RecursiveCteNode;
import org.apache.doris.planner.RecursiveCteScanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRecCTETarget;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TTopnFilterDesc;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ThriftPlansBuilderTest {
    private final IdGenerator<RuntimeFilterId> runtimeFilterIds = RuntimeFilterId.createGenerator();

    @Test
    public void testLocalOnlyRfsOmitMergeParamsOnWire() throws Exception {
        BackendWorker worker = newBackendWorker(0, 1, "127.0.0.1");
        PipelineDistributedPlan plan = mockPlanWithRoot(Mockito.mock(PlanNode.class), worker);
        assertRfMergeParams(List.of(plan), List.of(newRuntimeFilter(false)), true);
        assertRfMergeParams(List.of(plan), Collections.emptyList(), true);
    }

    @Test
    public void testRemoteAndMixedRfsKeepMergeParamsOnWire() throws Exception {
        BackendWorker worker = newBackendWorker(0, 1, "127.0.0.1");
        PipelineDistributedPlan plan = mockPlanWithRoot(Mockito.mock(PlanNode.class), worker);
        RuntimeFilter remote = newRuntimeFilter(true);
        assertRfMergeParams(List.of(plan), List.of(remote), false);
        assertRfMergeParams(List.of(plan), List.of(newRuntimeFilter(false), remote), false);
    }

    @Test
    public void testRecursiveAndIncompletePlansKeepMergeParamsOnWire() throws Exception {
        BackendWorker worker = newBackendWorker(0, 1, "127.0.0.1");
        List<RuntimeFilter> filters = List.of(newRuntimeFilter(false));
        Assertions.assertFalse(ThriftPlansBuilder.canOmitRfMergeParams(Collections.emptyList(), filters));
        PipelineDistributedPlan mergePlan = mockPlanWithRoot(Mockito.mock(PlanNode.class), worker);
        // A recursive node on another fragment can still send an empty global-reset RPC.
        for (PlanNode root : List.of(Mockito.mock(RecursiveCteNode.class), Mockito.mock(RecursiveCteScanNode.class))) {
            PipelineDistributedPlan recursivePlan = mockPlanWithRoot(root, worker);
            assertRfMergeParams(List.of(mergePlan, recursivePlan), filters, false);
        }
        Mockito.when(mergePlan.getFragmentJob().getFragment().getPlanRoot()).thenReturn(null);
        assertRfMergeParams(List.of(mergePlan), filters, false);
    }

    private PipelineDistributedPlan mockPlanWithRoot(PlanNode root, BackendWorker worker) {
        PipelineDistributedPlan plan = Mockito.mock(PipelineDistributedPlan.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(plan.getFragmentJob().getFragment().getPlanRoot()).thenReturn(root);
        AssignedJob job = newAssignedJob(worker, new TUniqueId());
        Mockito.when(plan.getInstanceJobs()).thenReturn(List.of(job));
        Mockito.when(root.collectInCurrentFragment(Mockito.any())).thenAnswer(invocation -> {
            Predicate<PlanNode> predicate = invocation.getArgument(0);
            return predicate.test(root) ? List.of(root) : Collections.emptyList();
        });
        return plan;
    }

    @Test
    public void testDistinctWorkersAtSameEndpointKeepMergeParamsOnWire() throws Exception {
        BackendWorker internalWorker = newBackendWorker(0, 1, "127.0.0.1");
        BackendWorker catalogWorker = newBackendWorker(1, 1, "127.0.0.1");
        BackendWorker remoteWorker = newBackendWorker(0, 2, "127.0.0.2");
        AssignedJob catalogJob = newAssignedJob(catalogWorker, new TUniqueId(2, 2));
        AssignedJob sameWorkerJob = newAssignedJob(internalWorker, new TUniqueId(2, 2));
        AssignedJob remoteJob = newAssignedJob(remoteWorker, new TUniqueId(2, 2));

        PipelineDistributedPlan firstPlan = mockPlanWithRoot(Mockito.mock(PlanNode.class), internalWorker);
        PipelineDistributedPlan secondPlan = mockPlanWithRoot(Mockito.mock(PlanNode.class), catalogWorker);
        Mockito.when(secondPlan.getInstanceJobs()).thenReturn(List.of(catalogJob));
        Assertions.assertEquals(internalWorker.brpcAddress(), catalogWorker.brpcAddress());
        Assertions.assertNotEquals(internalWorker, catalogWorker);
        List<RuntimeFilter> filters = List.of(newRuntimeFilter(false));
        assertRfMergeParams(List.of(firstPlan, secondPlan), filters, false);

        Mockito.when(secondPlan.getInstanceJobs()).thenReturn(List.of(sameWorkerJob));
        assertRfMergeParams(List.of(firstPlan, secondPlan), filters, true);

        Mockito.when(secondPlan.getInstanceJobs()).thenReturn(List.of(remoteJob));
        assertRfMergeParams(List.of(firstPlan, secondPlan), filters, true);
    }

    private BackendWorker newBackendWorker(long catalogId, long backendId, String host) {
        Backend backend = new Backend(backendId, host, 9050);
        backend.setBrpcPort(8060);
        return new BackendWorker(catalogId, backend);
    }

    private RuntimeFilter newRuntimeFilter(boolean remote) {
        RuntimeFilterId id = runtimeFilterIds.getNextId();
        TRuntimeFilterDesc desc = new TRuntimeFilterDesc()
                .setFilterId(id.asInt()).setSrcExpr(new TExpr(Collections.emptyList())).setExprOrder(0)
                .setPlanIdToTargetExpr(Collections.emptyMap()).setIsBroadcastJoin(false)
                .setHasLocalTargets(!remote).setHasRemoteTargets(remote).setType(TRuntimeFilterType.BLOOM);
        RuntimeFilter filter = Mockito.mock(RuntimeFilter.class);
        Mockito.when(filter.getFilterId()).thenReturn(id);
        Mockito.when(filter.hasRemoteTargets()).thenReturn(remote);
        Mockito.when(filter.toThrift()).thenReturn(desc);
        return filter;
    }

    private void assertRfMergeParams(List<PipelineDistributedPlan> plans, List<RuntimeFilter> filters,
            boolean omitted) throws Exception {
        Set<RuntimeFilterId> ids = filters.stream().map(RuntimeFilter::getFilterId).collect(Collectors.toSet());
        for (int i = 0; i < plans.size(); i++) {
            Mockito.when(plans.get(i).getFragmentJob().getFragment().getFragmentId()).thenReturn(new PlanFragmentId(i));
            Mockito.when(plans.get(i).getFragmentJob().getFragment().getBuilderRuntimeFilterIds()).thenReturn(ids);
            Mockito.when(plans.get(i).getFragmentJob().getFragment().getTargetRuntimeFilterIds()).thenReturn(ids);
        }
        RuntimeFiltersThriftBuilder builder = RuntimeFiltersThriftBuilder.compute(filters, plans);
        DistributedPlanWorker worker = plans.get(plans.size() - 1).getInstanceJobs().get(0).getAssignedWorker();
        boolean omitRfMergeParams = ThriftPlansBuilder.canOmitRfMergeParams(plans, filters);
        Assertions.assertEquals(omitted, omitRfMergeParams);
        TTopnFilterDesc topn = new TTopnFilterDesc(9, true, false, Collections.emptyMap());
        Supplier<List<TTopnFilterDesc>> topnFilters = () -> List.of(topn);
        TPipelineFragmentParamsList params = Deencapsulation.invoke(
                ThriftPlansBuilder.class, "beToThrift", worker, builder, topnFilters, omitRfMergeParams);
        TPipelineFragmentParamsList decoded = new TPipelineFragmentParamsList();
        new TDeserializer().deserialize(decoded, new TSerializer().serialize(params));

        TRuntimeFilterParams expected = new TRuntimeFilterParams().setRuntimeFilterMergeAddr(builder.mergeAddress);
        if (!omitted) {
            // Compare all merge metadata with the original builder, including mixed local/remote RFs.
            builder.populateRuntimeFilterParams(expected);
        }
        Assertions.assertEquals(expected, decoded.getRuntimeFilterInfo().getRuntimeFilterParams());
        Assertions.assertEquals(List.of(topn), decoded.getRuntimeFilterInfo().getTopnFilterDescs());
    }

    @Test
    public void testSetRuntimePredicateForNonOlapScanNode() {
        ScanNode scanNode = Mockito.mock(ScanNode.class);
        SortNode sortNode = Mockito.mock(SortNode.class);
        Mockito.when(scanNode.getTopnFilterSortNodes()).thenReturn(Collections.singletonList(sortNode));

        ThriftPlansBuilder.setRuntimePredicateIfNeed(Collections.singletonList(scanNode));

        Mockito.verify(sortNode).setHasRuntimePredicate();
    }

    @Test
    public void testBuildRecCTETargetsKeepsAllInstancesOnSameBackend() {
        DistributedPlanWorker worker = Mockito.mock(DistributedPlanWorker.class);
        Mockito.when(worker.host()).thenReturn("127.0.0.1");
        Mockito.when(worker.brpcPort()).thenReturn(9060);

        TUniqueId firstInstanceId = new TUniqueId(1, 2);
        TUniqueId secondInstanceId = new TUniqueId(3, 4);
        AssignedJob firstJob = newAssignedJob(worker, firstInstanceId);
        AssignedJob secondJob = newAssignedJob(worker, secondInstanceId);
        RecursiveCteScanNode scanNode = new RecursiveCteScanNode("r", new PlanNodeId(11),
                new TupleDescriptor(new TupleId(12)));

        List<TRecCTETarget> targets = ThriftPlansBuilder.buildRecCTETargets(
                Arrays.asList(firstJob, secondJob), scanNode);

        Assertions.assertEquals(2, targets.size());
        Assertions.assertEquals(firstInstanceId, targets.get(0).getFragmentInstanceId());
        Assertions.assertEquals(secondInstanceId, targets.get(1).getFragmentInstanceId());
        Assertions.assertEquals("127.0.0.1", targets.get(0).getAddr().getHostname());
        Assertions.assertEquals(9060, targets.get(0).getAddr().getPort());
        Assertions.assertEquals(11, targets.get(0).getNodeId());
        Assertions.assertEquals("127.0.0.1", targets.get(1).getAddr().getHostname());
        Assertions.assertEquals(9060, targets.get(1).getAddr().getPort());
        Assertions.assertEquals(11, targets.get(1).getNodeId());
    }

    @Test
    public void testBuildRecCTETargetsKeepsOneLocalShuffleInstancePerBackend() {
        DistributedPlanWorker firstWorker = newWorker("127.0.0.1", 9060);
        DistributedPlanWorker secondWorker = newWorker("127.0.0.2", 9060);

        TUniqueId scanInstanceId = new TUniqueId(1, 2);
        TUniqueId localShuffleInstanceId = new TUniqueId(3, 4);
        TUniqueId secondWorkerInstanceId = new TUniqueId(5, 6);
        LocalShuffleAssignedJob scanJob = newLocalShuffleAssignedJob(0, 10, firstWorker, scanInstanceId);
        LocalShuffleAssignedJob localShuffleJob = newLocalShuffleAssignedJob(
                1, 10, firstWorker, localShuffleInstanceId);
        LocalShuffleAssignedJob secondWorkerScanJob = newLocalShuffleAssignedJob(
                2, 11, secondWorker, secondWorkerInstanceId);
        RecursiveCteScanNode scanNode = new RecursiveCteScanNode("r", new PlanNodeId(11),
                new TupleDescriptor(new TupleId(12)));

        List<TRecCTETarget> targets = ThriftPlansBuilder.buildRecCTETargets(
                Arrays.asList(scanJob, localShuffleJob, secondWorkerScanJob), scanNode);

        Assertions.assertEquals(2, targets.size());
        Assertions.assertEquals(scanInstanceId, targets.get(0).getFragmentInstanceId());
        Assertions.assertEquals(secondWorkerInstanceId, targets.get(1).getFragmentInstanceId());
        Assertions.assertEquals("127.0.0.1", targets.get(0).getAddr().getHostname());
        Assertions.assertEquals(9060, targets.get(0).getAddr().getPort());
        Assertions.assertEquals(11, targets.get(0).getNodeId());
        Assertions.assertEquals("127.0.0.2", targets.get(1).getAddr().getHostname());
        Assertions.assertEquals(9060, targets.get(1).getAddr().getPort());
        Assertions.assertEquals(11, targets.get(1).getNodeId());
    }

    @Test
    public void compactsOnlyTheAssignedInstanceTransportCopy() {
        FileQueryScanNode node = Mockito.mock(FileQueryScanNode.class);
        Mockito.when(node.getId()).thenReturn(new PlanNodeId(1));
        ScanRanges assigned = new ScanRanges();
        for (int i = 0; i < 1000; i++) {
            TFileScanRange files = new TFileScanRange().setRanges(new ArrayList<>(List.of(
                    new TFileRangeDesc().setPath("file" + i).setStartOffset(0).setSize(10))));
            assigned.addScanRange(new TScanRangeParams().setScanRange(new TScanRange()
                    .setExtScanRange(new TExternalScanRange().setFileScanRange(files))), 10);
        }
        AssignedJob job = Mockito.mock(AssignedJob.class);
        Mockito.when(job.getScanSource()).thenReturn(new DefaultScanSource(Map.of(node, assigned)));
        TPipelineFragmentParams fragment = new TPipelineFragmentParams()
                .setFileScanParams(Map.of(1, new TFileScanRangeParams()))
                .setQueryOptions(new TQueryOptions().setMaxFileScannersConcurrency(16).setParallelInstance(1));
        TPipelineInstanceParams instance = new TPipelineInstanceParams();

        Deencapsulation.invoke(ThriftPlansBuilder.class, "setScanSourceParam", fragment, job, instance);

        List<TScanRangeParams> transported = instance.getPerNodeScanRanges().get(1);
        Assertions.assertTrue(transported.size() >= 16);
        Assertions.assertTrue(transported.size() < 1000);
        Assertions.assertEquals(1000, transported.stream().mapToInt(r -> r.getScanRange()
                .getExtScanRange().getFileScanRange().getRangesSize()).sum());
        Assertions.assertEquals(1000, assigned.params.size());
        Assertions.assertEquals(10000, assigned.totalBytes);
    }

    private AssignedJob newAssignedJob(DistributedPlanWorker worker, TUniqueId instanceId) {
        AssignedJob assignedJob = Mockito.mock(AssignedJob.class);
        Mockito.when(assignedJob.getAssignedWorker()).thenReturn(worker);
        Mockito.when(assignedJob.instanceId()).thenReturn(instanceId);
        return assignedJob;
    }

    private DistributedPlanWorker newWorker(String host, int brpcPort) {
        DistributedPlanWorker worker = Mockito.mock(DistributedPlanWorker.class);
        Mockito.when(worker.host()).thenReturn(host);
        Mockito.when(worker.brpcPort()).thenReturn(brpcPort);
        return worker;
    }

    private LocalShuffleAssignedJob newLocalShuffleAssignedJob(int indexInUnassignedJob, int shareScanId,
            DistributedPlanWorker worker, TUniqueId instanceId) {
        return new LocalShuffleAssignedJob(indexInUnassignedJob, shareScanId, instanceId,
                Mockito.mock(UnassignedJob.class), worker, DefaultScanSource.empty());
    }
}
