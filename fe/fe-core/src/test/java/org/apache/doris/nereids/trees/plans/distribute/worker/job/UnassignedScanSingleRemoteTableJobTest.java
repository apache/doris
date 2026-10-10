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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TSplitSource;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class UnassignedScanSingleRemoteTableJobTest {

    @Test
    public void testOneAssignedRangeCountPushdownUsesOneInstance() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);

        Assertions.assertEquals(1, job.degreeOfParallelism(source(job, 1, false), 1, true));
    }

    @Test
    public void testCountWithMultiplePhysicalSplitsKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);

        Assertions.assertEquals(8, job.degreeOfParallelism(source(job, 2, false), 1, true));
    }

    @Test
    public void testNonCountKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.NONE);

        Assertions.assertEquals(8, job.degreeOfParallelism(source(job, 1, false), 1, true));
    }

    @Test
    public void testCountWithMultipleRangeEntriesKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);
        DefaultScanSource source = source(job, 1, false);
        ScanRanges ranges = source.scanNodeToScanRanges.get(job.scanNodes.get(0));
        DefaultScanSource multipleRanges = new DefaultScanSource(ImmutableMap.of(job.scanNodes.get(0),
                new ScanRanges(ImmutableList.of(ranges.params.get(0), ranges.params.get(0)),
                        ImmutableList.of(1L, 1L))));
        Assertions.assertEquals(8, job.degreeOfParallelism(multipleRanges, 2, true));
    }

    @Test
    public void testDynamicSplitSourceKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);
        Assertions.assertEquals(8, job.degreeOfParallelism(source(job, 0, true), 1, true));
    }

    @Test
    public void testExternalTableCountKeepsLocalShuffleParallelism() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT, ScanNode.class);
        Assertions.assertEquals(8, job.degreeOfParallelism(source(job, 1, false), 1, true));
    }

    @Test
    public void testAssignedInstancesPreserveDynamicSource() {
        UnassignedScanSingleRemoteTableJob job = createJob(TPushAggOp.COUNT);
        DefaultScanSource source = source(job, 0, true);
        Mockito.when(job.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        DistributedPlanWorker worker = Mockito.mock(DistributedPlanWorker.class);
        DistributeContext context = new DistributeContext(Mockito.mock(DistributedPlanWorkerManager.class), false);

        List<AssignedJob> instances = job.insideMachineParallelization(
                ImmutableMap.of(worker, new UninstancedScanSource(source)), ArrayListMultimap.create(), context);

        Assertions.assertEquals(8, instances.size());
        Assertions.assertSame(source, instances.get(0).getScanSource());
        for (int i = 0; i < instances.size(); i++) {
            Assertions.assertInstanceOf(LocalShuffleAssignedJob.class, instances.get(i));
            Assertions.assertEquals(0, ((LocalShuffleAssignedJob) instances.get(i)).shareScanId);
            if (i > 0) {
                Assertions.assertTrue(instances.get(i).getScanSource().isEmpty());
            }
        }
    }

    private static DefaultScanSource source(UnassignedScanSingleRemoteTableJob job,
            int splitCount, boolean dynamic) {
        TFileScanRange fileRange = new TFileScanRange();
        for (int i = 0; i < splitCount; i++) {
            fileRange.addToRanges(new TFileRangeDesc());
        }
        if (dynamic) {
            fileRange.setSplitSource(new TSplitSource());
        }
        TScanRange range = new TScanRange();
        range.setExtScanRange(new TExternalScanRange().setFileScanRange(fileRange));
        TScanRangeParams params = new TScanRangeParams().setScanRange(range);
        return new DefaultScanSource(ImmutableMap.of(job.scanNodes.get(0),
                new ScanRanges(ImmutableList.of(params), ImmutableList.of(1L))));
    }

    private static UnassignedScanSingleRemoteTableJob createJob(TPushAggOp aggOp) {
        return createJob(aggOp, TVFScanNode.class);
    }

    private static UnassignedScanSingleRemoteTableJob createJob(TPushAggOp aggOp,
            Class<? extends ScanNode> scanNodeClass) {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(10, 10));
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select count(*) from files", 0));
        connectContext.setStatementContext(statementContext);

        ScanNode scanNode = Mockito.mock(scanNodeClass);
        Mockito.when(scanNode.getPushDownAggNoGroupingOp()).thenReturn(aggOp);
        // The global scan range count can differ from the ranges assigned to this worker.
        Mockito.when(scanNode.getScanRangeNum()).thenReturn(1);

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getDataPartition()).thenReturn(DataPartition.RANDOM);
        Mockito.when(fragment.getParallelExecNum()).thenReturn(8);

        return new UnassignedScanSingleRemoteTableJob(
                statementContext,
                fragment,
                scanNode,
                ArrayListMultimap.create(),
                Mockito.mock(ScanWorkerSelector.class));
    }
}
