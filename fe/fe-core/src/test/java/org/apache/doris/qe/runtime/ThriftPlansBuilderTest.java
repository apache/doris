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
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RecursiveCteScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.thrift.TRecCTETarget;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ThriftPlansBuilderTest {
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
