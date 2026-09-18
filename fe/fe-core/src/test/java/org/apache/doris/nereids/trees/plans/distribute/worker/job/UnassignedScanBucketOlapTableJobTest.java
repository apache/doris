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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;

public class UnassignedScanBucketOlapTableJobTest {

    @Test
    public void testDegreeOfParallelismWithExchangeNodes() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(1, 1));
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().colocateMaxParallelNum = 128;
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select * from t", 0));
        connectContext.setStatementContext(statementContext);

        OlapScanNode olapScanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(olapScanNode.getTotalTabletsNum()).thenReturn(100L);

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getDataPartition()).thenReturn(DataPartition.RANDOM);
        Mockito.when(fragment.getParallelExecNum()).thenReturn(5);

        ScanWorkerSelector scanWorkerSelector = Mockito.mock(ScanWorkerSelector.class);

        // Non-empty exchangeToChildJob simulates bucket shuffle join fragment.
        ExchangeNode exchangeNode = Mockito.mock(ExchangeNode.class);
        UnassignedJob mockChild = Mockito.mock(UnassignedJob.class);
        Mockito.when(mockChild.getAllChildrenTypes()).thenReturn(new BitSet());
        ArrayListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();
        exchangeToChildJob.put(exchangeNode, mockChild);

        UnassignedScanBucketOlapTableJob unassignedJob = new UnassignedScanBucketOlapTableJob(
                statementContext,
                fragment,
                ImmutableList.of(olapScanNode),
                exchangeToChildJob,
                scanWorkerSelector
        );

        // maxParallel = 3 (buckets), parallelExecNum = 5
        // Base class: min(maxParallel, max(parallelExecNum, 1)) = min(3, 5) = 3
        int result = unassignedJob.degreeOfParallelism(3, false);
        Assertions.assertEquals(3, result);
    }

    @Test
    public void testDegreeOfParallelismWithoutExchangeNodes() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        connectContext.setQueryId(new TUniqueId(2, 2));
        connectContext.getSessionVariable().parallelPipelineTaskNum = 1;
        connectContext.getSessionVariable().colocateMaxParallelNum = 128;
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement("select * from t", 0));
        connectContext.setStatementContext(statementContext);

        OlapScanNode olapScanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(olapScanNode.getTotalTabletsNum()).thenReturn(100L);
        Mockito.when(olapScanNode.shouldUseOneInstance(Mockito.any())).thenReturn(false);

        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getDataPartition()).thenReturn(DataPartition.RANDOM);
        Mockito.when(fragment.getParallelExecNum()).thenReturn(5);

        ScanWorkerSelector scanWorkerSelector = Mockito.mock(ScanWorkerSelector.class);

        // Empty exchangeToChildJob simulates pure colocate scan (no exchange nodes).
        ArrayListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob = ArrayListMultimap.create();

        UnassignedScanBucketOlapTableJob unassignedJob = new UnassignedScanBucketOlapTableJob(
                statementContext,
                fragment,
                ImmutableList.of(olapScanNode),
                exchangeToChildJob,
                scanWorkerSelector
        );

        // Tablet strategy: min(max(tabletNum=100, parallelExecNum=5), colocateMaxParallelNum=128) = 100
        int result = unassignedJob.degreeOfParallelism(3, false);
        Assertions.assertEquals(100, result);
    }

    @Test
    public void testShouldFillUpInstancesSkipsBucketShuffleIntersectOnly() {
        List<HashJoinNode> noJoins = ImmutableList.of();

        // A bucket-shuffle INTERSECT must NOT trigger missing-bucket receiver fill-up: a bucket the
        // basic child does not scan is empty in the anchor, so the intersect result for that bucket
        // is empty and the other children's rows shuffled there produce nothing. The skip must match
        // the legacy planner IntersectNode; the translated node is never an instance of the Nereids
        // algebra Intersect, so checking that type would silently never skip.
        IntersectNode bucketShuffleIntersect = Mockito.mock(IntersectNode.class);
        Mockito.when(bucketShuffleIntersect.isBucketShuffle()).thenReturn(true);
        Assertions.assertFalse(UnassignedScanBucketOlapTableJob.shouldFillUpInstances(
                noJoins, ImmutableList.<SetOperationNode>of(bucketShuffleIntersect)));

        // Bucket-shuffle UNION / EXCEPT still need fill-up: the other children's rows shuffled into
        // buckets the basic child does not scan must be received to be produced (union) or to
        // subtract against (except) correctly.
        UnionNode bucketShuffleUnion = Mockito.mock(UnionNode.class);
        Mockito.when(bucketShuffleUnion.isBucketShuffle()).thenReturn(true);
        Assertions.assertTrue(UnassignedScanBucketOlapTableJob.shouldFillUpInstances(
                noJoins, ImmutableList.<SetOperationNode>of(bucketShuffleUnion)));

        ExceptNode bucketShuffleExcept = Mockito.mock(ExceptNode.class);
        Mockito.when(bucketShuffleExcept.isBucketShuffle()).thenReturn(true);
        Assertions.assertTrue(UnassignedScanBucketOlapTableJob.shouldFillUpInstances(
                noJoins, ImmutableList.<SetOperationNode>of(bucketShuffleExcept)));

        // A union that did not choose bucket shuffle does not trigger fill-up.
        UnionNode plainUnion = Mockito.mock(UnionNode.class);
        Mockito.when(plainUnion.isBucketShuffle()).thenReturn(false);
        Assertions.assertFalse(UnassignedScanBucketOlapTableJob.shouldFillUpInstances(
                noJoins, ImmutableList.<SetOperationNode>of(plainUnion)));
    }
}
