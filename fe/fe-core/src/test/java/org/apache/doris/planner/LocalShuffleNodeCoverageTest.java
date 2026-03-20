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

package org.apache.doris.planner;

import org.apache.doris.analysis.AssertNumRowsElement;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalShuffleNodeCoverageTest {
    private static final AtomicInteger NEXT_ID = new AtomicInteger(1);

    @Test
    public void testSelectNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();

        TrackingPlanNode childNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        SelectNode selectWithNoopChild = new SelectNode(nextPlanNodeId(), childNoop);
        Pair<PlanNode, LocalExchangeType> output = selectWithNoopChild.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());

        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, output.second);
        Assertions.assertEquals(LocalExchangeNode.RequireHash.class, childNoop.lastRequire.getClass());
        assertChildLocalExchangeType(selectWithNoopChild, 0, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);

        TrackingPlanNode childBucket = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.BUCKET_HASH_SHUFFLE);
        SelectNode selectWithBucketChild = new SelectNode(nextPlanNodeId(), childBucket);
        Pair<PlanNode, LocalExchangeType> bucketOutput = selectWithBucketChild.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, bucketOutput.second);
        Assertions.assertSame(childBucket, selectWithBucketChild.getChild(0));
    }

    @Test
    public void testRepeatNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        GroupingInfo groupingInfo = Mockito.mock(GroupingInfo.class);
        TupleDescriptor outputTuple = new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement()));
        Mockito.when(groupingInfo.getOutputTupleDesc()).thenReturn(outputTuple);
        Mockito.when(groupingInfo.getPreRepeatExprs()).thenReturn(Collections.emptyList());

        TrackingPlanNode childNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        RepeatNode repeatNode = new RepeatNode(nextPlanNodeId(), childNoop, groupingInfo,
                Collections.singletonList(Collections.emptySet()), Collections.emptySet(),
                Collections.singletonList(Collections.emptyList()));
        Pair<PlanNode, LocalExchangeType> output = repeatNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, output.second);
        assertChildLocalExchangeType(repeatNode, 0, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
    }

    @Test
    public void testTableFunctionNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        TrackingPlanNode childNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TableFunctionNode tableFunctionNode = new TableFunctionNode(nextPlanNodeId(), childNoop,
                new TupleId(NEXT_ID.getAndIncrement()), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        Pair<PlanNode, LocalExchangeType> output = tableFunctionNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, output.second);
        assertChildLocalExchangeType(tableFunctionNode, 0, LocalExchangeType.PASSTHROUGH);
    }

    @Test
    public void testPartitionSortNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        SortInfo sortInfo = Mockito.mock(SortInfo.class);
        TupleDescriptor sortTuple = new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement()));
        Mockito.when(sortInfo.getOrderingExprs()).thenReturn(Collections.emptyList());
        Mockito.when(sortInfo.getIsAscOrder()).thenReturn(Collections.emptyList());
        Mockito.when(sortInfo.getSortTupleDescriptor()).thenReturn(sortTuple);

        TrackingPlanNode childNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        PartitionSortNode globalTopnNode = new PartitionSortNode(nextPlanNodeId(), childNoop,
                WindowFuncType.ROW_NUMBER, Collections.emptyList(), sortInfo, false, 1,
                PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN);
        Pair<PlanNode, LocalExchangeType> globalOutput = globalTopnNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, globalOutput.second);
        assertChildLocalExchangeType(globalTopnNode, 0, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);

        TrackingPlanNode childNoop2 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        PartitionSortNode passthroughNode = new PartitionSortNode(nextPlanNodeId(), childNoop2,
                WindowFuncType.ROW_NUMBER, Collections.emptyList(), sortInfo, false, 1,
                PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN);
        Pair<PlanNode, LocalExchangeType> passthroughOutput = passthroughNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, passthroughOutput.second);
        assertChildLocalExchangeType(passthroughNode, 0, LocalExchangeType.PASSTHROUGH);
    }

    @Test
    public void testMaterializationNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        TrackingPlanNode childNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement()));
        TestMaterializationNode node = new TestMaterializationNode(nextPlanNodeId(), tupleDescriptor, childNoop);

        Pair<PlanNode, LocalExchangeType> output = node.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, output.second);
        assertChildLocalExchangeType(node, 0, LocalExchangeType.PASSTHROUGH);
    }

    @Test
    public void testCteAndRecursiveNodesAndEmptySet() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();

        CTEScanNode cteScanNode = new CTEScanNode(new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        Pair<PlanNode, LocalExchangeType> cteOutput = cteScanNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, cteOutput.second);

        RecursiveCteScanNode recursiveScanNode = new RecursiveCteScanNode("r", nextPlanNodeId(),
                new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        Pair<PlanNode, LocalExchangeType> recursiveScanOutput = recursiveScanNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, recursiveScanOutput.second);

        EmptySetNode emptySetNode = new EmptySetNode(nextPlanNodeId(),
                new ArrayList<>(Collections.singletonList(new TupleId(NEXT_ID.getAndIncrement()))));
        Pair<PlanNode, LocalExchangeType> emptyOutput = emptySetNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, emptyOutput.second);

        TrackingPlanNode recursiveChild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        RecursiveCteNode recursiveNode = new RecursiveCteNode(nextPlanNodeId(), new TupleId(NEXT_ID.getAndIncrement()),
                "r", true);
        recursiveNode.addChild(recursiveChild);
        Pair<PlanNode, LocalExchangeType> recursiveOutput = recursiveNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, recursiveOutput.second);
        Assertions.assertEquals(LocalExchangeNode.NoRequire.class, recursiveChild.lastRequire.getClass());
    }

    @Test
    public void testHashJoinNodeBranches() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        List<Expr> eqConjuncts = Collections.singletonList(Mockito.mock(BinaryPredicate.class));

        TrackingPlanNode probe = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode build = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        HashJoinNode broadcastJoin = new HashJoinNode(nextPlanNodeId(), probe, build, JoinOperator.INNER_JOIN,
                eqConjuncts, Collections.emptyList(), null, false);
        broadcastJoin.setDistributionMode(DistributionMode.BROADCAST);
        Pair<PlanNode, LocalExchangeType> broadcastOutput = broadcastJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, broadcastOutput.second);
        Assertions.assertSame(probe, broadcastJoin.getChild(0));
        Assertions.assertSame(build, broadcastJoin.getChild(1));

        TrackingPlanNode probe2 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode build2 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        HashJoinNode bucketJoin = new HashJoinNode(nextPlanNodeId(), probe2, build2, JoinOperator.INNER_JOIN,
                eqConjuncts, Collections.emptyList(), null, false);
        bucketJoin.setDistributionMode(DistributionMode.BUCKET_SHUFFLE);
        Pair<PlanNode, LocalExchangeType> bucketOutput = bucketJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, bucketOutput.second);
        assertChildLocalExchangeType(bucketJoin, 0, LocalExchangeType.BUCKET_HASH_SHUFFLE);
        assertChildLocalExchangeType(bucketJoin, 1, LocalExchangeType.BUCKET_HASH_SHUFFLE);

        TrackingScanNode probeScan = new TrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode buildPlan = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        HashJoinNode hashJoin = new HashJoinNode(nextPlanNodeId(), probeScan, buildPlan, JoinOperator.INNER_JOIN,
                eqConjuncts, Collections.emptyList(), null, false);
        hashJoin.setDistributionMode(DistributionMode.PARTITIONED);
        Pair<PlanNode, LocalExchangeType> hashOutput = hashJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, hashOutput.second);
        assertChildLocalExchangeType(hashJoin, 0, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        assertChildLocalExchangeType(hashJoin, 1, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);

        TrackingPlanNode probe3 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode build3 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        HashJoinNode nullAwareJoin = new HashJoinNode(nextPlanNodeId(), probe3, build3,
                JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN, eqConjuncts, Collections.emptyList(), null, false);
        Pair<PlanNode, LocalExchangeType> nullAwareOutput = nullAwareJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, nullAwareOutput.second);
        Assertions.assertSame(probe3, nullAwareJoin.getChild(0));
        Assertions.assertSame(build3, nullAwareJoin.getChild(1));

        SerialTrackingPlanNode serialProbe = new SerialTrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        serialProbe.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(serialProbe.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        TrackingPlanNode nonSerialBuild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        nonSerialBuild.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(nonSerialBuild.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        HashJoinNode serialProbeBroadcast = new HashJoinNode(nextPlanNodeId(), serialProbe, nonSerialBuild,
                JoinOperator.INNER_JOIN, eqConjuncts, Collections.emptyList(), null, false);
        serialProbeBroadcast.setDistributionMode(DistributionMode.BROADCAST);
        Pair<PlanNode, LocalExchangeType> serialProbeOutput = serialProbeBroadcast.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, serialProbeOutput.second);
        assertChildLocalExchangeType(serialProbeBroadcast, 0, LocalExchangeType.PASSTHROUGH);
        Assertions.assertSame(nonSerialBuild, serialProbeBroadcast.getChild(1));

        TrackingPlanNode nonSerialProbe = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        nonSerialProbe.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(nonSerialProbe.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        SerialTrackingPlanNode serialBuild = new SerialTrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        serialBuild.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(serialBuild.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        HashJoinNode serialBuildBroadcast = new HashJoinNode(nextPlanNodeId(), nonSerialProbe, serialBuild,
                JoinOperator.INNER_JOIN, eqConjuncts, Collections.emptyList(), null, false);
        serialBuildBroadcast.setDistributionMode(DistributionMode.BROADCAST);
        Pair<PlanNode, LocalExchangeType> serialBuildOutput = serialBuildBroadcast.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, serialBuildOutput.second);
        Assertions.assertSame(nonSerialProbe, serialBuildBroadcast.getChild(0));
        assertChildLocalExchangeType(serialBuildBroadcast, 1, LocalExchangeType.PASS_TO_ONE);
    }

    @Test
    public void testLocalExchangeNodeIsNotSerializedAsSerialOperator() {
        SerialTrackingScanNode serialScan = new SerialTrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        LocalExchangeNode localExchangeNode = new LocalExchangeNode(nextPlanNodeId(), serialScan,
                LocalExchangeType.PASSTHROUGH);
        localExchangeNode.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(localExchangeNode.fragment.hasSerialScanNode()).thenReturn(true);
        Mockito.when(localExchangeNode.fragment.useSerialSource(Mockito.any())).thenReturn(true);

        TPlanNode thriftNode = new TPlanNode();
        localExchangeNode.toThrift(thriftNode);

        Assertions.assertFalse(thriftNode.isIsSerialOperator(),
                "local exchange source pipeline should not be marked serial in thrift");
    }

    @Test
    public void testNestedLoopJoinNodeBranches() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        List<TupleId> tupleIds = Lists.newArrayList(new TupleId(NEXT_ID.getAndIncrement()));

        TrackingPlanNode probe = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode build = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        NestedLoopJoinNode defaultJoin = new NestedLoopJoinNode(nextPlanNodeId(), probe, build, tupleIds,
                JoinOperator.INNER_JOIN, false);
        defaultJoin.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(defaultJoin.fragment.useSerialSource(Mockito.any())).thenReturn(false);
        Pair<PlanNode, LocalExchangeType> defaultOutput = defaultJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.ADAPTIVE_PASSTHROUGH, defaultOutput.second);
        assertChildLocalExchangeType(defaultJoin, 0, LocalExchangeType.ADAPTIVE_PASSTHROUGH);
        Assertions.assertSame(build, defaultJoin.getChild(1));

        TrackingScanNode probeScan = new TrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode buildNoop = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        NestedLoopJoinNode serialSourceJoin = new NestedLoopJoinNode(nextPlanNodeId(), probeScan, buildNoop,
                Lists.newArrayList(new TupleId(NEXT_ID.getAndIncrement())), JoinOperator.INNER_JOIN, false);
        serialSourceJoin.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(serialSourceJoin.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        Pair<PlanNode, LocalExchangeType> serialOutput = serialSourceJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.ADAPTIVE_PASSTHROUGH, serialOutput.second);
        assertChildLocalExchangeType(serialSourceJoin, 0, LocalExchangeType.ADAPTIVE_PASSTHROUGH);
        assertChildLocalExchangeType(serialSourceJoin, 1, LocalExchangeType.BROADCAST);

        // RIGHT_OUTER/FULL_OUTER: probe side must use NOOP (serial processing for unmatched rows).
        // BE: NestedLoopJoinProbeOperatorX returns NOOP for these join types.
        TrackingPlanNode probeRight = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode buildRight = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        NestedLoopJoinNode rightOuterJoin = new NestedLoopJoinNode(nextPlanNodeId(), probeRight, buildRight,
                Lists.newArrayList(new TupleId(NEXT_ID.getAndIncrement())),
                JoinOperator.RIGHT_OUTER_JOIN, false);
        rightOuterJoin.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(rightOuterJoin.fragment.useSerialSource(Mockito.any())).thenReturn(false);
        Pair<PlanNode, LocalExchangeType> rightOuterOutput = rightOuterJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, rightOuterOutput.second);
        Assertions.assertSame(probeRight, rightOuterJoin.getChild(0));

        TrackingPlanNode probe2 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode build2 = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        NestedLoopJoinNode nullAwareJoin = new NestedLoopJoinNode(nextPlanNodeId(), probe2, build2,
                Lists.newArrayList(new TupleId(NEXT_ID.getAndIncrement())),
                JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN, false);
        nullAwareJoin.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(nullAwareJoin.fragment.useSerialSource(Mockito.any())).thenReturn(false);
        Pair<PlanNode, LocalExchangeType> nullAwareOutput = nullAwareJoin.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, nullAwareOutput.second);
    }

    @Test
    public void testSetOperationAndAssertNumRowsNode() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();

        UnionNode unionNode = new UnionNode(nextPlanNodeId(), new TupleId(NEXT_ID.getAndIncrement()));
        TrackingPlanNode unionChild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        unionNode.addChild(unionChild);
        Pair<PlanNode, LocalExchangeType> unionOutput = unionNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, unionOutput.second);
        Assertions.assertEquals(LocalExchangeNode.NoRequire.class, unionChild.lastRequire.getClass());

        IntersectNode intersectNode = new IntersectNode(nextPlanNodeId(), new TupleId(NEXT_ID.getAndIncrement()));
        intersectNode.setColocate(false);
        TrackingScanNode left = new TrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        TrackingPlanNode right = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        intersectNode.addChild(left);
        intersectNode.addChild(right);
        Pair<PlanNode, LocalExchangeType> intersectOutput = intersectNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE, intersectOutput.second);
        assertChildLocalExchangeType(intersectNode, 0, LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE);
        assertChildLocalExchangeType(intersectNode, 1, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);

        // Colocated ExceptNode with OlapScan children: OlapScan already provides BUCKET_HASH_SHUFFLE,
        // so requireBucketHash() is satisfied and no LocalExchangeNode is inserted.
        ExceptNode exceptNode = new ExceptNode(nextPlanNodeId(), new TupleId(NEXT_ID.getAndIncrement()));
        exceptNode.setColocate(true);
        FakeOlapScanNode exceptLeft = new FakeOlapScanNode(nextPlanNodeId());
        FakeOlapScanNode exceptRight = new FakeOlapScanNode(nextPlanNodeId());
        exceptNode.addChild(exceptLeft);
        exceptNode.addChild(exceptRight);
        Pair<PlanNode, LocalExchangeType> exceptOutput = exceptNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, exceptOutput.second);
        // OlapScan already satisfies requireBucketHash(), so children are passed through unchanged.
        Assertions.assertSame(exceptLeft, exceptNode.getChild(0));
        Assertions.assertSame(exceptRight, exceptNode.getChild(1));

        TrackingPlanNode assertChild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        AssertNumRowsElement assertElement = Mockito.mock(AssertNumRowsElement.class);
        Mockito.when(assertElement.getDesiredNumOfRows()).thenReturn(1L);
        Mockito.when(assertElement.getSubqueryString()).thenReturn("subquery");
        Mockito.when(assertElement.getAssertion()).thenReturn(AssertNumRowsElement.Assertion.EQ);
        AssertNumRowsNode assertNode = new AssertNumRowsNode(nextPlanNodeId(), assertChild,
                assertElement, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        Pair<PlanNode, LocalExchangeType> assertOutput = assertNode.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, assertOutput.second);
        assertChildLocalExchangeType(assertNode, 0, LocalExchangeType.PASSTHROUGH);
    }

    @Test
    public void testSortNodeBranches() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();
        SortInfo sortInfo = mockSortInfo();

        TrackingPlanNode mergeChild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        SortNode mergeSort = new SortNode(nextPlanNodeId(), mergeChild, sortInfo, false);
        mergeSort.setMergeByExchange();
        Pair<PlanNode, LocalExchangeType> mergeOutput = mergeSort.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, mergeOutput.second);
        assertChildLocalExchangeType(mergeSort, 0, LocalExchangeType.PASSTHROUGH);

        SerialTrackingScanNode serialScan = new SerialTrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        SortNode scanSort = new SortNode(nextPlanNodeId(), serialScan, sortInfo, false);
        scanSort.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(scanSort.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        Pair<PlanNode, LocalExchangeType> scanOutput = scanSort.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, scanOutput.second);
        assertChildLocalExchangeType(scanSort, 0, LocalExchangeType.PASSTHROUGH);

        // Analytic sort (mergeByexchange=false): sort before analytic with partition → GLOBAL_HASH
        AnalyticEvalNode analyticChild = new AnalyticEvalNode(nextPlanNodeId(),
                new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP),
                Collections.emptyList(), Collections.singletonList(Mockito.mock(Expr.class)),
                Collections.singletonList(new OrderByElement(Mockito.mock(Expr.class), true, true)),
                null, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        analyticChild.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(analyticChild.fragment.useSerialSource(Mockito.any())).thenReturn(false);
        SortNode analyticSort = new SortNode(nextPlanNodeId(), analyticChild, sortInfo, false);
        Pair<PlanNode, LocalExchangeType> analyticOutput = analyticSort.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, analyticOutput.second);
        assertChildLocalExchangeType(analyticSort, 0, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);

        // Outer merge-sort above analytic (mergeByexchange=true): BE SortSink._merge_by_exchange=true → PASSTHROUGH.
        // Should NOT insert GLOBAL_HASH even though child is AnalyticEvalNode.
        AnalyticEvalNode analyticChild2 = new AnalyticEvalNode(nextPlanNodeId(),
                new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                null, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        analyticChild2.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(analyticChild2.fragment.useSerialSource(Mockito.any())).thenReturn(false);
        SortNode mergeAnalyticSort = new SortNode(nextPlanNodeId(), analyticChild2, sortInfo, false);
        mergeAnalyticSort.setMergeByExchange();
        Pair<PlanNode, LocalExchangeType> mergeAnalyticOutput = mergeAnalyticSort.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, mergeAnalyticOutput.second);
    }

    @Test
    public void testAnalyticEvalNodeBranches() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();

        TrackingPlanNode noPartitionChild = new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        AnalyticEvalNode noPartition = new AnalyticEvalNode(nextPlanNodeId(), noPartitionChild,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                null, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        Pair<PlanNode, LocalExchangeType> noPartitionOutput = noPartition.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, noPartitionOutput.second);
        assertChildLocalExchangeType(noPartition, 0, LocalExchangeType.PASSTHROUGH);

        TrackingScanNode hashChild = new TrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        AnalyticEvalNode hashAnalytic = new AnalyticEvalNode(nextPlanNodeId(), hashChild,
                Collections.emptyList(), Collections.singletonList(Mockito.mock(Expr.class)),
                Collections.emptyList(), null, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        Pair<PlanNode, LocalExchangeType> hashOutput = hashAnalytic.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE, hashOutput.second);
        assertChildLocalExchangeType(hashAnalytic, 0, LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE);

        SerialTrackingScanNode serialScan = new SerialTrackingScanNode(nextPlanNodeId(), LocalExchangeType.NOOP);
        AnalyticEvalNode orderedAnalytic = new AnalyticEvalNode(nextPlanNodeId(), serialScan,
                Collections.emptyList(), Collections.singletonList(Mockito.mock(Expr.class)),
                Collections.singletonList(new OrderByElement(Mockito.mock(Expr.class), true, true)),
                null, new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement())));
        orderedAnalytic.fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(orderedAnalytic.fragment.useSerialSource(Mockito.any())).thenReturn(true);
        Pair<PlanNode, LocalExchangeType> orderedOutput = orderedAnalytic.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.PASSTHROUGH, orderedOutput.second);
        assertChildLocalExchangeType(orderedAnalytic, 0, LocalExchangeType.PASSTHROUGH);
    }

    @Test
    public void testExchangeNodeBranches() {
        PlanTranslatorContext ctx = new PlanTranslatorContext();

        ExchangeNode hashExchange = new ExchangeNode(nextPlanNodeId(),
                new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP));
        hashExchange.setPartitionType(TPartitionType.HASH_PARTITIONED);
        Pair<PlanNode, LocalExchangeType> hashOutput = hashExchange.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE, hashOutput.second);

        ExchangeNode bucketExchange = new ExchangeNode(nextPlanNodeId(),
                new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP));
        bucketExchange.setPartitionType(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED);
        Pair<PlanNode, LocalExchangeType> bucketOutput = bucketExchange.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.noRequire());
        Assertions.assertEquals(LocalExchangeType.BUCKET_HASH_SHUFFLE, bucketOutput.second);

        ExchangeNode noopExchange = new ExchangeNode(nextPlanNodeId(),
                new TrackingPlanNode(nextPlanNodeId(), LocalExchangeType.NOOP));
        noopExchange.setPartitionType(TPartitionType.UNPARTITIONED);
        Pair<PlanNode, LocalExchangeType> noopOutput = noopExchange.enforceAndDeriveLocalExchange(
                ctx, null, LocalExchangeTypeRequire.requireHash());
        Assertions.assertEquals(LocalExchangeType.NOOP, noopOutput.second);
    }

    private static PlanNodeId nextPlanNodeId() {
        return new PlanNodeId(NEXT_ID.getAndIncrement());
    }

    private static void assertChildLocalExchangeType(PlanNode node, int index, LocalExchangeType expectedType) {
        Assertions.assertTrue(node.getChild(index) instanceof LocalExchangeNode,
                "expected child " + index + " to be LocalExchangeNode");
        LocalExchangeNode exchangeNode = (LocalExchangeNode) node.getChild(index);
        Assertions.assertEquals(expectedType, exchangeNode.getExchangeType());
    }

    private static SortInfo mockSortInfo() {
        SortInfo sortInfo = Mockito.mock(SortInfo.class);
        TupleDescriptor sortTuple = new TupleDescriptor(new TupleId(NEXT_ID.getAndIncrement()));
        Mockito.when(sortInfo.getOrderingExprs()).thenReturn(Collections.emptyList());
        Mockito.when(sortInfo.getIsAscOrder()).thenReturn(Collections.emptyList());
        Mockito.when(sortInfo.getSortTupleDescriptor()).thenReturn(sortTuple);
        return sortInfo;
    }

    private static class TrackingPlanNode extends PlanNode {
        private final LocalExchangeType providedType;
        private LocalExchangeTypeRequire lastRequire;

        TrackingPlanNode(PlanNodeId id, LocalExchangeType providedType) {
            super(id, Lists.newArrayList(new TupleId(id.asInt() + 10000)), "TRACKING");
            this.providedType = providedType;
        }

        @Override
        public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
                PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {
            this.lastRequire = parentRequire;
            return Pair.of(this, providedType);
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }

    private static class SerialTrackingPlanNode extends TrackingPlanNode {
        SerialTrackingPlanNode(PlanNodeId id, LocalExchangeType providedType) {
            super(id, providedType);
        }

        @Override
        public boolean isSerialOperator() {
            return true;
        }
    }

    private static class TrackingScanNode extends ScanNode {
        private final LocalExchangeType providedType;
        private LocalExchangeTypeRequire lastRequire;

        TrackingScanNode(PlanNodeId id, LocalExchangeType providedType) {
            super(id, new TupleDescriptor(new TupleId(id.asInt() + 20000)), "TRACKING_SCAN");
            this.providedType = providedType;
        }

        @Override
        public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
                PlanTranslatorContext translatorContext, PlanNode parent, LocalExchangeTypeRequire parentRequire) {
            this.lastRequire = parentRequire;
            return Pair.of(this, providedType);
        }

        @Override
        protected void createScanRangeLocations() throws UserException {
        }

        @Override
        public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
            return Collections.emptyList();
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }

        @Override
        public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
            return "";
        }
    }

    private static class SerialTrackingScanNode extends TrackingScanNode {
        SerialTrackingScanNode(PlanNodeId id, LocalExchangeType providedType) {
            super(id, providedType);
        }

        @Override
        public boolean isSerialOperator() {
            return true;
        }
    }

    private static class FakeOlapScanNode extends OlapScanNode {
        FakeOlapScanNode(PlanNodeId id) {
            super(id, mockTupleDescriptor(id), "FAKE_OLAP_SCAN");
        }

        @Override
        protected void createScanRangeLocations() throws UserException {
        }

        @Override
        public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
            return Collections.emptyList();
        }

        private static TupleDescriptor mockTupleDescriptor(PlanNodeId id) {
            TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
            org.apache.doris.catalog.OlapTable table = Mockito.mock(org.apache.doris.catalog.OlapTable.class);
            Mockito.when(desc.getId()).thenReturn(new TupleId(id.asInt() + 30000));
            Mockito.when(desc.getTable()).thenReturn(table);
            Mockito.when(desc.getSlots()).thenReturn(new ArrayList<SlotDescriptor>());
            Mockito.when(table.getDistributionColumnNames()).thenReturn(Collections.emptySet());
            return desc;
        }
    }

    private static class TestMaterializationNode extends MaterializationNode {
        TestMaterializationNode(PlanNodeId id, TupleDescriptor desc, PlanNode child) {
            super(id, desc, child);
        }

        @Override
        public void initNodeInfo() {
        }
    }
}
