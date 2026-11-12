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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.NestedLoopJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionSortNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.thrift.TSortAlgorithm;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TopnFilterStrictBoundsTest {
    private <T extends PlanNode> T node(Class<T> type, int id, PlanNode... children) {
        T node = Mockito.mock(type);
        Mockito.when(node.getId()).thenReturn(new PlanNodeId(id));
        Mockito.when(node.getChildren()).thenReturn(new ArrayList<>(Arrays.asList(children)));
        return node;
    }

    private OlapScanNode scan(int id) {
        OlapScanNode scan = node(OlapScanNode.class, id);
        Mockito.when(scan.getSelectedIndexKeyType()).thenReturn(KeysType.DUP_KEYS);
        return scan;
    }

    private TopnFilter filter(PlanNode child, ScanNode... targets) {
        TopN topn = Mockito.mock(TopN.class);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(new IntegerLiteral(1), true, false)));
        TopnFilter filter = new TopnFilter(topn, Mockito.mock(PhysicalRelation.class), new IntegerLiteral(1), true);
        SortNode source = node(SortNode.class, 10, child);
        SortInfo sortInfo = Mockito.mock(SortInfo.class);
        Mockito.when(sortInfo.getOrderingExprs()).thenReturn(List.<Expr>of(new IntLiteral(1)));
        Mockito.when(source.getSortInfo()).thenReturn(sortInfo);
        Mockito.when(source.getSortAlgorithm()).thenReturn(TSortAlgorithm.HEAP_SORT);
        Mockito.when(source.getLimit()).thenReturn(3L);
        Mockito.when(source.getOffset()).thenReturn(2L);
        filter.legacySortNode = source;
        for (ScanNode target : targets) {
            filter.legacyTargets.put(target, new IntLiteral(1));
        }
        return filter;
    }

    @Test
    public void testEveryActualTargetMustAllowStrictBounds() {
        ScanNode aggregateScan = scan(20);
        ScanNode siblingScan = scan(30);
        ScanNode disconnectedScan = scan(99);
        PlanNode aggregate = node(AggregationNode.class, 21, aggregateScan);
        PlanNode union = node(UnionNode.class, 11, node(ExchangeNode.class, 12, aggregate),
                node(ExchangeNode.class, 13, siblingScan));
        TopnFilter filter = filter(union, aggregateScan, siblingScan, disconnectedScan);
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
        Assertions.assertEquals(3, filter.toThrift().getTargetNodeIdToTargetExprSize());
    }

    @Test
    public void testExternalTargetsAndUnreachableTargetsKeepSourceInclusive() {
        ScanNode scan = scan(20);
        ScanNode file = node(FileQueryScanNode.class, 30);
        Assertions.assertTrue(filter(file, file).toThrift().getStrictTargetNodeIds().isEmpty());
        Assertions.assertTrue(filter(node(UnionNode.class, 11, scan, file), scan, file)
                .toThrift().getStrictTargetNodeIds().isEmpty());
        Assertions.assertTrue(filter(scan, scan, scan(99)).toThrift().getStrictTargetNodeIds().isEmpty());
        // A non-target branch must not disable the only actual target.
        Assertions.assertEquals(List.of(20), filter(node(UnionNode.class, 11, scan,
                node(AggregationNode.class, 31, scan(30))), scan).toThrift().getStrictTargetNodeIds());
    }

    @Test
    public void testRepeatedTargetRegistrationNeverRestoresStrictEligibility() {
        ScanNode scan = scan(20);
        for (boolean unsafeFirst : List.of(false, true)) {
            TopnFilter filter = filter(scan, scan);
            PhysicalRelation target = filter.targets.keySet().iterator().next();
            filter.addTarget(target, new IntegerLiteral(1), !unsafeFirst);
            filter.addTarget(target, new IntegerLiteral(1), unsafeFirst);
            filter.addTarget(target, new IntegerLiteral(1), true);
            Assertions.assertFalse(filter.isStrictEligible());
            Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
            Assertions.assertEquals(1, filter.targets.size());
        }
    }

    @Test
    public void testFinalAlgorithmAndOrderingKeys() {
        ScanNode scan = scan(20);
        TopnFilter filter = filter(scan, scan);
        for (TSortAlgorithm algorithm : TSortAlgorithm.values()) {
            Mockito.when(filter.legacySortNode.getSortAlgorithm()).thenReturn(algorithm);
            for (int keys : List.of(1, 2)) {
                Mockito.when(filter.legacySortNode.getSortInfo().getOrderingExprs())
                        .thenReturn(keys == 1 ? List.of(new IntLiteral(1))
                                : List.of(new IntLiteral(1), new IntLiteral(2)));
                Assertions.assertEquals(algorithm == TSortAlgorithm.HEAP_SORT && keys == 1 ? List.of(20) : List.of(),
                        filter.toThrift().getStrictTargetNodeIds());
            }
        }
        // Re-read the algorithm at serialization, rather than capturing its pre-scheduling value.
        Mockito.when(filter.legacySortNode.getSortInfo().getOrderingExprs()).thenReturn(List.of(new IntLiteral(1)));
        Mockito.when(filter.legacySortNode.getSortAlgorithm()).thenReturn(TSortAlgorithm.FULL_SORT);
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
        Mockito.when(filter.legacySortNode.getSortAlgorithm()).thenReturn(TSortAlgorithm.HEAP_SORT);
        Assertions.assertEquals(List.of(20), filter.toThrift().getStrictTargetNodeIds());
    }

    @Test
    public void testLimitsAndRowChangingOperatorsRetainTies() {
        ScanNode scan = scan(20);
        PlanNode select = node(SelectNode.class, 11, scan);
        TopnFilter filter = filter(select, scan);
        Assertions.assertEquals(List.of(20), filter.toThrift().getStrictTargetNodeIds());
        Mockito.when(select.hasLimit()).thenReturn(true);
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
        Mockito.when(select.hasLimit()).thenReturn(false);
        Mockito.when(scan.hasLimit()).thenReturn(true);
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
        Mockito.when(scan.hasLimit()).thenReturn(false);
        Mockito.when(select.getOffset()).thenReturn(1L);
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
        Mockito.when(select.getOffset()).thenReturn(0L);
        PlanNode limitedPath = node(SelectNode.class, 12, scan);
        Mockito.when(limitedPath.hasLimit()).thenReturn(true);
        // The same target reached by an unsafe path cannot be rescued by its safe occurrence.
        Assertions.assertTrue(filter(node(UnionNode.class, 13, select, limitedPath), scan)
                .toThrift().getStrictTargetNodeIds().isEmpty());
        for (Class<? extends PlanNode> type : List.of(AggregationNode.class,
                AnalyticEvalNode.class, AssertNumRowsNode.class, PartitionSortNode.class, PlanNode.class)) {
            Assertions.assertTrue(filter(node(type, 11, scan), scan).toThrift().getStrictTargetNodeIds().isEmpty(),
                    type.getSimpleName());
        }
    }

    @Test
    public void testJoinModes() {
        Set<JoinOperator> allowed = Set.of(JoinOperator.INNER_JOIN, JoinOperator.CROSS_JOIN,
                JoinOperator.LEFT_OUTER_JOIN, JoinOperator.LEFT_SEMI_JOIN, JoinOperator.LEFT_ANTI_JOIN);
        for (Class<? extends JoinNodeBase> type : List.of(HashJoinNode.class, NestedLoopJoinNode.class)) {
            for (JoinOperator join : JoinOperator.values()) {
                ScanNode scan = scan(20);
                JoinNodeBase joinNode = node(type, 11, scan, node(PlanNode.class, 31));
                Mockito.when(joinNode.getJoinOp()).thenReturn(join);
                Assertions.assertEquals(allowed.contains(join) ? List.of(20) : List.of(),
                        filter(joinNode, scan).toThrift().getStrictTargetNodeIds(), type.getSimpleName() + join);
            }
        }
    }

    @Test
    public void testSelectedAggregateIndexOnDuplicateTableRetainsTies() {
        OlapTable table = PlanConstructor.newOlapTable(10001, "topn_index_model", 0, KeysType.DUP_KEYS);
        table.setIndexMeta(10002, "aggregate_index", table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(1));
        tuple.setTable(table);
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(20), tuple, "scan", ScanContext.EMPTY);
        TopnFilter filter = filter(scan, scan);
        scan.setSelectedIndexInfo(table.getBaseIndexId(), true, "");
        Assertions.assertEquals(List.of(20), filter.toThrift().getStrictTargetNodeIds());
        scan.setSelectedIndexInfo(10002, true, "");
        Assertions.assertTrue(filter.toThrift().getStrictTargetNodeIds().isEmpty());
    }

    @Test
    public void testStorageMergesRetainTies() {
        for (KeysType keys : List.of(KeysType.DUP_KEYS, KeysType.AGG_KEYS, KeysType.UNIQUE_KEYS)) {
            for (boolean mow : List.of(false, true)) {
                for (boolean changelog : List.of(false, true)) {
                    OlapScanNode scan = node(OlapScanNode.class, 20);
                    OlapTable table = Mockito.mock(OlapTable.class);
                    Mockito.when(scan.getOlapTable()).thenReturn(table);
                    Mockito.when(scan.getSelectedIndexKeyType()).thenReturn(keys);
                    Mockito.when(scan.isReadChangelog()).thenReturn(changelog);
                    Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(mow);
                    boolean strict = !changelog && (keys == KeysType.DUP_KEYS || (keys == KeysType.UNIQUE_KEYS && mow));
                    Assertions.assertEquals(strict ? List.of(20) : List.of(),
                            filter(scan, scan).toThrift().getStrictTargetNodeIds());
                }
            }
        }
    }
}
