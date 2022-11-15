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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorder;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorderGroupLeft;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorderGroupRight;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class HyperGraphBuilder {
    private List<Integer> rowCounts = new ArrayList<>();
    private HashMap<BitSet, LogicalPlan> plans = new HashMap<>();
    private HashMap<BitSet, List<Integer>> schemas = new HashMap<>();

    public HyperGraph build() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        Plan planWithStats = extractJoinCluster(plan);
        HyperGraph graph = HyperGraph.fromPlan(planWithStats);
        return graph;
    }

    public HyperGraph randomBuildWith(int tableNum, int edgeNum) {
        Preconditions.checkArgument(edgeNum >= tableNum - 1,
                String.format("We can't build a connected graph with %d tables %d edges", tableNum, edgeNum));
        Preconditions.checkArgument(edgeNum <= tableNum * (tableNum - 1) / 2,
                String.format("The edges are redundant with %d tables %d edges", tableNum, edgeNum));

        int[] tableRowCounts = new int[tableNum];
        for (int i = 1; i <= tableNum; i++) {
            tableRowCounts[i - 1] = i;
        }
        this.init(tableRowCounts);

        List<Pair<Integer, Integer>> edges = new ArrayList<>();
        for (int i = 0; i < tableNum; i++) {
            for (int j = i + 1; j < tableNum; j++) {
                edges.add(Pair.of(i, j));
            }
        }

        while (edges.size() > 0) {
            int index = (int) (Math.random() * edges.size());
            Pair<Integer, Integer> edge = edges.get(index);
            edges.remove(index);
            this.addEdge(JoinType.INNER_JOIN, edge.first, edge.second);
            edgeNum -= 1;
            if (plans.size() - 1 == edgeNum) {
                // We must keep all tables connected.
                break;
            }
        }

        BitSet[] keys = new BitSet[plans.size()];
        plans.keySet().toArray(keys);
        int size = plans.size();
        for (int i = 1; i < size; i++) {
            int left = keys[0].nextSetBit(0);
            int right = keys[i].nextSetBit(0);
            this.addEdge(JoinType.INNER_JOIN, left, right);
        }
        return this.build();
    }

    public HyperGraphBuilder init(int... rowCounts) {
        for (int i = 0; i < rowCounts.length; i++) {
            this.rowCounts.add(rowCounts[i]);
            BitSet bitSet = new BitSet();
            bitSet.set(i);
            plans.put(bitSet, PlanConstructor.newLogicalOlapScan(i, String.valueOf(i), 0));
            List<Integer> schema = new ArrayList<>();
            schema.add(i);
            schemas.put(bitSet, schema);
        }
        return this;
    }

    public HyperGraphBuilder addEdge(JoinType joinType, int node1, int node2) {
        Preconditions.checkArgument(node1 >= 0 && node1 < rowCounts.size(),
                String.format("%d must in [%d, %d)", node1, 0, rowCounts.size()));
        Preconditions.checkArgument(node2 >= 0 && node1 < rowCounts.size(),
                String.format("%d must in [%d, %d)", node1, 0, rowCounts.size()));

        BitSet leftBitmap = Bitmap.newBitmap(node1);
        BitSet rightBitmap = Bitmap.newBitmap(node2);
        BitSet fullBitmap = Bitmap.newBitmapUnion(leftBitmap, rightBitmap);
        Optional<BitSet> fullKey = findPlan(fullBitmap);
        if (!fullKey.isPresent()) {
            Optional<BitSet> leftKey = findPlan(leftBitmap);
            Optional<BitSet> rightKey = findPlan(rightBitmap);
            assert leftKey.isPresent() && rightKey.isPresent() : String.format("can not find plan %s-%s", leftBitmap,
                    rightBitmap);
            Plan leftPlan = plans.get(leftKey.get());
            Plan rightPlan = plans.get(rightKey.get());
            LogicalJoin join = new LogicalJoin<>(joinType, new ArrayList<>(), leftPlan, rightPlan);

            BitSet key = new BitSet();
            key.or(leftKey.get());
            key.or(rightKey.get());
            plans.remove(leftKey.get());
            plans.remove(rightKey.get());
            plans.put(key, join);

            List<Integer> schema = schemas.get(leftKey.get());
            schema.addAll(schemas.get(rightKey.get()));
            schemas.remove(leftKey);
            schemas.remove(rightKey);
            schemas.put(key, schema);
            fullKey = Optional.of(key);
        }
        assert fullKey.isPresent();
        addCondition(node1, node2, fullKey.get());
        return this;
    }

    private Optional<BitSet> findPlan(BitSet bitSet) {
        for (BitSet key : plans.keySet()) {
            if (isSubset(bitSet, key)) {
                return Optional.of(key);
            }
        }
        return Optional.empty();
    }

    private boolean isSubset(BitSet bitSet1, BitSet bitSet2) {
        BitSet bitSet = new BitSet();
        bitSet.or(bitSet1);
        bitSet.or(bitSet2);
        return bitSet.equals(bitSet2);
    }

    private Rule selectRuleForPlan(Plan plan) {
        Assertions.assertTrue(plan instanceof LogicalJoin);
        LogicalJoin join = (LogicalJoin) plan;
        if (!(join.left() instanceof LogicalJoin)) {
            return new HyperGraphJoinReorderGroupLeft().build();
        } else if (!(join.right() instanceof LogicalJoin)) {
            return new HyperGraphJoinReorderGroupRight().build();
        }
        return new HyperGraphJoinReorder().build();
    }

    private Plan extractJoinCluster(Plan plan) {
        Rule rule = selectRuleForPlan(plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(),
                cascadesContext.getMemo().getRoot().getLogicalExpression());
        List<Plan> planList = new ArrayList<>();
        for (Plan matchingPlan : groupExpressionMatching) {
            planList.add(matchingPlan);
        }
        assert planList.size() == 1 : "Now we only support one join cluster";
        injectRowcount(planList.get(0));
        return planList.get(0);
    }

    private void injectRowcount(Plan plan) {
        if (plan instanceof GroupPlan) {
            GroupPlan olapGroupPlan = (GroupPlan) plan;
            StatsCalculator.estimate(olapGroupPlan.getGroup().getLogicalExpression());
            LogicalOlapScan scanPlan = (LogicalOlapScan) olapGroupPlan.getGroup().getLogicalExpression().getPlan();
            StatsDeriveResult stats = olapGroupPlan.getGroup().getStatistics();
            olapGroupPlan.getGroup()
                    .setStatistics(stats
                            .updateRowCount(rowCounts.get(Integer.parseInt(scanPlan.getTable().getName()))));
            return;
        }
        LogicalJoin join = (LogicalJoin) plan;
        injectRowcount(join.left());
        injectRowcount(join.right());
        // Because the children stats has been changed, so we need to recalculate it
        StatsCalculator.estimate(join.getGroupExpression().get());
    }

    private void addCondition(int node1, int node2, BitSet key) {
        LogicalJoin join = (LogicalJoin) plans.get(key);
        List<Expression> conditions = new ArrayList<>(join.getExpressions());
        conditions.add(makeCondition(node1, node2, key));
        LogicalJoin newJoin = new LogicalJoin<>(join.getJoinType(), conditions, join.left(), join.right());
        plans.put(key, newJoin);
    }

    private Expression makeCondition(int node1, int node2, BitSet bitSet) {
        Plan plan = plans.get(bitSet);
        List<Integer> schema = schemas.get(bitSet);
        int size = schema.size();
        int leftIndex = -1;
        int rightIndex = -1;
        for (int i = 0; i < size; i++) {
            if (schema.get(i) == node1) {
                // Each table has two column: id and name.
                // Therefore, offset = numberOfTables * 2
                leftIndex = i * 2;
            }
            if (schema.get(i) == node2) {
                rightIndex = i * 2;
            }
        }
        assert leftIndex != -1 && rightIndex != -1;
        EqualTo hashConjunts =
                new EqualTo(plan.getOutput().get(leftIndex), plan.getOutput().get(rightIndex));
        return hashConjunts;
    }
}
