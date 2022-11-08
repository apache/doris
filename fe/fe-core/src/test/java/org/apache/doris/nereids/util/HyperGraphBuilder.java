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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorderGroupPlan;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.HyperGraph;
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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class HyperGraphBuilder {
    List<Integer> rowCounts = new ArrayList<>();
    HashMap<BitSet, LogicalPlan> plans = new HashMap<>();
    HashMap<BitSet, List<Integer>> schemas = new HashMap<>();

    public HyperGraph build() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        Plan planWithStats = extractJoinCluster(plan);
        HyperGraph graph = HyperGraph.fromPlan(planWithStats);
        return graph;
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
        assert node1 >= 0 && node1 < rowCounts.size() : String.format("%d must in [%d, %d)", node1, 0,
                rowCounts.size());
        assert node2 >= 0 && node2 < rowCounts.size() : String.format("%d must in [%d, %d)", node1, 0,
                rowCounts.size());

        BitSet leftBitmap = new BitSet();
        leftBitmap.set(node1);
        BitSet rightBitmap = new BitSet();
        rightBitmap.set(node2);
        BitSet fullBitmap = new BitSet();
        fullBitmap.or(leftBitmap);
        fullBitmap.or(rightBitmap);
        Optional<BitSet> fullKey = findPlan(fullBitmap);
        if (!fullKey.isPresent()) {
            Optional<BitSet> leftKey = findPlan(leftBitmap);
            Optional<BitSet> rightKey = findPlan(rightBitmap);
            assert leftKey.isPresent() && rightKey.isPresent();
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

    private Plan extractJoinCluster(Plan plan) {
        Rule rule = new HyperGraphJoinReorderGroupPlan().build();
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
            stats.setRowCount(rowCounts.get(Integer.valueOf(scanPlan.getTable().getName())));
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
