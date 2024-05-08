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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsCacheKey;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HyperGraphBuilder {
    private final List<Integer> rowCounts = new ArrayList<>();
    private final List<LogicalOlapScan> tables = new ArrayList<>();
    private final HashMap<BitSet, LogicalPlan> plans = new HashMap<>();
    private final HashMap<BitSet, List<Integer>> schemas = new HashMap<>();

    private ImmutableList<JoinType> fullJoinTypes = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN
    );

    private ImmutableList<JoinType> leftFullJoinTypes = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN
    );

    private ImmutableList<JoinType> rightFullJoinTypes = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.RIGHT_ANTI_JOIN
    );

    public HyperGraphBuilder() {}

    public HyperGraphBuilder(Set<JoinType> validJoinType) {
        fullJoinTypes = fullJoinTypes.stream()
                .filter(validJoinType::contains)
                .collect(ImmutableList.toImmutableList());
        leftFullJoinTypes = leftFullJoinTypes.stream()
                .filter(validJoinType::contains)
                .collect(ImmutableList.toImmutableList());
        rightFullJoinTypes = rightFullJoinTypes.stream()
                .filter(validJoinType::contains)
                .collect(ImmutableList.toImmutableList());
    }

    public HyperGraph build() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return buildHyperGraph(plan);
    }

    public Plan buildPlan() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return plan;
    }

    public Plan buildJoinPlan() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return buildPlanWithJoinType(plan, new BitSet(), false);
    }

    public Plan randomBuildPlanWith(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        return this.buildJoinPlan();
    }

    public HyperGraph randomBuildWith(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        return this.build();
    }

    public Plan buildJoinPlanWithJoinHint(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return buildPlanWithJoinType(plan, new BitSet(), true);
    }

    private void randomBuildInit(int tableNum, int edgeNum) {
        Preconditions.checkArgument(edgeNum >= tableNum - 1,
                "We can't build a connected graph with %s tables %s edges", tableNum, edgeNum);
        Preconditions.checkArgument(edgeNum <= tableNum * (tableNum - 1) / 2,
                "The edges are redundant with %s tables %s edges", tableNum, edgeNum);

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
    }

    public HyperGraphBuilder init(int... rowCounts) {
        for (int i = 0; i < rowCounts.length; i++) {
            this.rowCounts.add(rowCounts[i]);
            BitSet bitSet = new BitSet();
            bitSet.set(i);
            LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(i, String.valueOf(i), 0);
            plans.put(bitSet, scan);
            tables.add(scan);
            List<Integer> schema = new ArrayList<>();
            schema.add(i);
            schemas.put(bitSet, schema);
        }
        return this;
    }

    public void initStats(String dbName, CascadesContext context) {
        for (Group group : context.getMemo().getGroups()) {
            GroupExpression groupExpression = group.getLogicalExpression();
            if (groupExpression.getPlan() instanceof LogicalOlapScan) {
                LogicalOlapScan scan = (LogicalOlapScan) groupExpression.getPlan();
                OlapTable table = scan.getTable();
                if (Strings.isNullOrEmpty(table.getQualifiedDbName())) {
                    table.setQualifiedDbName(dbName);
                }
                Statistics stats = injectRowcount((LogicalOlapScan) groupExpression.getPlan());
                for (Expression expr : stats.columnStatistics().keySet()) {
                    SlotReference slot = (SlotReference) expr;
                    Env.getCurrentEnv().getStatisticsCache().putCache(
                            new StatisticsCacheKey(table.getDatabase().getCatalog().getId(),
                                    table.getDatabase().getId(), table.getId(), -1, slot.getName()),
                            stats.columnStatistics().get(expr));
                }
            }
        }
    }

    public HyperGraphBuilder addEdge(JoinType joinType, int node1, int node2) {
        Preconditions.checkArgument(node1 >= 0 && node1 < rowCounts.size(),
                "%d must in [%s, %ds", node1, 0, rowCounts.size());
        Preconditions.checkArgument(node2 >= 0 && node1 < rowCounts.size(),
                "%d must in [%d, %d)", node1, 0, rowCounts.size());

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
            Preconditions.checkArgument(leftKey.isPresent() && rightKey.isPresent(),
                    "can not find plan %s-%s", leftBitmap, rightBitmap);
            Plan leftPlan = plans.get(leftKey.get());
            Plan rightPlan = plans.get(rightKey.get());
            LogicalJoin join = new LogicalJoin<>(joinType, leftPlan, rightPlan, null);

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
        constructJoin(node1, node2, fullKey.get());
        return this;
    }

    private Plan buildPlanWithJoinType(Plan plan, BitSet requireTable, boolean withJoinHint) {
        if (!(plan instanceof LogicalJoin)) {
            return plan;
        }
        LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin) plan;
        BitSet leftSchema = findPlanSchema(join.left());
        BitSet rightSchema = findPlanSchema(join.right());
        JoinType joinType;
        if (isSubset(requireTable, leftSchema)) {
            int index = (int) (Math.random() * leftFullJoinTypes.size());
            joinType = leftFullJoinTypes.get(index);
        } else if (isSubset(requireTable, rightSchema)) {
            int index = (int) (Math.random() * rightFullJoinTypes.size());
            joinType = rightFullJoinTypes.get(index);
        } else {
            int index = (int) (Math.random() * fullJoinTypes.size());
            joinType = fullJoinTypes.get(index);
        }
        Set<Slot> requireSlots = join.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getOutput().stream().anyMatch(slot -> requireSlots.contains(slot))) {
                requireTable.set(i);
            }
        }

        Plan left = buildPlanWithJoinType(join.left(), requireTable, withJoinHint);
        Plan right = buildPlanWithJoinType(join.right(), requireTable, withJoinHint);
        Set<Slot> outputs = Stream.concat(left.getOutput().stream(), right.getOutput().stream())
                .collect(Collectors.toSet());
        assert outputs.containsAll(requireSlots);
        if (withJoinHint) {
            DistributeType[] values = DistributeType.values();
            Random random = new Random();
            int randomIndex = random.nextInt(values.length);
            DistributeType hint = values[randomIndex];
            Plan hintJoin = ((LogicalJoin) join.withChildren(left, right)).withJoinTypeAndContext(joinType, null);
            ((LogicalJoin) hintJoin).setHint(new DistributeHint(hint));
            return hintJoin;
        }
        return ((LogicalJoin) join.withChildren(left, right)).withJoinTypeAndContext(joinType, null);
    }

    private Optional<BitSet> findPlan(BitSet bitSet) {
        for (BitSet key : plans.keySet()) {
            if (isSubset(bitSet, key)) {
                return Optional.of(key);
            }
        }
        return Optional.empty();
    }

    private BitSet findPlanSchema(Plan plan) {
        BitSet bitSet = new BitSet();
        if (plan instanceof LogicalOlapScan) {
            for (int i = 0; i < tables.size(); i++) {
                if (tables.get(i).equals(plan)) {
                    bitSet.set(i);
                }
            }
            assert !bitSet.isEmpty();
            return bitSet;
        }

        bitSet.or(findPlanSchema(((LogicalJoin) plan).left()));
        bitSet.or(findPlanSchema(((LogicalJoin) plan).right()));
        assert !bitSet.isEmpty();
        return bitSet;
    }

    private boolean isSubset(BitSet bitSet1, BitSet bitSet2) {
        BitSet bitSet = new BitSet();
        bitSet.or(bitSet1);
        bitSet.or(bitSet2);
        return bitSet.equals(bitSet2);
    }

    private HyperGraph buildHyperGraph(Plan plan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        injectRowcount(cascadesContext.getMemo().getRoot());
        return HyperGraph.builderForDPhyper(cascadesContext.getMemo().getRoot()).build();
    }

    public static HyperGraph buildHyperGraphFromPlan(Plan plan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        return HyperGraph.builderForDPhyper(cascadesContext.getMemo().getRoot()).build();
    }

    private void injectRowcount(Group group) {
        if (!HyperGraph.isValidJoin(group.getLogicalExpression().getPlan())) {
            LogicalOlapScan scanPlan = (LogicalOlapScan) group.getLogicalExpression().getPlan();
            Statistics stats = injectRowcount(scanPlan);
            group.setStatistics(stats);
            return;
        }
        injectRowcount(group.getLogicalExpression().child(0));
        injectRowcount(group.getLogicalExpression().child(1));
    }

    private Statistics injectRowcount(LogicalOlapScan scanPlan) {
        HashMap<Expression, ColumnStatistic> slotIdToColumnStats = new HashMap<Expression, ColumnStatistic>();
        int count = rowCounts.get(Integer.parseInt(scanPlan.getTable().getName()));
        for (Slot slot : scanPlan.getOutput()) {
            slotIdToColumnStats.put(slot,
                    new ColumnStatistic(count, count, null, 1, 0, 0, 0,
                            count, null, null, true,
                            new Date().toString()));
        }
        return new Statistics(count, slotIdToColumnStats);
    }

    private void constructJoin(int node1, int node2, BitSet key) {
        LogicalJoin join = (LogicalJoin) plans.get(key);
        Expression condition = makeCondition(node1, node2, key);
        plans.put(key, attachCondition(condition, join));
    }

    private LogicalJoin attachCondition(Expression condition, LogicalJoin join) {
        Plan left = join.left();
        Set<Slot> leftSlots = new HashSet<>(left.getOutput());
        Plan right = join.right();
        Set<Slot> rightSlots = new HashSet<>(right.getOutput());
        List<Expression> conditions = new ArrayList<>(join.getExpressions());
        Set<Slot> inputs = condition.getInputSlots();
        if (leftSlots.containsAll(inputs)) {
            left = attachCondition(condition, (LogicalJoin) left);
        } else if (rightSlots.containsAll(inputs)) {
            right = attachCondition(condition, (LogicalJoin) right);
        } else {
            conditions.add(condition);
        }
        return new LogicalJoin<>(join.getJoinType(), conditions, left, right, null);
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

    public Set<List<String>> evaluate(Plan plan) {
        JoinEvaluator evaluator = new JoinEvaluator(rowCounts);
        Map<Slot, List<Integer>> res = evaluator.evaluate(plan);
        int rowCount = 0;
        if (res.size() > 0) {
            rowCount = res.values().iterator().next().size();
        }
        List<Slot> keySet = res.keySet().stream()
                .sorted(
                        (slot1, slot2) ->
                                String.CASE_INSENSITIVE_ORDER.compare(slot1.toString(), slot2.toString()))
                .collect(Collectors.toList());
        Set<List<String>> tuples = new HashSet<>();
        tuples.add(keySet.stream().map(s -> s.toString()).collect(Collectors.toList()));
        for (int i = 0; i < rowCount; i++) {
            List<String> tuple = new ArrayList<>();
            for (Slot key : keySet) {
                tuple.add(String.valueOf(res.get(key).get(i)));
            }
            tuples.add(tuple);
        }
        return tuples;
    }

    class JoinEvaluator {
        List<Integer> rowCounts;

        JoinEvaluator(List<Integer> rowCounts) {
            this.rowCounts = rowCounts;
        }

        Map<Slot, List<Integer>> evaluate(Plan plan) {
            if (plan instanceof LogicalOlapScan || plan instanceof PhysicalOlapScan) {
                return evaluateScan(plan);
            }
            if (plan instanceof LogicalJoin || plan instanceof AbstractPhysicalJoin) {
                return evaluateJoin(plan);
            }
            assert plan.children().size() == 1;
            return evaluate(plan.child(0));
        }

        public Map<Slot, List<Integer>> evaluateScan(Plan plan) {
            String name;
            if (plan instanceof LogicalOlapScan) {
                name = ((LogicalOlapScan) plan).getTable().getName();
            } else {
                Preconditions.checkArgument(plan instanceof PhysicalOlapScan);
                name = ((PhysicalOlapScan) plan).getTable().getName();
            }
            int rowCount = rowCounts.get(Integer.parseInt(name));
            Map<Slot, List<Integer>> rows = new HashMap<>();
            for (Slot slot : plan.getOutput()) {
                rows.put(slot, new ArrayList<>());
                for (int i = 0; i < rowCount; i++) {
                    rows.get(slot).add(i);
                }
            }
            return rows;
        }

        public Map<Slot, List<Integer>> evaluateJoin(Plan plan) {
            Map<Slot, List<Integer>> left;
            Map<Slot, List<Integer>> right;
            List<? extends Expression> expressions = plan.getExpressions();
            JoinType joinType;
            if (plan instanceof LogicalJoin) {
                left = this.evaluate(((LogicalJoin<?, ?>) plan).left());
                right = this.evaluate(((LogicalJoin<?, ?>) plan).right());
                joinType = ((LogicalJoin<?, ?>) plan).getJoinType();
            } else {
                Preconditions.checkArgument(plan instanceof AbstractPhysicalJoin);
                left = this.evaluate(((AbstractPhysicalJoin<?, ?>) plan).left());
                right = this.evaluate(((AbstractPhysicalJoin<?, ?>) plan).right());
                joinType = ((AbstractPhysicalJoin<?, ?>) plan).getJoinType();
            }

            List<Pair<Integer, Integer>> matchPair = new ArrayList<>();
            for (int i = 0; i < getTableRC(left); i++) {
                for (int j = 0; j < getTableRC(right); j++) {
                    int leftIndex = i;
                    int rightIndex = j;
                    Boolean matched = true;
                    for (Expression expr : expressions) {
                        Boolean res = evaluateExpr(joinType, expr, left, leftIndex, right, rightIndex);
                        if (res == null) {
                            matched = null;
                        } else if (res == false) {
                            matched = false;
                            break;
                        }
                    }
                    if (matched == null) {
                        // NAAJ return nothing when right has null
                        for (int i1 = 0; i1 < getTableRC(left); i1++) {
                            for (int j1 = 0; j1 < getTableRC(right); j1++) {
                                matchPair.add(Pair.of(i1, j1));
                            }
                        }
                        return calJoin(joinType, left, right, matchPair);
                    }
                    if (matched) {
                        matchPair.add(Pair.of(i, j));
                    }
                }
            }
            return calJoin(joinType, left, right, matchPair);

        }

        Map<Slot, List<Integer>> calJoin(JoinType joinType, Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            switch (joinType) {
                case INNER_JOIN:
                    return calIJ(left, right, matchPair);
                case LEFT_OUTER_JOIN:
                    return calLOJ(left, right, matchPair);
                case RIGHT_OUTER_JOIN:
                    return calLOJ(right, left,
                            matchPair.stream().map(p -> Pair.of(p.second, p.first)).collect(Collectors.toList()));
                case FULL_OUTER_JOIN:
                    return calFOJ(left, right, matchPair);
                case LEFT_SEMI_JOIN:
                    return calLSJ(left, right, matchPair);
                case RIGHT_SEMI_JOIN:
                    return calLSJ(right, left,
                            matchPair.stream().map(p -> Pair.of(p.second, p.first)).collect(Collectors.toList()));
                case LEFT_ANTI_JOIN:
                    return calLAJ(left, right, matchPair);
                case RIGHT_ANTI_JOIN:
                    return calLAJ(right, left,
                            matchPair.stream().map(p -> Pair.of(p.second, p.first)).collect(Collectors.toList()));
                case NULL_AWARE_LEFT_ANTI_JOIN:
                    return calLNAAJ(left, right, matchPair);
                case CROSS_JOIN:
                    return calFOJ(left, right, matchPair);
                default:
                    assert false;
            }
            assert false;
            return new HashMap<>();
        }

        Map<Slot, List<Integer>> calIJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            Map<Slot, List<Integer>> outputs = new HashMap<>();
            for (Slot slot : left.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            for (Slot slot : right.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            for (Pair<Integer, Integer> p : matchPair) {
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(p.first));
                }
                for (Slot slot : right.keySet()) {
                    outputs.get(slot).add(right.get(slot).get(p.second));
                }
            }
            return outputs;
        }

        Map<Slot, List<Integer>> calFOJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            Map<Slot, List<Integer>> outputs = calIJ(left, right, matchPair);
            Set<Integer> leftIndices = matchPair.stream().map(p -> p.first).collect(Collectors.toSet());
            Set<Integer> rightIndices = matchPair.stream().map(p -> p.second).collect(Collectors.toSet());

            for (int i = 0; i < getTableRC(left); i++) {
                if (leftIndices.contains(i)) {
                    continue;
                }
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(i));
                }
                for (Slot slot : right.keySet()) {
                    outputs.get(slot).add(null);
                }
            }

            for (int i = 0; i < getTableRC(right); i++) {
                if (rightIndices.contains(i)) {
                    continue;
                }
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(null);
                }
                for (Slot slot : right.keySet()) {
                    outputs.get(slot).add(right.get(slot).get(i));
                }
            }

            return outputs;
        }

        Map<Slot, List<Integer>> calLOJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            Map<Slot, List<Integer>> outputs = calIJ(left, right, matchPair);
            Set<Integer> leftIndices = matchPair.stream().map(p -> p.first).collect(Collectors.toSet());
            for (int i = 0; i < getTableRC(left); i++) {
                if (leftIndices.contains(i)) {
                    continue;
                }
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(i));
                }
                for (Slot slot : right.keySet()) {
                    outputs.get(slot).add(null);
                }
            }
            return outputs;
        }

        Map<Slot, List<Integer>> calLSJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            Map<Slot, List<Integer>> outputs = new HashMap<>();
            for (Slot slot : left.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            for (Pair<Integer, Integer> p : matchPair) {
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(p.first));
                }
            }
            return outputs;
        }

        Map<Slot, List<Integer>> calLAJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            Map<Slot, List<Integer>> outputs = new HashMap<>();
            for (Slot slot : left.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            Set<Integer> leftIndices = matchPair.stream().map(p -> p.first).collect(Collectors.toSet());
            for (int i = 0; i < getTableRC(left); i++) {
                if (leftIndices.contains(i)) {
                    continue;
                }
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(i));
                }
            }
            return outputs;
        }

        Map<Slot, List<Integer>> calLNAAJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair) {
            return calLAJ(left, right, matchPair);
        }

        Boolean evaluateExpr(JoinType joinType, Expression expr, Map<Slot, List<Integer>> left, int leftIndex,
                Map<Slot, List<Integer>> right, int rightIndex) {
            List<Slot> slots = Lists.newArrayList(expr.getInputSlots());
            Preconditions.checkArgument(slots.size() == 2);
            Integer lv;
            Integer rv;
            if (left.containsKey(slots.get(0))) {
                lv = left.get(slots.get(0)).get(leftIndex);
            } else {
                lv = right.get(slots.get(0)).get(rightIndex);
            }
            if (right.containsKey(slots.get(1))) {
                rv = right.get(slots.get(1)).get(rightIndex);
            } else {
                rv = left.get(slots.get(1)).get(leftIndex);
            }
            Boolean res = (lv == rv) && (lv != null) && (rv != null);
            if (joinType.isNullAwareLeftAntiJoin()) {
                res |= (lv == null);
            }
            if (joinType.isNullAwareLeftAntiJoin() && rv == null) {
                res = null;
            }
            return res;
        }
    }

    private int getTableRC(Map<Slot, List<Integer>> m) {
        return m.entrySet().iterator().next().getValue().size();
    }
}
