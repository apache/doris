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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
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

    private ImmutableList<JoinType> fullJoinTypes = ImmutableList.copyOf(JoinType.values());

    private ImmutableList<JoinType> leftFullJoinTypes = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN);

    private ImmutableList<JoinType> rightFullJoinTypes = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.RIGHT_ANTI_JOIN);

//    private ImmutableList<JoinType> fullJoinTypes = ImmutableList.of(
//            JoinType.INNER_JOIN,
//            JoinType.LEFT_OUTER_JOIN);
//
//    private ImmutableList<JoinType> leftFullJoinTypes = ImmutableList.of(
//            JoinType.INNER_JOIN,
//            JoinType.LEFT_OUTER_JOIN);
//
//    private ImmutableList<JoinType> rightFullJoinTypes = ImmutableList.of(
//            JoinType.INNER_JOIN,
//            JoinType.LEFT_OUTER_JOIN);

    // limit the number of CROSS_JOIN nodes to avoid data explosion during tests
    private int crossJoinCount = 0;
    private final int maxCrossJoins = 2;

    public HyperGraphBuilder() {
    }

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

    public org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph buildv2() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return buildHyperGraphv2(plan);
    }

    public Plan buildPlan() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        return plan;
    }

    public Plan buildJoinPlan() {
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        crossJoinCount = 0;
        Plan result = buildPlanWithJoinType(plan, new BitSet(), false);
        // limit final output columns to at most 10
        if (result.getOutput().size() > 10) {
            return new LogicalProject(result.getOutput().subList(0, 10), result);
        }
        return result;
    }

    public Plan randomBuildPlanWith(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        return this.buildJoinPlan();
    }

    public HyperGraph randomBuildWith(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        return this.build();
    }

    public org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph randomBuildWithv2(int tableNum,
            int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        return this.buildv2();
    }

    public Plan buildJoinPlanWithJoinHint(int tableNum, int edgeNum) {
        randomBuildInit(tableNum, edgeNum);
        assert plans.size() == 1 : "there are cross join";
        Plan plan = plans.values().iterator().next();
        crossJoinCount = 0;
        Plan result = buildPlanWithJoinType(plan, new BitSet(), true);
        if (result.getOutput().size() > 10) {
            return new LogicalProject(result.getOutput().subList(0, 10), result);
        }
        return result;
    }

    private void randomBuildInit(int tableNum, int edgeNum) {
        Preconditions.checkArgument(edgeNum >= tableNum - 1,
                "We can't build a connected graph with %s tables %s edges", tableNum, edgeNum);
        Preconditions.checkArgument(edgeNum <= tableNum * (tableNum - 1) / 2,
                "The edges are redundant with %s tables %s edges", tableNum, edgeNum);

        // Each test table has 10 rows (values in 1..10) to increase join match probability.
        int[] tableRowCounts = new int[tableNum];
        for (int i = 0; i < tableNum; i++) {
            tableRowCounts[i] = 5;
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
        // Determine base candidate list guided by ancestor requirements (original logic)
        ImmutableList<JoinType> baseCandidates;
        if (isSubset(requireTable, leftSchema)) {
            baseCandidates = leftFullJoinTypes;
        } else if (isSubset(requireTable, rightSchema)) {
            baseCandidates = rightFullJoinTypes;
        } else {
            baseCandidates = fullJoinTypes;
        }

        // Compute effective requirement: ancestor requirements + this join's referenced tables
        BitSet effectiveRequire = new BitSet();
        effectiveRequire.or(requireTable);
        final Set<Slot> requireSlots = join.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getOutput().stream().anyMatch(slot -> requireSlots.contains(slot))) {
                effectiveRequire.set(i);
            }
        }

        // Determine whether effective requirements include tables from left/right subtree
        boolean needLeft = false;
        boolean needRight = false;
        for (int i = leftSchema.nextSetBit(0); i >= 0; i = leftSchema.nextSetBit(i + 1)) {
            if (effectiveRequire.get(i)) {
                needLeft = true;
                break;
            }
        }
        for (int i = rightSchema.nextSetBit(0); i >= 0; i = rightSchema.nextSetBit(i + 1)) {
            if (effectiveRequire.get(i)) {
                needRight = true;
                break;
            }
        }
        final boolean fNeedLeft = needLeft;
        final boolean fNeedRight = needRight;
        // Filter out join types that would drop required outputs
        List<JoinType> filtered = baseCandidates.stream().filter(jt -> {
            if (fNeedLeft && fNeedRight) {
                return !jt.isSemiOrAntiJoin();
            } else if (fNeedLeft) {
                // must keep left outputs -> forbid join types that drop left
                return !jt.isRightSemiOrAntiJoin();
            } else if (fNeedRight) {
                // must keep right outputs -> forbid join types that drop right
                return !jt.isLeftSemiOrAntiJoin();
            }
            return true;
        }).collect(Collectors.toList());

        if (filtered.isEmpty()) {
            baseCandidates = ImmutableList.of(JoinType.INNER_JOIN);
        } else {
            baseCandidates = ImmutableList.copyOf(filtered);
        }

        // enforce max cross join cap
        List<JoinType> candidatesList = new ArrayList<>(baseCandidates);
        if (crossJoinCount >= maxCrossJoins) {
            candidatesList.removeIf(jt -> jt == JoinType.CROSS_JOIN);
        }
        if (candidatesList.isEmpty()) {
            candidatesList.add(JoinType.INNER_JOIN);
        }
        int index = (int) (Math.random() * candidatesList.size());
        JoinType joinType = candidatesList.get(index);
        if (joinType == JoinType.CROSS_JOIN) {
            crossJoinCount++;
        }
        final Set<Slot> requireSlots2 = join.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getOutput().stream().anyMatch(slot -> requireSlots2.contains(slot))) {
                requireTable.set(i);
            }
        }

        Plan left = buildPlanWithJoinType(join.left(), requireTable, withJoinHint);
        Plan right = buildPlanWithJoinType(join.right(), requireTable, withJoinHint);
        Set<Slot> outputs = Stream.concat(left.getOutput().stream(), right.getOutput().stream())
                .collect(Collectors.toSet());
        assert outputs.containsAll(requireSlots2);

        // ensure hash/other conjuncts satisfy requirement:
        // - if chosen joinType is not CROSS_JOIN, there must be at least one equality in hashJoinConjuncts
        // - if joinType is CROSS_JOIN, remove any equality conditions
        List<Expression> finalHash = new ArrayList<>(join.getHashJoinConjuncts());
        List<Expression> finalOther = new ArrayList<>(join.getOtherJoinConjuncts());
        if (joinType == JoinType.CROSS_JOIN) {
            // CROSS JOIN should not have any join conditions
            finalHash.clear();
            finalOther.clear();
        } else {
            // For non-CROSS joins, ensure there's at least one join condition (hash or other).
            if (finalHash.isEmpty() && finalOther.isEmpty()) {
                Slot lslot = left.getOutput().get(0);
                Slot rslot = right.getOutput().get(0);
                finalHash.add(new EqualTo(lslot, rslot));
            }
        }

        if (withJoinHint) {
            DistributeType[] values = DistributeType.values();
            Random random = new Random();
            int randomIndex = random.nextInt(values.length);
            DistributeType hint = values[randomIndex];
            LogicalJoin hintJoin = new LogicalJoin(joinType, finalHash, finalOther, left, right, null);
            hintJoin.setHint(new DistributeHint(hint));
            return hintJoin;
        }
        return new LogicalJoin(joinType, finalHash, finalOther, left, right, null);
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
        if (cascadesContext.getMemo() == null) {
            MemoTestUtils.initMemoAndValidState(cascadesContext);
        }
        injectRowcount(cascadesContext.getMemo().getRoot());
        return HyperGraph.builderForDPhyper(cascadesContext.getMemo().getRoot()).build();
    }

    private org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph buildHyperGraphv2(Plan plan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        if (cascadesContext.getMemo() == null) {
            MemoTestUtils.initMemoAndValidState(cascadesContext);
        }
        injectRowcount(cascadesContext.getMemo().getRoot());
        return org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph.builderForDPhyper(
                cascadesContext.getMemo().getRoot(), cascadesContext).build();
    }

    public static org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph buildHyperGraphFromPlan(Plan plan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
        if (cascadesContext.getMemo() == null) {
            MemoTestUtils.initMemoAndValidState(cascadesContext);
        }
        return org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph.builderForDPhyper(
                cascadesContext.getMemo().getRoot(), cascadesContext).build();
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
                            new Date().toString(), null));
        }
        return new Statistics(count, slotIdToColumnStats);
    }

    private void constructJoin(int node1, int node2, BitSet key) {
        LogicalJoin join = (LogicalJoin) plans.get(key);
        // generate up to two hash and up to two other conditions (most often 1 each)
        java.util.List<Expression> conditions = makeConditions(node1, node2, key);
        LogicalJoin current = join;
        for (Expression condition : conditions) {
            current = attachCondition(condition, current);
        }
        plans.put(key, current);
    }

    private LogicalJoin attachCondition(Expression condition, LogicalJoin join) {
        Plan left = join.left();
        Set<Slot> leftSlots = new HashSet<>(left.getOutput());
        Plan right = join.right();
        Set<Slot> rightSlots = new HashSet<>(right.getOutput());
        List<Expression> hashConjuncts = new ArrayList<>(join.getHashJoinConjuncts());
        List<Expression> otherConjuncts = new ArrayList<>(join.getOtherJoinConjuncts());
        Set<Slot> inputs = condition.getInputSlots();
        if (leftSlots.containsAll(inputs)) {
            if (left instanceof LogicalJoin) {
                left = attachCondition(condition, (LogicalJoin) left);
            } else {
                // child is not a join (e.g., a scan). Can't recurse further — attach at this level.
                if (condition instanceof EqualPredicate) {
                    if (hashConjuncts.size() < 2) {
                        hashConjuncts.add(condition);
                    }
                } else {
                    if (otherConjuncts.size() < 2) {
                        otherConjuncts.add(condition);
                    }
                }
            }
        } else if (rightSlots.containsAll(inputs)) {
            if (right instanceof LogicalJoin) {
                right = attachCondition(condition, (LogicalJoin) right);
            } else {
                // child is not a join — attach at this level.
                if (condition instanceof EqualPredicate) {
                    if (hashConjuncts.size() < 2) {
                        hashConjuncts.add(condition);
                    }
                } else {
                    if (otherConjuncts.size() < 2) {
                        otherConjuncts.add(condition);
                    }
                }
            }
        } else {
            if (condition instanceof EqualPredicate) {
                if (hashConjuncts.size() < 2) {
                    hashConjuncts.add(condition);
                }
            } else {
                if (otherConjuncts.size() < 2) {
                    otherConjuncts.add(condition);
                }
            }
        }
        return new LogicalJoin<>(join.getJoinType(), hashConjuncts, otherConjuncts, left, right, null);
    }

    /**
     * Generate a small set of join conditions for the pair of tables.
     * - At most two hash (equality) conditions
     * - At most two other conditions
     * - Most of the time produce only one of each
     */
    private java.util.List<Expression> makeConditions(int node1, int node2, BitSet bitSet) {
        Plan plan = plans.get(bitSet);
        List<Integer> schema = schemas.get(bitSet);
        int size = schema.size();
        int leftIndex = -1;
        int rightIndex = -1;
        for (int i = 0; i < size; i++) {
            if (schema.get(i) == node1) {
                leftIndex = i * 2;
            }
            if (schema.get(i) == node2) {
                rightIndex = i * 2;
            }
        }
        assert leftIndex != -1 && rightIndex != -1;

        java.util.Random rand = new java.util.Random();
        java.util.List<Integer> leftCandidates = new java.util.ArrayList<>();
        java.util.List<Integer> rightCandidates = new java.util.ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (schema.get(i) == node1) {
                leftCandidates.add(i * 2);
                leftCandidates.add(i * 2 + 1);
            }
            if (schema.get(i) == node2) {
                rightCandidates.add(i * 2);
                rightCandidates.add(i * 2 + 1);
            }
        }

        // Decide counts: make it very likely to have exactly 1 hash and 1 other condition
        int hashRoll = rand.nextInt(100);
        int hashCount;
        if (hashRoll < 90) {
            hashCount = 1; // most
        } else if (hashRoll < 98) {
            hashCount = 2; // some
        } else {
            hashCount = 0; // rare
        }
        int otherRoll = rand.nextInt(100);
        int otherCount;
        if (otherRoll < 90) {
            otherCount = 1; // most
        } else if (otherRoll < 98) {
            otherCount = 2; // some
        } else {
            otherCount = 0; // rare
        }
        hashCount = Math.min(hashCount, 2);
        otherCount = Math.min(otherCount, 2);

        java.util.List<Expression> conditions = new java.util.ArrayList<>();

        // build hash (equality) conditions
        for (int h = 0; h < hashCount; h++) {
            int lPos = leftCandidates.get(rand.nextInt(leftCandidates.size()));
            int rPos = rightCandidates.get(rand.nextInt(rightCandidates.size()));
            // with some probability use arithmetic on right side
            if (rand.nextInt(100) < 30) {
                conditions.add(new EqualTo(plan.getOutput().get(lPos),
                        new Add(plan.getOutput().get(rPos), Literal.of(1))));
            } else {
                conditions.add(new EqualTo(plan.getOutput().get(lPos), plan.getOutput().get(rPos)));
            }
        }

        // build other (non-equality) conditions
        for (int o = 0; o < otherCount; o++) {
            int choice = rand.nextInt(3);
            if (choice == 0) {
                // single-table predicate on left
                int lPos = leftCandidates.get(rand.nextInt(leftCandidates.size()));
                conditions.add(new GreaterThan(plan.getOutput().get(lPos), Literal.of(1)));
            } else if (choice == 1) {
                int lPos = leftCandidates.get(rand.nextInt(leftCandidates.size()));
                int rPos = rightCandidates.get(rand.nextInt(rightCandidates.size()));
                conditions.add(new GreaterThan(new Add(plan.getOutput().get(lPos), Literal.of(1)),
                        plan.getOutput().get(rPos)));
            } else {
                // another single-table predicate on right
                int rPos = rightCandidates.get(rand.nextInt(rightCandidates.size()));
                conditions.add(new GreaterThan(plan.getOutput().get(rPos), Literal.of(0)));
            }
        }

        return conditions;
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
                        (slot1, slot2) -> String.CASE_INSENSITIVE_ORDER.compare(slot1.toString(), slot2.toString()))
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

            if (plan instanceof LogicalProject) {
                LogicalProject lp = (LogicalProject) plan;
                Map<Slot, List<Integer>> child = evaluate((Plan) lp.child(0));
                List<NamedExpression> projects = lp.getProjects();
                Map<Slot, List<Integer>> outputs = new HashMap<>();
                int rc = child.isEmpty() ? 0 : getTableRC(child);
                for (NamedExpression proj : projects) {
                    outputs.put(proj.toSlot(), new ArrayList<>());
                }
                for (int i = 0; i < rc; i++) {
                    for (NamedExpression proj : projects) {
                        Integer v = evalExprOnSingle(proj, i, child);
                        outputs.get(proj.toSlot()).add(v);
                    }
                }
                return outputs;
            }

            if (plan instanceof PhysicalProject) {
                PhysicalProject pp = (PhysicalProject) plan;
                Map<Slot, List<Integer>> child = evaluate((Plan) pp.child(0));
                List<NamedExpression> projects = pp.getProjects();
                Map<Slot, List<Integer>> outputs = new HashMap<>();
                int rc = child.isEmpty() ? 0 : getTableRC(child);
                for (NamedExpression proj : projects) {
                    outputs.put(proj.toSlot(), new ArrayList<>());
                }
                for (int i = 0; i < rc; i++) {
                    for (NamedExpression proj : projects) {
                        Integer v = evalExprOnSingle(proj, i, child);
                        outputs.get(proj.toSlot()).add(v);
                    }
                }
                return outputs;
            }
            if (plan instanceof LogicalJoin || plan instanceof AbstractPhysicalJoin) {
                return evaluateJoin(plan);
            }
            if (plan instanceof PhysicalStorageLayerAggregate) {
                return evaluate(((PhysicalStorageLayerAggregate) plan).getRelation());
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
                    // produce values in range 1..rowCount, but include one NULL deterministically
                    // (first row is NULL) to exercise NULL-handling without explosion
                    if (i == 0) {
                        rows.get(slot).add(null);
                    } else {
                        rows.get(slot).add(i + 0); // values 2..rowCount
                    }
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
            // Track left rows that have at least one UNKNOWN (null) comparison
            Set<Integer> leftHasUnknown = new HashSet<>();
            for (int i = 0; i < getTableRC(left); i++) {
                for (int j = 0; j < getTableRC(right); j++) {
                    int leftIndex = i;
                    int rightIndex = j;
                    Boolean matched = true;
                    for (Expression expr : expressions) {
                        Boolean res = evaluateExpr(joinType, expr, left, leftIndex, right, rightIndex);
                        if (res == null) {
                            // For Null-Aware Left Anti Join, record unknown per-left-row
                            if (joinType.isNullAwareLeftAntiJoin()) {
                                leftHasUnknown.add(leftIndex);
                                matched = false; // treat unknown as non-match for pair
                                break;
                            } else {
                                matched = false;
                                break;
                            }
                        } else if (res == false) {
                            matched = false;
                            break;
                        }
                    }
                    if (matched) {
                        matchPair.add(Pair.of(i, j));
                    }
                }
            }
            if (joinType.isNullAwareLeftAntiJoin()) {
                return calLNAAJ(left, right, matchPair, leftHasUnknown);
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
                case CROSS_JOIN:
                    return calCJ(left, right);
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

        /**
         * Calculate Cross Join (Cartesian product) of left and right.
         */
        Map<Slot, List<Integer>> calCJ(Map<Slot, List<Integer>> left,
                Map<Slot, List<Integer>> right) {
            Map<Slot, List<Integer>> outputs = new HashMap<>();
            for (Slot slot : left.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            for (Slot slot : right.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            int leftRC = getTableRC(left);
            int rightRC = getTableRC(right);
            for (int i = 0; i < leftRC; i++) {
                for (int j = 0; j < rightRC; j++) {
                    for (Slot slot : left.keySet()) {
                        outputs.get(slot).add(left.get(slot).get(i));
                    }
                    for (Slot slot : right.keySet()) {
                        outputs.get(slot).add(right.get(slot).get(j));
                    }
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
                Map<Slot, List<Integer>> right, List<Pair<Integer, Integer>> matchPair,
                Set<Integer> leftHasUnknown) {
            Map<Slot, List<Integer>> outputs = new HashMap<>();
            for (Slot slot : left.keySet()) {
                outputs.put(slot, new ArrayList<>());
            }
            Set<Integer> leftMatched = matchPair.stream().map(p -> p.first).collect(Collectors.toSet());
            int leftRC = getTableRC(left);
            for (int i = 0; i < leftRC; i++) {
                // include left row only when it has no matches and no unknowns
                if (leftMatched.contains(i)) {
                    continue;
                }
                if (leftHasUnknown.contains(i)) {
                    continue;
                }
                for (Slot slot : left.keySet()) {
                    outputs.get(slot).add(left.get(slot).get(i));
                }
            }
            return outputs;
        }

        Boolean evaluateExpr(JoinType joinType, Expression expr, Map<Slot, List<Integer>> left, int leftIndex,
                Map<Slot, List<Integer>> right, int rightIndex) {
            // Use a dedicated recursive method to avoid lambda self-reference issues
            // Evaluate EqualTo/Gt operands using evalExprRecursive
            if (expr instanceof EqualTo) {
                EqualTo eq = (EqualTo) expr;
                Integer lv = evalExprRecursive(eq.left(), leftIndex, rightIndex, left, right);
                Integer rv = evalExprRecursive(eq.right(), leftIndex, rightIndex, left, right);
                Boolean res;
                if (lv == null || rv == null) {
                    res = null;
                } else {
                    res = lv.equals(rv);
                }
                if (joinType.isNullAwareLeftAntiJoin()) {
                    if (lv == null) {
                        res = Boolean.TRUE;
                    }
                    if (rv == null) {
                        res = null;
                    }
                }
                return res;
            }

            if (expr instanceof GreaterThan) {
                GreaterThan gt = (GreaterThan) expr;
                Integer lv = evalExprRecursive(gt.left(), leftIndex, rightIndex, left, right);
                Integer rv = evalExprRecursive(gt.right(), leftIndex, rightIndex, left, right);
                if (lv == null || rv == null) {
                    return null;
                }
                return lv > rv;
            }

            // fallback: previous slot-based behaviour for binary slot comparisons
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

        private Integer evalExprRecursive(Expression e, int leftIndex, int rightIndex,
                Map<Slot, List<Integer>> left, Map<Slot, List<Integer>> right) {
            if (e instanceof Slot) {
                Slot s = (Slot) e;
                if (left.containsKey(s)) {
                    return left.get(s).get(leftIndex);
                } else {
                    return right.get(s).get(rightIndex);
                }
            }
            if (e instanceof org.apache.doris.nereids.trees.expressions.literal.Literal) {
                Object val = ((org.apache.doris.nereids.trees.expressions.literal.Literal) e).getValue();
                if (val == null) {
                    return null;
                }
                if (val instanceof Number) {
                    return ((Number) val).intValue();
                }
                try {
                    return Integer.parseInt(String.valueOf(val));
                } catch (Exception ex) {
                    return null;
                }
            }
            if (e instanceof Add) {
                Add add = (Add) e;
                Integer a = evalExprRecursive(add.left(), leftIndex, rightIndex, left, right);
                Integer b = evalExprRecursive(add.right(), leftIndex, rightIndex, left, right);
                if (a == null || b == null) {
                    return null;
                }
                return a + b;
            }
            return null;
        }

        private Integer evalExprOnSingle(Expression e, int rowIndex, Map<Slot, List<Integer>> child) {
            if (e instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) e;
                if (!ne.children().isEmpty()) {
                    return evalExprOnSingle(ne.child(0), rowIndex, child);
                }
            }
            if (e instanceof Slot) {
                Slot s = (Slot) e;
                if (child.containsKey(s)) {
                    return child.get(s).get(rowIndex);
                }
                return null;
            }
            if (e instanceof org.apache.doris.nereids.trees.expressions.literal.Literal) {
                Object val = ((org.apache.doris.nereids.trees.expressions.literal.Literal) e).getValue();
                if (val == null) {
                    return null;
                }
                if (val instanceof Number) {
                    return ((Number) val).intValue();
                }
                try {
                    return Integer.parseInt(String.valueOf(val));
                } catch (Exception ex) {
                    return null;
                }
            }
            if (e instanceof Add) {
                Add add = (Add) e;
                Integer a = evalExprOnSingle(add.left(), rowIndex, child);
                Integer b = evalExprOnSingle(add.right(), rowIndex, child);
                if (a == null || b == null) {
                    return null;
                }
                return a + b;
            }
            return null;
        }
    }

    private int getTableRC(Map<Slot, List<Integer>> m) {
        return m.entrySet().iterator().next().getValue().size();
    }
}
