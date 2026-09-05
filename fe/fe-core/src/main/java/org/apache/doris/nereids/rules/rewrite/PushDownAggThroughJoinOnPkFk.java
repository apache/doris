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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.thrift.annotation.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Push down agg through join with foreign key:
 *    Agg(group by fk/pk)
 *     |
 *   Join(pk = fk)
 *   /  \
 *  pk  fk
 *  ======>
 *   Join(pk = fk)
 *   /     \
 *  |  Agg(group by fk)
 *  |      |
 *  pk    fk
 *
 * Constraints applied on the pattern:
 *   - Join is Inner Join (no semi/anti/outer join, no mark join).
 *   - otherJoinConjuncts is empty: only equi-join conditions are allowed,
 *     and each condition must reference exactly one slot from each side.
 *   - All GROUP BY expressions are Slot (complex grouping expressions
 *     are not supported).
 *   - If an intermediate LogicalProject exists, it must be isAllSlots
 *     (projections that introduce computations are not supported).
 */
public class PushDownAggThroughJoinOnPkFk implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(
                        innerLogicalJoin()
                                .when(j -> !j.isMarkJoin()
                                        && j.getOtherJoinConjuncts().isEmpty()))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(Slot.class::isInstance))
                        .thenApply(ctx -> pushAgg(ctx.root, ctx.root.child()))
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK),
                logicalAggregate(
                        logicalProject(
                                innerLogicalJoin()
                                        .when(j -> j.getJoinType().isInnerJoin()
                                                && !j.isMarkJoin()
                                                && j.getOtherJoinConjuncts().isEmpty()))
                                .when(Project::isAllSlots))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(Slot.class::isInstance))
                        .thenApply(ctx -> pushAgg(ctx.root, ctx.root.child().child()))
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK)
        );
    }

    private @Nullable Plan pushAgg(LogicalAggregate<?> agg, LogicalJoin<?, ?> join) {
        InnerJoinCluster innerJoinCluster = new InnerJoinCluster();
        innerJoinCluster.collectContiguousInnerJoins(join);
        if (!innerJoinCluster.isValid()) {
            return null;
        }
        for (Entry<BitSet, LogicalJoin<?, ?>> e : innerJoinCluster.getJoinsMap().entrySet()) {
            LogicalJoin<?, ?> subJoin = e.getValue();
            PrimaryForeignInfo primaryForeignInfo = tryExtractPrimaryForeign(subJoin);
            if (primaryForeignInfo == null) {
                continue;
            }
            LogicalAggregate<?> newAgg = eliminatePrimaryOutput(agg, subJoin, primaryForeignInfo);
            if (newAgg == null) {
                continue;
            }
            LogicalJoin<?, ?> newJoin = innerJoinCluster
                    .constructJoinWithPrimary(e.getKey(), subJoin, primaryForeignInfo.primary);
            if (newJoin != null && newJoin.left() == primaryForeignInfo.primary) {
                newJoin = (LogicalJoin<?, ?>) newJoin
                        .withChildren(newJoin.left(), newAgg.withChildren(newJoin.right()));
                if (Sets.union(newJoin.left().getOutputSet(), newJoin.right().getOutputSet())
                        .containsAll(newJoin.getInputSlots())) {
                    return newJoin;
                }
            } else if (newJoin != null && newJoin.right() == primaryForeignInfo.primary) {
                newJoin = (LogicalJoin<?, ?>) newJoin
                        .withChildren(newAgg.withChildren(newJoin.left()), newJoin.right());
                if (Sets.union(newJoin.left().getOutputSet(), newJoin.right().getOutputSet())
                        .containsAll(newJoin.getInputSlots())) {
                    return newJoin;
                }
            }
        }
        return null;
    }

    // eliminate the slot of primary plan in agg
    // e.g.
    // select primary_table_pk, primary_table_other from primary_table join foreign_table on pk = fk
    // group by pk, primary_table_other_cols;
    private LogicalAggregate<?> eliminatePrimaryOutput(LogicalAggregate<?> agg, Plan child,
            PrimaryForeignInfo primaryForeignInfo) {
        Set<Slot> groupBySlots = agg.getGroupByExpressions().stream()
                .map(Slot.class::cast)
                .collect(ImmutableSet.toImmutableSet());
        DataTrait dataTrait = child.getLogicalProperties().getTrait();
        if (!groupByDeterminesForeignKey(groupBySlots, primaryForeignInfo.foreignKeys, dataTrait)) {
            return null;
        }

        Plan primary = primaryForeignInfo.primary;
        Plan foreign = primaryForeignInfo.foreign;
        Set<Slot> aggInputs = agg.getInputSlots();
        // An indirectly determined foreign key still needs to be added to the pushed aggregate.
        if (primary.getOutputSet().stream().noneMatch(aggInputs::contains)
                && groupBySlots.containsAll(primaryForeignInfo.foreignKeys)) {
            return agg;
        }
        // Firstly, using fd to eliminate group by key.
        // group by pk, primary_table_other_cols;
        // -> group by pk;
        Set<Expression> removeExpression = EliminateGroupByKey.findCanBeRemovedExpressions(agg,
                Sets.intersection(agg.getOutputSet(), foreign.getOutputSet()),
                dataTrait);
        List<Expression> minGroupBySlotList = new ArrayList<>();
        for (Expression expression : agg.getGroupByExpressions()) {
            if (!removeExpression.contains(expression)) {
                minGroupBySlotList.add(expression);
            }
        }

        // Secondly, put bijective slot into map: {pk : fk}
        // Bijective slots are mutually interchangeable within GROUP BY keys.
        // group by pk -> group by fk
        Set<Slot> primaryOutputSet = primary.getOutputSet();
        Set<Slot> primarySlots = Sets.intersection(aggInputs, primaryOutputSet);
        HashMap<Slot, Slot> primaryToForeignDeps = new HashMap<>();
        FuncDeps funcDepsForJoin = dataTrait.getAllValidFuncDeps(
                Sets.union(primaryOutputSet, foreign.getOutputSet()));
        for (Slot slot : primarySlots) {
            Set<Set<Slot>> replacedSlotSets = funcDepsForJoin.findBijectionSlots(ImmutableSet.of(slot));
            for (Set<Slot> replacedSlots : replacedSlotSets) {
                if (primaryOutputSet.stream().noneMatch(replacedSlots::contains)
                        && replacedSlots.size() == 1) {
                    primaryToForeignDeps.put(slot, replacedSlots.iterator().next());
                    break;
                }
            }
        }

        // Thirdly, construct new Agg below join.
        // For the pk-fk join, the foreign table side will not expand rows.
        // As a result, executing agg(group by fk) before join is same with executing agg(group by fk) after join.
        List<Expression> newGroupBySlots = constructNewGroupBy(minGroupBySlotList, primaryOutputSet,
                primaryToForeignDeps, primaryForeignInfo.foreignKeys);
        if (newGroupBySlots == null) {
            return null;
        }
        List<NamedExpression> newOutput = constructNewOutput(
                agg, primaryOutputSet, primaryToForeignDeps, newGroupBySlots);
        if (newOutput == null) {
            return null;
        }
        return agg.withGroupByAndOutput(ImmutableList.copyOf(newGroupBySlots), ImmutableList.copyOf(newOutput));
    }

    private @Nullable List<Expression> constructNewGroupBy(List<? extends Expression> gbyExpression,
            Set<Slot> primaryOutputs, Map<Slot, Slot> primaryToForeignBiDeps, Set<Slot> foreignKeys) {
        Set<Expression> newGroupBySlots = new LinkedHashSet<>(foreignKeys);
        for (Expression expression : gbyExpression) {
            if (!(expression instanceof Slot)) {
                return null;
            }
            if (primaryOutputs.contains((Slot) expression)
                    && !primaryToForeignBiDeps.containsKey((Slot) expression)) {
                return null;
            }
            expression = primaryToForeignBiDeps.getOrDefault(expression, (Slot) expression);
            newGroupBySlots.add(expression);
        }
        return Utils.fastToImmutableList(newGroupBySlots);
    }

    private @Nullable List<NamedExpression> constructNewOutput(LogicalAggregate<?> agg, Set<Slot> primaryOutput,
            Map<Slot, Slot> primaryToForeignDeps, List<Expression> newGroupBySlots) {
        List<NamedExpression> newOutput = new ArrayList<NamedExpression>((List) newGroupBySlots);
        for (NamedExpression expression : agg.getOutputExpressions()) {
            if (expression instanceof Slot) {
                continue;
            }
            if (expression instanceof Alias
                    && expression.child(0) instanceof Count
                    && expression.child(0).arity() > 0
                    && expression.child(0).child(0) instanceof Slot) {
                // TODO: Rewrite COUNT arguments using direct PK-FK equalities from the current join instead of
                // generic bidirectional functional dependencies, which alone do not preserve NULL semantics.
                Slot slot = (Slot) expression.child(0).child(0);
                if (primaryToForeignDeps.containsKey(slot)) {
                    expression = (NamedExpression) expression.rewriteUp(e ->
                            e instanceof Slot
                                    ? primaryToForeignDeps.getOrDefault((Slot) e, (Slot) e)
                                    : e);
                }
            }
            if (expression.getInputSlots().stream().anyMatch(primaryOutput::contains)) {
                return null;
            }
            newOutput.add(expression);
        }
        return newOutput;
    }

    private static class PrimaryForeignInfo {
        final Plan primary;
        final Plan foreign;
        final Set<Slot> primaryKeys;
        final Set<Slot> foreignKeys;

        PrimaryForeignInfo(
                Plan primary,
                Plan foreign,
                Set<Slot> primaryKeys,
                Set<Slot> foreignKeys) {
            this.primary = primary;
            this.foreign = foreign;
            this.primaryKeys = ImmutableSet.copyOf(primaryKeys);
            this.foreignKeys = ImmutableSet.copyOf(foreignKeys);
        }
    }

    // try to extract primary key table and foreign key table
    private @Nullable PrimaryForeignInfo tryExtractPrimaryForeign(LogicalJoin<?, ?> join) {
        Pair<Set<Slot>, Set<Slot>> res = JoinUtils.canEliminateByFk2(join, join.left(), join.right());
        if (res != null) {
            return new PrimaryForeignInfo(join.left(), join.right(), res.first, res.second);
        }
        res = JoinUtils.canEliminateByFk2(join, join.right(), join.left());
        if (res != null) {
            return new PrimaryForeignInfo(join.right(), join.left(), res.first, res.second);
        }
        return null;
    }

    /**
     * This class flattens nested join clusters and optimizes aggregation pushdown.
     *
     * Example of flattening:
     *     Join1                   Join1         Join2
     *    /    \                   /  \         /    \
     *   a    Join2      =====>   a    b       b      c
     *       /     \
     *      b       c
     *
     * After flattening, we attempt to push down aggregations for each join.
     * For instance, if b is a primary key table and c is a foreign key table:
     *
     * Original (can't push down):     After flattening (can push down):
     *    agg(Join1)                       Join1         Join2
     *    /    \                           /  \         /    \
     *   a    Join2            =====>     a    b       b   agg(c)
     *       /     \
     *      b       c
     *
     * Finally, we can reorganize the join tree:
     *     Join2
     *    /     \
     * agg(c)   Join1
     *         /     \
     *        a       b
     */
    static class InnerJoinCluster {
        private final Map<BitSet, LogicalJoin<?, ?>> innerJoins = new LinkedHashMap<>();
        private final List<Plan> leaf = new ArrayList<>();

        void collectContiguousInnerJoins(Plan plan) {
            if (!isSlotProject(plan) && !isInnerJoin(plan)) {
                leaf.add(plan);
                return;
            }
            for (Plan child : plan.children()) {
                collectContiguousInnerJoins(child);
            }
            if (isInnerJoin(plan)) {
                LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
                Set<Slot> inputSlots = join.getInputSlots();
                BitSet childrenIndices = new BitSet();
                List<Plan> children = new ArrayList<>();
                for (int i = 0; i < leaf.size(); i++) {
                    if (!Sets.intersection(leaf.get(i).getOutputSet(), inputSlots).isEmpty()) {
                        childrenIndices.set(i);
                        children.add(leaf.get(i));
                    }
                }
                if (childrenIndices.cardinality() == 2) {
                    join = join.withChildren(children);
                }
                innerJoins.put(childrenIndices, join);
            }
        }

        boolean isValid() {
            // we cannot handle the case that there is any join with more than 2 children
            return innerJoins.keySet().stream().allMatch(x -> x.cardinality() == 2);
        }

        @Nullable LogicalJoin<?, ?> constructJoinWithPrimary(BitSet bitSet, LogicalJoin<?, ?> join, Plan primary) {
            Set<BitSet> forbiddenJoin = new HashSet<>();
            forbiddenJoin.add(bitSet);
            BitSet totalBitset = new BitSet();
            totalBitset.set(0, leaf.size());
            totalBitset.set(leaf.indexOf(primary), false);
            Plan childPlan = constructPlan(totalBitset, forbiddenJoin);
            if (childPlan == null) {
                return null;
            }
            return (LogicalJoin<?, ?>) join.withChildren(childPlan, primary);
        }

        @Nullable Plan constructPlan(BitSet bitSet, Set<BitSet> forbiddenJoin) {
            if (bitSet.cardinality() == 1) {
                return leaf.get(bitSet.nextSetBit(0));
            }

            BitSet currentBitset = new BitSet();
            Plan currentPlan = null;
            while (!currentBitset.equals(bitSet)) {
                boolean addJoin = false;
                for (Entry<BitSet, LogicalJoin<?, ?>> entry : innerJoins.entrySet()) {
                    if (forbiddenJoin.contains(entry.getKey())) {
                        continue;
                    }
                    if (currentBitset.isEmpty()) {
                        addJoin = true;
                        currentBitset.or(entry.getKey());
                        currentPlan = entry.getValue();
                        forbiddenJoin.add(entry.getKey());
                    } else if (currentBitset.intersects(entry.getKey())) {
                        // The new join shares leaves with the current accumulated plan.
                        // We need to connect the newChild with current plan
                        BitSet entryBitset = entry.getKey();

                        BitSet newBits = (BitSet) entryBitset.clone();
                        newBits.andNot(currentBitset);
                        LogicalJoin<?, ?> entryJoin = entry.getValue();

                        // Determine the new child: it must be a single leaf
                        Plan newChild;
                        if (newBits.cardinality() == 1) {
                            newChild = leaf.get(newBits.nextSetBit(0));
                        } else {
                            return null;
                        }

                        currentPlan = entryJoin.withChildren(newChild, currentPlan);
                        addJoin = true;
                        currentBitset.or(entryBitset);
                        forbiddenJoin.add(entry.getKey());
                    }
                }
                if (!addJoin) {
                    // if we cannot find any join to add, just return null
                    // It means we cannot construct a join
                    return null;
                }
            }
            return currentPlan;
        }

        Map<BitSet, LogicalJoin<?, ?>> getJoinsMap() {
            return innerJoins;
        }

        boolean isSlotProject(Plan plan) {
            return plan instanceof LogicalProject
                    && ((LogicalProject<?>) (plan)).isAllSlots();

        }

        boolean isInnerJoin(Plan plan) {
            return plan instanceof LogicalJoin
                    && ((LogicalJoin<?, ?>) plan).getJoinType().isInnerJoin()
                    && !((LogicalJoin<?, ?>) plan).isMarkJoin()
                    && ((LogicalJoin<?, ?>) plan).getOtherJoinConjuncts().isEmpty();
        }
    }

    /** Check whether the functional-dependency closure of GROUP BY contains the complete foreign key. */
    private boolean groupByDeterminesForeignKey(
            Set<Slot> groupBySlots,
            Set<Slot> foreignKeySlots,
            DataTrait dataTrait) {
        if (groupBySlots.containsAll(foreignKeySlots)) {
            return true;
        }

        Set<Slot> validSlots = Sets.union(
                groupBySlots, foreignKeySlots).immutableCopy();

        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(validSlots);
        Set<Slot> closure = new HashSet<>(groupBySlots);

        boolean changed;
        do {
            changed = false;
            for (FuncDeps.FuncDepsItem item : funcDeps.getItems()) {
                if (closure.containsAll(item.determinants)) {
                    changed |= closure.addAll(item.dependencies);
                }
            }
        } while (changed);

        return closure.containsAll(foreignKeySlots);
    }
}
