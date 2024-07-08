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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.thrift.annotation.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
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
            Pair<Plan, Plan> primaryAndForeign = tryExtractPrimaryForeign(subJoin);
            if (primaryAndForeign == null) {
                continue;
            }
            LogicalAggregate<?> newAgg =
                    eliminatePrimaryOutput(agg, subJoin, primaryAndForeign.first, primaryAndForeign.second);
            if (newAgg == null) {
                return null;
            }
            LogicalJoin<?, ?> newJoin = innerJoinCluster
                    .constructJoinWithPrimary(e.getKey(), subJoin, primaryAndForeign.first);
            if (newJoin != null && newJoin.left() == primaryAndForeign.first) {
                newJoin = (LogicalJoin<?, ?>) newJoin
                        .withChildren(newJoin.left(), newAgg.withChildren(newJoin.right()));
                if (Sets.union(newJoin.left().getOutputSet(), newJoin.right().getOutputSet())
                        .containsAll(newJoin.getInputSlots())) {
                    return newJoin;
                }
            } else if (newJoin != null && newJoin.right() == primaryAndForeign.first) {
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
    private LogicalAggregate<?> eliminatePrimaryOutput(LogicalAggregate<?> agg, Plan child,
            Plan primary, Plan foreign) {
        Set<Slot> aggInputs = agg.getInputSlots();
        if (primary.getOutputSet().stream().noneMatch(aggInputs::contains)) {
            return agg;
        }
        Set<Slot> primaryOutputSet = primary.getOutputSet();
        Set<Slot> primarySlots = Sets.intersection(aggInputs, primaryOutputSet);
        DataTrait dataTrait = child.getLogicalProperties().getTrait();
        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(Sets.union(foreign.getOutputSet(), primary.getOutputSet()));
        HashMap<Slot, Slot> primaryToForeignDeps = new HashMap<>();
        for (Slot slot : primarySlots) {
            Set<Set<Slot>> replacedSlotSets = funcDeps.findDeterminats(ImmutableSet.of(slot));
            for (Set<Slot> replacedSlots : replacedSlotSets) {
                if (primaryOutputSet.stream().noneMatch(replacedSlots::contains)
                        && replacedSlots.size() == 1) {
                    primaryToForeignDeps.put(slot, replacedSlots.iterator().next());
                    break;
                }
            }
        }

        Set<Expression> newGroupBySlots = constructNewGroupBy(agg, primaryOutputSet, primaryToForeignDeps);
        List<NamedExpression> newOutput = constructNewOutput(
                agg, primaryOutputSet, primaryToForeignDeps, funcDeps, primary);
        if (newGroupBySlots == null || newOutput == null) {
            return null;
        }
        return agg.withGroupByAndOutput(ImmutableList.copyOf(newGroupBySlots), ImmutableList.copyOf(newOutput));
    }

    private @Nullable Set<Expression> constructNewGroupBy(LogicalAggregate<?> agg, Set<Slot> primaryOutputs,
            Map<Slot, Slot> primaryToForeignBiDeps) {
        Set<Expression> newGroupBySlots = new HashSet<>();
        for (Expression expression : agg.getGroupByExpressions()) {
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
        return newGroupBySlots;
    }

    private @Nullable List<NamedExpression> constructNewOutput(LogicalAggregate<?> agg, Set<Slot> primaryOutput,
            Map<Slot, Slot> primaryToForeignDeps, FuncDeps funcDeps, Plan primaryPlan) {
        List<NamedExpression> newOutput = new ArrayList<>();
        for (NamedExpression expression : agg.getOutputExpressions()) {
            // There are three cases for output expressions:
            // 1. Slot: the slot is from primary plan, we need to replace it with
            //             the corresponding slot from foreign plan,
            //             or skip it when it isn't in group by.
            // 2. Count: the count is from primary plan,
            //             we need to replace the slot in the count with the corresponding slot
            //             from foreign plan
            if (expression instanceof Slot && primaryPlan.getOutput().contains(expression)) {
                if (primaryToForeignDeps.containsKey(expression)) {
                    expression = primaryToForeignDeps.getOrDefault(expression, expression.toSlot());
                } else {
                    continue;
                }
            }
            if (expression instanceof Alias
                    && expression.child(0) instanceof Count
                    && expression.child(0).child(0) instanceof Slot) {
                // count(slot) can be rewritten by circle deps
                Slot slot = (Slot) expression.child(0).child(0);
                if (primaryToForeignDeps.containsKey(slot)
                        && funcDeps.isCircleDeps(
                                ImmutableSet.of(slot), ImmutableSet.of(primaryToForeignDeps.get(slot)))) {
                    expression = (NamedExpression) expression.rewriteUp(e ->
                            e instanceof Slot
                                    ? primaryToForeignDeps.getOrDefault((Slot) e, (Slot) e)
                                    : e);
                }
            }
            if (!(expression instanceof Slot)
                    && expression.getInputSlots().stream().anyMatch(primaryOutput::contains)) {
                return null;
            }
            newOutput.add(expression);
        }
        return newOutput;
    }

    // try to extract primary key table and foreign key table
    private @Nullable Pair<Plan, Plan> tryExtractPrimaryForeign(LogicalJoin<?, ?> join) {
        Plan primary;
        Plan foreign;
        if (JoinUtils.canEliminateByFk(join, join.left(), join.right())) {
            primary = join.left();
            foreign = join.right();
        } else if (JoinUtils.canEliminateByFk(join, join.right(), join.left())) {
            primary = join.right();
            foreign = join.left();
        } else {
            return null;
        }
        return Pair.of(primary, foreign);
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
        private final Map<BitSet, LogicalJoin<?, ?>> innerJoins = new HashMap<>();
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
                        addJoin = true;
                        currentBitset.or(entry.getKey());
                        currentPlan = currentPlan.withChildren(currentPlan, entry.getValue());
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
}
