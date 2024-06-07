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

import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.thrift.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
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
public class PushDownAggThroughJoinByFk implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(
                        innerLogicalJoin()
                                .when(j -> j.getJoinType().isInnerJoin()
                                        && !j.isMarkJoin()
                                        && j.getOtherJoinConjuncts().isEmpty()))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(Slot.class::isInstance)
                                    && agg.getOutputExpressions().stream().allMatch(Slot.class::isInstance))
                        .thenApply(ctx -> pushAgg(ctx.root, ctx.root.child()))
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_FK_JOIN),
                logicalAggregate(
                        logicalProject(
                                innerLogicalJoin()
                                        .when(j -> j.getJoinType().isInnerJoin()
                                                && !j.isMarkJoin()
                                                && j.getOtherJoinConjuncts().isEmpty()))
                                .when(Project::isAllSlots))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(Slot.class::isInstance))
                        .thenApply(ctx -> pushAgg(ctx.root, ctx.root.child().child()))
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_FK_JOIN)
        );
    }

    private @Nullable Plan pushAgg(LogicalAggregate<?> agg, LogicalJoin<?, ?> join) {
        Plan foreign = tryExtractForeign(join);
        if (foreign == null) {
            return null;
        }
        Set<Slot> foreignKey = Sets.intersection(join.getEqualSlots().getAllItemSet(), foreign.getOutputSet());
        LogicalAggregate<?> newAgg = tryGroupByForeign(agg, foreign, foreignKey);
        if (newAgg == null) {
            return null;
        }

        if (join.left() == foreign) {
            return join.withChildren(newAgg, join.right());
        } else if (join.right() == foreign) {
            return join.withChildren(join.left(), newAgg);
        }
        return null;
    }

    private @Nullable LogicalAggregate<?> tryGroupByForeign(
            LogicalAggregate<?> agg, Plan foreign, Set<Slot> foreignKey) {
        Set<Slot> groupBySlots = new HashSet<>();
        for (Expression expr : agg.getGroupByExpressions()) {
            groupBySlots.addAll(expr.getInputSlots());
        }
        if (groupBySlots.containsAll(foreignKey)) {
            return null;
        }
        if (foreign.getOutputSet().containsAll(groupBySlots)) {
            return agg;
        }
        Set<Slot> foreignOutput = foreign.getOutputSet();
        Set<Slot> primarySlots = Sets.difference(groupBySlots, foreignOutput);
        DataTrait dataTrait = agg.child().getLogicalProperties().getTrait();
        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(Sets.union(groupBySlots, foreign.getOutputSet()));
        Set<Slot> newGroupBySlots = new HashSet<>(groupBySlots);
        for (Slot slot : primarySlots) {
            Set<Set<Slot>> replacedSlotSets = funcDeps.calBinaryDependencies(ImmutableSet.of(slot));
            for (Set<Slot> replacedSlots : replacedSlotSets) {
                if (foreignOutput.containsAll(replacedSlots)) {
                    newGroupBySlots.remove(slot);
                    newGroupBySlots.addAll(replacedSlots);
                    break;
                }
            }
            if (newGroupBySlots.contains(slot)) {
                return null;
            }
        }
        LogicalAggregate<?> newAgg =
                agg.withGroupByAndOutput(ImmutableList.copyOf(newGroupBySlots), ImmutableList.copyOf(newGroupBySlots));
        return (LogicalAggregate<?>) newAgg.withChildren(foreign);
    }

    private @Nullable Plan tryExtractForeign(LogicalJoin<?, ?> join) {
        Plan foreign;
        if (JoinUtils.canEliminateByFk(join, join.left(), join.right())) {
            foreign = join.right();
        } else if (JoinUtils.canEliminateByFk(join, join.right(), join.left())) {
            foreign = join.left();
        } else {
            return null;
        }
        return foreign;
    }
}
