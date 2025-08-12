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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Eliminate join by foreign.
 */
public class EliminateJoinByFK extends OneRewriteRuleFactory {

    // Right now we only support eliminate inner join, which should meet the following condition:
    // 1. only contain null-reject equal condition, and which all meet fk-pk constraint
    // 2. only output foreign table output or can be converted to foreign table output
    // 3. if foreign key is null, add a isNotNull predicate for null-reject join condition
    @Override
    public Rule build() {
        return logicalProject(
                logicalJoin().when(join -> join.getJoinType().isInnerJoin())
        ).then(project -> {
            LogicalJoin<Plan, Plan> join = project.child();
            ImmutableEqualSet<Slot> equalSet = join.getEqualSlots();
            Set<Slot> residualSlot = Sets.difference(project.getInputSlots(), equalSet.getAllItemSet());
            Plan res = null;
            if (join.left().getOutputSet().containsAll(residualSlot)) {
                res = tryEliminatePrimary(project, equalSet, join.right(), join.left());
            }
            if (res == null && join.right().getOutputSet().containsAll(residualSlot)) {
                res = tryEliminatePrimary(project, equalSet, join.left(), join.right());
            }
            return res;
        }).toRule(RuleType.ELIMINATE_JOIN_BY_FK);
    }

    private @Nullable Plan tryEliminatePrimary(LogicalProject<LogicalJoin<Plan, Plan>> project,
            ImmutableEqualSet<Slot> equalSet, Plan primary, Plan foreign) {
        if (!JoinUtils.canEliminateByFk(project.child(), primary, foreign)) {
            return null;
        }
        Set<Slot> output = project.getInputSlots();
        Set<Slot> foreignKeys = Sets.intersection(foreign.getOutputSet(), equalSet.getAllItemSet());
        Map<Slot, Slot> outputToForeign = tryMapOutputToForeignPlan(foreign, output, equalSet);
        if (outputToForeign != null) {
            Pair<Plan, Set<Slot>> newChildPair = applyNullCompensationFilter(foreign, foreignKeys);
            Map<Slot, Expression> replacedSlots = getReplaceSlotMap(outputToForeign, newChildPair.second);
            List<NamedExpression> newProjects = project.getProjects().stream()
                    .map(e -> replacedSlots.containsKey(e)
                            ? new Alias(e.getExprId(), replacedSlots.get(e), e.toSql())
                            : (NamedExpression) e.rewriteUp(s -> replacedSlots.getOrDefault(s, s)))
                    .collect(ImmutableList.toImmutableList());
            return project.withProjects(newProjects).withChildren(newChildPair.first);
        }
        return project;
    }

    /**
     * get replace slots, include replace the primary slots and replace the nullable foreign slots.
     * @param outputToForeign primary slot to foreign slot map
     * @param compensationForeignSlots foreign slots which are nullable but add a filter 'slot is not null'
     * @return the replaced map, include primary slot to foreign slot, and foreign nullable slot to non-nullable(slot)
     */
    @VisibleForTesting
    public Map<Slot, Expression> getReplaceSlotMap(Map<Slot, Slot> outputToForeign,
            Set<Slot> compensationForeignSlots) {
        Map<Slot, Expression> replacedSlots = Maps.newHashMap();
        for (Map.Entry<Slot, Slot> entry : outputToForeign.entrySet()) {
            Slot forgeinSlot = entry.getValue();
            Expression replacedExpr = compensationForeignSlots.contains(forgeinSlot)
                    ? new NonNullable(forgeinSlot) : forgeinSlot;
            replacedSlots.put(entry.getKey(), replacedExpr);
        }
        for (Slot forgeinSlot : compensationForeignSlots) {
            replacedSlots.put(forgeinSlot, new NonNullable(forgeinSlot));
        }
        return replacedSlots;
    }

    private @Nullable Map<Slot, Slot> tryMapOutputToForeignPlan(Plan foreignPlan,
            Set<Slot> output, ImmutableEqualSet<Slot> equalSet) {
        Set<Slot> residualPrimary = Sets.difference(output, foreignPlan.getOutputSet());
        ImmutableMap.Builder<Slot, Slot> builder = new ImmutableMap.Builder<>();
        for (Slot primarySlot : residualPrimary) {
            Optional<Slot> replacedForeign = equalSet.calEqualSet(primarySlot).stream()
                    .filter(foreignPlan.getOutputSet()::contains)
                    .findFirst();
            if (!replacedForeign.isPresent()) {
                return null;
            }
            builder.put(primarySlot, replacedForeign.get());
        }
        return builder.build();
    }

    /**
     * add a filter for foreign slots which is nullable, the filter is 'slot is not null'
     */
    @VisibleForTesting
    public Pair<Plan, Set<Slot>> applyNullCompensationFilter(Plan child, Set<Slot> childSlots) {
        ImmutableSet.Builder<Expression> predicatesBuilder = ImmutableSet.builder();
        Set<Slot> filterNotNullSlots = Sets.newHashSet();
        for (Slot slot : childSlots) {
            if (slot.nullable()) {
                filterNotNullSlots.add(slot);
                predicatesBuilder.add(new Not(new IsNull(slot)));
            }
        }
        Plan newChild = filterNotNullSlots.isEmpty() ? child : new LogicalFilter<>(predicatesBuilder.build(), child);
        return Pair.of(newChild, filterNotNullSlots);
    }
}
