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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
        Map<Expression, Expression> outputToForeign =
                tryMapOutputToForeignPlan(foreign, output, equalSet);
        if (outputToForeign != null) {
            List<NamedExpression> newProjects = project.getProjects().stream()
                    .map(e -> outputToForeign.containsKey(e)
                            ? new Alias(e.getExprId(), outputToForeign.get(e), e.toSql())
                            : (NamedExpression) e.rewriteUp(s -> outputToForeign.getOrDefault(s, s)))
                    .collect(ImmutableList.toImmutableList());
            return project.withProjects(newProjects)
                    .withChildren(applyNullCompensationFilter(foreign, foreignKeys));
        }
        return project;
    }

    private @Nullable Map<Expression, Expression> tryMapOutputToForeignPlan(Plan foreignPlan,
            Set<Slot> output, ImmutableEqualSet<Slot> equalSet) {
        Set<Slot> residualPrimary = Sets.difference(output, foreignPlan.getOutputSet());
        ImmutableMap.Builder<Expression, Expression> builder = new ImmutableMap.Builder<>();
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

    private Plan applyNullCompensationFilter(Plan child, Set<Slot> childSlots) {
        Set<Expression> predicates = childSlots.stream()
                .filter(ExpressionTrait::nullable)
                .map(s -> new Not(new IsNull(s)))
                .collect(ImmutableSet.toImmutableSet());
        if (predicates.isEmpty()) {
            return child;
        }
        return new LogicalFilter<>(predicates, child);
    }
}
