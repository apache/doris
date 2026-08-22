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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sleep;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * For an INNER / CROSS join whose one side is a single-row of constant expressions
 * (typically produced by inlining a constant CTE), rewrite the join into a plain
 * {@code Project + Filter} on the other side.
 */
public class EliminateJoinByConstantOneRowRelation implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalJoin(any(), logicalOneRowRelation())
                        .when(join -> isEnabled())
                        .whenNot(LogicalJoin::isMarkJoin)
                        .when(join -> supportedJoinType(join.getJoinType()))
                        .then(join -> tryRewrite(join, /* constantOnRight= */ true))
                        .toRule(RuleType.ELIMINATE_JOIN_BY_CONSTANT_ONE_ROW_RELATION),
                logicalJoin(logicalOneRowRelation(), any())
                        .when(join -> isEnabled())
                        .whenNot(LogicalJoin::isMarkJoin)
                        .when(join -> supportedJoinType(join.getJoinType()))
                        .then(join -> tryRewrite(join, /* constantOnRight= */ false))
                        .toRule(RuleType.ELIMINATE_JOIN_BY_CONSTANT_ONE_ROW_RELATION));
    }

    private static boolean isEnabled() {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && ctx.getSessionVariable().enableEliminateJoinByConstantOneRowRelation;
    }

    private static boolean supportedJoinType(JoinType joinType) {
        return joinType.isInnerJoin() || joinType.isCrossJoin();
    }

    private static Plan tryRewrite(LogicalJoin<? extends Plan, ? extends Plan> join, boolean constantOnRight) {
        LogicalOneRowRelation constantSide = constantOnRight
                ? (LogicalOneRowRelation) join.right()
                : (LogicalOneRowRelation) join.left();
        Plan otherSide = constantOnRight ? join.left() : join.right();

        Map<Slot, Expression> slotToConstant = buildSlotToConstantMap(constantSide);
        if (slotToConstant == null) {
            return null;
        }

        Set<Expression> filterConjuncts = new LinkedHashSet<>();
        collectRewrittenConjuncts(join.getHashJoinConjuncts(), slotToConstant, filterConjuncts);
        collectRewrittenConjuncts(join.getOtherJoinConjuncts(), slotToConstant, filterConjuncts);
        collectRewrittenConjuncts(join.getMarkJoinConjuncts(), slotToConstant, filterConjuncts);

        List<NamedExpression> newProjects = new ArrayList<>(otherSide.getOutput().size()
                + constantSide.getProjects().size());
        newProjects.addAll(otherSide.getOutput());
        newProjects.addAll(constantSide.getProjects());

        Plan rewritten = new LogicalProject<>(newProjects, otherSide);
        if (!filterConjuncts.isEmpty()) {
            rewritten = new LogicalFilter<>(filterConjuncts, rewritten);
        }
        return rewritten;
    }

    private static Map<Slot, Expression> buildSlotToConstantMap(LogicalOneRowRelation relation) {
        Map<Slot, Expression> map = new LinkedHashMap<>();
        for (NamedExpression project : relation.getProjects()) {
            Expression payload = project instanceof Alias ? project.child(0) : project;
            if (!isSafeConstantExpr(payload)) {
                return null;
            }
            map.put(project.toSlot(), payload);
        }
        return map;
    }

    private static boolean isSafeConstantExpr(Expression e) {
        return !e.containsVolatileExpression()
                && !e.anyMatch(Sleep.class::isInstance)
                && !e.anyMatch(SubqueryExpr.class::isInstance);
    }

    private static void collectRewrittenConjuncts(
            List<Expression> conjuncts,
            Map<Slot, Expression> slotToConstant,
            Set<Expression> out) {
        for (Expression conjunct : conjuncts) {
            out.add(ExpressionUtils.replace(conjunct, slotToConstant));
        }
    }
}
