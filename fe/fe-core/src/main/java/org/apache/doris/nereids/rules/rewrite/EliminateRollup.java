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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/** EliminateRollup */
public class EliminateRollup implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
        logicalFilter(logicalAggregate(logicalProject(logicalRepeat()))).thenApply(ctx -> {
            LogicalFilter filter = ctx.root;
            LogicalAggregate agg = (LogicalAggregate) filter.child();
            Set<Expression> groupNotNull = ExpressionUtils.inferNotNull(
                    filter.getConjuncts(), agg.getOutputSet(), ctx.cascadesContext);
            LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) agg.child().child(0);
            if (repeat.getGroupingSets().size() > 2) {
                return null;
            }
            Expression groupingExpr = repeat.getGroupingSets().get(0).get(0);
            if (!ExpressionUtils.getInputSlotSet(groupNotNull).contains(groupingExpr)) {
                return null;
            }

            assert (repeat.getGroupingSets().size() == 2);
            ImmutableList.Builder<NamedExpression> newOutput = ImmutableList.builder();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                NamedExpression expr = (NamedExpression) agg.getOutputExpressions().get(i);
                if (expr instanceof VirtualSlotReference
                        && ((VirtualSlotReference) expr).getName().equals("GROUPING_ID")) {
                    continue;
                } else {
                    newOutput.add(expr);
                }
            }

            ImmutableList.Builder<Expression> newGroupByExprs = ImmutableList.builder();
            for (int i = 0; i < agg.getGroupByExpressions().size(); i++) {
                Expression expr = (Expression) agg.getGroupByExpressions().get(i);
                if (expr instanceof VirtualSlotReference
                        && ((VirtualSlotReference) expr).getName().equals("GROUPING_ID")) {
                    continue;
                } else {
                    newGroupByExprs.add(expr);
                }
            }
            // eliminate repeat
            return new LogicalFilter(filter.getConjuncts(),
                    new LogicalAggregate<>((List<Expression>) newGroupByExprs.build(),
                    (List<NamedExpression>) newOutput.build(), (Plan) agg.child().child(0).child(0)));
        }).toRule(RuleType.ELIMINATE_AGGREGATE),
        logicalAggregate(logicalProject(logicalRepeat())).thenApply(ctx -> {
            LogicalAggregate agg = ctx.root;
            LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) agg.child().child(0);
            if (repeat.getGroupingSets().size() > 2) {
                return null;
            }
            Expression groupingExpr = repeat.getGroupingSets().get(0).get(0);
            if (!(groupingExpr.isSlot() && !((SlotReference) groupingExpr).nullable())) {
                return null;
            }

            assert (repeat.getGroupingSets().size() == 2);
            ImmutableList.Builder<NamedExpression> newOutput = ImmutableList.builder();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                NamedExpression expr = (NamedExpression) agg.getOutputExpressions().get(i);
                if (expr instanceof VirtualSlotReference
                        && ((VirtualSlotReference) expr).getName().equals("GROUPING_ID")) {
                    continue;
                } else {
                    newOutput.add(expr);
                }
            }

            ImmutableList.Builder<Expression> newGroupByExprs = ImmutableList.builder();
            for (int i = 0; i < agg.getGroupByExpressions().size(); i++) {
                Expression expr = (Expression) agg.getGroupByExpressions().get(i);
                if (expr instanceof VirtualSlotReference
                        && ((VirtualSlotReference) expr).getName().equals("GROUPING_ID")) {
                    continue;
                } else {
                    newGroupByExprs.add(expr);
                }
            }
            // eliminate repeat
            return new LogicalAggregate<>((List<Expression>) newGroupByExprs.build(),
                    (List<NamedExpression>) newOutput.build(), (Plan) agg.child().child(0).child(0));
        }).toRule(RuleType.ELIMINATE_AGGREGATE)
        );
    }

    private boolean isSame(List<Expression> list1, List<Expression> list2) {
        return list1.size() == list2.size() && list2.containsAll(list1);
    }

    private boolean onlyHasSlots(List<? extends Expression> exprs) {
        return exprs.stream().allMatch(SlotReference.class::isInstance);
    }
}
