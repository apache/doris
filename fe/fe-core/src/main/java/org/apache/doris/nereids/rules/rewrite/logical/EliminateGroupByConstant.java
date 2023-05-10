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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * eliminate group by constant, like:
 * select 1, 'str', count(*) from t group by t.id, 1, 'str', 3, 2;
 * transform to:
 * select 1, 'str', count(*) from t group by t.id.
 * we are ensured before:
 * 1. aggregation node output contains all the expressions of group by expressions.
 * 2. others are aggregation functions.
 * so we can do the rule by:
 * 1. eliminate all the literal expression that are not integerLiteral of group by expressions.
 * 2. check whether the left literal expressions are in range and replace them to slots.
 */
public class EliminateGroupByConstant extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().thenApply(ctx -> {
            LogicalAggregate<Plan> aggregate = ctx.root;
            ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
            List<Expression> groupByExprs = aggregate.getGroupByExpressions();
            List<NamedExpression> outputExprs = aggregate.getOutputExpressions();
            Set<Expression> slotGroupByExprs = Sets.newLinkedHashSet();
            Expression lit = null;
            for (Expression expression : groupByExprs) {
                expression = FoldConstantRule.INSTANCE.rewrite(expression, context);
                if (!(expression instanceof Literal)) {
                    slotGroupByExprs.add(expression);
                } else {
                    lit = expression;
                }
            }
            if (slotGroupByExprs.isEmpty() && lit != null && aggregate.getAggregateFunctions().isEmpty()) {
                slotGroupByExprs.add(lit);
            }
            return aggregate.withGroupByAndOutput(ImmutableList.copyOf(slotGroupByExprs), outputExprs);
        }).toRule(RuleType.ELIMINATE_GROUP_BY_CONSTANT);
    }
}
