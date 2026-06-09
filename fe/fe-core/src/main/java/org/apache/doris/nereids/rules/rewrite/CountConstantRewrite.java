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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils.CollectNonWindowedAggFuncsWithSessionVar;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Rewrite count(constant_expr) to if(constant_expr is null, 0, count(*)).
 *
 * <p>The expression is independent from child rows, so the scan side only needs to provide row count.
 * NormalizeAggregate will move the scalar if expression into the upper project and keep only count(*)
 * inside the aggregate.
 */
public class CountConstantRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(agg -> agg.getSourceRepeat().isPresent())
                .whenNot(agg -> agg.isNormalized())
                .then(agg -> {
                    Map<Expression, Expression> replaced = new HashMap<>();
                    Set<AggregateFunction> aggFunctions = CollectNonWindowedAggFuncsWithSessionVar
                            .collect(agg.getOutputExpressions()).keySet();
                    for (AggregateFunction aggFunction : aggFunctions) {
                        if (isCountConstantExpression(aggFunction)) {
                            Expression countArgument = aggFunction.child(0);
                            replaced.put(aggFunction, new If(new IsNull(countArgument),
                                    new BigIntLiteral(0), new Count()));
                        }
                    }
                    if (replaced.isEmpty()) {
                        return null;
                    }

                    return agg.withAggOutput(ExpressionUtils.replaceNamedExpressions(
                            agg.getOutputExpressions(), replaced));
                }).toRule(RuleType.COUNT_CONSTANT_REWRITE);
    }

    private boolean isCountConstantExpression(AggregateFunction aggFunction) {
        if (!(aggFunction instanceof Count) || aggFunction.isDistinct() || aggFunction.arity() != 1) {
            return false;
        }
        Expression arg = aggFunction.child(0);
        return !arg.isLiteral()
                && arg.foldable()
                && !arg.containsNondeterministic()
                && !arg.containsVolatileExpression()
                && arg.getInputSlots().isEmpty()
                && !arg.containsType(AggregateFunction.class, SubqueryExpr.class,
                        TableGeneratingFunction.class, WindowExpression.class);
    }
}
