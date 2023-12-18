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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * count(1) ==> count(*)
 */
public class CountLiteralToCountStar extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(
                agg -> {
                    List<NamedExpression> newExprs = Lists.newArrayListWithCapacity(agg.getOutputExpressions().size());
                    if (rewriteCountLiteral(agg.getOutputExpressions(), newExprs)) {
                        return agg.withAggOutput(newExprs);
                    }
                    return agg;
                }
        ).toRule(RuleType.COUNT_LITERAL_TO_COUNT_STAR);
    }

    private boolean rewriteCountLiteral(List<NamedExpression> oldExprs, List<NamedExpression> newExprs) {
        boolean changed = false;
        for (Expression expr : oldExprs) {
            Map<Expression, Expression> replaced = new HashMap<>();
            Set<AggregateFunction> oldAggFuncSet = expr.collect(AggregateFunction.class::isInstance);
            oldAggFuncSet.stream()
                    .filter(this::isCountLiteral)
                    .forEach(c -> replaced.put(c, new Count()));
            expr = expr.rewriteUp(s -> replaced.getOrDefault(s, s));
            changed |= !replaced.isEmpty();
            newExprs.add((NamedExpression) expr);
        }
        return changed;
    }

    private boolean isCountLiteral(AggregateFunction aggFunc) {
        return !aggFunc.isDistinct()
                && aggFunc instanceof Count
                && aggFunc.children().stream().allMatch(e -> e.isLiteral() && !e.isNullLiteral());
    }
}
