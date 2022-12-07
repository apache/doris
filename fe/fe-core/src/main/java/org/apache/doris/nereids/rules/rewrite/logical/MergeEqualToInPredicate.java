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
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Exp;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * a=1 or a=2 or a=3 or ... => a in ( 1,2,3, ...)
 */
public class MergeEqualToInPredicate extends OneRewriteRuleFactory {
    private static final int MERGE_LIMIT = 10;

    public Expression merge(Expression expression) {
        List<Expression> conjuncts = ExpressionUtils.extractDisjunction(expression);
        if (conjuncts.size() < MERGE_LIMIT) {
            return expression;
        }
        Map<Slot, List<Expression>> equalMap = new HashMap<>();
        Expression result = null;
        for (Expression expr: conjuncts) {
            if (expr instanceof EqualTo) {
                EqualTo equalTo  = (EqualTo) expr;
                if (equalTo.right() instanceof Literal && equalTo.left() instanceof Slot) {
                    equalMap.computeIfAbsent((Slot) equalTo.left(), k -> Lists.newArrayList());
                    equalMap.get((Slot) equalTo.left()).add(equalTo.right());
                } else {
                    result = addDisconjunct(result, expr);
                }
            } else {
                result = addDisconjunct(result, expr);
            }
        }

        for(Entry<Slot, List<Expression>> entry: equalMap.entrySet()) {
            if (entry.getValue().size() >= MERGE_LIMIT) {
                InPredicate in = new InPredicate(entry.getKey(), entry.getValue());
                result = addDisconjunct(result, in);
            } else {
                for (Expression right: entry.getValue()) {
                    result = addDisconjunct(result,
                            new EqualTo(entry.getKey(), right));
                }
            }
        }
        return result;
    }

    private Expression addDisconjunct(Expression result, Expression conjunct) {
        if (result == null) {
            return conjunct;
        } else {
            return new Or(result, conjunct);
        }
    }

    private Expression addConjunct(Expression result, Expression conjunct) {
        if (result == null) {
            return conjunct;
        } else {
            return new And(result, conjunct);
        }
    }


    @Override
    public Rule build() {
        return logicalFilter(any()).then(filter -> {
            Expression result = null;
            List<Expression> conjuncts = filter.getConjuncts();
            for (Expression expr: conjuncts) {
                result = addConjunct(result, merge(expr));
            }
            return new LogicalFilter(result, filter.child());
        }).toRule(RuleType.MERGEE_EQUALS_TO_IN_PREDICATE);
    }
}
