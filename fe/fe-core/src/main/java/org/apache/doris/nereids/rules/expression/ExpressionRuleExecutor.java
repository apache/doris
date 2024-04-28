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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Expression rewrite entry, which contains all rewrite rules.
 */
public class ExpressionRuleExecutor {
    private final List<ExpressionRewriteRule> rules;

    public ExpressionRuleExecutor(List<ExpressionRewriteRule> rules) {
        this.rules = rules;
    }

    public List<Expression> rewrite(List<Expression> exprs, ExpressionRewriteContext ctx) {
        ImmutableList.Builder<Expression> result = ImmutableList.builderWithExpectedSize(exprs.size());
        for (Expression expr : exprs) {
            result.add(rewrite(expr, ctx));
        }
        return result.build();
    }

    /**
     * Given an expression, returns a rewritten expression.
     */
    public Expression rewrite(Expression root, ExpressionRewriteContext ctx) {
        Expression result = root;
        for (ExpressionRewriteRule rule : rules) {
            result = applyRule(result, rule, ctx);
        }
        return result;
    }

    /**
     * Given an expression, returns a rewritten expression.
     */
    public Optional<Expression> rewrite(Optional<Expression> root, ExpressionRewriteContext ctx) {
        return root.map(r -> this.rewrite(r, ctx));
    }

    private Expression applyRule(Expression expr, ExpressionRewriteRule rule, ExpressionRewriteContext ctx) {
        return rule.rewrite(expr, ctx);
    }

    /** normalize */
    public static Expression normalize(Expression expression) {
        return expression.rewriteUp(expr -> {
            if (expr instanceof ComparisonPredicate) {
                return NormalizeBinaryPredicatesRule.normalize((ComparisonPredicate) expression);
            } else {
                return expr;
            }
        });
    }

}
