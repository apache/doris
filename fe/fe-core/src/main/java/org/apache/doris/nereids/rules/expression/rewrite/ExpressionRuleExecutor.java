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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Expression rewrite entry, which contains all rewrite rules.
 */
public class ExpressionRuleExecutor {

    private final ExpressionRewriteContext ctx;
    private final List<ExpressionRewriteRule> rules;

    public ExpressionRuleExecutor(List<ExpressionRewriteRule> rules, ConnectContext context) {
        this.rules = rules;
        this.ctx = new ExpressionRewriteContext(context);
    }

    public ExpressionRuleExecutor(List<ExpressionRewriteRule> rules) {
        this(rules, null);
    }

    public List<Expression> rewrite(List<Expression> exprs) {
        return exprs.stream().map(this::rewrite).collect(Collectors.toList());
    }

    /**
     * Given an expression, returns a rewritten expression.
     */
    public Expression rewrite(Expression root) {
        Expression result = root;
        for (ExpressionRewriteRule rule : rules) {
            result = applyRule(result, rule);
        }
        return result;
    }

    /**
     * Given an expression, returns a rewritten expression.
     */
    public Optional<Expression> rewrite(Optional<Expression> root) {
        return root.map(this::rewrite);
    }

    private Expression applyRule(Expression expr, ExpressionRewriteRule rule) {
        return rule.rewrite(expr, ctx);
    }

}
