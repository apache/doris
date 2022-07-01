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

import org.apache.doris.nereids.rules.expression.rewrite.rules.NormalizeExpressionRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Expression rewrite entry, which contains all rewrite rules.
 */
public class ExpressionRuleExecutor {

    public static final List<ExpressionRewriteRule> REWRITE_RULES = ImmutableList.of(
        new SimplifyNotExprRule(),
        new NormalizeExpressionRule()
    );

    private final ExpressionRewriteContext ctx;
    private final List<ExpressionRewriteRule> rules;

    public ExpressionRuleExecutor() {
        this.rules = REWRITE_RULES;
        this.ctx = new ExpressionRewriteContext();
    }

    public ExpressionRuleExecutor(List<ExpressionRewriteRule> rules) {
        this.rules = rules;
        this.ctx = new ExpressionRewriteContext();
    }

    public ExpressionRuleExecutor(ExpressionRewriteRule rule) {
        this.rules = Lists.newArrayList(rule);
        this.ctx = new ExpressionRewriteContext();
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

    private Expression applyRule(Expression expr, ExpressionRewriteRule rule) {
        return rule.rewrite(expr, ctx);
    }

}
