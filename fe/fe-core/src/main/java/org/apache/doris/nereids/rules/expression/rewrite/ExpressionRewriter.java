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

import com.google.common.collect.Lists;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NormalizeExpressionRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.NotExpressionRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LeafExpression;

import java.util.List;

public class ExpressionRewriter {

    public static final List<ExpressionRewriteRule> REWRITE_RULES = Lists.newArrayList(
        new NotExpressionRule(),
        new NormalizeExpressionRule()
    );

    private final ExpressionRewriteContext ctx;
    private final List<ExpressionRewriteRule> rules;

    public ExpressionRewriter(List<ExpressionRewriteRule> rules) {
        this.rules = rules;
        this.ctx = new ExpressionRewriteContext();
    }

    public ExpressionRewriter(ExpressionRewriteRule rule) {
        this.rules = Lists.newArrayList(rule);
        this.ctx = new ExpressionRewriteContext();
    }


    public Expression rewrite(Expression root) {
        Expression result = root;
        for (ExpressionRewriteRule rule : rules) {
            result = applyRule(result, rule);
        }
        return result;
    }

    private Expression applyRule(Expression expr, ExpressionRewriteRule rule) {
        Expression rewrittenExpr = expr;
        rewrittenExpr = applyRuleBottomUp(rewrittenExpr, rule);
        return rewrittenExpr;
    }

    private Expression applyRuleBottomUp(Expression expr, ExpressionRewriteRule rule) {
        List<Expression> children = Lists.newArrayList();
        for (int i = 0; i < expr.children().size(); ++i) {
            children.add(applyRuleBottomUp(expr.child(i), rule));
        }
        if (!(expr instanceof LeafExpression)) {
            expr = expr.newChildren(children);
        }
        return rule.rewrite(expr, ctx);
    }
}
