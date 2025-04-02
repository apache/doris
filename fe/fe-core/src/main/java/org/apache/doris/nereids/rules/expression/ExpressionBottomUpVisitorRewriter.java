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

import org.apache.doris.nereids.pattern.ExpressionPatternRules;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.Optional;

/** ExpressionBottomUpVisitorRewriter */
public class ExpressionBottomUpVisitorRewriter implements ExpressionRewriteRule<ExpressionRewriteContext> {
    private final ExpressionPatternRules rules;
    private String rewriteStateKey = "Rewrite_" + this;

    public ExpressionBottomUpVisitorRewriter(ExpressionPatternRules rules) {
        this.rules = rules;
    }

    public Expression rewrite(Expression expression, ExpressionRewriteContext context) {
        return rewrite(expression, context, null);
    }

    private Expression rewrite(Expression expression, ExpressionRewriteContext context, Expression parent) {
        if (!rules.hasCurrentAndChildrenRules(expression)) {
            return expression;
        }
        if (expression.arity() != 0) {
            Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(expression.arity());
            boolean changed = false;
            for (Expression child : expression.children()) {
                Expression oldChild = child;
                while (true) {
                    Expression newChild = rewrite(oldChild, context, expression);
                    if (!newChild.equals(oldChild)) {
                        changed = true;
                        oldChild = newChild;
                    } else {
                        newChildren.add(newChild);
                        break;
                    }
                }
            }
            if (changed) {
                expression = expression.withChildren(newChildren.build());
            }
        }
        Optional<Expression> result = rules.matchesAndApply(expression, context, parent);
        return result.orElse(expression);
    }

    @Override
    public String getRewriteStateKey() {
        return rewriteStateKey;
    }
}
