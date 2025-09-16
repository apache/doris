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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Default implementation for expression rewriting, delegating to child expressions and rewrite current root
 * when any one of its children changed.
 */
public abstract class DefaultExpressionRewriter<C> extends ExpressionVisitor<Expression, C> {

    @Override
    public Expression visit(Expression expr, C context) {
        return rewriteChildren(this, expr, context);
    }

    @Override
    public Expression visitWhenClause(WhenClause whenClause, C context) {
        // should not rewrite when clause to other expression because CaseWhen require WhenClause as children
        return rewriteChildren(this, whenClause, context);
    }

    /** rewriteChildren */
    public static final <E extends Expression, C> E rewriteChildren(
            ExpressionVisitor<Expression, C> rewriter, E expr, C context) {
        switch (expr.arity()) {
            case 1: {
                Expression originChild = expr.child(0);
                Expression newChild = originChild.accept(rewriter, context);
                return (originChild != newChild) ? (E) expr.withChildren(ImmutableList.of(newChild)) : expr;
            }
            case 2: {
                Expression originLeft = expr.child(0);
                Expression newLeft = originLeft.accept(rewriter, context);
                Expression originRight = expr.child(1);
                Expression newRight = originRight.accept(rewriter, context);
                return (originLeft != newLeft || originRight != newRight)
                        ? (E) expr.withChildren(ImmutableList.of(newLeft, newRight))
                        : expr;
            }
            case 0: {
                return expr;
            }
            default: {
                boolean hasNewChildren = false;
                Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(expr.arity());
                for (Expression child : expr.children()) {
                    Expression newChild = child.accept(rewriter, context);
                    if (newChild != child) {
                        hasNewChildren = true;
                    }
                    newChildren.add(newChild);
                }
                return hasNewChildren ? (E) expr.withChildren(newChildren.build()) : expr;
            }
        }
    }
}
