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

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for expression rewriting, delegating to child expressions and rewrite current root
 * when any one of its children changed.
 */
public abstract class DefaultExpressionRewriter<C> extends ExpressionVisitor<Expression, C> {

    @Override
    public Expression visit(Expression expr, C context) {
        return rewrite(this, expr, context);
    }

    /** rewrite */
    public static final <C> Expression rewrite(ExpressionVisitor<Expression, C> rewriter, Expression expr, C context) {
        List<Expression> newChildren = new ArrayList<>(expr.arity());
        boolean hasNewChildren = false;
        for (Expression child : expr.children()) {
            Expression newChild = child.accept(rewriter, context);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? expr.withChildren(newChildren) : expr;
    }
}
