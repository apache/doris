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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;

import javax.ws.rs.NotSupportedException;

/**
 * Default implementation for expression rewriting, delegating to child expressions and rewrite current root
 * when any one of its children changed.
 */
public abstract class DefaultExpressionRewriter<C> extends ExpressionVisitor<Expression, C> {

    @Override
    public Expression visit(Expression expr, C context) {
        return expr.accept(this, context);
    }

    @Override
    public Expression visitAlias(Alias alias, C context) {
        Expression child = visit(alias.child(), context);
        if (child != alias.child()) {
            return new Alias(child, alias.getName());
        } else {
            return alias;
        }
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, C context) {
        Expression left = visit(cp.left(), context);
        Expression right = visit(cp.right(), context);
        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, C context) {
        return visitComparisonPredicate(equalTo, context);
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, C context) {
        return visitComparisonPredicate(greaterThan, context);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, C context) {
        return visitComparisonPredicate(greaterThanEqual, context);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, C context) {
        return visitComparisonPredicate(lessThan, context);
    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, C context) {
        return visitComparisonPredicate(lessThanEqual, context);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, C context) {
        return visitComparisonPredicate(nullSafeEqual, context);
    }

    @Override
    public Expression visitNamedExpression(NamedExpression namedExpression, C context) {
        throw new NotSupportedException("Not supported for rewrite abstract class NamedExpression.");
    }

    @Override
    public Expression visitNot(Not not, C context) {
        Expression child = visit(not.child(), context);
        if (child != not.child()) {
            return new Not(child);
        } else {
            return not;
        }
    }

    @Override
    public Expression visitSlotReference(SlotReference slotReference, C context) {
        return slotReference;
    }

    @Override
    public Expression visitLiteral(Literal literal, C context) {
        return literal;
    }

    @Override
    public Expression visitUnboundAlias(UnboundAlias unboundAlias, C context) {
        Expression child = visit(unboundAlias.child(), context);
        if (child != unboundAlias.child()) {
            return new UnboundAlias(child);
        } else {
            return unboundAlias;
        }
    }

    @Override
    public Expression visitUnboundSlot(UnboundSlot unboundSlot, C context) {
        return unboundSlot;
    }

    @Override
    public Expression visitUnboundStar(UnboundStar unboundStar, C context) {
        return unboundStar;
    }
}
