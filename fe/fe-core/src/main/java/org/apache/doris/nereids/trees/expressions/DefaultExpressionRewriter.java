// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
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
    public Expression visit(FunctionCall function, C context) {
        throw new NotSupportedException("Not supported for rewrite abstract class FunctionCall.");
    }

    @Override
    public Expression visit(BetweenPredicate betweenPredicate, C context) {
        throw new NotSupportedException("Not supported for rewrite abstract class betweenPredicate.");
    }

    @Override
    public Expression visit(Arithmetic arithmetic, C context) {
        throw new NotSupportedException("Not supported for rewrite abstract class Arithmetic.");
    }

    @Override
    public Expression visit(Alias alias, C context) {
        Expression child = alias.child().accept(this, context);
        if (child != alias.child()) {
            return new Alias(child, alias.getName());
        } else {
            return alias;
        }
    }

    @Override
    public Expression visit(ComparisonPredicate cp, C context) {
        Expression left = cp.left().accept(this, context);
        Expression right = cp.right().accept(this, context);
        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    @Override
    public Expression visit(EqualTo equalTo, C context) {
        return visit((ComparisonPredicate) equalTo, context);
    }

    @Override
    public Expression visit(GreaterThan greaterThan, C context) {
        return visit((ComparisonPredicate) greaterThan, context);
    }

    @Override
    public Expression visit(GreaterThanEqual greaterThanEqual, C context) {
        return visit((ComparisonPredicate) greaterThanEqual, context);
    }

    @Override
    public Expression visit(LessThan lessThan, C context) {
        return visit((ComparisonPredicate) lessThan, context);
    }

    @Override
    public Expression visit(LessThanEqual lessThanEqual, C context) {
        return visit((ComparisonPredicate) lessThanEqual, context);
    }

    @Override
    public Expression visit(NullSafeEqual nullSafeEqual, C context) {
        return visit((ComparisonPredicate) nullSafeEqual, context);
    }

    @Override
    public Expression visit(NamedExpression namedExpression, C context) {
        throw new NotSupportedException("Not supported for rewrite abstract class NamedExpression.");
    }

    @Override
    public Expression visit(Not not, C context) {
        Expression child = not.child().accept(this, context);
        if (child != not.child()) {
            return new Not(child);
        } else {
            return not;
        }
    }

    @Override
    public Expression visit(CompoundPredicate cp, C context) {
        Expression left = cp.left().accept(this, context);
        Expression right = cp.right().accept(this, context);
        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    @Override
    public Expression visit(SlotReference slotReference, C context) {
        return slotReference;
    }

    @Override
    public Expression visit(Literal literal, C context) {
        return literal;
    }

    @Override
    public Expression visit(UnboundAlias unboundAlias, C context) {
        Expression child = unboundAlias.child().accept(this, context);
        if (child != unboundAlias.child()) {
            return new UnboundAlias(child);
        } else {
            return unboundAlias;
        }
    }

    @Override
    public Expression visit(UnboundSlot unboundSlot, C context) {
        return unboundSlot;
    }

    @Override
    public Expression visit(UnboundStar unboundStar, C context) {
        return unboundStar;
    }
}
