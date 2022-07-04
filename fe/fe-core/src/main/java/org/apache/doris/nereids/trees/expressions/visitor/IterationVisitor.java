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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;

/**
 * Iterative traversal of an expression.
 */
public abstract class IterationVisitor<C> extends DefaultExpressionVisitor<Void, C> {

    @Override
    public Void visit(Expression expr, C context) {
        return expr.accept(this, context);
    }

    @Override
    public Void visitNot(Not expr, C context) {
        visit(expr.child(), context);
        return null;
    }

    @Override
    public Void visitCompoundPredicate(CompoundPredicate expr, C context) {
        visit(expr.left(), context);
        visit(expr.right(), context);
        return null;
    }

    @Override
    public Void visitLiteral(Literal literal, C context) {
        return null;
    }

    @Override
    public Void visitArithmetic(Arithmetic arithmetic, C context) {
        visit(arithmetic.child(0), context);
        if (arithmetic.getArithmeticOperator().isBinary()) {
            visit(arithmetic.child(1), context);
        }
        return null;
    }

    @Override
    public Void visitBetween(Between betweenPredicate, C context) {
        visit(betweenPredicate.getCompareExpr(), context);
        visit(betweenPredicate.getLowerBound(), context);
        visit(betweenPredicate.getUpperBound(), context);
        return null;
    }

    @Override
    public Void visitAlias(Alias alias, C context) {
        return visitNamedExpression(alias, context);
    }

    @Override
    public Void visitComparisonPredicate(ComparisonPredicate cp, C context) {
        visit(cp.left(), context);
        visit(cp.right(), context);
        return null;
    }

    @Override
    public Void visitEqualTo(EqualTo equalTo, C context) {
        return visitComparisonPredicate(equalTo, context);
    }

    @Override
    public Void visitGreaterThan(GreaterThan greaterThan, C context) {
        return visitComparisonPredicate(greaterThan, context);
    }

    @Override
    public Void visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, C context) {
        return visitComparisonPredicate(greaterThanEqual, context);
    }

    @Override
    public Void visitLessThan(LessThan lessThan, C context) {
        return visitComparisonPredicate(lessThan, context);
    }

    @Override
    public Void visitLessThanEqual(LessThanEqual lessThanEqual, C context) {
        return visitComparisonPredicate(lessThanEqual, context);
    }

    @Override
    public Void visitNullSafeEqual(NullSafeEqual nullSafeEqual, C context) {
        return visitComparisonPredicate(nullSafeEqual, context);
    }

    @Override
    public Void visitSlot(Slot slot, C context) {
        return null;
    }

    @Override
    public Void visitNamedExpression(NamedExpression namedExpression, C context) {
        for (Expression child : namedExpression.children()) {
            visit(child, context);
        }
        return null;
    }

    @Override
    public Void visitBoundFunction(BoundFunction boundFunction, C context) {
        for (Expression argument : boundFunction.getArguments()) {
            visit(argument, context);
        }
        return null;
    }

    @Override
    public Void visitAggregateFunction(AggregateFunction aggregateFunction, C context) {
        return visitBoundFunction(aggregateFunction, context);
    }

    @Override
    public Void visitAdd(Add add, C context) {
        return visitArithmetic(add, context);
    }

    @Override
    public Void visitSubtract(Subtract subtract, C context) {
        return visitArithmetic(subtract, context);
    }

    @Override
    public Void visitMultiply(Multiply multiply, C context) {
        return visitArithmetic(multiply, context);
    }

    @Override
    public Void visitDivide(Divide divide, C context) {
        return visitArithmetic(divide, context);
    }

    @Override
    public Void visitMod(Mod mod, C context) {
        return visitArithmetic(mod, context);
    }

}
