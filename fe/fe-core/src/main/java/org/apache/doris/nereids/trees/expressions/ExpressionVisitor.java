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

/**
 * Use the visitor pattern to iterate over all expressions for expression rewriting.
 */
public abstract class ExpressionVisitor<R, C> {

    public abstract R visit(Expression expr, C context);


    public R visitAlias(Alias alias, C context) {
        return visit(alias, context);
    }

    public R visitComparisonPredicate(ComparisonPredicate cp, C context) {
        return visit(cp, context);
    }

    public R visitEqualTo(EqualTo equalTo, C context) {
        return visit(equalTo, context);
    }

    public R visitGreaterThan(GreaterThan greaterThan, C context) {
        return visit(greaterThan, context);
    }

    public R visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, C context) {
        return visit(greaterThanEqual, context);
    }

    public R visitLessThan(LessThan lessThan, C context) {
        return visit(lessThan, context);
    }

    public R visitLessThanEqual(LessThanEqual lessThanEqual, C context) {
        return visit(lessThanEqual, context);
    }

    public R visitNamedExpression(NamedExpression namedExpression, C context) {
        return visit(namedExpression, context);
    }

    public R visitNot(Not not, C context) {
        return visit(not, context);
    }

    public R visitNullSafeEqual(NullSafeEqual nullSafeEqual, C context) {
        return visit(nullSafeEqual, context);
    }

    public R visitSlotReference(SlotReference slotReference, C context) {
        return visit(slotReference, context);
    }

    public R visitLiteral(Literal literal, C context) {
        return visit(literal, context);
    }

    public R visitFunctionCall(FunctionCall function, C context) {
        return visit(function, context);
    }

    public R visitBetweenPredicate(BetweenPredicate betweenPredicate, C context) {
        return visit(betweenPredicate, context);
    }

    public R visitCompoundPredicate(CompoundPredicate compoundPredicate, C context) {
        return visit(compoundPredicate, context);
    }

    public R visitArithmetic(Arithmetic arithmetic, C context) {
        return visit(arithmetic, context);
    }


    /* ********************************************************************************************
     * Unbound expressions
     * ********************************************************************************************/

    public R visitUnboundAlias(UnboundAlias unboundAlias, C context) {
        return visit(unboundAlias, context);
    }

    public R visitUnboundSlot(UnboundSlot unboundSlot, C context) {
        return visit(unboundSlot, context);
    }

    public R visitUnboundStar(UnboundStar unboundStar, C context) {
        return visit(unboundStar, context);
    }
}
