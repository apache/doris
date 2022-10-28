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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StringRegexPredicate;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

/**
 * Use the visitor to visit expression and forward to unified method(visitExpression).
 */
public abstract class ExpressionVisitor<R, C>
        implements ScalarFunctionVisitor<R, C>, AggregateFunctionVisitor<R, C> {

    public abstract R visit(Expression expr, C context);

    @Override
    public R visitAggregateFunction(AggregateFunction aggregateFunction, C context) {
        return visitBoundFunction(aggregateFunction, context);
    }

    @Override
    public R visitScalarFunction(ScalarFunction scalarFunction, C context) {
        return visitBoundFunction(scalarFunction, context);
    }

    public R visitBoundFunction(BoundFunction boundFunction, C context) {
        return visit(boundFunction, context);
    }

    public R visitAlias(Alias alias, C context) {
        return visitNamedExpression(alias, context);
    }

    public R visitBinaryOperator(BinaryOperator binaryOperator, C context) {
        return visit(binaryOperator, context);
    }

    public R visitComparisonPredicate(ComparisonPredicate cp, C context) {
        return visitBinaryOperator(cp, context);
    }

    public R visitEqualTo(EqualTo equalTo, C context) {
        return visitComparisonPredicate(equalTo, context);
    }

    public R visitGreaterThan(GreaterThan greaterThan, C context) {
        return visitComparisonPredicate(greaterThan, context);
    }

    public R visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, C context) {
        return visitComparisonPredicate(greaterThanEqual, context);
    }

    public R visitLessThan(LessThan lessThan, C context) {
        return visitComparisonPredicate(lessThan, context);
    }

    public R visitLessThanEqual(LessThanEqual lessThanEqual, C context) {
        return visitComparisonPredicate(lessThanEqual, context);
    }

    public R visitNullSafeEqual(NullSafeEqual nullSafeEqual, C context) {
        return visitComparisonPredicate(nullSafeEqual, context);
    }

    public R visitNot(Not not, C context) {
        return visit(not, context);
    }

    public R visitSlot(Slot slot, C context) {
        return visitNamedExpression(slot, context);
    }

    public R visitNamedExpression(NamedExpression namedExpression, C context) {
        return visit(namedExpression, context);
    }

    public R visitSlotReference(SlotReference slotReference, C context) {
        return visitSlot(slotReference, context);
    }

    public R visitLiteral(Literal literal, C context) {
        return visit(literal, context);
    }

    public R visitNullLiteral(NullLiteral nullLiteral, C context) {
        return visitLiteral(nullLiteral, context);
    }

    public R visitBooleanLiteral(BooleanLiteral booleanLiteral, C context) {
        return visitLiteral(booleanLiteral, context);
    }

    public R visitCharLiteral(CharLiteral charLiteral, C context) {
        return visitLiteral(charLiteral, context);
    }

    public R visitVarcharLiteral(VarcharLiteral varcharLiteral, C context) {
        return visitLiteral(varcharLiteral, context);
    }

    public R visitStringLiteral(StringLiteral stringLiteral, C context) {
        return visitLiteral(stringLiteral, context);
    }

    public R visitTinyIntLiteral(TinyIntLiteral tinyIntLiteral, C context) {
        return visitLiteral(tinyIntLiteral, context);
    }

    public R visitSmallIntLiteral(SmallIntLiteral smallIntLiteral, C context) {
        return visitLiteral(smallIntLiteral, context);
    }

    public R visitIntegerLiteral(IntegerLiteral integerLiteral, C context) {
        return visitLiteral(integerLiteral, context);
    }

    public R visitBigIntLiteral(BigIntLiteral bigIntLiteral, C context) {
        return visitLiteral(bigIntLiteral, context);
    }

    public R visitLargeIntLiteral(LargeIntLiteral largeIntLiteral, C context) {
        return visitLiteral(largeIntLiteral, context);
    }

    public R visitDecimalLiteral(DecimalLiteral decimalLiteral, C context) {
        return visitLiteral(decimalLiteral, context);
    }

    public R visitFloatLiteral(FloatLiteral floatLiteral, C context) {
        return visitLiteral(floatLiteral, context);
    }

    public R visitDoubleLiteral(DoubleLiteral doubleLiteral, C context) {
        return visitLiteral(doubleLiteral, context);
    }

    public R visitDateLiteral(DateLiteral dateLiteral, C context) {
        return visitLiteral(dateLiteral, context);
    }

    public R visitDateTimeLiteral(DateTimeLiteral dateTimeLiteral, C context) {
        return visitLiteral(dateTimeLiteral, context);
    }

    public R visitBetween(Between between, C context) {
        return visit(between, context);
    }

    public R visitCompoundPredicate(CompoundPredicate compoundPredicate, C context) {
        return visitBinaryOperator(compoundPredicate, context);
    }

    public R visitAnd(And and, C context) {
        return visitCompoundPredicate(and, context);
    }

    public R visitOr(Or or, C context) {
        return visitCompoundPredicate(or, context);
    }

    public R visitStringRegexPredicate(StringRegexPredicate stringRegexPredicate, C context) {
        return visit(stringRegexPredicate, context);
    }

    public R visitLike(Like like, C context) {
        return visitStringRegexPredicate(like, context);
    }

    public R visitRegexp(Regexp regexp, C context) {
        return visitStringRegexPredicate(regexp, context);
    }

    public R visitCast(Cast cast, C context) {
        return visit(cast, context);
    }

    public R visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, C context) {
        return visitBinaryOperator(binaryArithmetic, context);
    }

    public R visitAdd(Add add, C context) {
        return visitBinaryArithmetic(add, context);
    }

    public R visitSubtract(Subtract subtract, C context) {
        return visitBinaryArithmetic(subtract, context);
    }

    public R visitMultiply(Multiply multiply, C context) {
        return visitBinaryArithmetic(multiply, context);
    }

    public R visitDivide(Divide divide, C context) {
        return visitBinaryArithmetic(divide, context);
    }

    public R visitMod(Mod mod, C context) {
        return visitBinaryArithmetic(mod, context);
    }

    public R visitWhenClause(WhenClause whenClause, C context) {
        return visit(whenClause, context);
    }

    public R visitCaseWhen(CaseWhen caseWhen, C context) {
        return visit(caseWhen, context);
    }

    public R visitInPredicate(InPredicate inPredicate, C context) {
        return visit(inPredicate, context);
    }

    public R visitIsNull(IsNull isNull, C context) {
        return visit(isNull, context);
    }

    public R visitInSubquery(InSubquery in, C context) {
        return visitSubqueryExpr(in, context);
    }

    public R visitExistsSubquery(Exists exists, C context) {
        return visitSubqueryExpr(exists, context);
    }

    public R visitSubqueryExpr(SubqueryExpr subqueryExpr, C context) {
        return visit(subqueryExpr, context);
    }

    public R visitTimestampArithmetic(TimestampArithmetic arithmetic, C context) {
        return visit(arithmetic, context);
    }

    public R visitScalarSubquery(ScalarSubquery scalar, C context) {
        return visitSubqueryExpr(scalar, context);
    }

    public R visitListQuery(ListQuery listQuery, C context) {
        return visitSubqueryExpr(listQuery, context);
    }

    public R visitAssertNumRowsElement(AssertNumRowsElement assertNumRowsElement, C context) {
        return visit(assertNumRowsElement, context);
    }

    /* ********************************************************************************************
     * Unbound expressions
     * ********************************************************************************************/

    public R visitUnboundFunction(UnboundFunction unboundFunction, C context) {
        return visit(unboundFunction, context);
    }

    public R visitUnboundAlias(UnboundAlias unboundAlias, C context) {
        return visitNamedExpression(unboundAlias, context);
    }

    public R visitUnboundSlot(UnboundSlot unboundSlot, C context) {
        return visitSlot(unboundSlot, context);
    }

    public R visitUnboundStar(UnboundStar unboundStar, C context) {
        return visitNamedExpression(unboundStar, context);
    }
}
