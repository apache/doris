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

import org.apache.doris.nereids.analyzer.PlaceholderExpression;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.BoundStar;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.MatchPhraseEdge;
import org.apache.doris.nereids.trees.expressions.MatchPhrasePrefix;
import org.apache.doris.nereids.trees.expressions.MatchRegexp;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.UnaryArithmetic;
import org.apache.doris.nereids.trees.expressions.UnaryOperator;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.VariableDesc;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IPv4Literal;
import org.apache.doris.nereids.trees.expressions.literal.IPv6Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

/**
 * Use the visitor to visit expression and forward to unified method(visitExpression).
 */
public abstract class ExpressionVisitor<R, C>
        implements ScalarFunctionVisitor<R, C>, AggregateFunctionVisitor<R, C>,
        TableValuedFunctionVisitor<R, C>, TableGeneratingFunctionVisitor<R, C>,
        WindowFunctionVisitor<R, C> {

    public abstract R visit(Expression expr, C context);

    @Override
    public R visitAggregateFunction(AggregateFunction aggregateFunction, C context) {
        return visitBoundFunction(aggregateFunction, context);
    }

    public R visitLambda(Lambda lambda, C context) {
        return visit(lambda, context);
    }

    @Override
    public R visitScalarFunction(ScalarFunction scalarFunction, C context) {
        return visitBoundFunction(scalarFunction, context);
    }

    @Override
    public R visitTableValuedFunction(TableValuedFunction tableValuedFunction, C context) {
        return visitBoundFunction(tableValuedFunction, context);
    }

    @Override
    public R visitTableGeneratingFunction(TableGeneratingFunction tableGeneratingFunction, C context) {
        return visitBoundFunction(tableGeneratingFunction, context);
    }

    @Override
    public R visitWindowFunction(WindowFunction windowFunction, C context) {
        return visitBoundFunction(windowFunction, context);
    }

    public R visitBoundFunction(BoundFunction boundFunction, C context) {
        return visit(boundFunction, context);
    }

    public R visitAggregateExpression(AggregateExpression aggregateExpression, C context) {
        return visit(aggregateExpression, context);
    }

    public R visitAlias(Alias alias, C context) {
        return visitNamedExpression(alias, context);
    }

    public R visitBinaryOperator(BinaryOperator binaryOperator, C context) {
        return visit(binaryOperator, context);
    }

    public R visitUnaryOperator(UnaryOperator unaryOperator, C context) {
        return visit(unaryOperator, context);
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

    public R visitVariable(Variable variable, C context) {
        return visit(variable, context);
    }

    public R visitNamedExpression(NamedExpression namedExpression, C context) {
        return visit(namedExpression, context);
    }

    public R visitSlotReference(SlotReference slotReference, C context) {
        return visitSlot(slotReference, context);
    }

    public R visitDefaultValue(DefaultValueSlot defaultValueSlot, C context) {
        return visitSlot(defaultValueSlot, context);
    }

    public R visitArrayItemSlot(ArrayItemReference.ArrayItemSlot arrayItemSlot, C context) {
        return visitSlotReference(arrayItemSlot, context);
    }

    public R visitMarkJoinReference(MarkJoinSlotReference markJoinSlotReference, C context) {
        return visitSlotReference(markJoinSlotReference, context);
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

    public R visitDecimalV3Literal(DecimalV3Literal decimalV3Literal, C context) {
        return visitLiteral(decimalV3Literal, context);
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

    public R visitDateV2Literal(DateV2Literal dateV2Literal, C context) {
        return visitLiteral(dateV2Literal, context);
    }

    public R visitDateTimeLiteral(DateTimeLiteral dateTimeLiteral, C context) {
        return visitLiteral(dateTimeLiteral, context);
    }

    public R visitDateTimeV2Literal(DateTimeV2Literal dateTimeV2Literal, C context) {
        return visitLiteral(dateTimeV2Literal, context);
    }

    public R visitIPv4Literal(IPv4Literal ipv4Literal, C context) {
        return visitLiteral(ipv4Literal, context);
    }

    public R visitIPv6Literal(IPv6Literal ipv6Literal, C context) {
        return visitLiteral(ipv6Literal, context);
    }

    public R visitArrayLiteral(ArrayLiteral arrayLiteral, C context) {
        return visitLiteral(arrayLiteral, context);
    }

    public R visitMapLiteral(MapLiteral mapLiteral, C context) {
        return visitLiteral(mapLiteral, context);
    }

    public R visitStructLiteral(StructLiteral structLiteral, C context) {
        return visitLiteral(structLiteral, context);
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

    public R visitCast(Cast cast, C context) {
        return visit(cast, context);
    }

    public R visitUnaryArithmetic(UnaryArithmetic unaryArithmetic, C context) {
        return visitUnaryOperator(unaryArithmetic, context);
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

    public R visitBitXor(BitXor bitXor, C context) {
        return visitBinaryArithmetic(bitXor, context);
    }

    public R visitBitOr(BitOr bitOr, C context) {
        return visitBinaryArithmetic(bitOr, context);
    }

    public R visitBitAnd(BitAnd bitAnd, C context) {
        return visitBinaryArithmetic(bitAnd, context);
    }

    public R visitBitNot(BitNot bitNot, C context) {
        return visitUnaryArithmetic(bitNot, context);
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

    public R visitGroupingScalarFunction(GroupingScalarFunction groupingScalarFunction, C context) {
        return visit(groupingScalarFunction, context);
    }

    public R visitVirtualReference(VirtualSlotReference virtualSlotReference, C context) {
        return visitSlotReference(virtualSlotReference, context);
    }

    public R visitArrayItemReference(ArrayItemReference arrayItemReference, C context) {
        return visit(arrayItemReference, context);
    }

    public R visitVariableDesc(VariableDesc variableDesc, C context) {
        return visit(variableDesc, context);
    }

    public R visitProperties(Properties properties, C context) {
        return visit(properties, context);
    }

    public R visitInterval(Interval interval, C context) {
        return visit(interval, context);
    }

    public R visitBoundStar(BoundStar boundStar, C context) {
        return visit(boundStar, context);
    }

    public R visitIntegralDivide(IntegralDivide integralDivide, C context) {
        return visitBinaryArithmetic(integralDivide, context);
    }

    public R visitOrderExpression(OrderExpression orderExpression, C context) {
        return visit(orderExpression, context);
    }

    public R visitWindow(WindowExpression windowExpression, C context) {
        return visit(windowExpression, context);
    }

    public R visitWindowFrame(WindowFrame windowFrame, C context) {
        return visit(windowFrame, context);
    }

    public R visitMatch(Match match, C context) {
        return visit(match, context);
    }

    public R visitMatchAny(MatchAny matchAny, C context) {
        return visitMatch(matchAny, context);
    }

    public R visitMatchAll(MatchAll matchAll, C context) {
        return visitMatch(matchAll, context);
    }

    public R visitMatchPhrase(MatchPhrase matchPhrase, C context) {
        return visitMatch(matchPhrase, context);
    }

    public R visitMatchPhrasePrefix(MatchPhrasePrefix matchPhrasePrefix, C context) {
        return visitMatch(matchPhrasePrefix, context);
    }

    public R visitMatchRegexp(MatchRegexp matchRegexp, C context) {
        return visitMatch(matchRegexp, context);
    }

    public R visitMatchPhraseEdge(MatchPhraseEdge matchPhraseEdge, C context) {
        return visitMatch(matchPhraseEdge, context);
    }

    public R visitPlaceholder(Placeholder placeholder, C context) {
        return visit(placeholder, context);
    }

    public R visitAny(Any any, C context) {
        return visit(any, context);
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

    public R visitUnboundVariable(UnboundVariable unboundVariable, C context) {
        return visit(unboundVariable, context);
    }

    /* ********************************************************************************************
     * Placeholder expressions
     * ********************************************************************************************/

    public R visitPlaceholderExpression(PlaceholderExpression placeholderExpression, C context) {
        return visit(placeholderExpression, context);
    }
}
