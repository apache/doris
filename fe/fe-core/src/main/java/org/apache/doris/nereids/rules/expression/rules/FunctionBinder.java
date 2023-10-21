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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.ArithmeticFunctionBinder;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * function binder
 */
public class FunctionBinder extends AbstractExpressionRewriteRule {

    public static final FunctionBinder INSTANCE = new FunctionBinder();

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext context) {
        expr = super.visit(expr, context);
        expr.checkLegalityBeforeTypeCoercion();
        // this cannot be removed, because some function already construct in parser.
        if (expr instanceof ImplicitCastInputTypes) {
            List<AbstractDataType> expectedInputTypes = ((ImplicitCastInputTypes) expr).expectedInputTypes();
            if (!expectedInputTypes.isEmpty()) {
                return TypeCoercionUtils.implicitCastInputTypes(expr, expectedInputTypes);
            }
        }
        return expr;
    }

    /* ********************************************************************************************
     * bind function
     * ******************************************************************************************** */

    @Override
    public Expression visitUnboundFunction(UnboundFunction unboundFunction, ExpressionRewriteContext context) {
        unboundFunction = unboundFunction.withChildren(unboundFunction.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList()));

        // bind function
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        String functionName = unboundFunction.getName();
        List<Object> arguments = unboundFunction.isDistinct()
                ? ImmutableList.builder()
                .add(unboundFunction.isDistinct())
                .addAll(unboundFunction.getArguments())
                .build()
                : (List) unboundFunction.getArguments();

        // we will change arithmetic function like add(), subtract(), bitnot() to the corresponding objects rather than
        // BoundFunction.
        ArithmeticFunctionBinder functionBinder = new ArithmeticFunctionBinder();
        if (functionBinder.isBinaryArithmetic(unboundFunction.getName())) {
            return functionBinder.bindBinaryArithmetic(unboundFunction.getName(), unboundFunction.children())
                    .accept(this, context);
        }

        FunctionBuilder builder = functionRegistry.findFunctionBuilder(
                unboundFunction.getDbName(), functionName, arguments);
        if (builder instanceof AliasUdfBuilder) {
            // we do type coercion in build function in alias function, so it's ok to return directly.
            return builder.build(functionName, arguments);
        } else {
            return TypeCoercionUtils.processBoundFunction((BoundFunction) builder.build(functionName, arguments));
        }
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        boundFunction = (BoundFunction) super.visitBoundFunction(boundFunction, context);
        return TypeCoercionUtils.processBoundFunction(boundFunction);
    }

    /**
     * gets the method for calculating the time.
     * e.g. YEARS_ADD、YEARS_SUB、DAYS_ADD 、DAYS_SUB
     */
    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        Expression left = arithmetic.left().accept(this, context);
        Expression right = arithmetic.right().accept(this, context);

        arithmetic = (TimestampArithmetic) arithmetic.withChildren(left, right);

        // bind function
        String funcOpName;
        if (arithmetic.getFuncName() == null) {
            // e.g. YEARS_ADD, MONTHS_SUB
            funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                    (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
        } else {
            funcOpName = arithmetic.getFuncName();
        }
        arithmetic = (TimestampArithmetic) arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));

        // type coercion
        return TypeCoercionUtils.processTimestampArithmetic(arithmetic, left, right);
    }

    /* ********************************************************************************************
     * type coercion
     * ******************************************************************************************** */

    @Override
    public Expression visitBitNot(BitNot bitNot, ExpressionRewriteContext context) {
        Expression child = bitNot.child().accept(this, context);
        // type coercion
        if (!(child.getDataType().isIntegralType() || child.getDataType().isBooleanType())) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        return bitNot.withChildren(child);
    }

    @Override
    public Expression visitDivide(Divide divide, ExpressionRewriteContext context) {
        Expression left = divide.left().accept(this, context);
        Expression right = divide.right().accept(this, context);
        divide = (Divide) divide.withChildren(left, right);

        // type coercion
        return TypeCoercionUtils.processDivide(divide, left, right);
    }

    @Override
    public Expression visitIntegralDivide(IntegralDivide integralDivide, ExpressionRewriteContext context) {
        Expression left = integralDivide.left().accept(this, context);
        Expression right = integralDivide.right().accept(this, context);
        integralDivide = (IntegralDivide) integralDivide.withChildren(left, right);

        // type coercion
        return TypeCoercionUtils.processIntegralDivide(integralDivide, left, right);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        Expression left = binaryArithmetic.left().accept(this, context);
        Expression right = binaryArithmetic.right().accept(this, context);
        binaryArithmetic = (BinaryArithmetic) binaryArithmetic.withChildren(left, right);
        return TypeCoercionUtils.processBinaryArithmetic(binaryArithmetic, left, right);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, ExpressionRewriteContext context) {
        Expression left = compoundPredicate.left().accept(this, context);
        Expression right = compoundPredicate.right().accept(this, context);
        CompoundPredicate ret = (CompoundPredicate) compoundPredicate.withChildren(left, right);
        return TypeCoercionUtils.processCompoundPredicate(ret);
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        Expression child = not.child().accept(this, context);
        if (!child.getDataType().isBooleanType() && !child.getDataType().isNullType()) {
            throw new AnalysisException(String.format(
                    "Operand '%s' part of predicate " + "'%s' should return type 'BOOLEAN' but "
                            + "returns type '%s'.",
                    child.toSql(), not.toSql(), child.getDataType()));
        }
        return not.withChildren(child);
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = cp.left().accept(this, context);
        Expression right = cp.right().accept(this, context);
        cp = (ComparisonPredicate) cp.withChildren(left, right);
        return TypeCoercionUtils.processComparisonPredicate(cp, left, right);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        List<Expression> rewrittenChildren = caseWhen.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        CaseWhen newCaseWhen = caseWhen.withChildren(rewrittenChildren);
        newCaseWhen.checkLegalityBeforeTypeCoercion();
        return TypeCoercionUtils.processCaseWhen(newCaseWhen);
    }

    @Override
    public Expression visitWhenClause(WhenClause whenClause, ExpressionRewriteContext context) {
        return whenClause.withChildren(TypeCoercionUtils.castIfNotSameType(
                whenClause.getOperand().accept(this, context), BooleanType.INSTANCE),
                whenClause.getResult().accept(this, context));
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        List<Expression> rewrittenChildren = inPredicate.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        InPredicate newInPredicate = inPredicate.withChildren(rewrittenChildren);
        return TypeCoercionUtils.processInPredicate(newInPredicate);
    }

    @Override
    public Expression visitInSubquery(InSubquery inSubquery, ExpressionRewriteContext context) {
        Expression newCompareExpr = inSubquery.getCompareExpr().accept(this, context);
        Expression newListQuery = inSubquery.getListQuery().accept(this, context);
        ComparisonPredicate newCpAfterUnNestingSubquery =
                new EqualTo(newCompareExpr, ((ListQuery) newListQuery).getQueryPlan().getOutput().get(0));
        ComparisonPredicate afterTypeCoercion = (ComparisonPredicate) TypeCoercionUtils.processComparisonPredicate(
                newCpAfterUnNestingSubquery, newCompareExpr, newListQuery);
        if (newListQuery.getDataType().isBitmapType()) {
            if (!newCompareExpr.getDataType().isBigIntType()) {
                newCompareExpr = new Cast(newCompareExpr, BigIntType.INSTANCE);
            }
        } else {
            newCompareExpr = afterTypeCoercion.left();
        }
        return new InSubquery(newCompareExpr, (ListQuery) afterTypeCoercion.right(),
            inSubquery.getCorrelateSlots(), ((ListQuery) afterTypeCoercion.right()).getTypeCoercionExpr(),
            inSubquery.isNot());
    }

    @Override
    public Expression visitMatch(Match match, ExpressionRewriteContext context) {
        Expression left = match.left().accept(this, context);
        Expression right = match.right().accept(this, context);
        // check child type
        if (!left.getDataType().isStringLikeType()) {
            throw new AnalysisException(String.format(
                    "left operand '%s' part of predicate " + "'%s' should return type 'STRING' but "
                            + "returns type '%s'.",
                    left.toSql(), match.toSql(), left.getDataType()));
        }

        if (!right.getDataType().isStringLikeType() && !right.getDataType().isNullType()) {
            throw new AnalysisException(String.format(
                    "right operand '%s' part of predicate " + "'%s' should return type 'STRING' but "
                            + "returns type '%s'.",
                    right.toSql(), match.toSql(), right.getDataType()));
        }
        return match.withChildren(left, right);
    }
}
