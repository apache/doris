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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * function binder
 */
public class FunctionBinder extends DefaultExpressionRewriter<CascadesContext> {
    public static final FunctionBinder INSTANCE = new FunctionBinder();

    public <E extends Expression> E bind(E expression, CascadesContext context) {
        return (E) expression.accept(this, context);
    }

    @Override
    public Expression visit(Expression expr, CascadesContext context) {
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
    public Expression visitUnboundFunction(UnboundFunction unboundFunction, CascadesContext context) {
        unboundFunction = unboundFunction.withChildren(unboundFunction.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList()));

        // bind function
        FunctionRegistry functionRegistry = context.getConnectContext().getEnv().getFunctionRegistry();
        String functionName = unboundFunction.getName();
        List<Object> arguments = unboundFunction.isDistinct()
                ? ImmutableList.builder()
                .add(unboundFunction.isDistinct())
                .addAll(unboundFunction.getArguments())
                .build()
                : (List) unboundFunction.getArguments();

        FunctionBuilder builder = functionRegistry.findFunctionBuilder(functionName, arguments);
        BoundFunction boundFunction = builder.build(functionName, arguments);
        return TypeCoercionUtils.processBoundFunction(boundFunction);
    }

    /**
     * gets the method for calculating the time.
     * e.g. YEARS_ADD、YEARS_SUB、DAYS_ADD 、DAYS_SUB
     */
    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, CascadesContext context) {
        Expression left = arithmetic.left().accept(this, context);
        Expression right = arithmetic.right().accept(this, context);

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
    public Expression visitBitNot(BitNot bitNot, CascadesContext context) {
        Expression child = bitNot.child().accept(this, context);
        // type coercion
        if (child.getDataType().toCatalogDataType().getPrimitiveType().ordinal() > PrimitiveType.LARGEINT.ordinal()) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        return bitNot.withChildren(child);
    }

    @Override
    public Expression visitDivide(Divide divide, CascadesContext context) {
        Expression left = divide.left().accept(this, context);
        Expression right = divide.right().accept(this, context);

        // type coercion
        return TypeCoercionUtils.processDivide(divide, left, right);
    }

    @Override
    public Expression visitIntegralDivide(IntegralDivide integralDivide, CascadesContext context) {
        Expression left = integralDivide.left().accept(this, context);
        Expression right = integralDivide.right().accept(this, context);

        // type coercion
        return TypeCoercionUtils.processIntegralDivide(integralDivide, left, right);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, CascadesContext context) {
        Expression left = binaryArithmetic.left().accept(this, context);
        Expression right = binaryArithmetic.right().accept(this, context);
        return TypeCoercionUtils.processBinaryArithmetic(binaryArithmetic, left, right);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, CascadesContext context) {
        Expression left = compoundPredicate.left().accept(this, context);
        Expression right = compoundPredicate.right().accept(this, context);
        CompoundPredicate ret = (CompoundPredicate) compoundPredicate.withChildren(left, right);
        return TypeCoercionUtils.processCompoundPredicate(ret);
    }

    @Override
    public Expression visitNot(Not not, CascadesContext context) {
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
    public Expression visitComparisonPredicate(ComparisonPredicate cp, CascadesContext context) {
        Expression left = cp.left().accept(this, context);
        Expression right = cp.right().accept(this, context);
        return TypeCoercionUtils.processComparisonPredicate(cp, left, right);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, CascadesContext context) {
        List<Expression> rewrittenChildren = caseWhen.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        CaseWhen newCaseWhen = caseWhen.withChildren(rewrittenChildren);
        newCaseWhen.checkLegalityBeforeTypeCoercion();
        return TypeCoercionUtils.processCaseWhen(newCaseWhen);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, CascadesContext context) {
        List<Expression> rewrittenChildren = inPredicate.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        InPredicate newInPredicate = inPredicate.withChildren(rewrittenChildren);
        return TypeCoercionUtils.processInPredicate(newInPredicate);
    }

    @Override
    public Expression visitBetween(Between between, CascadesContext context) {
        List<Expression> rewrittenChildren = between.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        Between newBetween = between.withChildren(rewrittenChildren);
        return TypeCoercionUtils.processBetween(newBetween);
    }
}
