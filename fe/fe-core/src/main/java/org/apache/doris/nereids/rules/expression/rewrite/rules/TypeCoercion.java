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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.batch.CheckLegalityBeforeTypeCoercion;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
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
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * a rule to add implicit cast for expressions.
 * This class is inspired by spark's TypeCoercion.
 */
@Deprecated
@DependsRules(CheckLegalityBeforeTypeCoercion.class)
public class TypeCoercion extends AbstractExpressionRewriteRule {

    // TODO:
    //  1. DecimalPrecision Process
    //  2. String promote with numeric in binary arithmetic
    //  3. Date and DateTime process

    public static final TypeCoercion INSTANCE = new TypeCoercion();

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof ImplicitCastInputTypes) {
            List<AbstractDataType> expectedInputTypes = ((ImplicitCastInputTypes) expr).expectedInputTypes();
            if (!expectedInputTypes.isEmpty()) {
                return visitImplicitCastInputTypes(expr, expectedInputTypes, ctx);
            }
        }

        return super.visit(expr, ctx);
    }

    @Override
    public Expression visitBitNot(BitNot bitNot, ExpressionRewriteContext context) {
        Expression child = rewrite(bitNot.child(), context);
        if (child.getDataType().toCatalogDataType().getPrimitiveType().ordinal()
                > PrimitiveType.LARGEINT.ordinal()) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        return bitNot.withChildren(child);
    }

    @Override
    public Expression visitDivide(Divide divide, ExpressionRewriteContext context) {
        Expression left = rewrite(divide.left(), context);
        Expression right = rewrite(divide.right(), context);
        return TypeCoercionUtils.processDivide(divide, left, right);
    }

    @Override
    public Expression visitIntegralDivide(IntegralDivide integralDivide, ExpressionRewriteContext context) {
        Expression left = rewrite(integralDivide.left(), context);
        Expression right = rewrite(integralDivide.right(), context);
        return TypeCoercionUtils.processIntegralDivide(integralDivide, left, right);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        Expression left = rewrite(binaryArithmetic.left(), context);
        Expression right = rewrite(binaryArithmetic.right(), context);
        return TypeCoercionUtils.processBinaryArithmetic(binaryArithmetic, left, right);
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        Expression child = rewrite(not.child(), context);
        if (!child.getDataType().isBooleanType() && !child.getDataType().isNullType()) {
            throw new AnalysisException(String.format(
                    "Operand '%s' part of predicate " + "'%s' should return type 'BOOLEAN' but "
                            + "returns type '%s'.",
                    child.toSql(), not.toSql(), child.getDataType()));
        }
        return not.withChildren(child);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, ExpressionRewriteContext context) {
        Expression left = rewrite(compoundPredicate.left(), context);
        Expression right = rewrite(compoundPredicate.right(), context);
        Expression ret = compoundPredicate.withChildren(left, right);
        ret.children().forEach(e -> {
                    if (!e.getDataType().isBooleanType() && !e.getDataType().isNullType()) {
                        throw new AnalysisException(String.format(
                                "Operand '%s' part of predicate " + "'%s' should return type 'BOOLEAN' but "
                                        + "returns type '%s'.",
                                e.toSql(), ret.toSql(), e.getDataType()));
                    }
                }
        );
        return ret;
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = rewrite(cp.left(), context);
        Expression right = rewrite(cp.right(), context);
        return TypeCoercionUtils.processComparisonPredicate(cp, left, right);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        List<Expression> rewrittenChildren = caseWhen.children().stream()
                .map(e -> rewrite(e, context)).collect(Collectors.toList());
        CaseWhen newCaseWhen = caseWhen.withChildren(rewrittenChildren);
        List<DataType> dataTypesForCoercion = newCaseWhen.dataTypesForCoercion();
        if (dataTypesForCoercion.size() <= 1) {
            return newCaseWhen;
        }
        DataType first = dataTypesForCoercion.get(0);
        if (dataTypesForCoercion.stream().allMatch(dataType -> dataType.equals(first))) {
            return newCaseWhen;
        }

        Map<Boolean, List<Expression>> filteredStringLiteral = newCaseWhen.expressionForCoercion()
                .stream().collect(Collectors.partitioningBy(e -> e.isLiteral() && e.getDataType().isStringLikeType()));

        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(filteredStringLiteral.get(false)
                .stream().map(Expression::getDataType).collect(Collectors.toList()));

        if (!optionalCommonType.isPresent()) {
            return newCaseWhen;
        }
        DataType commonType = optionalCommonType.get();

        // process character literal
        for (Expression stringLikeLiteral : filteredStringLiteral.get(true)) {
            Literal literal = (Literal) stringLikeLiteral;
            if (!TypeCoercionUtils.characterLiteralTypeCoercion(
                    literal.getStringValue(), commonType).isPresent()) {
                commonType = StringType.INSTANCE;
                break;
            }
        }

        List<Expression> newChildren = Lists.newArrayList();
        for (WhenClause wc : newCaseWhen.getWhenClauses()) {
            newChildren.add(wc.withChildren(wc.getOperand(),
                    TypeCoercionUtils.castIfNotMatchType(wc.getResult(), commonType)));
        }
        if (newCaseWhen.getDefaultValue().isPresent()) {
            newChildren.add(TypeCoercionUtils.castIfNotMatchType(newCaseWhen.getDefaultValue().get(), commonType));
        }
        return newCaseWhen.withChildren(newChildren);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        List<Expression> rewrittenChildren = inPredicate.children().stream()
                .map(e -> rewrite(e, context)).collect(Collectors.toList());
        InPredicate newInPredicate = inPredicate.withChildren(rewrittenChildren);

        if (newInPredicate.getOptions().stream().map(Expression::getDataType)
                .allMatch(dt -> dt.equals(newInPredicate.getCompareExpr().getDataType()))) {
            return newInPredicate;
        }

        Map<Boolean, List<Expression>> filteredStringLiteral = newInPredicate.children()
                .stream().collect(Collectors.partitioningBy(e -> e.isLiteral() && e.getDataType().isStringLikeType()));
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(filteredStringLiteral.get(false)
                .stream().map(Expression::getDataType).collect(Collectors.toList()));

        if (!optionalCommonType.isPresent()) {
            return newInPredicate;
        }
        DataType commonType = optionalCommonType.get();

        // process character literal
        for (Expression stringLikeLiteral : filteredStringLiteral.get(true)) {
            Literal literal = (Literal) stringLikeLiteral;
            if (!TypeCoercionUtils.characterLiteralTypeCoercion(
                    literal.getStringValue(), commonType).isPresent()) {
                commonType = StringType.INSTANCE;
                break;
            }
        }

        List<Expression> newChildren = Lists.newArrayList();
        for (Expression child : newInPredicate.children()) {
            newChildren.add(TypeCoercionUtils.castIfNotMatchType(child, commonType));
        }
        return newInPredicate.withChildren(newChildren);
    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        Expression left = rewrite(arithmetic.left(), context);
        Expression right = rewrite(arithmetic.right(), context);
        return TypeCoercionUtils.processTimestampArithmetic(arithmetic, left, right);
    }

    /**
     * Do implicit cast for expression's children.
     */
    private Expression visitImplicitCastInputTypes(Expression expr,
            List<AbstractDataType> expectedInputTypes, ExpressionRewriteContext ctx) {
        expr = expr.withChildren(child -> rewrite(child, ctx));
        return TypeCoercionUtils.implicitCastInputTypes(expr, expectedInputTypes);
    }
}
