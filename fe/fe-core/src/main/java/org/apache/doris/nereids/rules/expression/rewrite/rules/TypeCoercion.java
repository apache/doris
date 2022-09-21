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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * a rule to add implicit cast for expressions.
 * This class is inspired by spark's TypeCoercion.
 */
@Developing
public class TypeCoercion extends AbstractExpressionRewriteRule {

    // TODO:
    //  1. DecimalPrecision Process
    //  2. Divide process
    //  3. String promote with numeric in binary arithmetic
    //  4. Date and DateTime process

    public static final TypeCoercion INSTANCE = new TypeCoercion();

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof ImplicitCastInputTypes) {
            return visitImplicitCastInputTypes(expr, ctx);
        } else {
            return super.visit(expr, ctx);
        }
    }

    // TODO: add other expression visitor function to do type coercion if necessary.

    @Override
    public Expression visitBinaryOperator(BinaryOperator binaryOperator, ExpressionRewriteContext context) {
        Expression left = rewrite(binaryOperator.left(), context);
        Expression right = rewrite(binaryOperator.right(), context);

        return Optional.of(TypeCoercionUtils.canHandleTypeCoercion(left.getDataType(), right.getDataType()))
                .filter(Boolean::booleanValue)
                .map(b -> TypeCoercionUtils.findTightestCommonType(left.getDataType(), right.getDataType()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(ct -> binaryOperator.inputType().acceptsType(ct))
                .filter(ct -> !left.getDataType().equals(ct) || !right.getDataType().equals(ct))
                .map(commonType -> {
                    Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
                    Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
                    return binaryOperator.withChildren(newLeft, newRight);
                })
                .orElse(binaryOperator.withChildren(left, right));
    }

    @Override
    public Expression visitDivide(Divide divide, ExpressionRewriteContext context) {
        Expression left = rewrite(divide.left(), context);
        Expression right = rewrite(divide.right(), context);
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        DataType commonType = TypeCoercionUtils.findCommonType(t1, t2);
        if (divide.getLegacyOperator() == Operator.DIVIDE) {
            if (commonType.isBigIntType() || commonType.isLargeIntType()) {
                commonType = DoubleType.INSTANCE;
            }
        }
        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
        return divide.withChildren(newLeft, newRight);
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
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(dataTypesForCoercion);
        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren
                            = newCaseWhen.getWhenClauses().stream()
                            .map(wc -> wc.withChildren(wc.getOperand(),
                                    TypeCoercionUtils.castIfNotSameType(wc.getResult(), commonType)))
                            .collect(Collectors.toList());
                    newCaseWhen.getDefaultValue()
                            .map(dv -> TypeCoercionUtils.castIfNotSameType(dv, commonType))
                            .ifPresent(newChildren::add);
                    return newCaseWhen.withChildren(newChildren);
                })
                .orElse(newCaseWhen);
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
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(newInPredicate.children()
                .stream().map(Expression::getDataType).collect(Collectors.toList()));

        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren = newInPredicate.children().stream()
                            .map(e -> TypeCoercionUtils.castIfNotSameType(e, commonType))
                            .collect(Collectors.toList());
                    return newInPredicate.withChildren(newChildren);
                })
                .orElse(newInPredicate);
    }

    /**
     * Do implicit cast for expression's children.
     */
    private Expression visitImplicitCastInputTypes(Expression expr, ExpressionRewriteContext ctx) {
        ImplicitCastInputTypes implicitCastInputTypes = (ImplicitCastInputTypes) expr;
        List<Expression> newChildren = Lists.newArrayListWithCapacity(expr.arity());
        boolean changed = false;
        for (int i = 0; i < implicitCastInputTypes.expectedInputTypes().size(); i++) {
            AbstractDataType expectedType = implicitCastInputTypes.expectedInputTypes().get(i);
            Optional<Expression> castResult = implicitCast(expr.child(i), expectedType, ctx);
            if (castResult.isPresent()) {
                changed = true;
            }
            newChildren.add(castResult.orElse(expr.child(i)));
        }
        return changed ? expr.withChildren(newChildren) : expr;
    }

    /**
     * Return Optional.empty() if we cannot do or do not need to do implicit cast.
     */
    @Developing
    private Optional<Expression> implicitCast(Expression input, AbstractDataType expected,
            ExpressionRewriteContext ctx) {
        Expression rewrittenInput = rewrite(input, ctx);
        Optional<DataType> castDataType = TypeCoercionUtils.implicitCast(rewrittenInput.getDataType(), expected);
        if (castDataType.isPresent() && !castDataType.get().equals(rewrittenInput.getDataType())) {
            return Optional.of(new Cast(rewrittenInput, castDataType.get()));
        } else {
            if (rewrittenInput == input) {
                return Optional.empty();
            } else {
                return Optional.of(rewrittenInput);
            }
        }
    }
}
