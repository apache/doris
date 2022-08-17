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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * a rule to add implicit cast for expressions.
 */
@Developing
public class TypeCoercion extends AbstractExpressionRewriteRule {

    public static final TypeCoercion INSTANCE = new TypeCoercion();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof ImplicitCastInputTypes) {
            return visitImplicitCastInputTypes(expr, ctx);
        } else {
            return super.rewrite(expr, ctx);
        }
    }

    // TODO: add other expression visitor function to do type coercion if necessary.

    @Override
    public Expression visitBinaryOperator(BinaryOperator binaryOperator, ExpressionRewriteContext context) {
        Expression left = binaryOperator.left();
        Expression right = binaryOperator.right();
        if (!TypeCoercionUtils.canHandleTypeCoercion(left.getDataType(), right.getDataType())) {
            return binaryOperator;
        }
        return TypeCoercionUtils.findTightestCommonType(left.getDataType(), right.getDataType())
                .map(commonType -> {
                    if (binaryOperator.inputType().acceptsType(commonType) && (
                            !left.getDataType().equals(commonType) || !right.getDataType().equals(commonType))) {
                        Expression newLeft = left;
                        Expression newRight = right;
                        if (!left.getDataType().equals(commonType)) {
                            newLeft = new Cast(left, commonType);
                        }
                        if (!right.getDataType().equals(commonType)) {
                            newRight = new Cast(right, commonType);
                        }
                        return binaryOperator.withChildren(newLeft, newRight);
                    } else {
                        return binaryOperator;
                    }
                })
                .orElse(binaryOperator);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        List<DataType> dataTypesForCoercion = caseWhen.dataTypesForCoercion();
        if (dataTypesForCoercion.size() <= 1) {
            return caseWhen;
        }
        DataType first = dataTypesForCoercion.get(0);
        if (dataTypesForCoercion.stream().allMatch(dataType -> dataType.equals(first))) {
            return caseWhen;
        }
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(dataTypesForCoercion);
        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren
                            = caseWhen.getWhenClauses().stream()
                            .map(wc -> wc.withChildren(wc.getOperand(),
                                    TypeCoercionUtils.castIfNotSameType(wc.getResult(), commonType)))
                            .collect(Collectors.toList());
                    caseWhen.getDefaultValue()
                            .map(dv -> TypeCoercionUtils.castIfNotSameType(dv, commonType))
                            .ifPresent(newChildren::add);
                    return caseWhen.withChildren(newChildren);
                })
                .orElse(caseWhen);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        if (inPredicate.getOptions().stream().map(Expression::getDataType)
                .allMatch(dt -> dt.equals(inPredicate.getCompareExpr().getDataType()))) {
            return inPredicate;
        }
        Optional<DataType> optionalCommonType = TypeCoercionUtils.findWiderCommonType(inPredicate.children()
                .stream().map(Expression::getDataType).collect(Collectors.toList()));

        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren = inPredicate.children().stream()
                            .map(e -> TypeCoercionUtils.castIfNotSameType(e, commonType))
                            .collect(Collectors.toList());
                    return inPredicate.withChildren(newChildren);
                })
                .orElse(inPredicate);
    }

    /**
     * Do implicit cast for expression's children.
     */
    private Expression visitImplicitCastInputTypes(Expression expr, ExpressionRewriteContext ctx) {
        ImplicitCastInputTypes implicitCastInputTypes = (ImplicitCastInputTypes) expr;
        List<Expression> newChildren = Lists.newArrayListWithCapacity(expr.arity());
        AtomicInteger changed = new AtomicInteger(0);
        for (int i = 0; i < implicitCastInputTypes.expectedInputTypes().size(); i++) {
            newChildren.add(implicitCast(expr.child(i), implicitCastInputTypes.expectedInputTypes().get(i), ctx)
                    .map(e -> {
                        changed.incrementAndGet();
                        return e;
                    })
                    .orElse(expr.child(0))
            );
        }
        if (changed.get() != 0) {
            return expr.withChildren(newChildren);
        } else {
            return expr;
        }
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
            // TODO: there maybe has performance problem, we need use ctx to save whether children is changed.
            if (rewrittenInput.equals(input)) {
                return Optional.empty();
            } else {
                return Optional.of(rewrittenInput);
            }
        }
    }
}
