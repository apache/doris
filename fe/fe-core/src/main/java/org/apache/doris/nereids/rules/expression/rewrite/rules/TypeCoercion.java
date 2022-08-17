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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.NumericType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * a rule to add implicit cast for expressions.
 */
@Developing
public class TypeCoercion extends AbstractExpressionRewriteRule {

    public static final TypeCoercion INSTANCE = new TypeCoercion();

    public static final List<DataType> NUMERIC_PRECEDENCE = ImmutableList.of(
            DoubleType.INSTANCE,
            LargeIntType.INSTANCE,
            FloatType.INSTANCE,
            BigIntType.INSTANCE,
            IntegerType.INSTANCE,
            SmallIntType.INSTANCE,
            TinyIntType.INSTANCE
    );

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof ImplicitCastInputTypes) {
            return visitImplicitCastInputTypes(expr, ctx);
        } else {
            return super.rewrite(expr, ctx);
        }
    }

    // TODO: add other expression visitor function to do type coercion.

    @Override
    public Expression visitBinaryOperator(BinaryOperator binaryOperator, ExpressionRewriteContext context) {
        Expression left = binaryOperator.left();
        Expression right = binaryOperator.right();
        if (!canHandleTypeCoercion(left.getDataType(), right.getDataType())) {
            return binaryOperator;
        }
        return findTightestCommonType(left.getDataType(), right.getDataType())
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
        Optional<DataType> optionalCommonType = findWiderCommonType(dataTypesForCoercion);
        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren
                            = caseWhen.getWhenClauses().stream()
                            .map(wc -> wc.withChildren(wc.getOperand(),
                                    castIfNotSameType(wc.getResult(), commonType)))
                            .collect(Collectors.toList());
                    caseWhen.getDefaultValue()
                            .map(dv -> castIfNotSameType(dv, commonType))
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
        Optional<DataType> optionalCommonType = findWiderCommonType(inPredicate.children()
                .stream().map(Expression::getDataType).collect(Collectors.toList()));

        return optionalCommonType
                .map(commonType -> {
                    List<Expression> newChildren = inPredicate.children().stream()
                            .map(e -> castIfNotSameType(e, commonType))
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
    private Optional<Expression> implicitCast(Expression input, AbstractDataType expected,
            ExpressionRewriteContext ctx) {
        Expression rewrittenInput = rewrite(input, ctx);
        Optional<DataType> castDataType = implicitCast(rewrittenInput.getDataType(), expected);
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

    /**
     * Return Optional.empty() if cannot do implicit cast.
     * TODO: datetime and date type
     */
    private Optional<DataType> implicitCast(DataType input, AbstractDataType expected) {
        DataType returnType = null;
        if (expected.acceptsType(input)) {
            // If the expected type is already a parent of the input type, no need to cast.
            return Optional.of(input);
        }
        if (expected instanceof TypeCollection) {
            TypeCollection typeCollection = (TypeCollection) expected;
            // use origin datatype first. use implicit cast instead if origin type cannot be accepted.
            return typeCollection.getTypes().stream()
                    .filter(e -> e.acceptsType(input))
                    .map(e -> input)
                    .map(Optional::of)
                    .findFirst()
                    .orElse(typeCollection.getTypes().stream()
                            .map(e -> implicitCast(input, e))
                            .findFirst()
                            .orElse(Optional.empty()));
        }
        if (input.isNullType()) {
            // Cast null type (usually from null literals) into target types
            returnType = expected.defaultConcreteType();
        } else if (input.isNumericType() && expected instanceof DecimalType) {
            // If input is a numeric type but not decimal, and we expect a decimal type,
            // cast the input to decimal.
            returnType = DecimalType.forType(input);
        } else if (input.isNumericType() && expected instanceof NumericType) {
            // For any other numeric types, implicitly cast to each other, e.g. bigint -> int, int -> bigint
            returnType = (DataType) expected;
        } else if (input.isStringType()) {
            if (expected instanceof DecimalType) {
                returnType = DecimalType.SYSTEM_DEFAULT;
            } else if (expected instanceof NumericType) {
                returnType = expected.defaultConcreteType();
            }
        } else if (input.isPrimitive() && !input.isStringType() && expected instanceof CharacterType) {
            returnType = StringType.INSTANCE;
        }

        // could not do implicit cast, just return null. Throw exception in check analysis.
        return Optional.ofNullable(returnType);
    }

    private boolean canHandleTypeCoercion(DataType leftType, DataType rightType) {
        if (leftType instanceof DecimalType && rightType instanceof NullType) {
            return true;
        }
        if (leftType instanceof NullType && rightType instanceof DecimalType) {
            return true;
        }
        if (!(leftType instanceof DecimalType) && !(rightType instanceof DecimalType) && !leftType.equals(rightType)) {
            return true;
        }
        return false;
    }

    // TODO: compatible with origin planner and BE
    // TODO: when add new type, add it to here
    @Developing
    private Optional<DataType> findTightestCommonType(DataType left, DataType right) {
        DataType tightestCommonType = null;
        if (left.equals(right)) {
            tightestCommonType = left;
        } else if (left instanceof NullType) {
            tightestCommonType = right;
        } else if (right instanceof NullType) {
            tightestCommonType = left;
        } else if (left instanceof IntegralType && right instanceof DecimalType
                && ((DecimalType) right).isWiderThan(left)) {
            tightestCommonType = right;
        } else if (right instanceof IntegralType && left instanceof DecimalType
                && ((DecimalType) left).isWiderThan(right)) {
            tightestCommonType = left;
        } else if (left instanceof NumericType && right instanceof NumericType
                && !(left instanceof DecimalType) && !(right instanceof DecimalType)) {
            for (DataType dataType : NUMERIC_PRECEDENCE) {
                if (dataType.equals(left) || dataType.equals(right)) {
                    tightestCommonType = dataType;
                    break;
                }
            }
        } else if (left instanceof CharacterType || right instanceof CharacterType) {
            if (left instanceof StringType || right instanceof StringType) {
                tightestCommonType = StringType.INSTANCE;
            } else if (left instanceof CharacterType && right instanceof CharacterType) {
                tightestCommonType = VarcharType.createVarcharType(
                        Math.max(((CharacterType) left).getLen(), ((CharacterType) right).getLen()));
            } else if (left instanceof CharacterType) {
                tightestCommonType = VarcharType.createVarcharType(((CharacterType) left).getLen());
            } else {
                tightestCommonType = VarcharType.createVarcharType(((CharacterType) right).getLen());
            }
        }
        return Optional.ofNullable(tightestCommonType);
    }

    // TODO
    @Developing
    private Optional<DataType> findWiderCommonType(List<DataType> dataTypes) {
        return Optional.empty();
    }

    @Developing
    private Optional<DataType> findWiderTypeForTwo(DataType left, DataType right) {
        return Stream
                .<Supplier<Optional<DataType>>>of(
                        () -> findTightestCommonType(left, right),
                        () -> findWiderTypeForDecimal(left, right),
                        () -> stringPromotion(left, right),
                        () -> findTypeForComplex(left, right))
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    @Developing
    private Optional<DataType> findWiderTypeForDecimal(DataType left, DataType right) {
        return Optional.empty();
    }

    @Developing
    private Optional<DataType> stringPromotion(DataType left, DataType right) {
        return Optional.empty();
    }

    @Developing
    private Optional<DataType> findTypeForComplex(DataType left, DataType right) {
        // TODO: we need to add real logical here, if we add array type in Nereids
        return Optional.empty();
    }

    private Expression castIfNotSameType(Expression input, DataType dataType) {
        if (input.getDataType().equals(dataType)) {
            return input;
        } else {
            return new Cast(input, dataType);
        }
    }
}
