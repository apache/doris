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
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
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
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * function binder
 */
class FunctionBinder extends DefaultExpressionRewriter<CascadesContext> {
    public static final FunctionBinder INSTANCE = new FunctionBinder();

    public <E extends Expression> E bind(E expression, CascadesContext context) {
        return (E) expression.accept(this, context);
    }

    @Override
    public Expression visit(Expression expr, CascadesContext context) {
        expr = super.visit(expr, context);
        expr.checkLegalityBeforeTypeCoercion();
        if (expr instanceof ImplicitCastInputTypes) {
            List<AbstractDataType> expectedInputTypes = ((ImplicitCastInputTypes) expr).expectedInputTypes();
            if (!expectedInputTypes.isEmpty()) {
                return visitImplicitCastInputTypes(expr, expectedInputTypes);
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

        // check
        boundFunction.checkLegalityBeforeTypeCoercion();

        // type coercion
        return visitImplicitCastInputTypes(boundFunction, boundFunction.expectedInputTypes());
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
        arithmetic = (TimestampArithmetic) arithmetic.withChildren(left, right);
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
        return visitImplicitCastInputTypes(arithmetic, arithmetic.expectedInputTypes());
    }

    /* ********************************************************************************************
     * type coercion
     * ******************************************************************************************** */

    @Override
    public Expression visitIntegralDivide(IntegralDivide integralDivide, CascadesContext context) {
        Expression left = integralDivide.left().accept(this, context);
        Expression right = integralDivide.right().accept(this, context);

        // check before bind
        integralDivide.checkLegalityBeforeTypeCoercion();

        // type coercion
        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, BigIntType.INSTANCE);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, BigIntType.INSTANCE);
        return integralDivide.withChildren(newLeft, newRight);
    }

    @Override
    public Expression visitBinaryOperator(BinaryOperator binaryOperator, CascadesContext context) {
        Expression left = binaryOperator.left().accept(this, context);
        Expression right = binaryOperator.right().accept(this, context);
        // check
        binaryOperator.checkLegalityBeforeTypeCoercion();

        // characterLiteralTypeCoercion
        if (left instanceof Literal || right instanceof Literal) {
            if (left instanceof Literal && ((Literal) left).isCharacterLiteral()) {
                left = characterLiteralTypeCoercion(((Literal) left).getStringValue(), right.getDataType())
                        .orElse(left);
            }
            if (right instanceof Literal && ((Literal) right).isCharacterLiteral()) {
                right = characterLiteralTypeCoercion(((Literal) right).getStringValue(), left.getDataType())
                        .orElse(right);
            }
        }
        binaryOperator = (BinaryOperator) binaryOperator.withChildren(left, right);

        // type coercion
        if (binaryOperator instanceof ImplicitCastInputTypes) {
            List<AbstractDataType> expectedInputTypes = ((ImplicitCastInputTypes) binaryOperator).expectedInputTypes();
            if (!expectedInputTypes.isEmpty()) {
                binaryOperator.children().stream().filter(e -> e instanceof StringLikeLiteral)
                        .forEach(expr -> {
                            try {
                                new BigDecimal(((StringLikeLiteral) expr).getStringValue());
                            } catch (NumberFormatException e) {
                                throw new IllegalStateException(String.format(
                                        "string literal %s cannot be cast to double", expr.toSql()));
                            }
                        });
                binaryOperator = (BinaryOperator) visitImplicitCastInputTypes(binaryOperator, expectedInputTypes);
            }
        }

        BinaryOperator op = binaryOperator;
        Expression opLeft = op.left();
        Expression opRight = op.right();

        return Optional.of(TypeCoercionUtils.canHandleTypeCoercion(left.getDataType(), right.getDataType()))
                .filter(Boolean::booleanValue)
                .map(b -> TypeCoercionUtils.findTightestCommonType(op, opLeft.getDataType(), opRight.getDataType()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(ct -> op.inputType().acceptsType(ct))
                .filter(ct -> !opLeft.getDataType().equals(ct) || !opRight.getDataType().equals(ct))
                .map(commonType -> {
                    Expression newLeft = TypeCoercionUtils.castIfNotSameType(opLeft, commonType);
                    Expression newRight = TypeCoercionUtils.castIfNotSameType(opRight, commonType);
                    return op.withChildren(newLeft, newRight);
                })
                .orElse(op.withChildren(opLeft, opRight));
    }

    @Override
    public Expression visitDivide(Divide divide, CascadesContext context) {
        Expression left = divide.left().accept(this, context);
        Expression right = divide.right().accept(this, context);
        divide = (Divide) divide.withChildren(left, right);

        // check
        divide.checkLegalityBeforeTypeCoercion();

        // type coercion
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        DataType commonType = TypeCoercionUtils.findCommonNumericsType(t1, t2);

        if (divide.getLegacyOperator() == Operator.DIVIDE
                && (commonType.isBigIntType() || commonType.isLargeIntType())) {
            commonType = DoubleType.INSTANCE;
        }
        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
        return divide.withChildren(newLeft, newRight);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, CascadesContext context) {
        List<Expression> rewrittenChildren = caseWhen.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        CaseWhen newCaseWhen = caseWhen.withChildren(rewrittenChildren);

        // check
        newCaseWhen.checkLegalityBeforeTypeCoercion();

        // type coercion
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
    public Expression visitInPredicate(InPredicate inPredicate, CascadesContext context) {
        List<Expression> rewrittenChildren = inPredicate.children().stream()
                .map(e -> e.accept(this, context)).collect(Collectors.toList());
        InPredicate newInPredicate = inPredicate.withChildren(rewrittenChildren);

        // check
        newInPredicate.checkLegalityBeforeTypeCoercion();

        // type coercion
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

    @Override
    public Expression visitBitNot(BitNot bitNot, CascadesContext context) {
        Expression child = bitNot.child().accept(this, context);
        // check
        bitNot.checkLegalityBeforeTypeCoercion();

        // type coercion
        if (child.getDataType().toCatalogDataType().getPrimitiveType().ordinal() > PrimitiveType.LARGEINT.ordinal()) {
            child = new Cast(child, BigIntType.INSTANCE);
        }
        return bitNot.withChildren(child);
    }

    private Optional<Expression> characterLiteralTypeCoercion(String value, DataType dataType) {
        Expression ret = null;
        try {
            if (dataType instanceof BooleanType) {
                if ("true".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.TRUE;
                }
                if ("false".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.FALSE;
                }
            } else if (dataType instanceof IntegralType) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
                    ret = new TinyIntLiteral(bigInt.byteValue());
                } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
                    ret = new SmallIntLiteral(bigInt.shortValue());
                } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
                    ret = new IntegerLiteral(bigInt.intValue());
                } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof FloatType) {
                ret = new FloatLiteral(Float.parseFloat(value));
            } else if (dataType instanceof DoubleType) {
                ret = new DoubleLiteral(Double.parseDouble(value));
            } else if (dataType instanceof DecimalV2Type) {
                ret = new DecimalLiteral(new BigDecimal(value));
            } else if (dataType instanceof CharType) {
                ret = new CharLiteral(value, value.length());
            } else if (dataType instanceof VarcharType) {
                ret = new VarcharLiteral(value, value.length());
            } else if (dataType instanceof StringType) {
                ret = new StringLiteral(value);
            } else if (dataType instanceof DateType) {
                ret = new DateLiteral(value);
            } else if (dataType instanceof DateTimeType) {
                ret = new DateTimeLiteral(value);
            }
        } catch (Exception e) {
            // ignore
        }
        return Optional.ofNullable(ret);
    }

    private Expression visitImplicitCastInputTypes(Expression expr, List<AbstractDataType> expectedInputTypes) {
        List<Optional<DataType>> inputImplicitCastTypes
                = getInputImplicitCastTypes(expr.children(), expectedInputTypes);
        return castInputs(expr, inputImplicitCastTypes);
    }

    private List<Optional<DataType>> getInputImplicitCastTypes(
            List<Expression> inputs, List<AbstractDataType> expectedTypes) {
        Builder<Optional<DataType>> implicitCastTypes = ImmutableList.builder();
        for (int i = 0; i < inputs.size(); i++) {
            DataType argType = inputs.get(i).getDataType();
            AbstractDataType expectedType = expectedTypes.get(i);
            Optional<DataType> castType = TypeCoercionUtils.implicitCast(argType, expectedType);
            // TODO: complete the cast logic like FunctionCallExpr.analyzeImpl
            boolean legacyCastCompatible = expectedType instanceof DataType
                    && !(expectedType.getClass().equals(NumericType.class))
                    && !(expectedType.getClass().equals(IntegralType.class))
                    && !(expectedType.getClass().equals(FractionalType.class))
                    && !(expectedType.getClass().equals(CharacterType.class))
                    && !argType.toCatalogDataType().matchesType(expectedType.toCatalogDataType());
            if (!castType.isPresent() && legacyCastCompatible) {
                castType = Optional.of((DataType) expectedType);
            }
            implicitCastTypes.add(castType);
        }
        return implicitCastTypes.build();
    }

    private Expression castInputs(Expression expr, List<Optional<DataType>> castTypes) {
        return expr.withChildren((child, childIndex) -> {
            DataType argType = child.getDataType();
            Optional<DataType> castType = castTypes.get(childIndex);
            if (castType.isPresent() && !castType.get().equals(argType)) {
                return TypeCoercionUtils.castIfNotSameType(child, castType.get());
            } else {
                return child;
            }
        });
    }
}
