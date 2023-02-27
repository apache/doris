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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utils for type coercion.
 */
public class TypeCoercionUtils {

    /**
     * numeric type precedence for type promotion.
     * bigger numeric has smaller ordinal
     */
    public static final List<DataType> NUMERIC_PRECEDENCE = ImmutableList.of(
            DoubleType.INSTANCE,
            FloatType.INSTANCE,
            LargeIntType.INSTANCE,
            BigIntType.INSTANCE,
            IntegerType.INSTANCE,
            SmallIntType.INSTANCE,
            TinyIntType.INSTANCE
    );

    /**
     * Return Optional.empty() if we cannot do implicit cast.
     */
    @Developing
    public static Optional<DataType> implicitCast(DataType input, AbstractDataType expected) {
        DataType returnType = null;
        if (expected.acceptsType(input)) {
            // If the expected type
            // is already a parent of the input type, no need to cast.
            return Optional.of(input);
        }
        if (input instanceof NullType) {
            // Cast null type (usually from null literals) into target types
            returnType = expected.defaultConcreteType();
        } else if (input instanceof NumericType) {
            if (expected instanceof DecimalV2Type) {
                // If input is a numeric type but not decimal, and we expect a decimal type,
                // cast the input to decimal.
                returnType = DecimalV2Type.forType(input);
            } else if (expected instanceof DateTimeType) {
                returnType = DateTimeType.INSTANCE;
            } else if (expected instanceof NumericType) {
                // For any other numeric types, implicitly cast to each other, e.g. bigint -> int, int -> bigint
                returnType = expected.defaultConcreteType();
            }
        } else if (input instanceof CharacterType) {
            if (expected instanceof DecimalV2Type) {
                returnType = DecimalV2Type.SYSTEM_DEFAULT;
            } else if (expected instanceof NumericType) {
                returnType = expected.defaultConcreteType();
            } else if (expected instanceof DateTimeType) {
                returnType = DateTimeType.INSTANCE;
            }
        } else if (input.isDateType()) {
            if (expected instanceof DateTimeType) {
                returnType = expected.defaultConcreteType();
            }
        }

        if (returnType == null && input instanceof PrimitiveType
                && expected instanceof CharacterType) {
            returnType = expected.defaultConcreteType();
        }

        // could not do implicit cast, just return null. Throw exception in check analysis.
        return Optional.ofNullable(returnType);
    }

    /**
     * return ture if datatype has character type in it, cannot use instance of CharacterType because of complex type.
     */
    @Developing
    public static boolean hasCharacterType(DataType dataType) {
        // TODO: consider complex type
        return dataType instanceof CharacterType;
    }

    /**
     * The type used for arithmetic operations.
     */
    public static DataType getNumResultType(DataType type) {
        if (type.isNumericType()) {
            return type;
        }
        if (type.isBooleanType()) {
            return TinyIntType.INSTANCE;
        }
        if (type.isDateLikeType()) {
            return BigIntType.INSTANCE;
        }
        if (type.isStringLikeType() || type.isHllType() || type.isTimeType() || type.isTimeV2Type()) {
            return DoubleType.INSTANCE;
        }
        if (type.isNullType()) {
            return TinyIntType.INSTANCE;
        }
        throw new AnalysisException("Cannot cast from " + type + " to numeric type.");
    }

    /**
     * find wider common type for data type list.
     */
    @Developing
    public static Optional<DataType> findWiderCommonType(List<DataType> dataTypes) {
        // TODO: do not consider complex type
        Map<Boolean, List<DataType>> partitioned = dataTypes.stream()
                .collect(Collectors.partitioningBy(TypeCoercionUtils::hasCharacterType));
        List<DataType> needTypeCoercion = Lists.newArrayList(Sets.newHashSet(partitioned.get(true)));
        needTypeCoercion.addAll(partitioned.get(false));
        return needTypeCoercion.stream().map(Optional::of).reduce(Optional.of(NullType.INSTANCE),
                (r, c) -> {
                    if (r.isPresent() && c.isPresent()) {
                        return findWiderTypeForTwo(r.get(), c.get());
                    } else {
                        return Optional.empty();
                    }
                });
    }

    /**
     * find wider common type for two data type.
     */
    @Developing
    public static Optional<DataType> findWiderTypeForTwo(DataType left, DataType right) {
        // TODO: need to rethink how to handle char and varchar to return char or varchar as much as possible.
        return Stream
                .<Supplier<Optional<DataType>>>of(
                        () -> findPrimitiveCommonType(left, right),
                        () -> findTypeForComplex(left, right))
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /**
     * find common type for complex type.
     */
    @Developing
    public static Optional<DataType> findTypeForComplex(DataType left, DataType right) {
        // TODO: we need to add real logical here, if we add array type in Nereids
        return Optional.empty();
    }

    /**
     * cast input type if input's datatype is not match with dateType.
     */
    public static Expression castIfNotMatchType(Expression input, DataType dataType) {
        if (input.getDataType().toCatalogDataType().matchesType(dataType.toCatalogDataType())) {
            return input;
        } else {
            return castIfNotSameType(input, dataType);
        }
    }

    /**
     * cast input type if input's datatype is not same with dateType.
     */
    public static Expression castIfNotSameType(Expression input, DataType dataType) {
        if (input.getDataType().equals(dataType)) {
            return input;
        }
        if (input instanceof Literal) {
            DataType type = input.getDataType();
            while (!type.equals(dataType)) {
                DataType promoted = type.promotion();
                if (type.equals(promoted)) {
                    break;
                }
                type = promoted;
            }
            if (type.equals(dataType)) {
                Literal promoted = DataType.promoteLiteral(((Literal) input).getValue(), dataType);
                if (promoted != null) {
                    return promoted;
                }
            }
        }
        return new Cast(input, dataType);
    }

    /**
     * characterLiteralTypeCoercionOnBinaryOperator.
     */
    @Developing
    public static <T extends BinaryOperator> T processCharacterLiteralInBinaryOperator(
            T op, Expression left, Expression right) {
        if (!(left instanceof Literal) && !(right instanceof Literal)) {
            return (T) op.withChildren(left, right);
        }
        if (left instanceof Literal && right instanceof Literal) {
            // process by constant folding
            return (T) op.withChildren(left, right);
        }
        if (left instanceof Literal && ((Literal) left).isCharacterLiteral()) {
            left = TypeCoercionUtils.characterLiteralTypeCoercion(
                    ((Literal) left).getStringValue(), right.getDataType()).orElse(left);
        }
        if (right instanceof Literal && ((Literal) right).isCharacterLiteral()) {
            right = TypeCoercionUtils.characterLiteralTypeCoercion(
                    ((Literal) right).getStringValue(), left.getDataType()).orElse(right);

        }
        return (T) op.withChildren(left, right);
    }

    /**
     * characterLiteralTypeCoercion.
     */
    @Developing
    private static Optional<Expression> characterLiteralTypeCoercion(String value, DataType dataType) {
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
                ret = new VarcharLiteral(value, ((CharType) dataType).getLen());
            } else if (dataType instanceof VarcharType) {
                ret = new VarcharLiteral(value, ((VarcharType) dataType).getLen());
            } else if (dataType instanceof StringType) {
                ret = new StringLiteral(value);
            } else if (dataType.isDateTimeV2Type()) {
                ret = new DateTimeV2Literal(value);
            } else if (dataType.isDateTimeType()) {
                ret = new DateTimeLiteral(value);
            } else if (dataType.isDateV2Type()) {
                try {
                    ret = new DateV2Literal(value);
                } catch (AnalysisException e) {
                    ret = new DateTimeV2Literal(value);
                }
            } else if (dataType.isDateType()) {
                try {
                    ret = new DateLiteral(value);
                } catch (AnalysisException e) {
                    ret = new DateTimeLiteral(value);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return Optional.ofNullable(ret);

    }

    public static Expression implicitCastInputTypes(Expression expr, List<AbstractDataType> expectedInputTypes) {
        List<Optional<DataType>> inputImplicitCastTypes
                = getInputImplicitCastTypes(expr.children(), expectedInputTypes);
        return castInputs(expr, inputImplicitCastTypes);
    }

    private static List<Optional<DataType>> getInputImplicitCastTypes(
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

    private static Expression castInputs(Expression expr, List<Optional<DataType>> castTypes) {
        return expr.withChildren((child, childIndex) -> {
            DataType argType = child.getDataType();
            Optional<DataType> castType = castTypes.get(childIndex);
            if (castType.isPresent() && !castType.get().equals(argType)) {
                return TypeCoercionUtils.castIfNotMatchType(child, castType.get());
            } else {
                return child;
            }
        });
    }

    /**
     * process divide
     */
    public static Expression processDivide(Divide divide, Expression left, Expression right) {
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        left = castIfNotSameType(left, t1);
        right = castIfNotSameType(right, t2);

        DataType commonType = DoubleType.INSTANCE;
        if (t1.isDoubleType() || t1.isFloatType() || t1.isLargeIntType()
                || t2.isDoubleType() || t2.isFloatType() || t2.isLargeIntType()) {
            // double type
        } else if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            commonType = DecimalV2Type.SYSTEM_DEFAULT;
        }

        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
        return divide.withChildren(newLeft, newRight);
    }

    /**
     * process divide
     */
    public static Expression processIntegralDivide(IntegralDivide divide, Expression left, Expression right) {
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        left = castIfNotSameType(left, t1);
        right = castIfNotSameType(right, t2);

        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, BigIntType.INSTANCE);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, BigIntType.INSTANCE);
        return divide.withChildren(newLeft, newRight);
    }

    /**
     * binary arithmetic type coercion
     */
    public static Expression processBinaryArithmetic(BinaryArithmetic binaryArithmetic,
            Expression left, Expression right) {
        // characterLiteralTypeCoercion
        // we do this because string is cast to double by default
        // but if string literal could be cast to small type, we could use smaller type than double.
        binaryArithmetic = TypeCoercionUtils.processCharacterLiteralInBinaryOperator(binaryArithmetic, left, right);

        // check string literal can cast to double
        binaryArithmetic.children().stream().filter(e -> e instanceof StringLikeLiteral)
                .forEach(expr -> {
                    try {
                        new BigDecimal(((StringLikeLiteral) expr).getStringValue());
                    } catch (NumberFormatException e) {
                        throw new IllegalStateException(String.format(
                                "string literal %s cannot be cast to double", expr.toSql()));
                    }
                });

        // 1. choose default numeric type for left and right
        DataType t1 = TypeCoercionUtils.getNumResultType(left.getDataType());
        DataType t2 = TypeCoercionUtils.getNumResultType(right.getDataType());
        // 2. find common type for left and right
        DataType commonType = t1;
        for (DataType dataType : NUMERIC_PRECEDENCE) {
            if (t1.equals(dataType) || t2.equals(dataType)) {
                commonType = dataType;
                break;
            }
        }
        if (t1 instanceof DecimalV2Type || t2 instanceof DecimalV2Type) {
            if (commonType.isFloatType() || commonType.isDoubleType() || commonType.isLargeIntType()) {
                commonType = DoubleType.INSTANCE;
            } else {
                commonType = DecimalV2Type.SYSTEM_DEFAULT;
            }
        }
        // 3. cast left and right to common type
        Expression newLeft = TypeCoercionUtils.castIfNotSameType(left, commonType);
        Expression newRight = TypeCoercionUtils.castIfNotSameType(right, commonType);
        return binaryArithmetic.withChildren(newLeft, newRight);
    }

    /**
     * process timestamp arithmetic type coercion.
     */
    public static Expression processTimestampArithmetic(TimestampArithmetic timestampArithmetic,
            Expression left, Expression right) {
        // left
        DataType leftType = left.getDataType();

        if (!leftType.isDateLikeType()) {
            if (Config.enable_date_conversion && canCastTo(leftType, DateTimeV2Type.SYSTEM_DEFAULT)) {
                leftType = DateTimeV2Type.SYSTEM_DEFAULT;
            } else if (canCastTo(leftType, DateTimeType.INSTANCE)) {
                leftType = DateTimeType.INSTANCE;
            } else {
                throw new AnalysisException("Operand '" + left.toSql()
                        + "' of timestamp arithmetic expression '" + timestampArithmetic.toSql() + "' returns type '"
                        + left.getDataType() + "'. Expected type 'TIMESTAMP/DATE/DATETIME'.");
            }
        }
        if (leftType.isDateType() && timestampArithmetic.getTimeUnit().isDateTimeUnit()) {
            leftType = DateTimeType.INSTANCE;
        }
        if (leftType.isDateV2Type() && timestampArithmetic.getTimeUnit().isDateTimeUnit()) {
            leftType = DateTimeV2Type.SYSTEM_DEFAULT;
        }
        if (!left.getDataType().isDateLikeType() && !left.getDataType().isNullType()) {
            checkCanCastTo(left.getDataType(), leftType);
            left = checkCast(left, leftType);
        }

        // right
        if (!(right.getDataType() instanceof PrimitiveType)) {
            throw new AnalysisException("the second argument must be a scalar type. but it is " + right.toSql());
        }
        if (!right.getDataType().isIntegerType()) {
            if (!ScalarType.canCastTo((ScalarType) right.getDataType().toCatalogDataType(), Type.INT)) {
                throw new AnalysisException("Operand '" + right.toSql()
                        + "' of timestamp arithmetic expression '" + timestampArithmetic.toSql() + "' returns type '"
                        + right.getDataType() + "' which is incompatible with expected type 'INT'.");
            }
            right = castIfNotSameType(right, IntegerType.INSTANCE);
        }

        return timestampArithmetic.withChildren(left, right);
    }

    /**
     * process comparison predicate type coercion.
     */
    public static Expression processComparisonPredicate(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        // same type
        if (left.getDataType().equals(right.getDataType())) {
            return comparisonPredicate.withChildren(left, right);
        }

        // process string literal with numeric
        comparisonPredicate = TypeCoercionUtils
                .processCharacterLiteralInBinaryOperator(comparisonPredicate, left, right);
        left = comparisonPredicate.left();
        right = comparisonPredicate.right();

        DataType commonType = getCommonTypeForCompare(left.getDataType(), right.getDataType());
        left = checkCast(left, commonType);
        right = checkCast(right, commonType);
        return comparisonPredicate.withChildren(left, right);
    }

    private static boolean canCastTo(DataType input, DataType target) {
        return Type.canCastTo(input.toCatalogDataType(), target.toCatalogDataType());
    }

    private static void checkCanCastTo(DataType input, DataType target) {
        if (canCastTo(input, target)) {
            return;
        }
        throw new AnalysisException("can not cast from origin type " + input + " to target type=" + target);
    }

    private static Expression checkCast(Expression input, DataType targetType) {
        checkCanCastTo(input.getDataType(), targetType);
        return castIfNotSameType(input, targetType);
    }

    private static boolean canCompareDate(DataType t1, DataType t2) {
        DataType dateType = t1;
        DataType anotherType = t2;
        if (t2.isDateLikeType()) {
            dateType = t2;
            anotherType = t1;
        }
        if (dateType.isDateLikeType() && (anotherType.isDateLikeType() || anotherType.isStringLikeType()
                || anotherType.isHllType() || anotherType.isIntegerLikeType())) {
            return true;
        }
        return false;
    }

    private static boolean maybeCastToVarchar(DataType t) {
        return t.isVarcharType() || t.isCharType() || t.isTimeType() || t.isTimeV2Type()
                || t.isHllType() || t.isBitmapType() || t.isQuantileStateType();
    }

    /**
     * get common type for comparison
     */
    private static DataType getCommonTypeForCompare(DataType leftType, DataType rightType) {
        // same type
        if (leftType.equals(rightType)) {
            return leftType;
        }

        if (leftType.isNullType()) {
            return rightType;
        }
        if (rightType.isNullType()) {
            return leftType;
        }

        // decimal v3
        if (leftType.isDecimalV3Type() && rightType.isDecimalV3Type()) {
            return DecimalV3Type.widerDecimalV3Type((DecimalV3Type) leftType, (DecimalV3Type) rightType);
        }

        // decimal v2
        if (leftType.isDecimalV2Type() && rightType.isDecimalV2Type()) {
            return DecimalV2Type.widerDecimalV2Type((DecimalV2Type) leftType, (DecimalV2Type) rightType);
        }

        // date
        if (canCompareDate(leftType, rightType)) {
            if (leftType.isDateTimeV2Type() && rightType.isDateTimeV2Type()) {
                return DateTimeV2Type.getWiderDatetimeV2Type((DateTimeV2Type) leftType, (DateTimeV2Type) rightType);
            } else if (leftType.isDateTimeV2Type()) {
                if (rightType.isIntegerLikeType()) {
                    return leftType;
                } else {
                    return DateTimeV2Type.MAX;
                }
            } else if (rightType.isDateTimeV2Type()) {
                if (rightType.isIntegerLikeType()) {
                    return rightType;
                } else {
                    return DateTimeV2Type.MAX;
                }
            } else if (leftType.isDateV2Type() && (rightType.isDateType() || rightType.isDateV2Type())) {
                return leftType;
            } else if (rightType.isDateV2Type() && (leftType.isDateType() || leftType.isDateV2Type())) {
                return rightType;
            } else {
                return DateTimeType.INSTANCE;
            }
        }

        // varchar-like vs varchar-like
        if (maybeCastToVarchar(leftType) && maybeCastToVarchar(rightType)) {
            return VarcharType.SYSTEM_DEFAULT;
        }

        // varchar-like vs string
        if ((maybeCastToVarchar(leftType) && rightType.isStringLikeType())
                || (maybeCastToVarchar(rightType) && leftType.isStringLikeType())) {
            return StringType.INSTANCE;
        }

        // numeric
        if (leftType.isNumericType() && rightType.isNumericType()) {
            DataType commonType = leftType;
            for (DataType dataType : NUMERIC_PRECEDENCE) {
                if (leftType.equals(dataType) || rightType.equals(dataType)) {
                    commonType = dataType;
                    break;
                }
            }
            if (leftType instanceof DecimalV2Type || rightType instanceof DecimalV2Type) {
                if (commonType instanceof DoubleType || commonType instanceof FloatType
                        || commonType instanceof LargeIntType) {
                    return DoubleType.INSTANCE;
                } else if (leftType instanceof DecimalV2Type) {
                    return DecimalV2Type.widerDecimalV2Type(
                            (DecimalV2Type) leftType, DecimalV2Type.forType(rightType));
                } else {
                    return DecimalV2Type.widerDecimalV2Type(
                            DecimalV2Type.forType(leftType), (DecimalV2Type) rightType);
                }
            }
            return commonType;
        }

        return DoubleType.INSTANCE;
    }

    /**
     * two types' common type, see {@link TypeCoercionUtilsTest#testFindPrimitiveCommonType()}
     */
    @VisibleForTesting
    protected static Optional<DataType> findPrimitiveCommonType(DataType t1, DataType t2) {
        if (!(t1 instanceof PrimitiveType) || !(t2 instanceof PrimitiveType)) {
            return Optional.empty();
        }

        if (t1.equals(t2)) {
            return Optional.of(t1);
        }

        if (t1.isNullType()) {
            return Optional.of(t2);
        }
        if (t2.isNullType()) {
            return Optional.of(t1);
        }

        // objectType only support compare with itself, so return empty here.
        if (t1.isObjectType() || t2.isObjectType()) {
            return Optional.empty();
        }

        // TODO: support ALL type

        // string-like vs all other type
        if (t1.isStringLikeType() || t2.isStringLikeType()) {
            if ((t1.isCharType() || t1.isVarcharType()) && (t2.isCharType() || t2.isVarcharType())) {
                int len = Math.max(((CharacterType) t1).getLen(), ((CharacterType) t2).getLen());
                if (((CharacterType) t1).getLen() < 0 || ((CharacterType) t2).getLen() < 0) {
                    len = VarcharType.SYSTEM_DEFAULT.getLen();
                }
                return Optional.of(VarcharType.createVarcharType(len));
            }
            return Optional.of(StringType.INSTANCE);
        }

        // forbidden decimal with date
        if ((t1.isDecimalV2Type() && t2.isDateType()) || (t2.isDecimalV2Type() && t1.isDateType())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV2Type() && t2.isDateV2Type()) || (t2.isDecimalV2Type() && t1.isDateV2Type())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV3Type() && t2.isDateType()) || (t2.isDecimalV3Type() && t1.isDateType())) {
            return Optional.empty();
        }
        if ((t1.isDecimalV3Type() && t2.isDateV2Type()) || (t2.isDecimalV3Type() && t1.isDateV2Type())) {
            return Optional.empty();
        }

        // decimal precision derive
        if (t1.isDecimalV2Type() && t2.isDecimalV2Type()) {
            return Optional.of(DecimalV2Type.widerDecimalV2Type((DecimalV2Type) t1, (DecimalV2Type) t2));
        }

        if (t1.isDecimalV3Type() && t2.isDecimalV3Type()) {
            return Optional.of(DecimalV3Type.widerDecimalV3Type((DecimalV3Type) t1, (DecimalV3Type) t2));
        }

        // decimal v3 vs all other type
        if (t1.isDecimalV3Type() || t2.isDecimalV3Type()) {
            // decimal vs float, double or large-int
            if (t1.isDoubleType() || t1.isFloatType() || t1.isLargeIntType()
                    || t2.isDoubleType() || t2.isFloatType() || t2.isLargeIntType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
            // decimal with other numeric types
            return Optional.of(t1.isDecimalV3Type()
                    ? DecimalV3Type.widerDecimalV3Type((DecimalV3Type) t1, DecimalV3Type.forType(t2))
                    : DecimalV3Type.widerDecimalV3Type((DecimalV3Type) t2, DecimalV3Type.forType(t1)));
        }

        // decimal v2 vs all other type
        if (t1.isDecimalV2Type() || t2.isDecimalV2Type()) {
            if (t1.isDoubleType() || t1.isFloatType() || t1.isLargeIntType()
                    || t2.isDoubleType() || t2.isFloatType() || t2.isLargeIntType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
            return Optional.of(t1.isDecimalV2Type()
                    ? DecimalV2Type.widerDecimalV2Type((DecimalV2Type) t1, DecimalV2Type.forType(t2))
                    : DecimalV2Type.widerDecimalV2Type((DecimalV2Type) t2, DecimalV2Type.forType(t1)));
        }

        // date-like type
        if (t1.isDateTimeV2Type() && t2.isDateTimeV2Type()) {
            return Optional.of(DateTimeV2Type.getWiderDatetimeV2Type((DateTimeV2Type) t1, (DateTimeV2Type) t2));
        }
        if (t1.isDateLikeType() && t2.isDateLikeType()) {
            if (t1.isDateTimeV2Type()) {
                return Optional.of(t1);
            }
            if (t2.isDateTimeV2Type()) {
                return Optional.of(t2);
            }
            if (t1.isDateV2Type() || t2.isDateV2Type()) {
                // datev2 vs datetime
                if (t1.isDateTimeType() || t2.isDateTimeType()) {
                    return Optional.of(DateTimeV2Type.SYSTEM_DEFAULT);
                }
                // datev2 vs date
                return Optional.of(DateV2Type.INSTANCE);
            }
            // date vs datetime
            return Optional.of(DateTimeType.INSTANCE);
        }
        if (t1.isDateLikeType() || t2.isDateLikeType()) {
            DataType dateType = t1;
            DataType otherType = t2;
            if (t2.isDateLikeType()) {
                dateType = t2;
                otherType = t1;
            }
            if (dateType.isDateType() || dateType.isDateV2Type()) {
                if (otherType.isIntegerType() || otherType.isBigIntType() || otherType.isLargeIntType()) {
                    return Optional.of(otherType);
                }
            }
            if (dateType.isDateTimeType() || dateType.isDateTimeV2Type()) {
                if (otherType.isLargeIntType() || otherType.isDoubleType()) {
                    return Optional.of(otherType);
                }
            }
            return Optional.empty();
        }

        // time-like vs all other type
        if (t1.isTimeLikeType() && t2.isTimeLikeType()) {
            if (t1.isTimeType() && t2.isTimeType()) {
                return Optional.of(TimeType.INSTANCE);
            }
            return Optional.of(TimeV2Type.INSTANCE);
        }
        if (t1.isTimeLikeType() || t2.isTimeLikeType()) {
            if (t1.isNumericType() || t2.isNumericType() || t1.isBooleanType() || t2.isBooleanType()) {
                return Optional.of(DoubleType.INSTANCE);
            }
            return Optional.empty();
        }

        // string-like, null, objected, decimal, date-like and time already processed
        // so only need to process numeric type without decimal plus boolean.
        if ((t1.isFloatType() && t2.isLargeIntType()) || (t1.isLargeIntType() && t2.isFloatType())) {
            return Optional.of(DoubleType.INSTANCE);
        }
        for (DataType dataType : NUMERIC_PRECEDENCE) {
            if (t1.equals(dataType) || t2.equals(dataType)) {
                return Optional.of(dataType);
            }
        }

        return Optional.empty();
    }

    /**
     * add json type info as the last argument of the function.
     *
     * @param function function need to add json type info
     * @param checkKey check key not null
     * @return function already processed
     */
    public static BoundFunction fillJsonTypeArgument(BoundFunction function, boolean checkKey) {
        List<Expression> arguments = function.getArguments();
        try {
            List<Expression> newArguments = Lists.newArrayList();
            StringBuilder jsonTypeStr = new StringBuilder();
            for (int i = 0; i < arguments.size(); i++) {
                Expression argument = arguments.get(i);
                Type type = argument.getDataType().toCatalogDataType();
                int jsonType = FunctionCallExpr.computeJsonDataType(type);
                jsonTypeStr.append(jsonType);

                if (type.isNull()) {
                    if ((i & 1) == 0 && checkKey) {
                        throw new AnalysisException(function.getName() + " key can't be NULL: " + function.toSql());
                    }
                    // Not to return NULL directly, so save string, but flag is '0'
                    newArguments.add(new org.apache.doris.nereids.trees.expressions.literal.StringLiteral("NULL"));
                } else {
                    newArguments.add(argument);
                }
            }
            // add json type string to the last
            newArguments.add(new org.apache.doris.nereids.trees.expressions.literal.StringLiteral(
                    jsonTypeStr.toString()));
            return (BoundFunction) function.withChildren(newArguments);
        } catch (Throwable t) {
            throw new AnalysisException(t.getMessage());
        }
    }
}
