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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
            LargeIntType.INSTANCE,
            FloatType.INSTANCE,
            BigIntType.INSTANCE,
            IntegerType.INSTANCE,
            SmallIntType.INSTANCE,
            TinyIntType.INSTANCE
    );

    /**
     * Return Optional.empty() if cannot do implicit cast.
     * TODO: datetime and date type
     */
    @Developing
    public static Optional<DataType> implicitCast(DataType input, AbstractDataType expected) {
        DataType returnType = null;
        if (expected.acceptsType(input)) {
            // If the expected type is already a parent of the input type, no need to cast.
            return Optional.of(input);
        }
        if (expected instanceof TypeCollection) {
            TypeCollection typeCollection = (TypeCollection) expected;
            // use origin datatype first. use implicit cast instead if origin type cannot be accepted.
            return Stream.<Supplier<Optional<DataType>>>of(
                            () -> typeCollection.getTypes().stream()
                                    .filter(e -> e.acceptsType(input))
                                    .map(e -> input)
                                    .findFirst(),
                            () -> typeCollection.getTypes().stream()
                                    .map(e -> implicitCast(input, e))
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .findFirst())
                    .map(Supplier::get)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();
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
        } else if (input.isDate()) {
            if (expected instanceof DateTimeType) {
                returnType = expected.defaultConcreteType();
            }
        }

        if (returnType == null && input instanceof PrimitiveType
                && expected instanceof CharacterType) {
            returnType = StringType.INSTANCE;
        }

        // could not do implicit cast, just return null. Throw exception in check analysis.
        return Optional.ofNullable(returnType);
    }

    /**
     * return ture if two type could do type coercion.
     */
    public static boolean canHandleTypeCoercion(DataType leftType, DataType rightType) {
        if (leftType instanceof DecimalV2Type && rightType instanceof NullType) {
            return true;
        }
        if (leftType instanceof NullType && rightType instanceof DecimalV2Type) {
            return true;
        }
        if (leftType instanceof DecimalV2Type && rightType instanceof IntegralType
                || leftType instanceof IntegralType && rightType instanceof DecimalV2Type) {
            return true;
        }
        // TODO: add decimal promotion support
        if (!(leftType instanceof DecimalV2Type)
                && !(rightType instanceof DecimalV2Type)
                && !leftType.equals(rightType)) {
            return true;
        }
        return false;
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
     * find the tightest common type for two type
     */
    @Developing
    public static Optional<DataType> findTightestCommonType(DataType left, DataType right) {
        // TODO: compatible with origin planner and BE
        // TODO: when add new type, add it to here
        DataType tightestCommonType = null;
        if (left.equals(right)) {
            tightestCommonType = left;
        } else if (left instanceof NullType) {
            tightestCommonType = right;
        } else if (right instanceof NullType) {
            tightestCommonType = left;
        } else if (left instanceof IntegralType && right instanceof DecimalV2Type
                && ((DecimalV2Type) right).isWiderThan(left)) {
            tightestCommonType = right;
        } else if (right instanceof IntegralType && left instanceof DecimalV2Type
                && ((DecimalV2Type) left).isWiderThan(right)) {
            tightestCommonType = left;
        } else if (left instanceof NumericType && right instanceof NumericType
                && !(left instanceof DecimalV2Type) && !(right instanceof DecimalV2Type)) {
            for (DataType dataType : NUMERIC_PRECEDENCE) {
                if (dataType.equals(left) || dataType.equals(right)) {
                    tightestCommonType = dataType;
                    break;
                }
            }
        } else if (left instanceof CharacterType && right instanceof CharacterType) {
            tightestCommonType = CharacterType.widerCharacterType((CharacterType) left, (CharacterType) right);
        } else if (left instanceof CharacterType || right instanceof CharacterType) {
            tightestCommonType = StringType.INSTANCE;
        } else if (left instanceof DecimalV2Type && right instanceof IntegralType) {
            tightestCommonType = DecimalV2Type.widerDecimalV2Type((DecimalV2Type) left, DecimalV2Type.forType(right));
        } else if (left instanceof IntegralType && right instanceof DecimalV2Type) {
            tightestCommonType = DecimalV2Type.widerDecimalV2Type((DecimalV2Type) right, DecimalV2Type.forType(left));
        } else if (left instanceof DateLikeType && right instanceof DateLikeType) {
            if (left instanceof DateTimeV2Type && right instanceof DateTimeV2Type) {
                if (((DateTimeV2Type) left).getScale() > ((DateTimeV2Type) right).getScale()) {
                    tightestCommonType = left;
                } else {
                    tightestCommonType = right;
                }
            } else if (left instanceof DateTimeV2Type) {
                tightestCommonType = left;
            } else if (right instanceof DateTimeV2Type) {
                tightestCommonType = right;
            } else if (left instanceof DateTimeType || right instanceof DateTimeType) {
                tightestCommonType = DateTimeType.INSTANCE;
            } else if (left instanceof DateV2Type || right instanceof DateV2Type) {
                tightestCommonType = DateV2Type.INSTANCE;
            }
        }
        return Optional.ofNullable(tightestCommonType);
    }

    /**
     * The type used for arithmetic operations.
     */
    public static DataType getNumResultType(DataType type) {
        if (type.isTinyIntType() || type.isSmallIntType() || type.isIntType() || type.isBigIntType()) {
            return BigIntType.INSTANCE;
        } else if (type.isLargeIntType()) {
            return LargeIntType.INSTANCE;
        } else if (type.isFloatType() || type.isDoubleType() || type.isStringType()) {
            return DoubleType.INSTANCE;
        } else if (type.isDecimalType()) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        } else if (type.isNullType()) {
            return NullType.INSTANCE;
        }
        throw new AnalysisException("no found appropriate data type.");
    }

    /**
     * The common type used by arithmetic operations.
     */
    public static DataType findCommonNumericsType(DataType t1, DataType t2) {
        if (t1.isDoubleType() || t2.isDoubleType()) {
            return DoubleType.INSTANCE;
        } else if (t1.isDecimalType() || t2.isDecimalType()) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        } else if (t1.isLargeIntType() || t2.isLargeIntType()) {
            return LargeIntType.INSTANCE;
        } else {
            return BigIntType.INSTANCE;
        }
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
                        () -> findTightestCommonType(left, right),
                        () -> findWiderTypeForDecimal(left, right),
                        () -> characterPromotion(left, right),
                        () -> findTypeForComplex(left, right))
                .map(Supplier::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    /**
     * find wider type for two type that at least one of that is decimal.
     */
    @Developing
    public static Optional<DataType> findWiderTypeForDecimal(DataType left, DataType right) {
        DataType commonType = null;
        if (left instanceof DecimalV2Type && right instanceof DecimalV2Type) {
            commonType = DecimalV2Type.widerDecimalV2Type((DecimalV2Type) left, (DecimalV2Type) right);
        } else if (left instanceof IntegralType && right instanceof DecimalV2Type) {
            commonType = DecimalV2Type.widerDecimalV2Type(DecimalV2Type.forType(left), (DecimalV2Type) right);
        } else if (left instanceof DecimalV2Type && right instanceof IntegralType) {
            commonType = DecimalV2Type.widerDecimalV2Type((DecimalV2Type) left, DecimalV2Type.forType(right));
        } else if ((left instanceof FractionalType && right instanceof DecimalV2Type)
                || (left instanceof DecimalV2Type && right instanceof FractionalType)) {
            commonType = DoubleType.INSTANCE;
        }
        return Optional.ofNullable(commonType);
    }

    /**
     * do type promotion for two type that at least one of them is CharacterType.
     */
    @Developing
    public static Optional<DataType> characterPromotion(DataType left, DataType right) {
        // TODO: need to rethink how to handle char and varchar to return char or varchar as much as possible.
        if (left instanceof CharacterType && right instanceof PrimitiveType && !(right instanceof BooleanType)) {
            return Optional.of(StringType.INSTANCE);
        }
        if (left instanceof PrimitiveType && !(left instanceof BooleanType) && right instanceof CharacterType) {
            return Optional.of(StringType.INSTANCE);
        }
        return Optional.empty();
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
     * cast input type if input's datatype is not same with dateType.
     */
    public static Expression castIfNotSameType(Expression input, DataType dataType) {
        if (input.getDataType().equals(dataType)) {
            return input;
        } else {
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
                    Literal promoted = DataType.promoteNumberLiteral(((Literal) input).getValue(), dataType);
                    if (promoted != null) {
                        return promoted;
                    }
                }
            }
            return new Cast(input, dataType);
        }
    }
}
