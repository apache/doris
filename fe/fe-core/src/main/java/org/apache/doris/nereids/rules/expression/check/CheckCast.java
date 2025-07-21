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

package org.apache.doris.nereids.rules.expression.check;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.IPv4Type;
import org.apache.doris.nereids.types.IPv6Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * check cast valid
 */
public class CheckCast implements ExpressionPatternRuleFactory {
    public static CheckCast INSTANCE = new CheckCast();
    private static Map<Class<? extends DataType>, Set<Class<? extends DataType>>> strictForbiddenCast;
    private static Map<Class<? extends DataType>, Set<Class<? extends DataType>>> unStrictAllowedCast;

    static {
        Set<Class<? extends DataType>> forbiddenTypes = Sets.newHashSet();
        strictForbiddenCast = Maps.newHashMap();
        unStrictAllowedCast = Maps.newHashMap();

        /*------------------------------FOR STRICT CAST--------------------------------------*/
        // Boolean
        forbiddenTypes.add(DateType.class);
        forbiddenTypes.add(DateV2Type.class);
        forbiddenTypes.add(DateTimeType.class);
        forbiddenTypes.add(DateTimeV2Type.class);
        forbiddenTypes.add(TimeV2Type.class);
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(BooleanType.class, forbiddenTypes);

        // TinyInt
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(TinyIntType.class, forbiddenTypes);

        // SmallInt
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(SmallIntType.class, forbiddenTypes);

        // Integer
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(IntegerType.class, forbiddenTypes);

        // BigInt
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(BigIntType.class, forbiddenTypes);

        // LargeInt
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(LargeIntType.class, forbiddenTypes);

        // Float
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(FloatType.class, forbiddenTypes);

        // Double
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(DoubleType.class, forbiddenTypes);

        // Decimal
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
        strictForbiddenCast.put(DecimalV2Type.class, forbiddenTypes);
        strictForbiddenCast.put(DecimalV3Type.class, forbiddenTypes);

        // Date
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(BooleanType.class);
        forbiddenTypes.add(TinyIntType.class);
        forbiddenTypes.add(SmallIntType.class);
        forbiddenTypes.add(FloatType.class);
        forbiddenTypes.add(DoubleType.class);
        forbiddenTypes.add(DecimalV2Type.class);
        forbiddenTypes.add(DecimalV3Type.class);
        forbiddenTypes.add(TimeV2Type.class);
        strictForbiddenCast.put(DateType.class, forbiddenTypes);
        strictForbiddenCast.put(DateV2Type.class, forbiddenTypes);

        // DateTime
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(BooleanType.class);
        forbiddenTypes.add(TinyIntType.class);
        forbiddenTypes.add(SmallIntType.class);
        forbiddenTypes.add(IntegerType.class);
        forbiddenTypes.add(FloatType.class);
        forbiddenTypes.add(DoubleType.class);
        forbiddenTypes.add(DecimalV2Type.class);
        forbiddenTypes.add(DecimalV3Type.class);
        strictForbiddenCast.put(DateTimeType.class, forbiddenTypes);
        strictForbiddenCast.put(DateTimeV2Type.class, forbiddenTypes);

        // Time
        forbiddenTypes = Sets.newHashSet();
        forbiddenTypes.add(BooleanType.class);
        forbiddenTypes.add(FloatType.class);
        forbiddenTypes.add(DoubleType.class);
        forbiddenTypes.add(DecimalV2Type.class);
        forbiddenTypes.add(DecimalV3Type.class);
        strictForbiddenCast.put(TimeV2Type.class, forbiddenTypes);

        // IPV4
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        forbiddenTypes.remove(IPv4Type.class);
        forbiddenTypes.remove(IPv6Type.class);
        strictForbiddenCast.put(IPv4Type.class, forbiddenTypes);

        // IPV6
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        forbiddenTypes.remove(IPv6Type.class);
        strictForbiddenCast.put(IPv6Type.class, forbiddenTypes);

        // bitmap
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        strictForbiddenCast.put(BitmapType.class, forbiddenTypes);

        // hll
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        strictForbiddenCast.put(HllType.class, forbiddenTypes);

        // array
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        strictForbiddenCast.put(ArrayType.class, forbiddenTypes);

        // map
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        strictForbiddenCast.put(MapType.class, forbiddenTypes);

        // struct
        forbiddenTypes = Sets.newHashSet();
        forbiddenAll(forbiddenTypes);
        strictForbiddenCast.put(StructType.class, forbiddenTypes);

        // json
        // forbiddenTypes = Sets.newHashSet();
        // forbiddenAll(forbiddenTypes);
        // strictForbiddenCast.put(JsonType.class, forbiddenTypes);

        // variant
        // forbiddenTypes = Sets.newHashSet();
        // forbiddenAll(forbiddenTypes);
        // strictForbiddenCast.put(VariantType.class, forbiddenTypes);

        /*------------------------------FOR UN-STRICT CAST--------------------------------------*/
        // Date
        Set<Class<? extends DataType>> allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictAllowedCast.put(DateType.class, allowedTypes);
        unStrictAllowedCast.put(DateV2Type.class, allowedTypes);

        // DateTime
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictAllowedCast.put(DateTimeType.class, allowedTypes);
        unStrictAllowedCast.put(DateTimeV2Type.class, allowedTypes);

        // Time
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictAllowedCast.put(TimeV2Type.class, allowedTypes);
    }

    private static void forbiddenAll(Set<Class<? extends DataType>> forbiddenTypes) {
        forbiddenTypes.add(BooleanType.class);
        forbiddenTypes.add(TinyIntType.class);
        forbiddenTypes.add(SmallIntType.class);
        forbiddenTypes.add(IntegerType.class);
        forbiddenTypes.add(BigIntType.class);
        forbiddenTypes.add(LargeIntType.class);
        forbiddenTypes.add(FloatType.class);
        forbiddenTypes.add(DoubleType.class);
        forbiddenTypes.add(DecimalV2Type.class);
        forbiddenTypes.add(DecimalV3Type.class);
        forbiddenTypes.add(DateType.class);
        forbiddenTypes.add(DateV2Type.class);
        forbiddenTypes.add(DateTimeType.class);
        forbiddenTypes.add(DateTimeV2Type.class);
        forbiddenTypes.add(TimeV2Type.class);
        forbiddenTypes.add(IPv4Type.class);
        forbiddenTypes.add(IPv6Type.class);
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).then(CheckCast::check).toRule(ExpressionRuleType.CHECK_CAST)
        );
    }

    private static Expression check(Cast cast) {
        DataType originalType = cast.child().getDataType();
        DataType targetType = cast.getDataType();
        if (!check(originalType, targetType)) {
            throw new AnalysisException("cannot cast " + originalType.toSql() + " to " + targetType.toSql());
        }
        return cast;
    }

    protected static boolean check(DataType originalType, DataType targetType) {
        if (originalType.isVariantType() && (targetType instanceof PrimitiveType || targetType.isArrayType())) {
            // variant could cast to primitive types and array
            return true;
        }
        if (originalType.isNullType()) {
            return true;
        }
        if (originalType.equals(targetType)) {
            return true;
        }
        // New check strict and un-strict cast logic, the check logic is not completed yet.
        // So for now, if the new check logic return false,
        // we return false to disable this cast, otherwise, we still go through the old check logic.
        if (strictForbiddenCast.containsKey(originalType.getClass())
                && strictForbiddenCast.get(originalType.getClass()).contains(targetType.getClass())) {
            if (SessionVariable.enableStrictCast()) {
                return false;
            }
            if (!unStrictAllowedCast.containsKey(originalType.getClass())
                    || !unStrictAllowedCast.get(originalType.getClass()).contains(targetType.getClass())) {
                return false;
            }
        }
        // The following code is old check logic.
        if (originalType instanceof CharacterType && !(targetType instanceof PrimitiveType)) {
            return true;
        }
        if (originalType instanceof AggStateType && targetType instanceof CharacterType) {
            return true;
        }
        if (originalType instanceof ArrayType && targetType instanceof ArrayType) {
            return check(((ArrayType) originalType).getItemType(), ((ArrayType) targetType).getItemType());
        } else if (originalType instanceof MapType && targetType instanceof MapType) {
            return check(((MapType) originalType).getKeyType(), ((MapType) targetType).getKeyType())
                    && check(((MapType) originalType).getValueType(), ((MapType) targetType).getValueType());
        } else if (originalType instanceof StructType && targetType instanceof StructType) {
            List<StructField> targetFields = ((StructType) targetType).getFields();
            List<StructField> originalFields = ((StructType) originalType).getFields();
            if (targetFields.size() != originalFields.size()) {
                return false;
            }
            for (int i = 0; i < targetFields.size(); i++) {
                if (originalFields.get(i).isNullable() != targetFields.get(i).isNullable()) {
                    return false;
                }
                if (!check(originalFields.get(i).getDataType(), targetFields.get(i).getDataType())) {
                    return false;
                }
            }
            return true;
        } else if (originalType instanceof JsonType || targetType instanceof JsonType) {
            return !originalType.isComplexType() || checkMapKeyIsStringLikeForJson(originalType);
        } else {
            return checkPrimitiveType(originalType, targetType);
        }
    }

    private static boolean strictCheck(DataType originalType, DataType targetType) {
        // date type couldn't cast to tinyint, smallint, float, double, decimal and time.
        if ((originalType.isDateType() || originalType.isDateV2Type())
                && (targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isDecimalLikeType()
                || targetType.isFloatLikeType() || targetType.isTimeType())) {
            return false;
        }
        // datetime type couldn't cast to tinyint, smallint, int, float, double and decimal.
        if ((originalType.isDateTimeType() || originalType.isDateTimeV2Type())
                && (targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isIntegerType()
                || targetType.isFloatLikeType() || targetType.isDecimalLikeType())) {
            return false;
        }
        // time type couldn't cast to float, double and decimal.
        if (originalType.isTimeType() && (targetType.isFloatLikeType() || targetType.isDecimalLikeType())) {
            return false;
        }
        // TODO: handle json and variant
        return true;
    }

    private static boolean unStrictCheck(DataType originalType, DataType targetType) {
        // date type couldn't cast to tinyint, smallint, decimal and time.
        if ((originalType.isDateType() || originalType.isDateV2Type())
                && (targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isDecimalLikeType()
                || targetType.isTimeType())) {
            return false;
        }
        // datetime type couldn't cast to tinyint, smallint, int and decimal.
        if ((originalType.isDateTimeType() || originalType.isDateTimeV2Type())
                && (targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isIntegerType()
                || targetType.isDecimalLikeType())) {
            return false;
        }
        // time type couldn't cast to decimal.
        if (originalType.isTimeType() && targetType.isDecimalLikeType()) {
            return false;
        }
        // TODO: handle json and variant
        return true;
    }

    /**
     * forbid this original and target type
     *   1. original type is object type
     *   2. target type is object type
     *   3. original type is same with target type
     *   4. target type is null type
     */
    private static boolean checkPrimitiveType(DataType originalType, DataType targetType) {
        if (!originalType.isPrimitive() || !targetType.isPrimitive()) {
            return false;
        }
        if (originalType.equals(targetType)) {
            return false;
        }
        if (originalType.isNullType()) {
            return true;
        }
        if (originalType.isObjectType() || targetType.isObjectType()) {
            return false;
        }
        if (targetType.isNullType()) {
            return false;
        }
        return true;
    }

    /**
     * check if complexType type which contains map, make sure key is string like for json
     *
     * @param complexType need to check
     * @return true if complexType can cast to json
     */
    public static boolean checkMapKeyIsStringLikeForJson(DataType complexType) {
        if (complexType.isMapType()) {
            return ((MapType) complexType).getKeyType().isStringLikeType();
        } else if (complexType.isArrayType()) {
            return checkMapKeyIsStringLikeForJson(((ArrayType) complexType).getItemType());
        } else if (complexType.isStructType()) {
            for (StructField f : ((StructType) complexType).getFields()) {
                return checkMapKeyIsStringLikeForJson(f.getDataType());
            }
        }
        return true;
    }
}
