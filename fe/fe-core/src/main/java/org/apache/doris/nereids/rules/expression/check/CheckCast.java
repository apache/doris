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
import org.apache.doris.nereids.types.CharType;
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
import org.apache.doris.nereids.types.QuantileStateType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

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
    private static final Map<Class<? extends DataType>, Set<Class<? extends DataType>>> strictCastWhiteList;
    private static final Map<Class<? extends DataType>, Set<Class<? extends DataType>>> unStrictCastWhiteList;

    static {
        Set<Class<? extends DataType>> allowedTypes = Sets.newHashSet();
        strictCastWhiteList = Maps.newHashMap();
        unStrictCastWhiteList = Maps.newHashMap();

        /*------------------------------FOR STRICT CAST--------------------------------------*/
        // Boolean
        allowToBasicType(allowedTypes);
        allowedTypes.remove(DateType.class);
        allowedTypes.remove(DateV2Type.class);
        allowedTypes.remove(DateTimeType.class);
        allowedTypes.remove(DateTimeV2Type.class);
        allowedTypes.remove(TimeV2Type.class);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(BooleanType.class, allowedTypes);

        // Numeric
        allowedTypes = Sets.newHashSet();
        allowToBasicType(allowedTypes);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(TinyIntType.class, allowedTypes);
        strictCastWhiteList.put(SmallIntType.class, allowedTypes);
        strictCastWhiteList.put(IntegerType.class, allowedTypes);
        strictCastWhiteList.put(BigIntType.class, allowedTypes);
        strictCastWhiteList.put(LargeIntType.class, allowedTypes);
        strictCastWhiteList.put(FloatType.class, allowedTypes);
        strictCastWhiteList.put(DoubleType.class, allowedTypes);
        strictCastWhiteList.put(DecimalV2Type.class, allowedTypes);
        strictCastWhiteList.put(DecimalV3Type.class, allowedTypes);

        // Date
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(IntegerType.class);
        allowedTypes.add(BigIntType.class);
        allowedTypes.add(LargeIntType.class);
        allowedTypes.add(DateType.class);
        allowedTypes.add(DateV2Type.class);
        allowedTypes.add(DateTimeType.class);
        allowedTypes.add(DateTimeV2Type.class);
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(DateType.class, allowedTypes);
        strictCastWhiteList.put(DateV2Type.class, allowedTypes);

        // DateTime
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(BigIntType.class);
        allowedTypes.add(LargeIntType.class);
        allowedTypes.add(DateType.class);
        allowedTypes.add(DateV2Type.class);
        allowedTypes.add(DateTimeType.class);
        allowedTypes.add(DateTimeV2Type.class);
        allowedTypes.add(TimeV2Type.class);
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(DateTimeType.class, allowedTypes);
        strictCastWhiteList.put(DateTimeV2Type.class, allowedTypes);

        // Time
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(TinyIntType.class);
        allowedTypes.add(SmallIntType.class);
        allowedTypes.add(IntegerType.class);
        allowedTypes.add(BigIntType.class);
        allowedTypes.add(LargeIntType.class);
        allowedTypes.add(DateType.class);
        allowedTypes.add(DateV2Type.class);
        allowedTypes.add(DateTimeType.class);
        allowedTypes.add(DateTimeV2Type.class);
        allowedTypes.add(TimeV2Type.class);
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(TimeV2Type.class, allowedTypes);

        // Char, Varchar, String
        allowedTypes = Sets.newHashSet();
        allowToBasicType(allowedTypes);
        allowedTypes.add(IPv4Type.class);
        allowedTypes.add(IPv6Type.class);
        allowToComplexType(allowedTypes);
        allowedTypes.remove(HllType.class);
        allowedTypes.remove(BitmapType.class);
        allowedTypes.remove(QuantileStateType.class);
        strictCastWhiteList.put(CharType.class, allowedTypes);
        strictCastWhiteList.put(VarcharType.class, allowedTypes);
        strictCastWhiteList.put(StringType.class, allowedTypes);

        // IPV4
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(IPv4Type.class);
        allowedTypes.add(IPv6Type.class);
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(IPv4Type.class, allowedTypes);

        // IPV6
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(IPv6Type.class);
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(IPv6Type.class, allowedTypes);

        // bitmap
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(BitmapType.class);
        strictCastWhiteList.put(BitmapType.class, allowedTypes);

        // hll
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(HllType.class);
        strictCastWhiteList.put(HllType.class, allowedTypes);

        // quantile
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(QuantileStateType.class);
        strictCastWhiteList.put(QuantileStateType.class, allowedTypes);

        // array
        allowedTypes = Sets.newHashSet();
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(ArrayType.class);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(ArrayType.class, allowedTypes);

        // map
        allowedTypes = Sets.newHashSet();
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(MapType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(MapType.class, allowedTypes);

        // struct
        allowedTypes = Sets.newHashSet();
        allowToStringLikeType(allowedTypes);
        allowedTypes.add(StructType.class);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(StructType.class, allowedTypes);

        // json
        allowedTypes = Sets.newHashSet();
        allowToBasicType(allowedTypes);
        allowedTypes.remove(DateType.class);
        allowedTypes.remove(DateV2Type.class);
        allowedTypes.remove(DateTimeType.class);
        allowedTypes.remove(DateTimeV2Type.class);
        allowedTypes.remove(TimeV2Type.class);
        allowedTypes.add(ArrayType.class);
        allowedTypes.add(StructType.class);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(VariantType.class);
        strictCastWhiteList.put(JsonType.class, allowedTypes);

        // variant
        allowedTypes = Sets.newHashSet();
        allowToBasicType(allowedTypes);
        allowToComplexType(allowedTypes);
        allowedTypes.remove(JsonType.class);
        strictCastWhiteList.put(VariantType.class, allowedTypes);

        /*------------------------------FOR UN-STRICT CAST--------------------------------------*/
        // Date
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictCastWhiteList.put(DateType.class, allowedTypes);
        unStrictCastWhiteList.put(DateV2Type.class, allowedTypes);

        // DateTime
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictCastWhiteList.put(DateTimeType.class, allowedTypes);
        unStrictCastWhiteList.put(DateTimeV2Type.class, allowedTypes);

        // Time
        allowedTypes = Sets.newHashSet();
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        unStrictCastWhiteList.put(TimeV2Type.class, allowedTypes);
    }

    private static void allowToBasicType(Set<Class<? extends DataType>> allowedTypes) {
        allowedTypes.add(BooleanType.class);
        allowedTypes.add(TinyIntType.class);
        allowedTypes.add(SmallIntType.class);
        allowedTypes.add(IntegerType.class);
        allowedTypes.add(BigIntType.class);
        allowedTypes.add(LargeIntType.class);
        allowedTypes.add(FloatType.class);
        allowedTypes.add(DoubleType.class);
        allowedTypes.add(DecimalV2Type.class);
        allowedTypes.add(DecimalV3Type.class);
        allowedTypes.add(DateType.class);
        allowedTypes.add(DateV2Type.class);
        allowedTypes.add(DateTimeType.class);
        allowedTypes.add(DateTimeV2Type.class);
        allowedTypes.add(TimeV2Type.class);
        allowToStringLikeType(allowedTypes);
    }

    private static void allowToStringLikeType(Set<Class<? extends DataType>> allowedTypes) {
        allowedTypes.add(CharType.class);
        allowedTypes.add(VarcharType.class);
        allowedTypes.add(StringType.class);
    }

    private static void allowToComplexType(Set<Class<? extends DataType>> allowedTypes) {
        allowedTypes.add(BitmapType.class);
        allowedTypes.add(HllType.class);
        allowedTypes.add(JsonType.class);
        allowedTypes.add(ArrayType.class);
        allowedTypes.add(MapType.class);
        allowedTypes.add(StructType.class);
        allowedTypes.add(VariantType.class);
        allowedTypes.add(AggStateType.class);
        allowedTypes.add(QuantileStateType.class);
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).thenApply(ctx -> {
                    Cast cast = ctx.expr;
                    DataType originalType = cast.child().getDataType();
                    DataType targetType = cast.getDataType();
                    if (!check(originalType, targetType, ctx.cascadesContext.getConnectContext()
                            .getSessionVariable().enableStrictCast)) {
                        throw new AnalysisException("cannot cast " + originalType.toSql()
                                + " to " + targetType.toSql());
                    }
                    return cast;
                }).toRule(ExpressionRuleType.CHECK_CAST)
        );
    }

    protected static boolean check(DataType originalType, DataType targetType, boolean isStrictMode) {
        if (originalType.isVariantType() && targetType.isVariantType()) {
            return originalType.equals(targetType);
        }
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
        if (!strictCastWhiteList.containsKey(originalType.getClass())
                || !strictCastWhiteList.get(originalType.getClass()).contains(targetType.getClass())) {
            if (isStrictMode) {
                return false;
            } else if (!unStrictCastWhiteList.containsKey(originalType.getClass())
                    || !unStrictCastWhiteList.get(originalType.getClass()).contains(targetType.getClass())) {
                return false;
            }
        }
        // The following code is old check logic.
        if (originalType instanceof CharacterType && !(targetType instanceof PrimitiveType)) {
            // CharacterType couldn't cast to Object type which contains HllType, BitmapType or QuantileStateType
            return !checkTypeContainsType(targetType, HllType.class)
                    && !checkTypeContainsType(targetType, BitmapType.class)
                    && !checkTypeContainsType(targetType, QuantileStateType.class);
        }
        if (originalType instanceof AggStateType && targetType instanceof CharacterType) {
            return true;
        }
        if (originalType instanceof ArrayType && targetType instanceof ArrayType) {
            return check(((ArrayType) originalType).getItemType(), ((ArrayType) targetType).getItemType(),
                    isStrictMode);
        } else if (originalType instanceof MapType && targetType instanceof MapType) {
            return check(((MapType) originalType).getKeyType(), ((MapType) targetType).getKeyType(), isStrictMode)
                    && check(((MapType) originalType).getValueType(), ((MapType) targetType).getValueType(),
                    isStrictMode);
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
                if (!check(originalFields.get(i).getDataType(), targetFields.get(i).getDataType(), isStrictMode)) {
                    return false;
                }
            }
            return true;
        } else if (originalType.isComplexType() && targetType.isJsonType()) {
            return !checkTypeContainsType(originalType, MapType.class);
        } else {
            return checkPrimitiveType(originalType, targetType);
        }
    }

    /**
     * forbid this original and target type
     *   1. original type is object type
     *   2. target type is object type
     *   3. original type is same with target type
     *   4. target type is null type
     */
    private static boolean checkPrimitiveType(DataType originalType, DataType targetType) {
        if (originalType.isJsonType() || targetType.isJsonType()) {
            return true;
        }
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
     * check if sourceType contains the given targetType
     *
     * @param sourceType need to check
     * @param targetType targetType to find
     * @return true if complexType contains the targetType.
     */
    public static boolean checkTypeContainsType(DataType sourceType, Class<? extends DataType> targetType) {
        if (sourceType.getClass().equals(targetType)) {
            return true;
        }
        if (sourceType.isArrayType()) {
            return checkTypeContainsType(((ArrayType) sourceType).getItemType(), targetType);
        } else if (sourceType.isMapType()) {
            return checkTypeContainsType(((MapType) sourceType).getKeyType(), targetType)
                    || checkTypeContainsType(((MapType) sourceType).getValueType(), targetType);
        } else if (sourceType.isStructType()) {
            for (StructField f : ((StructType) sourceType).getFields()) {
                if (checkTypeContainsType(f.getDataType(), targetType)) {
                    return true;
                }
            }
        }
        return false;
    }
}
