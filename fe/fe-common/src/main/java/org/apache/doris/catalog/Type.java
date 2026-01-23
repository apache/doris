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

package org.apache.doris.catalog;

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TStructField;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type {
    // Currently only support Array type with max 9 depths.
    public static int MAX_NESTING_DEPTH = 9;

    // Static constant types for scalar types that don't require additional information.
    public static final ScalarType INVALID = new ScalarType(PrimitiveType.INVALID_TYPE);
    public static final ScalarType UNSUPPORTED = new ScalarType(PrimitiveType.UNSUPPORTED);
    public static final ScalarType NULL = new ScalarType(PrimitiveType.NULL_TYPE);
    public static final ScalarType BOOLEAN = new ScalarType(PrimitiveType.BOOLEAN);
    public static final ScalarType TINYINT = new ScalarType(PrimitiveType.TINYINT);
    public static final ScalarType SMALLINT = new ScalarType(PrimitiveType.SMALLINT);
    public static final ScalarType INT = new ScalarType(PrimitiveType.INT);
    public static final ScalarType BIGINT = new ScalarType(PrimitiveType.BIGINT);
    public static final ScalarType LARGEINT = new ScalarType(PrimitiveType.LARGEINT);
    public static final ScalarType FLOAT = new ScalarType(PrimitiveType.FLOAT);
    public static final ScalarType DOUBLE = new ScalarType(PrimitiveType.DOUBLE);
    public static final ScalarType IPV4 = new ScalarType(PrimitiveType.IPV4);
    public static final ScalarType IPV6 = new ScalarType(PrimitiveType.IPV6);
    public static final ScalarType DATE = new ScalarType(PrimitiveType.DATE);
    public static final ScalarType DATETIME = new ScalarType(PrimitiveType.DATETIME);
    public static final ScalarType DATEV2 = new ScalarType(PrimitiveType.DATEV2);
    public static final ScalarType TIMEV2 = new ScalarType(PrimitiveType.TIMEV2);
    public static final ScalarType TIMESTAMPTZ = new ScalarType(PrimitiveType.TIMESTAMPTZ);
    public static final ScalarType STRING = ScalarType.createStringType();
    public static final ScalarType VARBINARY = ScalarType.createVarbinaryType(-1);
    public static final ScalarType DEFAULT_DECIMALV2 = ScalarType.createDecimalType(PrimitiveType.DECIMALV2,
            ScalarType.DEFAULT_PRECISION, ScalarType.DEFAULT_SCALE);

    public static final ScalarType MAX_DECIMALV2_TYPE = ScalarType.createDecimalType(PrimitiveType.DECIMALV2,
            ScalarType.MAX_DECIMALV2_PRECISION, ScalarType.MAX_DECIMALV2_SCALE);

    public static final ScalarType DEFAULT_DECIMAL32 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL32, ScalarType.MAX_DECIMAL32_PRECISION,
                    ScalarType.DEFAULT_SCALE);

    public static final ScalarType DEFAULT_DECIMAL64 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL64, ScalarType.MAX_DECIMAL64_PRECISION,
                    ScalarType.DEFAULT_SCALE);

    public static final ScalarType DEFAULT_DECIMAL128 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL128, ScalarType.MAX_DECIMAL128_PRECISION,
                    ScalarType.DEFAULT_SCALE);

    public static final ScalarType DEFAULT_DECIMAL256 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL256, ScalarType.MAX_DECIMAL256_PRECISION,
                    ScalarType.DEFAULT_SCALE);
    public static final ScalarType DEFAULT_DECIMALV3 = DEFAULT_DECIMAL32;
    public static final ScalarType DEFAULT_DATETIMEV2 = ScalarType.createDatetimeV2Type(0);
    public static final ScalarType DATETIMEV2 = DEFAULT_DATETIMEV2;
    public static final ScalarType DATETIMEV2_WITH_MAX_SCALAR = ScalarType.createDatetimeV2Type(6);
    public static final ScalarType DEFAULT_TIMESTAMP_TZ = ScalarType.createTimeStampTzType(0);
    public static final ScalarType TIMESTAMP_TZ = DEFAULT_TIMESTAMP_TZ;
    public static final ScalarType TIMESTAMP_TZ_WITH_MAX_SCALAR = ScalarType.createTimeStampTzType(6);
    public static final ScalarType DEFAULT_TIMEV2 = ScalarType.createTimeV2Type(0);
    public static final ScalarType DECIMALV2 = DEFAULT_DECIMALV2;
    public static final ScalarType DECIMAL32 = DEFAULT_DECIMAL32;
    public static final ScalarType DECIMAL64 = DEFAULT_DECIMAL64;
    public static final ScalarType DECIMAL128 = DEFAULT_DECIMAL128;
    public static final ScalarType DECIMAL256 = DEFAULT_DECIMAL256;
    public static final ScalarType WILDCARD_DECIMAL = ScalarType.createDecimalType(-1, -1);
    public static final ScalarType JSONB = new ScalarType(PrimitiveType.JSONB);
    // (ScalarType) ScalarType.createDecimalTypeInternal(-1, -1);
    public static final ScalarType DEFAULT_VARCHAR = ScalarType.createVarcharType(-1);
    public static final ScalarType VARCHAR = ScalarType.createVarcharType(-1);
    public static final ScalarType HLL = ScalarType.createHllType();
    public static final ScalarType CHAR = ScalarType.createCharType(-1);
    public static final ScalarType BITMAP = new ScalarType(PrimitiveType.BITMAP);
    public static final ScalarType QUANTILE_STATE = new ScalarType(PrimitiveType.QUANTILE_STATE);
    public static final ScalarType LAMBDA_FUNCTION = new ScalarType(PrimitiveType.LAMBDA_FUNCTION);
    public static final MapType MAP = new MapType();
    public static final ArrayType ARRAY = ArrayType.create();
    public static final StructType GENERIC_STRUCT = new StructType(Lists.newArrayList(
            new StructField("generic_struct", new ScalarType(PrimitiveType.NULL_TYPE))));
    public static final StructType STRUCT = new StructType();
    // In the past, variant metadata used the ScalarType type.
    // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
    public static final VariantType VARIANT = new VariantType();

    private static final ArrayList<ScalarType> integerTypes;
    private static final ArrayList<ScalarType> stringTypes;
    private static final ArrayList<ScalarType> numericTypes;
    private static final ArrayList<ScalarType> numericDateTimeTypes;
    private static final ArrayList<ScalarType> supportedTypes;
    private static final ArrayList<Type> arraySubTypes;
    private static final ArrayList<Type> mapSubTypes;
    private static final ArrayList<Type> structSubTypes;
    private static final ArrayList<Type> variantSubTypes;
    private static final ArrayList<ScalarType> trivialTypes;

    static {
        integerTypes = Lists.newArrayList();
        integerTypes.add(TINYINT);
        integerTypes.add(SMALLINT);
        integerTypes.add(INT);
        integerTypes.add(BIGINT);
        integerTypes.add(LARGEINT);

        stringTypes = Lists.newArrayList();
        stringTypes.add(CHAR);
        stringTypes.add(VARCHAR);
        stringTypes.add(STRING);

        numericTypes = Lists.newArrayList();
        numericTypes.addAll(integerTypes);
        numericTypes.add(FLOAT);
        numericTypes.add(DOUBLE);
        numericTypes.add(MAX_DECIMALV2_TYPE);
        numericTypes.add(DECIMAL32);
        numericTypes.add(DECIMAL64);
        numericTypes.add(DECIMAL128);
        numericTypes.add(DECIMAL256);

        numericDateTimeTypes = Lists.newArrayList();
        numericDateTimeTypes.add(DATE);
        numericDateTimeTypes.add(DATETIME);
        numericDateTimeTypes.add(DATEV2);
        numericDateTimeTypes.add(DATETIMEV2);
        numericDateTimeTypes.add(TIMEV2);
        numericDateTimeTypes.addAll(numericTypes);

        trivialTypes = Lists.newArrayList();
        trivialTypes.addAll(numericTypes);
        trivialTypes.add(BOOLEAN);
        trivialTypes.add(VARCHAR);
        trivialTypes.add(STRING);
        trivialTypes.add(CHAR);
        trivialTypes.add(DATE);
        trivialTypes.add(DATETIME);
        trivialTypes.add(DATEV2);
        trivialTypes.add(DATETIMEV2);
        trivialTypes.add(IPV4);
        trivialTypes.add(IPV6);
        trivialTypes.add(TIMEV2);
        trivialTypes.add(JSONB);
        trivialTypes.add(VARIANT);

        supportedTypes = Lists.newArrayList();
        supportedTypes.addAll(trivialTypes);
        supportedTypes.add(NULL);
        supportedTypes.add(HLL);
        supportedTypes.add(BITMAP);
        supportedTypes.add(QUANTILE_STATE);

        arraySubTypes = Lists.newArrayList();
        arraySubTypes.add(BOOLEAN);
        arraySubTypes.addAll(integerTypes);
        arraySubTypes.add(FLOAT);
        arraySubTypes.add(DOUBLE);
        arraySubTypes.add(DECIMALV2);
        arraySubTypes.add(TIMEV2);
        arraySubTypes.add(DATE);
        arraySubTypes.add(DATETIME);
        arraySubTypes.add(DATEV2);
        arraySubTypes.add(DATETIMEV2);
        arraySubTypes.add(TIMESTAMP_TZ);
        arraySubTypes.add(IPV4);
        arraySubTypes.add(IPV6);
        arraySubTypes.add(CHAR);
        arraySubTypes.add(VARCHAR);
        arraySubTypes.add(STRING);
        arraySubTypes.add(DECIMAL32);
        arraySubTypes.add(DECIMAL64);
        arraySubTypes.add(DECIMAL128);
        arraySubTypes.add(DECIMAL256);
        arraySubTypes.add(NULL);
        arraySubTypes.add(ARRAY);
        arraySubTypes.add(MAP);
        arraySubTypes.add(STRUCT);

        mapSubTypes = Lists.newArrayList();
        mapSubTypes.add(BOOLEAN);
        mapSubTypes.addAll(integerTypes);
        mapSubTypes.add(FLOAT);
        mapSubTypes.add(DOUBLE);
        mapSubTypes.add(DECIMALV2);
        mapSubTypes.add(DECIMAL32); // same DEFAULT_DECIMALV3
        mapSubTypes.add(DECIMAL64);
        mapSubTypes.add(DECIMAL128);
        mapSubTypes.add(DECIMAL256);
        mapSubTypes.add(TIMEV2);
        mapSubTypes.add(DATE);
        mapSubTypes.add(DATETIME);
        mapSubTypes.add(DATEV2);
        mapSubTypes.add(DATETIMEV2);
        mapSubTypes.add(TIMESTAMP_TZ);
        mapSubTypes.add(IPV4);
        mapSubTypes.add(IPV6);
        mapSubTypes.add(CHAR);
        mapSubTypes.add(VARCHAR);
        mapSubTypes.add(STRING);
        mapSubTypes.add(NULL);
        mapSubTypes.add(ARRAY);
        mapSubTypes.add(MAP);
        mapSubTypes.add(STRUCT);

        structSubTypes = Lists.newArrayList();
        structSubTypes.add(BOOLEAN);
        structSubTypes.addAll(integerTypes);
        structSubTypes.add(FLOAT);
        structSubTypes.add(DOUBLE);
        structSubTypes.add(DECIMALV2);
        structSubTypes.add(DECIMAL32); // same DEFAULT_DECIMALV3
        structSubTypes.add(DECIMAL64);
        structSubTypes.add(DECIMAL128);
        structSubTypes.add(DECIMAL256);
        structSubTypes.add(TIMEV2);
        structSubTypes.add(DATE);
        structSubTypes.add(DATETIME);
        structSubTypes.add(DATEV2);
        structSubTypes.add(DATETIMEV2);
        structSubTypes.add(TIMESTAMP_TZ);
        structSubTypes.add(IPV4);
        structSubTypes.add(IPV6);
        structSubTypes.add(CHAR);
        structSubTypes.add(VARCHAR);
        structSubTypes.add(STRING);
        structSubTypes.add(NULL);
        structSubTypes.add(ARRAY);
        structSubTypes.add(MAP);
        structSubTypes.add(STRUCT);

        variantSubTypes = Lists.newArrayList();
        variantSubTypes.add(BOOLEAN);
        variantSubTypes.addAll(integerTypes);
        variantSubTypes.add(FLOAT);
        variantSubTypes.add(DOUBLE);
        variantSubTypes.add(DECIMAL32); // same DEFAULT_DECIMALV3
        variantSubTypes.add(DECIMAL64);
        variantSubTypes.add(DECIMAL128);
        variantSubTypes.add(DECIMAL256);
        variantSubTypes.add(DATEV2);
        variantSubTypes.add(DATETIMEV2);
        variantSubTypes.add(TIMESTAMP_TZ);
        variantSubTypes.add(IPV4);
        variantSubTypes.add(IPV6);
        variantSubTypes.add(STRING);
        variantSubTypes.add(NULL);
    }

    public static final Set<Class> ARRAY_SUPPORTED_JAVA_TYPE = Sets.newHashSet(ArrayList.class, List.class);
    public static final Set<Class> MAP_SUPPORTED_JAVA_TYPE = Sets.newHashSet(HashMap.class, Map.class);
    public static final Set<Class> DATE_SUPPORTED_JAVA_TYPE = Sets.newHashSet(LocalDate.class, java.util.Date.class,
            org.joda.time.LocalDate.class);
    public static final Set<Class> DATETIME_SUPPORTED_JAVA_TYPE = Sets.newHashSet(LocalDateTime.class,
            org.joda.time.DateTime.class, org.joda.time.LocalDateTime.class);
    public static final ImmutableMap<PrimitiveType, Set<Class>> PrimitiveTypeToJavaClassType =
            new ImmutableMap.Builder<PrimitiveType, Set<Class>>()
                    .put(PrimitiveType.BOOLEAN, Sets.newHashSet(Boolean.class, boolean.class))
                    .put(PrimitiveType.TINYINT, Sets.newHashSet(Byte.class, byte.class))
                    .put(PrimitiveType.SMALLINT, Sets.newHashSet(Short.class, short.class))
                    .put(PrimitiveType.INT, Sets.newHashSet(Integer.class, int.class))
                    .put(PrimitiveType.FLOAT, Sets.newHashSet(Float.class, float.class))
                    .put(PrimitiveType.DOUBLE, Sets.newHashSet(Double.class, double.class))
                    .put(PrimitiveType.BIGINT, Sets.newHashSet(Long.class, long.class))
                    .put(PrimitiveType.IPV4, Sets.newHashSet(InetAddress.class))
                    .put(PrimitiveType.IPV6, Sets.newHashSet(InetAddress.class))
                    .put(PrimitiveType.STRING, Sets.newHashSet(String.class))
                    .put(PrimitiveType.VARBINARY, Sets.newHashSet(Byte[].class, byte[].class))
                    .put(PrimitiveType.DATE, DATE_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.DATEV2, DATE_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.DATETIME, DATETIME_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.DATETIMEV2, DATETIME_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.LARGEINT, Sets.newHashSet(BigInteger.class))
                    .put(PrimitiveType.DECIMALV2, Sets.newHashSet(BigDecimal.class))
                    .put(PrimitiveType.DECIMAL32, Sets.newHashSet(BigDecimal.class))
                    .put(PrimitiveType.DECIMAL64, Sets.newHashSet(BigDecimal.class))
                    .put(PrimitiveType.DECIMAL128, Sets.newHashSet(BigDecimal.class))
                    .put(PrimitiveType.ARRAY, ARRAY_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.MAP, MAP_SUPPORTED_JAVA_TYPE)
                    .put(PrimitiveType.STRUCT, ARRAY_SUPPORTED_JAVA_TYPE)
                    .build();

    public static ArrayList<ScalarType> getIntegerTypes() {
        return integerTypes;
    }

    public static ArrayList<ScalarType> getStringTypes() {
        return stringTypes;
    }

    public static ArrayList<ScalarType> getNumericTypes() {
        return numericTypes;
    }

    public static ArrayList<ScalarType> getNumericDateTimeTypes() {
        return numericDateTimeTypes;
    }

    public static ArrayList<ScalarType> getTrivialTypes() {
        return trivialTypes;
    }

    public static ArrayList<ScalarType> getSupportedTypes() {
        return supportedTypes;
    }

    public static ArrayList<Type> getArraySubTypes() {
        return arraySubTypes;
    }

    public static ArrayList<Type> getMapSubTypes() {
        return mapSubTypes;
    }

    public static ArrayList<Type> getStructSubTypes() {
        return structSubTypes;
    }

    public static ArrayList<Type> getVariantSubTypes() {
        return variantSubTypes;
    }

    /**
     * Return true if this is complex type and support subType
     */
    public boolean supportSubType(Type subType) {
        return false;
    }

    /**
    * Return true if this type can be as short key
    */
    public boolean couldBeShortKey() {
        return !(isFloatingPointType()
                        || getPrimitiveType() == PrimitiveType.STRING
                        || isJsonbType()
                        || isComplexType()
                        || isObjectStored()
                        || isVariantType());
    }

    /**
     * The output of this is stored directly in the hive metastore as the column type.
     * The string must match exactly.
     */
    public final String toSql() {
        return toSql(0);
    }

    /**
     * Recursive helper for toSql() to be implemented by subclasses. Keeps track of the
     * nesting depth and terminates the recursion if MAX_NESTING_DEPTH is reached.
     */
    protected abstract String toSql(int depth);

    /**
     * Same as toSql() but adds newlines and spaces for better readability of nested types.
     */
    public String prettyPrint() {
        return prettyPrint(0);
    }

    /**
     * Pretty prints this type with lpad number of leading spaces. Used to implement
     * prettyPrint() with space-indented nested types.
     */
    protected abstract String prettyPrint(int lpad);

    public boolean isInvalid() {
        return isScalarType(PrimitiveType.INVALID_TYPE);
    }

    public boolean isUnsupported() {
        return isScalarType(PrimitiveType.UNSUPPORTED);
    }

    public boolean isValid() {
        return !isInvalid();
    }

    public boolean isNull() {
        return isScalarType(PrimitiveType.NULL_TYPE);
    }

    public boolean isBoolean() {
        return isScalarType(PrimitiveType.BOOLEAN);
    }

    public boolean isDecimalV2() {
        return isScalarType(PrimitiveType.DECIMALV2);
    }

    public boolean typeContainsPrecision() {
        if (PrimitiveType.typeWithPrecision.contains(this.getPrimitiveType())) {
            return true;
        } else if (isStructType()) {
            for (StructField field : ((StructType) this).getFields()) {
                if (PrimitiveType.typeWithPrecision.contains(field.getType().getPrimitiveType())) {
                    return true;
                }
            }
        } else if (isMapType()) {
            return PrimitiveType.typeWithPrecision.contains(((MapType) this).getKeyType().getPrimitiveType())
                    || PrimitiveType.typeWithPrecision.contains(((MapType) this).getValueType().getPrimitiveType());
        } else if (isArrayType()) {
            return PrimitiveType.typeWithPrecision.contains(((ArrayType) this).getItemType().getPrimitiveType());
        }
        return false;
    }

    public String hideVersionForVersionColumn(Boolean isToSql) {
        if (isDatetime() || isDatetimeV2()) {
            StringBuilder typeStr = new StringBuilder("datetime");
            if (((ScalarType) this).getScalarScale() > 0) {
                typeStr.append("(").append(((ScalarType) this).getScalarScale()).append(")");
            }
            return typeStr.toString();
        } else if (isTimeStampTz()) {
            StringBuilder typeStr = new StringBuilder("timestamptz");
            if (((ScalarType) this).getScalarScale() > 0) {
                typeStr.append("(").append(((ScalarType) this).getScalarScale()).append(")");
            }
            return typeStr.toString();
        } else if (isDate() || isDateV2()) {
            return "date";
        } else if (isDecimalV2() || isDecimalV3()) {
            StringBuilder typeStr = new StringBuilder("decimal");
            ScalarType sType = (ScalarType) this;
            int scale = sType.getScalarScale();
            int precision = sType.getScalarPrecision();
            typeStr.append("(").append(precision).append(",").append(scale).append(")");
            return typeStr.toString();
        } else if (isTimeV2()) {
            StringBuilder typeStr = new StringBuilder("time");
            if (((ScalarType) this).getScalarScale() > 0) {
                typeStr.append("(").append(((ScalarType) this).getScalarScale()).append(")");
            }
            return typeStr.toString();
        } else if (isArrayType()) {
            String nestedDesc = ((ArrayType) this).getItemType().hideVersionForVersionColumn(isToSql);
            return "array<" + nestedDesc + ">";
        } else if (isMapType()) {
            String keyDesc = ((MapType) this).getKeyType().hideVersionForVersionColumn(isToSql);
            String valueDesc = ((MapType) this).getValueType().hideVersionForVersionColumn(isToSql);
            return "map<" + keyDesc + "," + valueDesc + ">";
        } else if (isStructType()) {
            List<String> fieldDesc = new ArrayList<>();
            StructType structType = (StructType) this;
            for (int i = 0; i < structType.getFields().size(); i++) {
                StructField field = structType.getFields().get(i);
                fieldDesc.add(field.getName() + ":" + field.getType().hideVersionForVersionColumn(isToSql));
            }
            return "struct<" + StringUtils.join(fieldDesc, ",") + ">";
        } else if (isToSql) {
            return this.toSql();
        }
        return this.toString();
    }

    public boolean isDecimalV3() {
        return isScalarType(PrimitiveType.DECIMAL32) || isScalarType(PrimitiveType.DECIMAL64)
                || isScalarType(PrimitiveType.DECIMAL128) || isScalarType(PrimitiveType.DECIMAL256);
    }

    public boolean isDatetimeV2() {
        return isScalarType(PrimitiveType.DATETIMEV2);
    }

    public boolean isTimeV2() {
        return isScalarType(PrimitiveType.TIMEV2);
    }

    public boolean isTimeStampTz() {
        return isScalarType(PrimitiveType.TIMESTAMPTZ);
    }

    public boolean isWildcardTimeV2() {
        return false;
    }

    public boolean isWildcardDatetimeV2() {
        return false;
    }

    public boolean isWildcardTimeStampTz() {
        return false;
    }

    public boolean isWildcardDecimal() {
        return false;
    }

    public boolean isWildcardVarchar() {
        return false;
    }

    public boolean isWildcardChar() {
        return false;
    }

    public boolean isWildcardVarbinary() {
        return false;
    }

    public boolean isStringType() {
        return isScalarType(PrimitiveType.VARCHAR)
                || isScalarType(PrimitiveType.CHAR)
                || isScalarType(PrimitiveType.STRING);
    }

    public boolean isVarcharOrStringType() {
        return isScalarType(PrimitiveType.VARCHAR)
                || isScalarType(PrimitiveType.STRING);
    }

    public boolean isVarchar() {
        return isScalarType(PrimitiveType.VARCHAR);
    }

    public boolean isChar() {
        return isScalarType(PrimitiveType.CHAR);
    }

    public boolean isJsonbType() {
        return isScalarType(PrimitiveType.JSONB);
    }

    public boolean isVariantType() {
        return isScalarType(PrimitiveType.VARIANT);
    }

    public boolean isVarbinaryType() {
        return isScalarType(PrimitiveType.VARBINARY);
    }

    // only metric types have the following constraint:
    // 1. don't support as key column
    // 2. don't support filter
    // 3. don't support group by
    // 4. don't support index
    public boolean isOnlyMetricType() {
        return isObjectStored() || isComplexType() || isJsonbType() || isVariantType();
    }

    public static final String OnlyMetricTypeErrorMsg =
            "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function, and don't"
                    + " support filter, group by or order by. please run 'help hll' or 'help bitmap' or 'help array'"
                    + " or 'help map' or 'help struct' or 'help jsonb' or 'help variant' in your mysql client.";

    public boolean isHllType() {
        return isScalarType(PrimitiveType.HLL);
    }

    public boolean isBitmapType() {
        return isScalarType(PrimitiveType.BITMAP);
    }

    public boolean isQuantileStateType() {
        return isScalarType(PrimitiveType.QUANTILE_STATE);
    }

    public boolean isLambdaFunctionType() {
        return isScalarType(PrimitiveType.LAMBDA_FUNCTION);
    }

    public boolean isObjectStored() {
        return isHllType() || isBitmapType() || isQuantileStateType();
    }

    public boolean isScalarType() {
        return this instanceof ScalarType;
    }

    public boolean isScalarType(PrimitiveType t) {
        return isScalarType() && this.getPrimitiveType() == t;
    }

    public boolean isFixedPointType() {
        return isScalarType(PrimitiveType.TINYINT)
                || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT)
                || isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isFloatingPointType() {
        return isScalarType(PrimitiveType.FLOAT) || isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isIntegerType() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isInteger32Type() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT);
    }

    public boolean isBigIntType() {
        return isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeIntType() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isNativeType() {
        return isFixedPointType() || isFloatingPointType() || isBoolean();
    }

    public boolean isDateType() {
        return isScalarType(PrimitiveType.DATE) || isScalarType(PrimitiveType.DATETIME)
                || isScalarType(PrimitiveType.DATEV2) || isScalarType(PrimitiveType.DATETIMEV2)
                || isScalarType(PrimitiveType.TIMESTAMPTZ);
    }

    public boolean isDatetime() {
        return isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isTimeType() {
        return isTimeV2();
    }

    public boolean isComplexType() {
        return isStructType() || isMapType() || isArrayType();
    }

    public boolean isMapType() {
        return this instanceof MapType;
    }

    public boolean isArrayType() {
        return this instanceof ArrayType;
    }

    public boolean isAggStateType() {
        return this instanceof AggStateType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isAnyType() {
        return this instanceof AnyType;
    }

    public boolean isDate() {
        return isScalarType(PrimitiveType.DATE);
    }

    public boolean isDateV2() {
        return isScalarType(PrimitiveType.DATEV2);
    }

    public boolean isIP() {
        return isScalarType(PrimitiveType.IPV4) || isScalarType(PrimitiveType.IPV6);
    }

    public boolean isIPv4() {
        return isScalarType(PrimitiveType.IPV4);
    }

    public boolean isIPv6() {
        return isScalarType(PrimitiveType.IPV6);
    }

    /**
     * Returns true if Impala supports this type in the metdata. It does not mean we
     * can manipulate data of this type. For tables that contain columns with these
     * types, we can safely skip over them.
     */
    public boolean isSupported() {
        return true;
    }

    public int getLength() {
        return -1;
    }

    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.INVALID_TYPE;
    }

    /**
     * Returns the size in bytes of the fixed-length portion that a slot of this type
     * occupies in a tuple.
     */
    public int getSlotSize() {
        throw new IllegalStateException("getSlotSize() not implemented for type " + toSql());
    }

    public TTypeDesc toThrift() {
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        toThrift(container);
        return container;
    }

    public TColumnType toColumnTypeThrift() {
        return null;
    }

    /**
     * Subclasses should override this method to add themselves to the thrift container.
     */
    public abstract void toThrift(TTypeDesc container);

    /**
     * Returns true if this type is equal to t, or if t is a wildcard variant of this
     * type. Subclasses should override this as appropriate. The default implementation
     * here is to avoid special-casing logic in callers for concrete types.
     */
    public boolean matchesType(Type t) {
        return false;
    }

    /**
     * Returns true if this type exceeds the MAX_NESTING_DEPTH, false otherwise.
     */
    public boolean exceedsMaxNestingDepth() {
        return exceedsMaxNestingDepth(0);
    }

    /**
     * Helper for exceedsMaxNestingDepth(). Recursively computes the max nesting depth,
     * terminating early if MAX_NESTING_DEPTH is reached. Returns true if this type
     * exceeds the MAX_NESTING_DEPTH, false otherwise.
     * <p>
     * Examples of types and their nesting depth:
     * INT --> 1
     * STRUCT<f1:INT> --> 2
     * STRUCT<f1:STRUCT<f2:INT>> --> 3
     * ARRAY<INT> --> 2
     * ARRAY<STRUCT<f1:INT>> --> 3
     * MAP<STRING,INT> --> 2
     * MAP<STRING,STRUCT<f1:INT>> --> 3
     */
    private boolean exceedsMaxNestingDepth(int d) {
        if (d > MAX_NESTING_DEPTH) {
            return true;
        }
        if (isStructType()) {
            StructType structType = (StructType) this;
            for (StructField f : structType.getFields()) {
                if (f.getType().exceedsMaxNestingDepth(d + 1)) {
                    return true;
                }
            }
        } else if (isArrayType()) {
            ArrayType arrayType = (ArrayType) this;
            Type itemType = arrayType.getItemType();
            return itemType.exceedsMaxNestingDepth(d + 1);
        } else if (isMapType()) {
            MapType mapType = (MapType) this;
            return mapType.getValueType().exceedsMaxNestingDepth(d + 1);
        } else {
            Preconditions.checkState(isScalarType() || isAggStateType());
        }
        return false;
    }

    public static Type fromPrimitiveType(PrimitiveType type) {
        switch (type) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case TINYINT:
                return Type.TINYINT;
            case SMALLINT:
                return Type.SMALLINT;
            case INT:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case IPV4:
                return Type.IPV4;
            case IPV6:
                return Type.IPV6;
            case DATE:
                return Type.DATE;
            case DATETIME:
                return Type.DATETIME;
            case DATEV2:
                return Type.DATEV2;
            case DATETIMEV2:
                return Type.DATETIMEV2;
            case TIMEV2:
                return Type.TIMEV2;
            case TIMESTAMPTZ:
                return Type.TIMESTAMPTZ;
            case DECIMALV2:
                return Type.DECIMALV2;
            case DECIMAL32:
                return Type.DECIMAL32;
            case DECIMAL64:
                return Type.DECIMAL64;
            case DECIMAL128:
                return Type.DECIMAL128;
            case CHAR:
                return Type.CHAR;
            case VARCHAR:
                return Type.VARCHAR;
            case JSONB:
                return Type.JSONB;
            case VARIANT:
                return Type.VARIANT;
            case STRING:
                return Type.STRING;
            case HLL:
                return Type.HLL;
            case ARRAY:
                return ArrayType.create();
            case MAP:
                return new MapType();
            case STRUCT:
                return new StructType();
            case BITMAP:
                return Type.BITMAP;
            case QUANTILE_STATE:
                return Type.QUANTILE_STATE;
            case LAMBDA_FUNCTION:
                return Type.LAMBDA_FUNCTION;
            case VARBINARY:
                return Type.VARBINARY;
            default:
                return null;
        }
    }

    public static List<TTypeDesc> toThrift(ArrayList<Type> types) {
        ArrayList<TTypeDesc> result = Lists.newArrayList();
        for (Type t : types) {
            result.add(t.toThrift());
        }
        return result;
    }

    public static List<TTypeDesc> toThrift(ArrayList<Type> types, ArrayList<Type> realTypes) {
        ArrayList<TTypeDesc> result = Lists.newArrayList();
        for (int i = 0; i < types.size(); i++) {
            if (PrimitiveType.typeWithPrecision.contains(realTypes.get(i).getPrimitiveType())) {
                result.add(realTypes.get(i).toThrift());
            } else {
                result.add(types.get(i).toThrift());
            }
        }
        return result;
    }

    public static Type fromThrift(TTypeDesc thrift) {
        Preconditions.checkState(thrift.types.size() > 0);
        Pair<Type, Integer> t = fromThrift(thrift, 0);
        Preconditions.checkState(t.second.equals(thrift.getTypesSize()));
        return t.first;
    }

    /**
     * Constructs a ColumnType rooted at the TTypeNode at nodeIdx in TColumnType.
     * Returned pair: The resulting ColumnType and the next nodeIdx that is not a child
     * type of the result.
     */
    protected static Pair<Type, Integer> fromThrift(TTypeDesc col, int nodeIdx) {
        TTypeNode node = col.getTypes().get(nodeIdx);
        Type type = null;
        int tmpNodeIdx = nodeIdx;
        switch (node.getType()) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case SCALAR: {
                Preconditions.checkState(node.isSetScalarType());
                TScalarType scalarType = node.getScalarType();
                if (scalarType.getType() == TPrimitiveType.CHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createCharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createVarcharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.HLL) {
                    type = ScalarType.createHllType();
                } else if (scalarType.getType() == TPrimitiveType.DECIMALV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetPrecision());
                    type = ScalarType.createDecimalType(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DECIMAL32
                        || scalarType.getType() == TPrimitiveType.DECIMAL64
                        || scalarType.getType() == TPrimitiveType.DECIMAL128I
                        || scalarType.getType() == TPrimitiveType.DECIMAL256) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalV3Type(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DATETIMEV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDatetimeV2Type(scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.TIMEV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createTimeV2Type(scalarType.getScale());
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                ++tmpNodeIdx;
                break;
            }
            case ARRAY: {
                Preconditions.checkState(tmpNodeIdx + 1 < col.getTypesSize());
                Pair<Type, Integer> childType = fromThrift(col, tmpNodeIdx + 1);
                type = new ArrayType(childType.first);
                tmpNodeIdx = childType.second;
                break;
            }
            case MAP: {
                Preconditions.checkState(tmpNodeIdx + 2 < col.getTypesSize());
                Pair<Type, Integer> keyType = fromThrift(col, tmpNodeIdx + 1);
                Pair<Type, Integer> valueType = fromThrift(col, keyType.second);
                type = new MapType(keyType.first, valueType.first);
                tmpNodeIdx = valueType.second;
                break;
            }
            case STRUCT: {
                Preconditions.checkState(tmpNodeIdx + node.getStructFieldsSize() < col.getTypesSize());
                ArrayList<StructField> structFields = Lists.newArrayList();
                ++tmpNodeIdx;
                for (int i = 0; i < node.getStructFieldsSize(); ++i) {
                    TStructField thriftField = node.getStructFields().get(i);
                    String name = thriftField.getName();
                    String comment = null;
                    if (thriftField.isSetComment()) {
                        comment = thriftField.getComment();
                    }
                    Pair<Type, Integer> res = fromThrift(col, tmpNodeIdx);
                    tmpNodeIdx = res.second.intValue();
                    structFields.add(new StructField(name, res.first, comment, true));
                }
                type = new StructType(structFields);
                break;
            }
        }
        return Pair.of(type, tmpNodeIdx);
    }

    /**
     * JDBC data type description
     * Returns the column size for this type.
     * For numeric data this is the maximum precision.
     * For character data this is the length in characters.
     * For datetime types this is the length in characters of the String representation
     * (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data this is the length in bytes.
     * Null is returned for data types where the column size is not applicable.
     */
    public Integer getColumnSize() {
        if (!isScalarType()) {
            return null;
        }
        if (isNumericType()) {
            return getPrecision();
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
            case STRING:
            case HLL:
                return t.getLength();
            default:
                return null;
        }
    }

    /**
     * For schema change, convert data type to string,
     * get the size of string representation
     */
    public int getColumnStringRepSize() throws TypeException {
        if (isScalarType(PrimitiveType.FLOAT)) {
            return 24; // see be/src/gutil/strings/numbers.h kFloatToBufferSize
        }
        if (isScalarType(PrimitiveType.DOUBLE)) {
            return 32; // see be/src/gutil/strings/numbers.h kDoubleToBufferSize
        }
        if (isNumericType()) {
            int size = getPrecision() + 1; // +1 for minus symbol
            if (isScalarType(PrimitiveType.DECIMALV2) || isDecimalV3()) {
                size += 1; // +1 for decimal point
            }
            return size;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
                return t.getLength();
            case STRING:
                return 2147483647; // defined by be/src/olap/olap_define.h, OLAP_STRING_MAX_LENGTH
            default:
                throw new TypeException("Can not change " + t.getPrimitiveType() + " to char/varchar/string");
        }
    }

    /**
     * JDBC data type description
     * For numeric types, returns the maximum precision for this type.
     * For non-numeric types, returns null.
     */
    public Integer getPrecision() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INT:
                return 10;
            case BIGINT:
                return 19;
            case LARGEINT:
                return 39;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
            case DATETIMEV2:
            case TIMESTAMPTZ:
            case TIMEV2:
                return t.decimalPrecision();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * Returns the number of fractional digits for this type, or null if not applicable.
     * For timestamp/time types, returns the number of digits in the fractional seconds
     * component.
     */
    public Integer getDecimalDigits() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return 0;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DATETIMEV2:
            case TIMESTAMPTZ:
            case TIMEV2:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return t.decimalScale();
            default:
                return null;
        }
    }

    public int getIndexSize() {
        if (this.getPrimitiveType() == PrimitiveType.CHAR) {
            return this.getLength();
        } else {
            return this.getPrimitiveType().getOlapColumnIndexSize();
        }
    }

    // only use for cast to generate no op
    public static boolean matchExactType(Type type1, Type type2, boolean ignorePrecision) {
        // we should make type decide to match other for itself to impl matchesType instead of switch case types
        if (type1.matchesType(type2)) {
            if (type1.isArrayType()) {
                return matchExactType(((ArrayType) type1).getItemType(), ((ArrayType) type2).getItemType(),
                        ignorePrecision);
            } else if (type1.isMapType()) {
                MapType map1 = (MapType) type1;
                MapType map2 = (MapType) type2;
                return matchExactType(map1.getKeyType(), map2.getKeyType(), ignorePrecision)
                        && matchExactType(map1.getValueType(), map2.getValueType(), ignorePrecision);
            } else if (type1.isStructType()) {
                StructType struct1 = (StructType) type1;
                StructType struct2 = (StructType) type2;
                if (struct1.getFields().size() != struct2.getFields().size()) {
                    return false;
                }
                for (int i = 0; i < struct1.getFields().size(); i++) {
                    if (!matchExactType(struct1.getFields().get(i).getType(),
                            struct2.getFields().get(i).getType(), ignorePrecision)) {
                        return false;
                    }
                }
                return true;
            } else if (type1.isVariantType()) {
                ArrayList<VariantField> fields1 = ((VariantType) type1).getPredefinedFields();
                ArrayList<VariantField> fields2 = ((VariantType) type2).getPredefinedFields();
                if (fields1.size() != fields2.size()) {
                    return false;
                }
                for (int i = 0; i < fields1.size(); i++) {
                    if (!matchExactType(fields1.get(i).getType(), fields2.get(i).getType(), ignorePrecision)) {
                        return false;
                    }
                }
                return true;
            } else if (PrimitiveType.typeWithPrecision.contains(type2.getPrimitiveType())) {
                //FIXME: this branch will never be executed ScalarType.matchesType and checks below are
                // self-contradictory. double check and remove the argument `ignorePrecision`.
                if ((((ScalarType) type2).decimalPrecision()
                        == ((ScalarType) type1).decimalPrecision()) && (((ScalarType) type2).decimalScale()
                        == ((ScalarType) type1).decimalScale())) {
                    return true;
                } else if (((ScalarType) type2).decimalScale() == ((ScalarType) type1).decimalScale()
                        && ignorePrecision) {
                    return isSameDecimalTypeWithDifferentPrecision(((ScalarType) type2).decimalPrecision(),
                            ((ScalarType) type1).decimalPrecision());
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public static boolean isSameDecimalTypeWithDifferentPrecision(int precision1, int precision2) {
        if (precision1 <= ScalarType.MAX_DECIMAL32_PRECISION && precision2 <= ScalarType.MAX_DECIMAL32_PRECISION) {
            return true;
        } else if (precision1 > ScalarType.MAX_DECIMAL32_PRECISION && precision2 > ScalarType.MAX_DECIMAL32_PRECISION
                && precision1 <= ScalarType.MAX_DECIMAL64_PRECISION
                && precision2 <= ScalarType.MAX_DECIMAL64_PRECISION) {
            return true;
        } else if (precision1 > ScalarType.MAX_DECIMAL64_PRECISION && precision2 > ScalarType.MAX_DECIMAL64_PRECISION
                && precision1 <= ScalarType.MAX_DECIMAL128_PRECISION
                && precision2 <= ScalarType.MAX_DECIMAL128_PRECISION) {
            return true;
        }
        return false;
    }
}
