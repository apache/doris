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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract class for all data type in Nereids.
 */
public abstract class DataType implements AbstractDataType {
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_PRECISION = 9;
    public static final int DATETIME_PRECISION = 18;

    private static final Pattern CHAR_PATTERN = Pattern.compile("char(\\s*\\(\\s*(\\d+)\\s*\\))?");
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("varchar(\\s*\\(\\s*(\\d+)\\s*\\))?");
    private static final Pattern DATETIME_PATTERN = Pattern.compile("datetime(\\s*\\(\\s*(\\d+)\\s*\\))?");
    private static final Pattern DATETIMEV2_PATTERN = Pattern.compile("datetimev2(\\s*\\(\\s*(\\d+)\\s*\\))?");
    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile("decimal(\\s*\\(\\s*(\\d+)(\\s*,\\s*(\\d+))?\\s*\\))?");
    private static final Pattern DECIMALV3_PATTERN =
            Pattern.compile("decimalv3(\\s*\\(\\s*(\\d+)(\\s*,\\s*(\\d+))?\\s*\\))?");

    // use class and supplier here to avoid class load deadlock.
    private static final Map<Class<? extends NumericType>, Supplier<DataType>> PROMOTION_MAP
            = ImmutableMap.<Class<? extends NumericType>, Supplier<DataType>>builder()
            .put(TinyIntType.class, () -> SmallIntType.INSTANCE)
            .put(SmallIntType.class, () -> IntegerType.INSTANCE)
            .put(IntegerType.class, () -> BigIntType.INSTANCE)
            .put(FloatType.class, () -> DoubleType.INSTANCE)
            .build();

    /**
     * create a specific Literal for a given dataType
     */
    public static Literal promoteNumberLiteral(Object value, DataType dataType) {
        if (! (value instanceof Number)) {
            return null;
        }

        if (dataType.equals(SmallIntType.INSTANCE)) {
            return new SmallIntLiteral(((Number) value).shortValue());
        } else if (dataType.equals(IntegerType.INSTANCE)) {
            return new IntegerLiteral(((Number) value).intValue());
        } else if (dataType.equals(BigIntType.INSTANCE)) {
            return new BigIntLiteral(((Number) value).longValue());
        } else if (dataType.equals(DoubleType.INSTANCE)) {
            return new DoubleLiteral(((Number) value).doubleValue());
        }
        return null;
    }

    /**
     * Convert data type in Doris catalog to data type in Nereids.
     * TODO: throw exception when cannot convert catalog type to Nereids type
     *
     * @param catalogType data type in Doris catalog
     * @return data type in Nereids
     */
    public static DataType convertFromCatalogDataType(Type catalogType) {
        if (catalogType instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) catalogType;
            switch (scalarType.getPrimitiveType()) {
                case BOOLEAN:
                    return BooleanType.INSTANCE;
                case TINYINT:
                    return TinyIntType.INSTANCE;
                case SMALLINT:
                    return SmallIntType.INSTANCE;
                case INT:
                    return IntegerType.INSTANCE;
                case BIGINT:
                    return BigIntType.INSTANCE;
                case LARGEINT:
                    return LargeIntType.INSTANCE;
                case FLOAT:
                    return FloatType.INSTANCE;
                case DOUBLE:
                    return DoubleType.INSTANCE;
                case CHAR:
                    return CharType.createCharType(scalarType.getLength());
                case VARCHAR:
                    return VarcharType.createVarcharType(scalarType.getLength());
                case STRING:
                    return StringType.INSTANCE;
                case DATE:
                    return DateType.INSTANCE;
                case DATEV2:
                    return DateV2Type.INSTANCE;
                case DATETIME:
                    return DateTimeType.INSTANCE;
                case DATETIMEV2:
                    return DateTimeV2Type.of(scalarType.getScalarScale());
                case DECIMALV2:
                    return DecimalType.createDecimalType(scalarType.decimalPrecision(), scalarType.decimalScale());
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return DecimalV3Type.createDecimalV3Type(scalarType.getScalarPrecision());
                case JSONB:
                    return JsonType.INSTANCE;
                case HLL:
                    return HllType.INSTANCE;
                case BITMAP:
                    return BitmapType.INSTANCE;
                case QUANTILE_STATE:
                    return QuantileStateType.INSTANCE;
                case NULL_TYPE:
                    return NullType.INSTANCE;
                default:
                    throw new AnalysisException("Nereids do not support type: " + scalarType.getPrimitiveType());
            }
        } else if (catalogType.isArrayType()) {
            org.apache.doris.catalog.ArrayType catalogArrayType = (org.apache.doris.catalog.ArrayType) catalogType;
            return ArrayType.of(
                    convertFromCatalogDataType(catalogArrayType.getItemType()),
                    catalogArrayType.getContainsNull());
        } else if (catalogType.isMapType()) {
            throw new AnalysisException("Nereids do not support map type.");
        } else if (catalogType.isStructType()) {
            throw new AnalysisException("Nereids do not support struct type.");
        } else if (catalogType.isMultiRowType()) {
            throw new AnalysisException("Nereids do not support multi row type.");
        } else {
            throw new AnalysisException("Nereids do not support type: " + catalogType);
        }
    }

    /**
     * Convert to data type in Nereids.
     * throw exception when cannot convert to Nereids type
     *
     * @param type data type in string representation
     * @return data type in Nereids
     */
    public static DataType convertFromString(String type) {
        // TODO: use a better way to resolve types
        // TODO: support varchar, char, decimal
        type = type.toLowerCase().trim();
        switch (type) {
            case "bool":
            case "boolean":
                return BooleanType.INSTANCE;
            case "tinyint":
            case "tinyint(4)":
                return TinyIntType.INSTANCE;
            case "smallint":
            case "smallint(6)":
                return SmallIntType.INSTANCE;
            case "int":
            case "int(11)":
                return IntegerType.INSTANCE;
            case "bigint":
            case "bigint(20)":
                return BigIntType.INSTANCE;
            case "largeint":
            case "largeint(40)":
                return LargeIntType.INSTANCE;
            case "float":
                return FloatType.INSTANCE;
            case "double":
                return DoubleType.INSTANCE;
            case "decimal":
                return fromCatalogType(ScalarType.createDecimalType());
            case "text":
            case "string":
                return StringType.INSTANCE;
            case "null":
            case "null_type": // ScalarType.NULL.toSql() return "null_type", so support it
                return NullType.INSTANCE;
            case "date":
                return fromCatalogType(ScalarType.createDateType());
            case "datev2":
                return DateV2Type.INSTANCE;
            case "time":
                return TimeType.INSTANCE;
            case "hll":
                return HllType.INSTANCE;
            case "bitmap":
                return BitmapType.INSTANCE;
            case "quantile_state":
                return QuantileStateType.INSTANCE;
            case "json":
                return JsonType.INSTANCE;
            default:
                Optional<DataType> newType = matchVarchar(type);
                if (newType.isPresent()) {
                    return newType.get();
                }

                newType = matchChar(type);
                if (newType.isPresent()) {
                    return newType.get();
                }

                newType = matchDateTime(type);
                if (newType.isPresent()) {
                    return newType.get();
                }

                newType = matchDecimalType(type);
                if (newType.isPresent()) {
                    return newType.get();
                }
                if (type.startsWith("array")) {
                    return resolveArrayType(type);
                }
                throw new AnalysisException("Nereids do not support type: " + type);
        }
    }

    /**
     * just for generate function and migrate to nereids
     * @param type legacy date type
     * @return nereids's data type
     */
    @Developing // should support decimal_v3
    public static DataType fromCatalogType(Type type) {
        if (type.isBoolean()) {
            return BooleanType.INSTANCE;
        } else if (type == Type.TINYINT) {
            return TinyIntType.INSTANCE;
        } else if (type == Type.SMALLINT) {
            return SmallIntType.INSTANCE;
        } else if (type == Type.INT) {
            return IntegerType.INSTANCE;
        } else if (type == Type.BIGINT) {
            return BigIntType.INSTANCE;
        } else if (type == Type.LARGEINT) {
            return LargeIntType.INSTANCE;
        } else if (type == Type.FLOAT) {
            return FloatType.INSTANCE;
        } else if (type == Type.DOUBLE) {
            return DoubleType.INSTANCE;
        } else if (type == Type.STRING) {
            return StringType.INSTANCE;
        } else if (type.isNull()) {
            return NullType.INSTANCE;
        } else if (type.isDatetimeV2()) {
            return DateTimeV2Type.of(((ScalarType) type).getScalarScale());
        } else if (type.isDatetime()) {
            return DateTimeType.INSTANCE;
        } else if (type.isDateV2()) {
            return DateV2Type.INSTANCE;
        } else if (type.isDateType()) {
            return DateType.INSTANCE;
        } else if (type.isTimeV2()) {
            return TimeV2Type.INSTANCE;
        } else if (type.isTime()) {
            return TimeType.INSTANCE;
        } else if (type.isHllType()) {
            return HllType.INSTANCE;
        } else if (type.isBitmapType()) {
            return BitmapType.INSTANCE;
        } else if (type.isQuantileStateType()) {
            return QuantileStateType.INSTANCE;
        } else if (type.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.CHAR) {
            return CharType.createCharType(type.getLength());
        } else if (type.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.VARCHAR) {
            return VarcharType.createVarcharType(type.getLength());
        } else if (type.isDecimalV3()) {
            ScalarType scalarType = (ScalarType) type;
            int precision = scalarType.getScalarPrecision();
            return DecimalV3Type.createDecimalV3Type(precision);
        } else if (type.isDecimalV2()) {
            ScalarType scalarType = (ScalarType) type;
            int precision = scalarType.getScalarPrecision();
            int scale = scalarType.getScalarScale();
            return DecimalV2Type.createDecimalV2Type(precision, scale);
        } else if (type.isJsonbType()) {
            return JsonType.INSTANCE;
        } else if (type.isStructType()) {
            return StructType.INSTANCE;
        } else if (type.isMapType()) {
            return MapType.INSTANCE;
        } else if (type.isArrayType()) {
            org.apache.doris.catalog.ArrayType arrayType = (org.apache.doris.catalog.ArrayType) type;
            return ArrayType.of(fromCatalogType(arrayType.getItemType()), arrayType.getContainsNull());
        }
        throw new AnalysisException("Nereids do not support type: " + type);
    }

    public abstract String toSql();

    @Override
    public String toString() {
        return toSql();
    }

    public String typeName() {
        return this.getClass().getSimpleName().replace("Type", "").toLowerCase(Locale.ROOT);
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return sameType(other);
    }

    /**
     * this and other is same type.
     */
    private boolean sameType(AbstractDataType other) {
        return this.equals(other);
    }

    @Override
    public String simpleString() {
        return typeName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public boolean isBooleanType() {
        return this instanceof BooleanType;
    }

    public boolean isTinyIntType() {
        return this instanceof TinyIntType;
    }

    public boolean isSmallIntType() {
        return this instanceof SmallIntType;
    }

    public boolean isIntType() {
        return this instanceof IntegerType;
    }

    public boolean isBigIntType() {
        return this instanceof BigIntType;
    }

    public boolean isLargeIntType() {
        return this instanceof LargeIntType;
    }

    public boolean isFloatType() {
        return this instanceof FloatType;
    }

    public boolean isDoubleType() {
        return this instanceof DoubleType;
    }

    public boolean isDecimalType() {
        return this instanceof DecimalType;
    }

    public boolean isDateTime() {
        return this instanceof DateTimeType;
    }

    public boolean isDate() {
        return this instanceof DateType;
    }

    public boolean isDateType() {
        return isDate() || isDateTime();
    }

    public boolean isNullType() {
        return this instanceof NullType;
    }

    public boolean isNumericType() {
        return this instanceof NumericType;
    }

    public boolean isCharType() {
        return this instanceof CharType;
    }

    public boolean isVarcharType() {
        return this instanceof VarcharType;
    }

    public boolean isStringType() {
        return this instanceof CharacterType;
    }

    public boolean isPrimitive() {
        return this instanceof PrimitiveType;
    }

    public DataType promotion() {
        if (PROMOTION_MAP.containsKey(this.getClass())) {
            return PROMOTION_MAP.get(this.getClass()).get();
        } else {
            return this;
        }
    }

    public abstract int width();

    private static Optional<DataType> matchChar(String type) {
        Matcher matcher = CHAR_PATTERN.matcher(type);
        if (matcher.find() && matcher.group().equals(type)) {
            String len = matcher.group(2);
            CharType charType = len != null
                    ? CharType.createCharType(Integer.valueOf(len))
                    : CharType.SYSTEM_DEFAULT;
            return Optional.of(charType);
        }
        return Optional.empty();
    }

    private static Optional<DataType> matchVarchar(String type) {
        Matcher matcher = VARCHAR_PATTERN.matcher(type);
        if (matcher.find() && matcher.group().equals(type)) {
            String scale = matcher.group(2);
            VarcharType varcharType = scale != null
                    ? VarcharType.createVarcharType(Integer.valueOf(scale))
                    : VarcharType.SYSTEM_DEFAULT;
            return Optional.of(varcharType);
        }
        return Optional.empty();
    }

    private static Optional<DataType> matchDateTime(String type) {
        Matcher matcher = DATETIME_PATTERN.matcher(type);
        if (matcher.find() && matcher.group().equals(type)) {
            String scale = matcher.group(2);
            return Optional.of(
                    fromCatalogType(scale != null
                        ? ScalarType.createDatetimeV2Type(Integer.valueOf(scale))
                        : ScalarType.createDatetimeType())
            );
        }
        matcher = DATETIMEV2_PATTERN.matcher(type);
        if (matcher.find()) {
            String scale = matcher.group(2);
            return Optional.of(
                    fromCatalogType(scale != null
                            ? ScalarType.createDatetimeV2Type(Integer.valueOf(scale))
                            : ScalarType.createDatetimeV2Type(0))
            );
        }
        return Optional.empty();
    }

    private static Optional<DataType> matchDecimalType(String type) {
        Matcher matcher = DECIMAL_PATTERN.matcher(type);
        if (matcher.find() && matcher.group().equals(type)) {
            String precision = matcher.group(2);
            String scale = matcher.group(4);
            if (scale != null) {
                return Optional.of(fromCatalogType(
                        ScalarType.createDecimalType(Integer.valueOf(precision), Integer.valueOf(scale))));
            }

            if (precision != null) {
                return Optional.of(fromCatalogType(ScalarType.createDecimalType(Integer.valueOf(precision))));
            }
            return Optional.of(fromCatalogType(ScalarType.createDecimalType()));
        }

        matcher = DECIMALV3_PATTERN.matcher(type);
        if (matcher.find() && matcher.group().equals(type)) {
            String precision = matcher.group(2);
            String scale = matcher.group(4);
            if (scale != null) {
                return Optional.of(fromCatalogType(
                        ScalarType.createDecimalV3Type(Integer.valueOf(precision), Integer.valueOf(scale))));
            }

            if (precision != null) {
                return Optional.of(fromCatalogType(ScalarType.createDecimalV3Type(Integer.valueOf(precision))));
            }
            return Optional.of(fromCatalogType(ScalarType.createDecimalV3Type()));
        }
        return Optional.empty();
    }

    private static ArrayType resolveArrayType(String type) {
        if (!type.startsWith("array")) {
            throw new AnalysisException("Not array type: " + type);
        }

        type = type.substring("array".length());
        if (type.startsWith("<") && type.endsWith(">")) {
            DataType itemType = convertFromString(type.substring(1, type.length() - 1));
            if (itemType.equals(NullType.INSTANCE)) {
                return ArrayType.SYSTEM_DEFAULT;
            }
            return ArrayType.of(itemType);
        } else if (type.isEmpty()) {
            return ArrayType.SYSTEM_DEFAULT;
        } else {
            throw new AnalysisException("Illegal array type: " + type);
        }
    }
}
