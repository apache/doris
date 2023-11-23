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
import org.apache.doris.common.Config;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract class for all data type in Nereids.
 */
public abstract class DataType implements AbstractDataType {
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_PRECISION = 9;

    protected static final NereidsParser PARSER = new NereidsParser();

    // use class and supplier here to avoid class load deadlock.
    private static final Map<Class<? extends PrimitiveType>, Supplier<DataType>> PROMOTION_MAP
            = ImmutableMap.<Class<? extends PrimitiveType>, Supplier<DataType>>builder()
            .put(TinyIntType.class, () -> SmallIntType.INSTANCE)
            .put(SmallIntType.class, () -> IntegerType.INSTANCE)
            .put(IntegerType.class, () -> BigIntType.INSTANCE)
            .put(FloatType.class, () -> DoubleType.INSTANCE)
            .put(VarcharType.class, () -> StringType.INSTANCE)
            .put(CharType.class, () -> StringType.INSTANCE)
            .build();

    @Developing("This map is just use to search which itemType of the ArrayType is implicit castable for temporary."
            + "Maybe complete it after refactor TypeCoercion.")
    private static final Map<Class<? extends DataType>, Promotion<DataType>> FULL_PRIMITIVE_TYPE_PROMOTION_MAP
            = Promotion.builder()
            .add(BooleanType.class, () -> ImmutableList.of(TinyIntType.INSTANCE))
            .add(TinyIntType.class, () -> ImmutableList.of(SmallIntType.INSTANCE))
            .add(SmallIntType.class, () -> ImmutableList.of(IntegerType.INSTANCE))
            .add(IntegerType.class, () -> ImmutableList.of(BigIntType.INSTANCE))
            .add(BigIntType.class, () -> ImmutableList.of(LargeIntType.INSTANCE))
            .add(LargeIntType.class, () -> ImmutableList.of(FloatType.INSTANCE, StringType.INSTANCE))
            .add(FloatType.class, () -> ImmutableList.of(
                    DoubleType.INSTANCE, DecimalV3Type.WILDCARD, StringType.INSTANCE))
            .add(DoubleType.class, () -> ImmutableList.of(DecimalV2Type.SYSTEM_DEFAULT, StringType.INSTANCE))
            .add(DecimalV2Type.class, () -> ImmutableList.of(DecimalV3Type.WILDCARD, StringType.INSTANCE))
            .add(DateType.class, () -> ImmutableList.of(
                    DateTimeType.INSTANCE, DateV2Type.INSTANCE, StringType.INSTANCE))
            .add(DateV2Type.class, () -> ImmutableList.of(DateTimeV2Type.SYSTEM_DEFAULT, StringType.INSTANCE))
            .build();

    /**
     * create a specific Literal for a given dataType
     */
    public static Literal promoteLiteral(Object value, DataType dataType) {
        if (dataType.equals(SmallIntType.INSTANCE)) {
            return new SmallIntLiteral(((Number) value).shortValue());
        } else if (dataType.equals(IntegerType.INSTANCE)) {
            return new IntegerLiteral(((Number) value).intValue());
        } else if (dataType.equals(BigIntType.INSTANCE)) {
            return new BigIntLiteral(((Number) value).longValue());
        } else if (dataType.equals(DoubleType.INSTANCE)) {
            return new DoubleLiteral(((Number) value).doubleValue());
        } else if (dataType.equals(StringType.INSTANCE)) {
            return new StringLiteral((String) value);
        }
        return null;
    }

    /**
     * Convert to data type in Nereids.
     * throw exception when cannot convert to Nereids type
     * NOTICE: only used in parser, if u want to convert string to type,
     * please use {@link this#convertFromString(String)}
     *
     * @param types data type in string representation
     * @return data type in Nereids
     */
    public static DataType convertPrimitiveFromStrings(List<String> types, boolean tryConvert) {
        String type = types.get(0).toLowerCase().trim();
        switch (type) {
            case "boolean":
                return BooleanType.INSTANCE;
            case "tinyint":
                return TinyIntType.INSTANCE;
            case "smallint":
                return SmallIntType.INSTANCE;
            case "int":
            case "integer":
                return IntegerType.INSTANCE;
            case "bigint":
                return BigIntType.INSTANCE;
            case "largeint":
                return LargeIntType.INSTANCE;
            case "float":
                return FloatType.INSTANCE;
            case "double":
                return DoubleType.INSTANCE;
            case "decimal":
                if (Config.enable_decimal_conversion && tryConvert) {
                    switch (types.size()) {
                        case 1:
                            return DecimalV3Type.SYSTEM_DEFAULT;
                        case 2:
                            return DecimalV3Type
                                    .createDecimalV3Type(Integer.parseInt(types.get(1)));
                        case 3:
                            return DecimalV3Type.createDecimalV3Type(Integer.parseInt(types.get(1)),
                                    Integer.parseInt(types.get(2)));
                        default:
                            throw new AnalysisException("Nereids do not support type: " + type);
                    }
                } else {
                    switch (types.size()) {
                        case 1:
                            return DecimalV2Type.SYSTEM_DEFAULT;
                        case 2:
                            return DecimalV2Type.createDecimalV2Type(Integer.parseInt(types.get(1)),
                                    0);
                        case 3:
                            return DecimalV2Type.createDecimalV2Type(Integer.parseInt(types.get(1)),
                                    Integer.parseInt(types.get(2)));
                        default:
                            throw new AnalysisException("Nereids do not support type: " + type);
                    }
                }
            case "decimalv3":
                switch (types.size()) {
                    case 1:
                        return DecimalV3Type.SYSTEM_DEFAULT;
                    case 2:
                        return DecimalV3Type.createDecimalV3Type(Integer.parseInt(types.get(1)));
                    case 3:
                        return DecimalV3Type.createDecimalV3Type(
                                Integer.parseInt(types.get(1)), Integer.parseInt(types.get(2)));
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
            case "text":
            case "string":
                return StringType.INSTANCE;
            case "varchar":
                switch (types.size()) {
                    case 1:
                        return VarcharType.SYSTEM_DEFAULT;
                    case 2:
                        if (types.get(1).equals("*")) {
                            return VarcharType.SYSTEM_DEFAULT;
                        } else {
                            return VarcharType.createVarcharType(Integer.parseInt(types.get(1)));
                        }
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
            case "character":
            case "char":
                switch (types.size()) {
                    case 1:
                        return CharType.SYSTEM_DEFAULT;
                    case 2:
                        if (types.get(1).equals("*")) {
                            return CharType.SYSTEM_DEFAULT;
                        } else {
                            return CharType.createCharType(Integer.parseInt(types.get(1)));
                        }
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
            case "null":
            case "null_type": // ScalarType.NULL.toSql() return "null_type", so support it
                return NullType.INSTANCE;
            case "date":
                return Config.enable_date_conversion && tryConvert ? DateV2Type.INSTANCE
                        : DateType.INSTANCE;
            case "datev2":
                return DateV2Type.INSTANCE;
            case "time":
                return TimeType.INSTANCE;
            case "datetime":
                switch (types.size()) {
                    case 1:
                        return Config.enable_date_conversion && tryConvert
                                ? DateTimeV2Type.SYSTEM_DEFAULT
                                : DateTimeType.INSTANCE;
                    case 2:
                        return DateTimeV2Type.of(Integer.parseInt(types.get(1)));
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
            case "datetimev2":
                switch (types.size()) {
                    case 1:
                        return DateTimeV2Type.SYSTEM_DEFAULT;
                    case 2:
                        return DateTimeV2Type.of(Integer.parseInt(types.get(1)));
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
            case "hll":
                return HllType.INSTANCE;
            case "bitmap":
                return BitmapType.INSTANCE;
            case "quantile_state":
                return QuantileStateType.INSTANCE;
            case "json":
            case "jsonb":
                return JsonType.INSTANCE;
            default:
                throw new AnalysisException("Nereids do not support type: " + type);
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
        try {
            List<String> types = PARSER.parseDataType(type);
            return DataType.convertPrimitiveFromStrings(types, false);
        } catch (Exception e) {
            // TODO: remove it when Nereids parser support array
            if (type.startsWith("array")) {
                return resolveArrayType(type);
            }
            throw e;
        }
    }

    /**
     * just for generate function and migrate to nereids
     * @param type legacy date type
     * @return nereids's data type
     */
    @Developing // should support map, struct
    public static DataType fromCatalogType(Type type) {
        if (type.isBoolean()) {
            return BooleanType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.TINYINT.getPrimitiveType()) {
            return TinyIntType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.SMALLINT.getPrimitiveType()) {
            return SmallIntType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.INT.getPrimitiveType()) {
            return IntegerType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.BIGINT.getPrimitiveType()) {
            return BigIntType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.LARGEINT.getPrimitiveType()) {
            return LargeIntType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.FLOAT.getPrimitiveType()) {
            return FloatType.INSTANCE;
        } else if (type.getPrimitiveType() == Type.DOUBLE.getPrimitiveType()) {
            return DoubleType.INSTANCE;
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
        } else if (type.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.STRING) {
            return StringType.INSTANCE;
        } else if (type.isDecimalV3()) {
            ScalarType scalarType = (ScalarType) type;
            int precision = scalarType.getScalarPrecision();
            int scale = scalarType.getScalarScale();
            return DecimalV3Type.createDecimalV3Type(precision, scale);
        } else if (type.isDecimalV2()) {
            ScalarType scalarType = (ScalarType) type;
            int precision = scalarType.getScalarPrecision();
            int scale = scalarType.getScalarScale();
            return DecimalV2Type.createDecimalV2Type(precision, scale);
        } else if (type.isJsonbType()) {
            return JsonType.INSTANCE;
        } else if (type.isStructType()) {
            // TODO: support struct type really
            return StructType.INSTANCE;
        } else if (type.isMapType()) {
            // TODO: support map type really
            return MapType.INSTANCE;
        } else if (type.isArrayType()) {
            // TODO: support array type really
            org.apache.doris.catalog.ArrayType arrayType = (org.apache.doris.catalog.ArrayType) type;
            return ArrayType.of(fromCatalogType(arrayType.getItemType()), arrayType.getContainsNull());
        } else if (type.isAggStateType()) {
            org.apache.doris.catalog.AggStateType catalogType = ((org.apache.doris.catalog.AggStateType) type);
            List<DataType> types = catalogType.getSubTypes().stream().map(t -> fromCatalogType(t))
                    .collect(Collectors.toList());
            return new AggStateType(catalogType.getFunctionName(), types, catalogType.getSubTypeNullables());
        } else {
            return UnsupportedType.INSTANCE;
        }
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

    public boolean isIntegerLikeType() {
        return this instanceof IntegralType && !(this instanceof LargeIntType);
    }

    public boolean isFloatLikeType() {
        return this.isFloatType() || isDoubleType();
    }

    public boolean isTinyIntType() {
        return this instanceof TinyIntType;
    }

    public boolean isSmallIntType() {
        return this instanceof SmallIntType;
    }

    public boolean isIntegerType() {
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

    public boolean isDecimalV2Type() {
        return this instanceof DecimalV2Type;
    }

    public boolean isDecimalV3Type() {
        return this instanceof DecimalV3Type;
    }

    public boolean isDecimalLikeType() {
        return isDecimalV2Type() || isDecimalV3Type();
    }

    public boolean isDateTimeType() {
        return this instanceof DateTimeType;
    }

    public boolean isDateType() {
        return this instanceof DateType;
    }

    public boolean isDateLikeType() {
        return isDateType() || isDateTimeType() || isDateV2Type() || isDateTimeV2Type();
    }

    public boolean isDateV2LikeType() {
        return isDateV2Type() || isDateTimeV2Type();
    }

    public boolean isTimeType() {
        return this instanceof TimeType;
    }

    public boolean isTimeV2Type() {
        return this instanceof TimeV2Type;
    }

    public boolean isTimeLikeType() {
        return isTimeType() || isTimeV2Type();
    }

    public boolean isNullType() {
        return this instanceof NullType;
    }

    public boolean isIntegralType() {
        return this instanceof IntegralType;
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
        return this instanceof StringType;
    }

    public boolean isJsonType() {
        return this instanceof JsonType;
    }

    public boolean isStringLikeType() {
        return this instanceof CharacterType;
    }

    public boolean isPrimitive() {
        return this instanceof PrimitiveType;
    }

    public boolean isDateV2Type() {
        return this instanceof DateV2Type;
    }

    public boolean isDateTimeV2Type() {
        return this instanceof DateTimeV2Type;
    }

    public boolean isBitmapType() {
        return this instanceof BitmapType;
    }

    public boolean isQuantileStateType() {
        return this instanceof QuantileStateType;
    }

    public boolean isAggStateType() {
        return this instanceof AggStateType;
    }

    public boolean isHllType() {
        return this instanceof HllType;
    }

    public boolean isComplexType() {
        return !isPrimitive();
    }

    public boolean isArrayType() {
        return this instanceof ArrayType;
    }

    public boolean isMapType() {
        return this instanceof MapType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isOnlyMetricType() {
        return isObjectType() || isComplexType() || isJsonType();
    }

    public boolean isObjectType() {
        return isHllType() || isBitmapType() || isQuantileStateType();
    }

    public DataType promotion() {
        if (PROMOTION_MAP.containsKey(this.getClass())) {
            return PROMOTION_MAP.get(this.getClass()).get();
        } else {
            return this;
        }
    }

    /** getAllPromotions */
    public List<DataType> getAllPromotions() {
        if (this instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) this;
            return arrayType.getItemType()
                    .getAllPromotions()
                    .stream()
                    .map(promotionType -> ArrayType.of(promotionType, arrayType.containsNull()))
                    .collect(ImmutableList.toImmutableList());
        }

        Promotion<DataType> promotion = FULL_PRIMITIVE_TYPE_PROMOTION_MAP.get(this.getClass());
        return promotion == null ? ImmutableList.of() : promotion.apply(this);
    }

    public abstract int width();

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

    public static List<DataType> trivialTypes() {
        return Type.getTrivialTypes()
                .stream()
                .map(DataType::fromCatalogType)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<DataType> supportedTypes() {
        return Type.getSupportedTypes()
                .stream()
                .map(DataType::fromCatalogType)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<DataType> nonNullNonCharTypes() {
        return supportedTypes()
                .stream()
                .filter(type -> !type.isNullType() && !type.isCharType())
                .collect(ImmutableList.toImmutableList());
    }

    interface Promotion<T extends DataType> {
        List<DataType> apply(T dataType);

        static PromotionBuilder builder() {
            return new PromotionBuilder();
        }
    }

    static class PromotionBuilder {
        private final Map<Class<? extends DataType>, Promotion<? extends DataType>> promotionMap
                = Maps.newLinkedHashMap();

        public <T extends DataType> PromotionBuilder add(Class<T> dataTypeClass, Promotion<T> promotion) {
            promotionMap.put(dataTypeClass, promotion);
            return this;
        }

        public <T extends DataType> PromotionBuilder add(Class<T> dataTypeClass, Supplier<List<DataType>> promotion) {
            promotionMap.put(dataTypeClass, type -> promotion.get());
            return this;
        }

        public Map<Class<? extends DataType>, Promotion<DataType>> build() {
            return (Map) ImmutableMap.copyOf(promotionMap);
        }
    }

    public double rangeLength(double high, double low) {
        return high - low;
    }
}
