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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
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
public abstract class DataType {
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
    public static DataType convertPrimitiveFromStrings(List<String> types, boolean unsigned) {
        String type = types.get(0).toLowerCase().trim();
        DataType dataType;
        switch (type) {
            case "boolean":
                dataType = BooleanType.INSTANCE;
                break;
            case "tinyint":
                dataType = TinyIntType.INSTANCE;
                break;
            case "smallint":
                dataType = SmallIntType.INSTANCE;
                break;
            case "int":
            case "integer":
                dataType = IntegerType.INSTANCE;
                break;
            case "bigint":
                dataType = BigIntType.INSTANCE;
                break;
            case "largeint":
                dataType = LargeIntType.INSTANCE;
                break;
            case "float":
                dataType = FloatType.INSTANCE;
                break;
            case "double":
                dataType = DoubleType.INSTANCE;
                break;
            case "decimal":
                // NOTICE, maybe convert to decimalv3, so do not truc here.
                switch (types.size()) {
                    case 1:
                        dataType = DecimalV2Type.CATALOG_DEFAULT;
                        break;
                    case 2:
                        dataType = DecimalV2Type.createDecimalV2TypeWithoutTruncate(
                                Integer.parseInt(types.get(1)), 0);
                        break;
                    case 3:
                        dataType = DecimalV2Type.createDecimalV2TypeWithoutTruncate(
                                Integer.parseInt(types.get(1)), Integer.parseInt(types.get(2)));
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "decimalv2":
                // NOTICE, maybe convert to decimalv3, so do not truc here.
                switch (types.size()) {
                    case 1:
                        dataType = DecimalV2Type.CATALOG_DEFAULT_NOT_CONVERSION;
                        break;
                    case 2:
                        dataType = DecimalV2Type.createDecimalV2TypeWithoutTruncate(
                                Integer.parseInt(types.get(1)), 0, false);
                        break;
                    case 3:
                        dataType = DecimalV2Type.createDecimalV2TypeWithoutTruncate(
                                Integer.parseInt(types.get(1)), Integer.parseInt(types.get(2)), false);
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "decimalv3":
                switch (types.size()) {
                    case 1:
                        dataType = DecimalV3Type.CATALOG_DEFAULT;
                        break;
                    case 2:
                        dataType = DecimalV3Type.createDecimalV3Type(Integer.parseInt(types.get(1)));
                        break;
                    case 3:
                        dataType = DecimalV3Type.createDecimalV3Type(
                                Integer.parseInt(types.get(1)), Integer.parseInt(types.get(2)));
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "text":
            case "string":
                dataType = StringType.INSTANCE;
                break;
            case "varchar":
                switch (types.size()) {
                    case 1:
                        dataType = VarcharType.SYSTEM_DEFAULT;
                        break;
                    case 2:
                        if (types.get(1).equals("*")) {
                            dataType = VarcharType.SYSTEM_DEFAULT;
                        } else {
                            dataType = VarcharType.createVarcharType(Integer.parseInt(types.get(1)));
                        }
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "character":
            case "char":
                switch (types.size()) {
                    case 1:
                        dataType = CharType.SYSTEM_DEFAULT;
                        break;
                    case 2:
                        if (types.get(1).equals("*")) {
                            dataType = CharType.SYSTEM_DEFAULT;
                        } else {
                            dataType = CharType.createCharType(Integer.parseInt(types.get(1)));
                        }
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "null":
            case "null_type": // ScalarType.NULL.toSql() return "null_type", so support it
                dataType = NullType.INSTANCE;
                break;
            case "date":
                dataType = DateType.INSTANCE;
                break;
            case "datev1":
                dataType = DateType.NOT_CONVERSION;
                break;
            case "datev2":
                dataType = DateV2Type.INSTANCE;
                break;
            case "time":
                dataType = TimeType.INSTANCE;
                break;
            case "datetime":
                switch (types.size()) {
                    case 1:
                        dataType = DateTimeType.INSTANCE;
                        break;
                    case 2:
                        dataType = DateTimeV2Type.of(Integer.parseInt(types.get(1)));
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "datetimev1":
                switch (types.size()) {
                    case 1:
                        dataType = DateTimeType.NOT_CONVERSION;
                        break;
                    case 2:
                        throw new AnalysisException("Nereids do not support datetimev1 type with precision");
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "datetimev2":
                switch (types.size()) {
                    case 1:
                        dataType = DateTimeV2Type.SYSTEM_DEFAULT;
                        break;
                    case 2:
                        dataType = DateTimeV2Type.of(Integer.parseInt(types.get(1)));
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
                break;
            case "hll":
                dataType = HllType.INSTANCE;
                break;
            case "bitmap":
                dataType = BitmapType.INSTANCE;
                break;
            case "quantile_state":
                dataType = QuantileStateType.INSTANCE;
                break;
            case "json":
            case "jsonb":
                dataType = JsonType.INSTANCE;
                break;
            case "ipv4":
                dataType = IPv4Type.INSTANCE;
                break;
            case "ipv6":
                dataType = IPv6Type.INSTANCE;
                break;
            case "variant":
                dataType = VariantType.INSTANCE;
                break;
            default:
                throw new AnalysisException("Nereids do not support type: " + type);
        }
        if (unsigned) {
            return dataType.promotion();
        } else {
            return dataType;
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
        return PARSER.parseDataType(type);
    }

    /**
     * just for generate function and migrate to nereids
     * @param type legacy date type
     * @return nereids's data type
     */
    @Developing // should support map, struct
    public static DataType fromCatalogType(Type type) {
        switch (type.getPrimitiveType()) {
            case BOOLEAN: return BooleanType.INSTANCE;
            case TINYINT: return TinyIntType.INSTANCE;
            case SMALLINT: return SmallIntType.INSTANCE;
            case INT: return IntegerType.INSTANCE;
            case BIGINT: return BigIntType.INSTANCE;
            case LARGEINT: return LargeIntType.INSTANCE;
            case FLOAT: return FloatType.INSTANCE;
            case DOUBLE: return DoubleType.INSTANCE;
            case NULL_TYPE: return NullType.INSTANCE;
            case DATETIMEV2: return DateTimeV2Type.of(((ScalarType) type).getScalarScale());
            case DATETIME: return DateTimeType.INSTANCE;
            case DATEV2: return DateV2Type.INSTANCE;
            case DATE: return DateType.INSTANCE;
            case TIMEV2: return TimeV2Type.INSTANCE;
            case TIME: return TimeType.INSTANCE;
            case HLL: return HllType.INSTANCE;
            case BITMAP: return BitmapType.INSTANCE;
            case QUANTILE_STATE: return QuantileStateType.INSTANCE;
            case CHAR: return CharType.createCharType(type.getLength());
            case VARCHAR: return VarcharType.createVarcharType(type.getLength());
            case STRING: return StringType.INSTANCE;
            case VARIANT: return VariantType.INSTANCE;
            case JSONB: return JsonType.INSTANCE;
            case IPV4: return IPv4Type.INSTANCE;
            case IPV6: return IPv6Type.INSTANCE;
            case AGG_STATE: {
                org.apache.doris.catalog.AggStateType catalogType = ((org.apache.doris.catalog.AggStateType) type);
                List<DataType> types = catalogType.getSubTypes().stream().map(DataType::fromCatalogType)
                        .collect(Collectors.toList());
                return new AggStateType(catalogType.getFunctionName(), types, catalogType.getSubTypeNullables());
            }
            case DECIMALV2: {
                ScalarType scalarType = (ScalarType) type;
                int precision = scalarType.getScalarPrecision();
                int scale = scalarType.getScalarScale();
                return DecimalV2Type.createDecimalV2Type(precision, scale);
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256: {
                ScalarType scalarType = (ScalarType) type;
                int precision = scalarType.getScalarPrecision();
                int scale = scalarType.getScalarScale();
                return DecimalV3Type.createDecimalV3TypeNoCheck(precision, scale);
            }
            default: {
            }
        }

        if (type.isStructType()) {
            List<StructField> structFields = ((org.apache.doris.catalog.StructType) (type)).getFields().stream()
                    .map(cf -> new StructField(cf.getName(), fromCatalogType(cf.getType()),
                            cf.getContainsNull(), cf.getComment() == null ? "" : cf.getComment()))
                    .collect(ImmutableList.toImmutableList());
            return new StructType(structFields);
        } else if (type.isMapType()) {
            org.apache.doris.catalog.MapType mapType = (org.apache.doris.catalog.MapType) type;
            return MapType.of(fromCatalogType(mapType.getKeyType()), fromCatalogType(mapType.getValueType()));
        } else if (type.isArrayType()) {
            org.apache.doris.catalog.ArrayType arrayType = (org.apache.doris.catalog.ArrayType) type;
            return ArrayType.of(fromCatalogType(arrayType.getItemType()), arrayType.getContainsNull());
        } else {
            return UnsupportedType.INSTANCE;
        }
    }

    /**
     * convert nereids's data type to legacy catalog data type
     * @return legacy catalog data type
     */
    public abstract Type toCatalogDataType();

    /**
     * sql format of this type
     */
    public abstract String toSql();

    @Override
    public String toString() {
        return toSql();
    }

    public String typeName() {
        return this.getClass().getSimpleName().replace("Type", "").toLowerCase(Locale.ROOT);
    }

    public DataType defaultConcreteType() {
        return this;
    }

    public boolean acceptsType(DataType other) {
        return sameType(other);
    }

    /**
     * this and other is same type.
     */
    private boolean sameType(DataType other) {
        return this.equals(other);
    }

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

    public boolean isIPv4Type() {
        return this instanceof IPv4Type;
    }

    public boolean isIPType() {
        return isIPv4Type() || isIPv6Type();
    }

    public boolean isIPv6Type() {
        return this instanceof IPv6Type;
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

    public boolean isVariantType() {
        return this instanceof VariantType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isOnlyMetricType() {
        return isObjectType() || isComplexType() || isJsonType() || isVariantType();
    }

    public boolean isObjectType() {
        return isHllType() || isBitmapType() || isQuantileStateType();
    }

    public DataType conversion() {
        return this;
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

    /**
     * whether the target dataType is assignable to this dataType
     * @param targetDataType the target data type
     * @return true if assignable
     */
    @Developing
    public boolean isAssignableFrom(DataType targetDataType) {
        if (this.equals(targetDataType)) {
            return true;
        }
        if (this instanceof CharacterType) {
            return true;
        }
        return false;
    }
}
