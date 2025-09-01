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
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.types.coercion.PrimitiveType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract class for all data type in Nereids.
 */
public abstract class DataType {
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_PRECISION = 9;

    protected static final NereidsParser PARSER = new NereidsParser();

    private static class TypeMapLoader {
        static final Map<org.apache.doris.catalog.PrimitiveType, DataType> LEGACY_MAP = initMap();

        private static Map<org.apache.doris.catalog.PrimitiveType, DataType> initMap() {
            return ImmutableMap.<org.apache.doris.catalog.PrimitiveType, DataType>builder()
                    .put(Type.BOOLEAN.getPrimitiveType(), BooleanType.INSTANCE)
                    .put(Type.TINYINT.getPrimitiveType(), TinyIntType.INSTANCE)
                    .put(Type.SMALLINT.getPrimitiveType(), SmallIntType.INSTANCE)
                    .put(Type.INT.getPrimitiveType(), IntegerType.INSTANCE)
                    .put(Type.BIGINT.getPrimitiveType(), BigIntType.INSTANCE)
                    .put(Type.LARGEINT.getPrimitiveType(), LargeIntType.INSTANCE)
                    .put(Type.FLOAT.getPrimitiveType(), FloatType.INSTANCE)
                    .put(Type.DOUBLE.getPrimitiveType(), DoubleType.INSTANCE)
                    .put(Type.CHAR.getPrimitiveType(), StringType.INSTANCE)
                    .put(Type.VARCHAR.getPrimitiveType(), StringType.INSTANCE)
                    .put(Type.STRING.getPrimitiveType(), StringType.INSTANCE)
                    .put(Type.DATE.getPrimitiveType(), DateType.INSTANCE)
                    .put(Type.DATEV2.getPrimitiveType(), DateType.INSTANCE)
                    .put(Type.DATETIME.getPrimitiveType(), DateTimeType.INSTANCE)
                    .put(Type.DATETIMEV2.getPrimitiveType(), DateTimeV2Type.SYSTEM_DEFAULT)
                    .put(Type.DECIMALV2.getPrimitiveType(), DecimalV2Type.SYSTEM_DEFAULT)
                    .put(Type.DECIMAL32.getPrimitiveType(), DecimalV3Type.SYSTEM_DEFAULT)
                    .put(Type.DECIMAL64.getPrimitiveType(), DecimalV3Type.SYSTEM_DEFAULT)
                    .put(Type.DECIMAL128.getPrimitiveType(), DecimalV3Type.SYSTEM_DEFAULT)
                    .put(Type.DECIMAL256.getPrimitiveType(), DecimalV3Type.SYSTEM_DEFAULT)
                    .put(Type.IPV4.getPrimitiveType(), IPv4Type.INSTANCE)
                    .put(Type.IPV6.getPrimitiveType(), IPv6Type.INSTANCE)
                    .build();
        }
    }

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
            .add(TimeV2Type.class, () -> ImmutableList.of(DateTimeV2Type.MAX, StringType.INSTANCE))
            .build();

    public static Map<org.apache.doris.catalog.PrimitiveType, DataType> legacyTypeToNereidsType() {
        return TypeMapLoader.LEGACY_MAP;
    }

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
    public static DataType convertPrimitiveFromStrings(List<String> types) {
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
                        if (Config.enable_decimal_conversion) {
                            return DecimalV3Type.createDecimalV3Type(38, 9);
                        } else {
                            dataType = DecimalV2Type.CATALOG_DEFAULT;
                        }
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
                        dataType = DecimalV3Type.createDecimalV3Type(38, 9);
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
            case "timev2":
                switch (types.size()) {
                    case 1:
                        dataType = TimeV2Type.INSTANCE;
                        break;
                    case 2:
                        dataType = TimeV2Type.of(Integer.parseInt(types.get(1)));
                        break;
                    default:
                        throw new AnalysisException("Nereids do not support type: " + type);
                }
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
        return dataType;
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
            case TIMEV2: return TimeV2Type.of(((ScalarType) type).getScalarScale());
            case HLL: return HllType.INSTANCE;
            case BITMAP: return BitmapType.INSTANCE;
            case QUANTILE_STATE: return QuantileStateType.INSTANCE;
            case CHAR: return CharType.createCharType(type.getLength());
            case VARCHAR: return VarcharType.createVarcharType(type.getLength());
            case STRING: return StringType.INSTANCE;
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
        } else if (type.isVariantType()) {
            // In the past, variant metadata used the ScalarType type.
            // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
            if (type instanceof org.apache.doris.catalog.VariantType) {
                List<VariantField> variantFields = ((org.apache.doris.catalog.VariantType) type)
                        .getPredefinedFields().stream()
                        .map(cf -> new VariantField(cf.getPattern(), fromCatalogType(cf.getType()),
                                cf.getComment() == null ? "" : cf.getComment(), cf.getPatternType().toString()))
                        .collect(ImmutableList.toImmutableList());
                return new VariantType(variantFields,
                        ((org.apache.doris.catalog.VariantType) type).getVariantMaxSubcolumnsCount(),
                        ((org.apache.doris.catalog.VariantType) type).getEnableTypedPathsToSparse());
            }
            return VariantType.INSTANCE;
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
        return this instanceof TimeV2Type;
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

    public boolean isObjectOrVariantType() {
        return isObjectType() || isVariantType();
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

    /**
     * whether the element type in array can be calculated in array function
     * @return true if the element type can be calculated
     */
    public boolean canBeCalculatedInArray() {
        return isNumericType() || isBooleanType() || isStringLikeType() || isNullType();
    }

    /**
     * whether the param dataType is same-like type for nested in complex type
     *  same-like type means: string-like, date-like, number type
     */
    public boolean isSameTypeForComplexTypeParam(DataType paramType) {
        if (this.isArrayType() && paramType.isArrayType()) {
            return ((ArrayType) this).getItemType()
                    .isSameTypeForComplexTypeParam(((ArrayType) paramType).getItemType());
        } else if (this.isMapType() && paramType.isMapType()) {
            MapType thisMapType = (MapType) this;
            MapType otherMapType = (MapType) paramType;
            return thisMapType.getKeyType().isSameTypeForComplexTypeParam(otherMapType.getKeyType())
                    && thisMapType.getValueType().isSameTypeForComplexTypeParam(otherMapType.getValueType());
        } else if (this.isStructType() && paramType.isStructType()) {
            StructType thisStructType = (StructType) this;
            StructType otherStructType = (StructType) paramType;
            if (thisStructType.getFields().size() != otherStructType.getFields().size()) {
                return false;
            }
            for (int i = 0; i < thisStructType.getFields().size(); i++) {
                if (!thisStructType.getFields().get(i).getDataType().isSameTypeForComplexTypeParam(
                        otherStructType.getFields().get(i).getDataType())) {
                    return false;
                }
            }
            return true;
        } else if (this.isStringLikeType() && paramType.isStringLikeType()) {
            return true;
        } else if (this.isDateLikeType() && paramType.isDateLikeType()) {
            return true;
        } else {
            return this.isNumericType() && paramType.isNumericType();
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

    public void validateDataType() {
        validateCatalogDataType(toCatalogDataType());
    }

    private static void validateCatalogDataType(Type catalogType) {
        if (catalogType.exceedsMaxNestingDepth()) {
            throw new AnalysisException(
                    String.format("Type exceeds the maximum nesting depth of %s:\n%s",
                            Type.MAX_NESTING_DEPTH, catalogType.toSql()));
        }
        if (!catalogType.isSupported()) {
            throw new AnalysisException("Unsupported data type: " + catalogType.toSql());
        }

        if (catalogType.isScalarType()) {
            validateScalarType((ScalarType) catalogType);
        } else if (catalogType.isComplexType()) {
            // now we not support array / map / struct nesting complex type
            if (catalogType.isArrayType()) {
                Type itemType = ((org.apache.doris.catalog.ArrayType) catalogType).getItemType();
                if (itemType instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) itemType);
                }
            }
            if (catalogType.isMapType()) {
                org.apache.doris.catalog.MapType mt =
                        (org.apache.doris.catalog.MapType) catalogType;
                if (mt.getKeyType() instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) mt.getKeyType());
                }
                if (mt.getValueType() instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) mt.getValueType());
                }
            }
            if (catalogType.isStructType()) {
                ArrayList<org.apache.doris.catalog.StructField> fields =
                        ((org.apache.doris.catalog.StructType) catalogType).getFields();
                Set<String> fieldNames = new HashSet<>();
                for (org.apache.doris.catalog.StructField field : fields) {
                    Type fieldType = field.getType();
                    if (fieldType instanceof ScalarType) {
                        validateNestedType(catalogType, (ScalarType) fieldType);
                        if (!fieldNames.add(field.getName())) {
                            throw new AnalysisException("Duplicate field name " + field.getName()
                                    + " in struct " + catalogType.toSql());
                        }
                    }
                }
            }
        }
    }

    private static void validateNestedType(Type parent, Type child) throws AnalysisException {
        if (child.isNull()) {
            throw new AnalysisException("Unsupported data type: " + child.toSql());
        }
        // check whether the sub-type is supported
        if (!parent.supportSubType(child)) {
            throw new AnalysisException(
                    parent.getPrimitiveType() + " unsupported sub-type: " + child.toSql());
        }
        validateCatalogDataType(child);
    }

    private static void validateScalarType(ScalarType scalarType) {
        org.apache.doris.catalog.PrimitiveType type = scalarType.getPrimitiveType();
        // When string type length is not assigned, it needs to be assigned to 1.
        if (scalarType.getPrimitiveType().isStringType() && !scalarType.isLengthSet()) {
            if (scalarType.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.VARCHAR) {
                // always set varchar length MAX_VARCHAR_LENGTH
                scalarType.setLength(ScalarType.MAX_VARCHAR_LENGTH);
            } else if (scalarType.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.STRING) {
                // always set text length MAX_STRING_LENGTH
                scalarType.setLength(ScalarType.MAX_STRING_LENGTH);
            } else {
                scalarType.setLength(1);
            }
        }
        switch (type) {
            case CHAR:
            case VARCHAR: {
                String name;
                int maxLen;
                if (type == org.apache.doris.catalog.PrimitiveType.VARCHAR) {
                    name = "VARCHAR";
                    maxLen = ScalarType.MAX_VARCHAR_LENGTH;
                } else {
                    name = "CHAR";
                    maxLen = ScalarType.MAX_CHAR_LENGTH;
                }
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.

                if (len <= 0) {
                    throw new AnalysisException(name + " size must be > 0: " + len);
                }
                if (scalarType.getLength() > maxLen) {
                    throw new AnalysisException(name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case DECIMALV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision < 1 || precision > ScalarType.MAX_DECIMALV2_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 27."
                            + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException("Scale of decimal must between 0 and 9."
                            + " Scale was set to: " + scale + ".");
                }
                if (precision - scale > ScalarType.MAX_DECIMALV2_PRECISION
                        - ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException("Invalid decimal type with precision = " + precision
                            + ", scale = " + scale);
                }
                // scale < precision
                if (scale > precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + scale + " and precision is " + precision);
                }
                break;
            }
            case DECIMAL32: {
                int decimal32Precision = scalarType.decimalPrecision();
                int decimal32Scale = scalarType.decimalScale();
                if (decimal32Precision < 1
                        || decimal32Precision > ScalarType.MAX_DECIMAL32_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 9."
                            + " Precision was set to: " + decimal32Precision + ".");
                }
                // scale >= 0
                if (decimal32Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal32Scale + ".");
                }
                // scale < precision
                if (decimal32Scale > decimal32Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal32Scale + " and precision is " + decimal32Precision);
                }
                break;
            }
            case DECIMAL64: {
                int decimal64Precision = scalarType.decimalPrecision();
                int decimal64Scale = scalarType.decimalScale();
                if (decimal64Precision < 1
                        || decimal64Precision > ScalarType.MAX_DECIMAL64_PRECISION) {
                    throw new AnalysisException("Precision of decimal64 must between 1 and 18."
                            + " Precision was set to: " + decimal64Precision + ".");
                }
                // scale >= 0
                if (decimal64Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal64Scale + ".");
                }
                // scale < precision
                if (decimal64Scale > decimal64Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal64Scale + " and precision is " + decimal64Precision);
                }
                break;
            }
            case DECIMAL128: {
                int decimal128Precision = scalarType.decimalPrecision();
                int decimal128Scale = scalarType.decimalScale();
                if (decimal128Precision < 1
                        || decimal128Precision > ScalarType.MAX_DECIMAL128_PRECISION) {
                    throw new AnalysisException("Precision of decimal128 must between 1 and 38."
                            + " Precision was set to: " + decimal128Precision + ".");
                }
                // scale >= 0
                if (decimal128Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal128Scale + ".");
                }
                // scale < precision
                if (decimal128Scale > decimal128Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal128Scale + " and precision is " + decimal128Precision);
                }
                break;
            }
            case DECIMAL256: {
                if (SessionVariable.getEnableDecimal256()) {
                    int precision = scalarType.decimalPrecision();
                    int scale = scalarType.decimalScale();
                    if (precision < 1 || precision > ScalarType.MAX_DECIMAL256_PRECISION) {
                        throw new AnalysisException("Precision of decimal256 must between 1 and 76."
                                + " Precision was set to: " + precision + ".");
                    }
                    // scale >= 0
                    if (scale < 0) {
                        throw new AnalysisException("Scale of decimal must not be less than 0."
                                + " Scale was set to: " + scale + ".");
                    }
                    // scale < precision
                    if (scale > precision) {
                        throw new AnalysisException(
                                "Scale of decimal must be smaller than precision." + " Scale is "
                                        + scale + " and precision is " + precision);
                    }
                    break;
                } else {
                    int precision = scalarType.decimalPrecision();
                    throw new AnalysisException("Column of type Decimal256 with precision "
                            + precision + " in not supported.");
                }
            }
            case TIMEV2:
            case DATETIMEV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision != ScalarType.DATETIME_PRECISION) {
                    throw new AnalysisException(
                            "Precision of Datetime/Time must be " + ScalarType.DATETIME_PRECISION
                                    + "." + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > 6) {
                    throw new AnalysisException("Scale of Datetime/Time must between 0 and 6."
                            + " Scale was set to: " + scale + ".");
                }
                break;
            }
            case VARIANT: {
                ArrayList<org.apache.doris.catalog.VariantField> predefinedFields =
                        ((org.apache.doris.catalog.VariantType) scalarType).getPredefinedFields();
                Set<String> fieldPatterns = new HashSet<>();
                for (org.apache.doris.catalog.VariantField field : predefinedFields) {
                    Type fieldType = field.getType();
                    validateNestedType(scalarType, fieldType);
                    if (!fieldPatterns.add(field.getPattern())) {
                        throw new AnalysisException("Duplicate field name " + field.getPattern()
                                + " in variant " + scalarType.toSql());
                    }
                }
                break;
            }
            case INVALID_TYPE:
                throw new AnalysisException("Invalid type.");
            default:
                break;
        }
    }
}
