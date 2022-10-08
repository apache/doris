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
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("varchar(\\(\\d+\\))?");

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
                case DATETIME:
                    return DateTimeType.INSTANCE;
                case DECIMALV2:
                    return DecimalType.createDecimalType(scalarType.decimalPrecision(), scalarType.decimalScale());
                case NULL_TYPE:
                    return NullType.INSTANCE;
                default:
                    throw new AnalysisException("Nereids do not support type: " + scalarType.getPrimitiveType());
            }
        } else if (catalogType.isArrayType()) {
            throw new AnalysisException("Nereids do not support array type.");
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
        type = type.toLowerCase();
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
                return DecimalType.SYSTEM_DEFAULT;
            case "char":
                return CharType.INSTANCE;
            case "varchar":
                return VarcharType.SYSTEM_DEFAULT;
            case "text":
            case "string":
                return StringType.INSTANCE;
            case "null":
            case "null_type": // ScalarType.NULL.toSql() return "null_type", so support it
                return NullType.INSTANCE;
            case "date":
                return DateType.INSTANCE;
            case "datetime":
            case "datetime(0)":
                return DateTimeType.INSTANCE;
            case "time":
                return TimeType.INSTANCE;
            case "hll":
                return HllType.INSTANCE;
            case "bitmap":
                return BitmapType.INSTANCE;
            case "quantile_state":
                return QuantileStateType.INSTANCE;
            default:
                Optional<VarcharType> varcharType = matchVarchar(type);
                if (varcharType.isPresent()) {
                    return varcharType.get();
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
    public static DataType fromLegacyType(Type type) {
        if (type == Type.BOOLEAN) {
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
        } else if (type == Type.NULL) {
            return NullType.INSTANCE;
        } else if (type == Type.DATE) {
            return DateType.INSTANCE;
        } else if (type == Type.DATEV2) {
            return DateV2Type.INSTANCE;
        } else if (type == Type.DATETIME) {
            return DateTimeType.INSTANCE;
        } else if (type == Type.DATETIMEV2) {
            return DateTimeV2Type.INSTANCE;
        } else if (type == Type.TIME) {
            return TimeType.INSTANCE;
        } else if (type == Type.TIMEV2) {
            return TimeV2Type.INSTANCE;
        } else if (type == Type.HLL) {
            return HllType.INSTANCE;
        } else if (type == Type.BITMAP) {
            return BitmapType.INSTANCE;
        } else if (type == Type.QUANTILE_STATE) {
            return QuantileStateType.INSTANCE;
        } else if (type.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.CHAR) {
            return CharType.createCharType(type.getLength());
        } else if (type.getPrimitiveType() == org.apache.doris.catalog.PrimitiveType.VARCHAR) {
            return VarcharType.createVarcharType(type.getLength());
        } else if (type == Type.DECIMALV2) {
            return DecimalType.SYSTEM_DEFAULT;
        } else if (type.isArrayType()) {
            return ArrayType.of(fromLegacyType(((org.apache.doris.catalog.ArrayType) type).getItemType()));
        }
        throw new AnalysisException("Nereids do not support type: " + type);
    }

    public abstract Type toCatalogDataType();

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
    public boolean acceptsType(DataType other) {
        return sameType(other);
    }

    /**
     * this and other is same type.
     */
    private boolean sameType(DataType other) {
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

    private static Optional<VarcharType> matchVarchar(String type) {
        Matcher matcher = VARCHAR_PATTERN.matcher(type);
        if (matcher.find()) {
            VarcharType varcharType = matcher.groupCount() > 1
                    ? VarcharType.createVarcharType(Integer.valueOf(matcher.group(1)))
                    : VarcharType.SYSTEM_DEFAULT;
            return Optional.of(varcharType);
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
            return new ArrayType(itemType);
        } else if (type.isEmpty()) {
            return ArrayType.SYSTEM_DEFAULT;
        } else {
            throw new AnalysisException("Illegal array type: " + type);
        }
    }
}
