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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.MultiRowType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
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
        } else if (catalogType instanceof MapType) {
            throw new AnalysisException("Nereids do not support map type.");
        } else if (catalogType instanceof StructType) {
            throw new AnalysisException("Nereids do not support struct type.");
        } else if (catalogType instanceof ArrayType) {
            throw new AnalysisException("Nereids do not support array type.");
        } else if (catalogType instanceof MultiRowType) {
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
                return TinyIntType.INSTANCE;
            case "smallint":
                return SmallIntType.INSTANCE;
            case "int":
                return IntegerType.INSTANCE;
            case "bigint":
                return BigIntType.INSTANCE;
            case "largeint":
                return LargeIntType.INSTANCE;
            case "double":
                return DoubleType.INSTANCE;
            case "string":
                return StringType.INSTANCE;
            case "null":
                return NullType.INSTANCE;
            case "datetime":
                return DateTimeType.INSTANCE;
            default:
                Optional<VarcharType> varcharType = matchVarchar(type);
                if (varcharType.isPresent()) {
                    return varcharType.get();
                }
                throw new AnalysisException("Nereids do not support type: " + type);
        }
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

    public boolean isDate() {
        return this instanceof DateType;
    }

    public boolean isIntType() {
        return this instanceof IntegerType;
    }

    public boolean isDateTime() {
        return this instanceof DateTimeType;
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
}
