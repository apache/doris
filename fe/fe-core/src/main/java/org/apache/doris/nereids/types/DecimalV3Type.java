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
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * Decimal type in Nereids.
 */
@Developing
public class DecimalV3Type extends FractionalType {
    public static final int MAX_DECIMALV2_SCALE = 9;
    public static final int MAX_DECIMAL32_PRECISION = 9;
    public static final int MAX_DECIMAL64_PRECISION = 18;
    public static final int MAX_DECIMAL128_PRECISION = 38;

    public static final DecimalV3Type DEFAULT_DECIMAL32 = new DecimalV3Type(DEFAULT_PRECISION, DEFAULT_SCALE);
    public static final DecimalV3Type DEFAULT_DECIMAL64 = new DecimalV3Type(DEFAULT_PRECISION, DEFAULT_SCALE);
    public static final DecimalV3Type DEFAULT_DECIMAL128 = new DecimalV3Type(MAX_DECIMAL128_PRECISION, DEFAULT_SCALE);
    public static final DecimalV3Type SYSTEM_DEFAULT = DEFAULT_DECIMAL32;

    private static final DecimalV3Type BOOLEAN_DECIMAL = DEFAULT_DECIMAL32;
    private static final DecimalV3Type TINYINT_DECIMAL = DEFAULT_DECIMAL32;
    private static final DecimalV3Type SMALLINT_DECIMAL = DEFAULT_DECIMAL32;
    private static final DecimalV3Type INTEGER_DECIMAL = DEFAULT_DECIMAL32;
    private static final DecimalV3Type BIGINT_DECIMAL = DEFAULT_DECIMAL64;
    private static final DecimalV3Type FLOAT_DECIMAL = DEFAULT_DECIMAL64;
    private static final DecimalV3Type DOUBLE_DECIMAL = DEFAULT_DECIMAL128;

    private static final Map<DataType, DecimalV3Type> FOR_TYPE_MAP = ImmutableMap.<DataType, DecimalV3Type>builder()
            .put(TinyIntType.INSTANCE, TINYINT_DECIMAL)
            .put(SmallIntType.INSTANCE, SMALLINT_DECIMAL)
            .put(IntegerType.INSTANCE, INTEGER_DECIMAL)
            .put(BigIntType.INSTANCE, BIGINT_DECIMAL)
            .put(LargeIntType.INSTANCE, DEFAULT_DECIMAL128)
            .put(FloatType.INSTANCE, FLOAT_DECIMAL)
            .put(DoubleType.INSTANCE, DOUBLE_DECIMAL)
            .build();

    private static final int WIDTH = 16;

    private final int precision;
    private final int scale;

    private DecimalV3Type(int precision, int scale) {
        Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL128_PRECISION);
        this.precision = precision;
        this.scale = scale;
    }

    public static DecimalV3Type forType(DataType dataType) {
        if (FOR_TYPE_MAP.containsKey(dataType)) {
            return FOR_TYPE_MAP.get(dataType);
        }
        throw new RuntimeException("Could not create decimal for type " + dataType);
    }

    /** createDecimalV3Type. */
    public static DecimalV3Type createDecimalV3Type(int precision) {
        if (precision <= MAX_DECIMAL32_PRECISION) {
            return DEFAULT_DECIMAL32;
        }
        if (precision <= MAX_DECIMAL64_PRECISION) {
            return DEFAULT_DECIMAL64;
        }
        if (precision <= MAX_DECIMAL128_PRECISION) {
            return DEFAULT_DECIMAL128;
        }

        return new DecimalV3Type(Math.min(precision, MAX_DECIMAL128_PRECISION), DEFAULT_SCALE);
    }

    public static DecimalV3Type createDecimalV3Type(BigDecimal bigDecimal) {
        int precision = org.apache.doris.analysis.DecimalLiteral.getBigDecimalPrecision(bigDecimal);
        return createDecimalV3Type(precision);
    }

    public static DecimalV3Type widerDecimalV3Type(DecimalV3Type left, DecimalV3Type right) {
        return widerDecimalV3Type(left.getPrecision(), right.getPrecision());
    }

    private static DecimalV3Type widerDecimalV3Type(int leftPrecision, int rightPrecision) {
        int maxPrecision = Math.max(leftPrecision, rightPrecision);
        return DecimalV3Type.createDecimalV3Type(maxPrecision);
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createDecimalV3Type(precision, scale);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isWiderThan(DataType other) {
        return isWiderThanInternal(other);
    }

    private boolean isWiderThanInternal(DataType other) {
        if (other instanceof DecimalV3Type) {
            DecimalV3Type dt = (DecimalV3Type) other;
            return this.precision - this.scale >= dt.precision - dt.scale && this.scale >= dt.scale;
        } else if (other instanceof IntegralType) {
            return isWiderThanInternal(forType(other));
        }
        return false;
    }

    @Override
    public DataType defaultConcreteType() {
        return SYSTEM_DEFAULT;
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof DecimalV3Type;
    }

    @Override
    public String simpleString() {
        return "decimal";
    }

    @Override
    public String toSql() {
        return "DECIMAL(" + precision + ", " + scale + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DecimalV3Type that = (DecimalV3Type) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }

    @Override
    public int width() {
        return WIDTH;
    }

}

