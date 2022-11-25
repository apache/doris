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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * Decimal v2 type in Nereids.
 */
public class DecimalV2Type extends FractionalType {

    public static int MAX_PRECISION = 27;
    public static int MAX_SCALE = 9;
    public static final DecimalV2Type SYSTEM_DEFAULT = new DecimalV2Type(MAX_PRECISION, MAX_SCALE);

    private static final DecimalV2Type BOOLEAN_DECIMAL = new DecimalV2Type(1, 0);
    private static final DecimalV2Type TINYINT_DECIMAL = new DecimalV2Type(3, 0);
    private static final DecimalV2Type SMALLINT_DECIMAL = new DecimalV2Type(5, 0);
    private static final DecimalV2Type INTEGER_DECIMAL = new DecimalV2Type(10, 0);
    private static final DecimalV2Type BIGINT_DECIMAL = new DecimalV2Type(20, 0);
    private static final DecimalV2Type LARGEINT_DECIMAL = new DecimalV2Type(MAX_PRECISION, 0);
    private static final DecimalV2Type FLOAT_DECIMAL = new DecimalV2Type(14, 7);
    private static final DecimalV2Type DOUBLE_DECIMAL = DecimalV2Type.SYSTEM_DEFAULT;

    private static final int WIDTH = 16;

    private static final Map<DataType, DecimalV2Type> FOR_TYPE_MAP = ImmutableMap.<DataType, DecimalV2Type>builder()
            .put(TinyIntType.INSTANCE, TINYINT_DECIMAL)
            .put(SmallIntType.INSTANCE, SMALLINT_DECIMAL)
            .put(IntegerType.INSTANCE, INTEGER_DECIMAL)
            .put(BigIntType.INSTANCE, BIGINT_DECIMAL)
            .put(LargeIntType.INSTANCE, LARGEINT_DECIMAL)
            .put(FloatType.INSTANCE, FLOAT_DECIMAL)
            .put(DoubleType.INSTANCE, DOUBLE_DECIMAL)
            .build();

    private final int precision;
    private final int scale;

    public DecimalV2Type(int precision, int scale) {
        Preconditions.checkArgument(precision >= scale);
        Preconditions.checkArgument(precision > 0 && precision <= MAX_PRECISION);
        Preconditions.checkArgument(scale >= 0 && scale <= MAX_SCALE);
        this.precision = precision;
        this.scale = scale;
    }

    /** createDecimalV2Type. */
    public static DecimalV2Type createDecimalV2Type(int precision, int scale) {
        if (precision == SYSTEM_DEFAULT.precision && scale == SYSTEM_DEFAULT.scale) {
            return SYSTEM_DEFAULT;
        }
        return new DecimalV2Type(Math.min(precision, MAX_PRECISION), Math.min(scale, MAX_SCALE));
    }

    public static DecimalV2Type createDecimalV2Type(BigDecimal bigDecimal) {
        int precision = org.apache.doris.analysis.DecimalLiteral.getBigDecimalPrecision(bigDecimal);
        int scale = org.apache.doris.analysis.DecimalLiteral.getBigDecimalScale(bigDecimal);
        return createDecimalV2Type(precision, scale);
    }

    public static DecimalV2Type forType(DataType dataType) {
        if (FOR_TYPE_MAP.containsKey(dataType)) {
            return FOR_TYPE_MAP.get(dataType);
        }
        throw new RuntimeException("Could not create decimal for type " + dataType);
    }

    public static DecimalV2Type widerDecimalV2Type(DecimalV2Type left, DecimalV2Type right) {
        return widerDecimalV2Type(left.getPrecision(), right.getPrecision(), left.getScale(), right.getScale());
    }

    private static DecimalV2Type widerDecimalV2Type(
            int leftPrecision, int rightPrecision,
            int leftScale, int rightScale) {
        int scale = Math.max(leftScale, rightScale);
        int range = Math.max(leftPrecision - leftScale, rightPrecision - rightScale);
        return DecimalV2Type.createDecimalV2Type(range + scale, scale);
    }

    @Override
    public Type toCatalogDataType() {
        return Type.MAX_DECIMALV2_TYPE;
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
        if (other instanceof DecimalV2Type) {
            DecimalV2Type dt = (DecimalV2Type) other;
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
        return other instanceof DecimalV2Type;
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
        DecimalV2Type that = (DecimalV2Type) o;
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

