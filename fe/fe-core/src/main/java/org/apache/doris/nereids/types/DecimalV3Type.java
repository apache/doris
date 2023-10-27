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
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.qe.ConnectContext;

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
    public static final int MAX_DECIMAL32_PRECISION = 9;
    public static final int MAX_DECIMAL64_PRECISION = 18;
    public static final int MAX_DECIMAL128_PRECISION = 38;
    public static final int MAX_DECIMAL256_PRECISION = 76;

    public static final DecimalV3Type WILDCARD = new DecimalV3Type(-1, -1);
    public static final DecimalV3Type SYSTEM_DEFAULT = new DecimalV3Type(MAX_DECIMAL128_PRECISION, DEFAULT_SCALE);
    public static final DecimalV3Type CATALOG_DEFAULT = new DecimalV3Type(MAX_DECIMAL32_PRECISION, DEFAULT_SCALE);

    private static final DecimalV3Type BOOLEAN_DECIMAL = new DecimalV3Type(1, 0);
    private static final DecimalV3Type TINYINT_DECIMAL = new DecimalV3Type(3, 0);
    private static final DecimalV3Type SMALLINT_DECIMAL = new DecimalV3Type(5, 0);
    private static final DecimalV3Type INTEGER_DECIMAL = new DecimalV3Type(10, 0);
    private static final DecimalV3Type BIGINT_DECIMAL = new DecimalV3Type(20, 0);
    private static final DecimalV3Type LARGEINT_DECIMAL = new DecimalV3Type(38, 0);
    private static final DecimalV3Type FLOAT_DECIMAL = new DecimalV3Type(14, 7);
    private static final DecimalV3Type DOUBLE_DECIMAL = new DecimalV3Type(30, 15);

    private static final Map<DataType, DecimalV3Type> FOR_TYPE_MAP = ImmutableMap.<DataType, DecimalV3Type>builder()
            .put(BooleanType.INSTANCE, BOOLEAN_DECIMAL)
            .put(TinyIntType.INSTANCE, TINYINT_DECIMAL)
            .put(SmallIntType.INSTANCE, SMALLINT_DECIMAL)
            .put(IntegerType.INSTANCE, INTEGER_DECIMAL)
            .put(BigIntType.INSTANCE, BIGINT_DECIMAL)
            .put(LargeIntType.INSTANCE, LARGEINT_DECIMAL)
            .put(FloatType.INSTANCE, FLOAT_DECIMAL)
            .put(DoubleType.INSTANCE, DOUBLE_DECIMAL)
            .put(NullType.INSTANCE, BOOLEAN_DECIMAL)
            .build();

    protected final int precision;
    protected final int scale;

    private DecimalV3Type(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * create DecimalV3Type with appropriate scale and precision.
     */
    public static DecimalV3Type forType(DataType dataType) {
        if (dataType instanceof DecimalV3Type) {
            return (DecimalV3Type) dataType;
        }
        if (dataType instanceof DecimalV2Type) {
            return createDecimalV3Type(
                    ((DecimalV2Type) dataType).getPrecision(), ((DecimalV2Type) dataType).getScale());
        }
        if (FOR_TYPE_MAP.containsKey(dataType)) {
            return FOR_TYPE_MAP.get(dataType);
        }
        if (dataType.isDateTimeV2Type()) {
            return createDecimalV3Type(14 + ((DateTimeV2Type) dataType).getScale(),
                    ((DateTimeV2Type) dataType).getScale());
        }
        return SYSTEM_DEFAULT;
    }

    /** createDecimalV3Type. */
    public static DecimalV3Type createDecimalV3Type(int precision) {
        return createDecimalV3Type(precision, DEFAULT_SCALE);
    }

    /** createDecimalV3Type. */
    public static DecimalV3Type createDecimalV3Type(int precision, int scale) {
        Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL256_PRECISION,
                "precision should in (0, " + MAX_DECIMAL256_PRECISION + "], but real precision is " + precision);
        Preconditions.checkArgument(scale >= 0, "scale should not smaller than 0, but real scale is " + scale);
        Preconditions.checkArgument(precision >= scale, "precision should not smaller than scale,"
                + " but precision is " + precision, ", scale is " + scale);
        boolean enableDecimal256 = false;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            enableDecimal256 = connectContext.getSessionVariable().enableDecimal256();
        }
        if (precision > MAX_DECIMAL128_PRECISION && !enableDecimal256) {
            throw new NotSupportedException("Datatype DecimalV3 with precision " + precision
                    + ", which is greater than 38 is disabled by default. set enable_decimal256 = true to enable it.");
        } else {
            return new DecimalV3Type(precision, scale);
        }
    }

    public static DecimalV3Type createDecimalV3Type(BigDecimal bigDecimal) {
        int precision = org.apache.doris.analysis.DecimalLiteral.getBigDecimalPrecision(bigDecimal);
        int scale = org.apache.doris.analysis.DecimalLiteral.getBigDecimalScale(bigDecimal);
        return createDecimalV3TypeLooseCheck(precision, scale);
    }

    /**
     * create DecimalV3Type, not throwing NotSupportedException.
     */
    public static DecimalV3Type createDecimalV3TypeLooseCheck(int precision, int scale) {
        boolean enableDecimal256 = false;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            enableDecimal256 = connectContext.getSessionVariable().enableDecimal256();
        }
        if (enableDecimal256) {
            Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL256_PRECISION,
                    "precision should in (0, " + MAX_DECIMAL256_PRECISION + "], but real precision is " + precision);
        } else {
            Preconditions.checkArgument(precision > 0 && precision <= MAX_DECIMAL128_PRECISION,
                    "precision should in (0, " + MAX_DECIMAL128_PRECISION + "], but real precision is " + precision);
        }
        Preconditions.checkArgument(scale >= 0, "scale should not smaller than 0, but real scale is " + scale);
        Preconditions.checkArgument(precision >= scale, "precision should not smaller than scale,"
                + " but precision is " + precision, ", scale is " + scale);
        return new DecimalV3Type(precision, scale);
    }

    /**
     * create DecimalV3Type, without checking precision and scale, e.g. for DataType.fromCatalogType
     */
    public static DecimalV3Type createDecimalV3TypeNoCheck(int precision, int scale) {
        return new DecimalV3Type(precision, scale);
    }

    public static DataType widerDecimalV3Type(DecimalV3Type left, DecimalV3Type right, boolean overflowToDouble) {
        return widerDecimalV3Type(left.getPrecision(), right.getPrecision(),
                left.getScale(), right.getScale(), overflowToDouble);
    }

    private static DataType widerDecimalV3Type(
            int leftPrecision, int rightPrecision,
            int leftScale, int rightScale,
            boolean overflowToDouble) {
        int scale = Math.max(leftScale, rightScale);
        int range = Math.max(leftPrecision - leftScale, rightPrecision - rightScale);
        boolean enableDecimal256 = false;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            enableDecimal256 = connectContext.getSessionVariable().enableDecimal256();
        }
        if (range + scale > (enableDecimal256 ? MAX_DECIMAL256_PRECISION : MAX_DECIMAL128_PRECISION)
                && overflowToDouble) {
            return DoubleType.INSTANCE;
        }
        return DecimalV3Type.createDecimalV3Type(range + scale, scale);
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

    public int getRange() {
        return precision - scale;
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other.equals(this);
    }

    @Override
    public String simpleString() {
        return "decimalv3";
    }

    @Override
    public String toSql() {
        return "DECIMALV3(" + precision + ", " + scale + ")";
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
        if (precision <= MAX_DECIMAL32_PRECISION) {
            return 4;
        } else if (precision <= MAX_DECIMAL64_PRECISION) {
            return 8;
        } else if (precision <= MAX_DECIMAL128_PRECISION) {
            return 16;
        } else {
            boolean enableDecimal256 = false;
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                enableDecimal256 = connectContext.getSessionVariable().enableDecimal256();
            }
            if (enableDecimal256) {
                return 32;
            } else {
                return 16;
            }
        }
    }
}
