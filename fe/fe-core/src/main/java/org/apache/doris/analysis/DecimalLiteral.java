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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TDecimalLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class DecimalLiteral extends NumericLiteralExpr {
    @SerializedName("v")
    private BigDecimal value;

    private DecimalLiteral() {
    }

    public DecimalLiteral(BigDecimal value) {
        init(value, Config.enable_decimal_conversion);
        this.nullable = false;
    }

    public DecimalLiteral(BigDecimal value, Type type) {
        this.value = value;
        this.type = type;
        this.nullable = false;
    }

    public DecimalLiteral(String value) throws AnalysisException {
        BigDecimal v = null;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(v);
        this.nullable = false;
    }

    protected DecimalLiteral(DecimalLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new DecimalLiteral(this);
    }

    /**
     * Get precision and scale of java BigDecimal.
     * The precision is the number of digits in the unscaled value for BigDecimal.
     * The unscaled value of BigDecimal computes this * 10^this.scale().
     * If zero or positive, the scale is the number of digits to the right of the decimal point.
     * If negative, the unscaled value of the number is multiplied by ten to the power of the negation of the scale.
     * There are two scenarios that do not meet the limit: 0 < P and 0 <= S <= P
     * case1: S >= 0 and S > P. i.e. BigDecimal(0.01234), precision = 4, scale = 5
     * case2: S < 0. i.e. BigDecimal(2000), precision = 1, scale = -3
     */
    public static int getBigDecimalPrecision(BigDecimal decimal) {
        int scale = decimal.scale();
        int precision = decimal.precision();
        if (scale < 0) {
            return Math.abs(scale) + precision;
        } else {
            return Math.max(scale, precision);
        }
    }

    public static int getBigDecimalScale(BigDecimal decimal) {
        return Math.max(0, decimal.scale());
    }

    private void init(BigDecimal value, boolean enforceV3) {
        this.value = value;
        int precision = getBigDecimalPrecision(this.value);
        int scale = getBigDecimalScale(this.value);
        int maxPrecision =
                SessionVariable.getEnableDecimal256() ? ScalarType.MAX_DECIMAL256_PRECISION
                        : ScalarType.MAX_DECIMAL128_PRECISION;
        int integerPart = precision - scale;
        if (precision > maxPrecision) {
            BigDecimal stripedValue = value.stripTrailingZeros();
            int stripedPrecision = getBigDecimalPrecision(stripedValue);
            if (stripedPrecision <= maxPrecision) {
                this.value = stripedValue.setScale(maxPrecision - integerPart);
                precision = getBigDecimalPrecision(this.value);
                scale = getBigDecimalScale(this.value);
            }
        }
        if (enforceV3) {
            type = ScalarType.createDecimalV3Type(precision, scale);
        } else {
            type = ScalarType.createDecimalType(precision, scale);
        }
    }

    private void init(BigDecimal value) {
        init(value, false);
    }

    public BigDecimal getValue() {
        return value;
    }

    public void checkPrecisionAndScale(int precision, int scale) throws AnalysisException {
        Preconditions.checkNotNull(this.value);
        int realPrecision = this.value.precision();
        int realScale = this.value.scale();
        boolean valid = true;
        if (precision != -1 && scale != -1) {
            if (precision < realPrecision || scale < realScale) {
                valid = false;
            }
        } else {
            valid = false;
        }

        if (!valid) {
            throw new AnalysisException(
                    String.format("Invalid precision and scale - expect (%d, %d), but (%d, %d)",
                            precision, scale, realPrecision, realScale));
        }
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        ByteBuffer buffer;
        // no need to consider the overflow when cast decimal to other type,
        // because this func only be used when querying, not storing.
        // e.g. For column A with type INT, the data stored certainly no overflow.
        switch (type) {
            case TINYINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.put(value.byteValue());
                break;
            case SMALLINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putShort(value.shortValue());
                break;
            case INT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(value.intValue());
                break;
            case BIGINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putLong(value.longValue());
                break;
            case DECIMALV2:
                buffer = ByteBuffer.allocate(12);
                buffer.order(ByteOrder.LITTLE_ENDIAN);

                long integerValue = value.longValue();
                int fracValue = getFracValue();
                buffer.putLong(integerValue);
                buffer.putInt(fracValue);
                break;
            case DECIMAL32:
                buffer = ByteBuffer.allocate(4);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(value.unscaledValue().intValue());
                break;
            case DECIMAL64:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putLong(value.unscaledValue().longValue());
                break;
            case DECIMAL128:
            case DECIMAL256:
                LargeIntLiteral tmp = new LargeIntLiteral(value.unscaledValue());
                return tmp.getHashValue(type);
            default:
                return super.getHashValue(type);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public Object getRealValue() {
        return value;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return this.compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr instanceof DecimalLiteral) {
            return this.value.compareTo(((DecimalLiteral) expr).value);
        } else {
            try {
                DecimalLiteral decimalLiteral = new DecimalLiteral(expr.getStringValue());
                return this.compareLiteral(decimalLiteral);
            } catch (AnalysisException e) {
                throw new ClassCastException("Those two values cannot be compared: " + value
                        + " and " + expr.toSqlImpl());
            }
        }
    }

    @Override
    public String getStringValueForQuery(FormatOptions options) {
        return value.toPlainString();
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        return value.toPlainString();
    }

    @Override
    public long getLongValue() {
        return value.longValue();
    }

    @Override
    public double getDoubleValue() {
        return value.doubleValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO(hujie01) deal with loss information
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        msg.decimal_literal = new TDecimalLiteral(value.toPlainString());
    }

    // To be compatible with OLAP, only need 9 digits.
    // Note: the return value is negative if value is negative.
    public int getFracValue() {
        BigDecimal integerPart = new BigDecimal(value.toBigInteger());
        BigDecimal fracPart = value.subtract(integerPart);
        fracPart = fracPart.setScale(9, BigDecimal.ROUND_DOWN);
        fracPart = fracPart.movePointRight(9);

        return fracPart.intValue();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }

}
