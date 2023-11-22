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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TDecimalLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class DecimalLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DecimalLiteral.class);
    private BigDecimal value;

    public DecimalLiteral() {
    }

    public DecimalLiteral(BigDecimal value) {
        this(value, Config.enable_decimal_conversion);
    }

    public DecimalLiteral(BigDecimal value, boolean isDecimalV3) {
        init(value, isDecimalV3);
        analysisDone();
    }

    public DecimalLiteral(BigDecimal value, Type type) {
        this.value = value;
        this.type = type;
        analysisDone();
    }

    public DecimalLiteral(String value) throws AnalysisException {
        BigDecimal v = null;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(v);
        analysisDone();
    }

    public DecimalLiteral(String value, int scale) throws AnalysisException {
        BigDecimal v = null;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        if (scale >= 0) {
            v = v.setScale(scale, RoundingMode.HALF_UP);
        }
        init(v);
        analysisDone();
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
        if (expr instanceof NullLiteral) {
            return 1;
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
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        return value.toString();
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
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

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = value.negate();
    }

    @Override
    protected void compactForLiteral(Type type) throws AnalysisException {
        if (type.isDecimalV3()) {
            int scale = Math.max(this.value.scale(), ((ScalarType) type).decimalScale());
            int integerPart = Math.max(this.value.precision() - this.value.scale(),
                    type.getPrecision() - ((ScalarType) type).decimalScale());
            this.type = ScalarType.createDecimalV3Type(integerPart + scale, scale);
        }
    }

    public void tryToReduceType() {
        if (this.type.isDecimalV3()) {
            try {
                value = new BigDecimal(value.longValueExact());
            } catch (ArithmeticException e) {
                // ignore
            }
            this.type = ScalarType.createDecimalV3Type(
                    Math.max(this.value.scale(), this.value.precision()), this.value.scale());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, value.toString());
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = new BigDecimal(Text.readString(in));
    }

    public static DecimalLiteral read(DataInput in) throws IOException {
        DecimalLiteral dec = new DecimalLiteral();
        dec.readFields(in);
        return dec;
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

    public void roundCeiling(int newScale) {
        value = value.setScale(newScale, RoundingMode.CEILING);
        type = ScalarType.createDecimalType(((ScalarType) type)
                .getPrimitiveType(), ((ScalarType) type).getScalarPrecision(), newScale);
    }

    public void roundFloor(int newScale) {
        value = value.setScale(newScale, RoundingMode.FLOOR);
        type = ScalarType.createDecimalType(((ScalarType) type)
                .getPrimitiveType(), ((ScalarType) type).getScalarPrecision(), newScale);
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDecimalV2() && type.isDecimalV2()) {
            setType(targetType);
            return this;
        } else if ((targetType.isDecimalV3() && type.isDecimalV3()
                && (((ScalarType) targetType).decimalPrecision() >= value.precision())
                && (((ScalarType) targetType).decimalScale() >= value.scale()))
                || (targetType.isDecimalV3() && type.isDecimalV2()
                && (((ScalarType) targetType).decimalScale() >= value.scale()))) {
            // If target type is DECIMALV3, we should set type for literal
            setType(targetType);
            return this;
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral(value.doubleValue(), targetType);
        } else if (targetType.isIntegerType()) {
            // If the integer part of BigDecimal is too big to fit into long,
            // longValue() will only return the low-order 64-bit value.
            if (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0
                    || value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) {
                throw new AnalysisException("Integer part of " + value + " exceeds storage range of Long Type.");
            }
            return new IntLiteral(value.longValue(), targetType);
        } else if (targetType.isStringType()) {
            return new StringLiteral(value.toString());
        } else if (targetType.isLargeIntType()) {
            return new LargeIntLiteral(value.toBigInteger().toString());
        }
        return super.uncheckedCastTo(targetType);
    }

    public Expr castToDecimalV3ByDivde(Type targetType) {
        // onlye use in DecimalLiteral divide DecimalV3
        CastExpr expr = new CastExpr(targetType, this);
        expr.setNotFold(true);
        return expr;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }

    @Override
    public void setupParamFromBinary(ByteBuffer data) {
        int len = getParmLen(data);
        BigDecimal v = null;
        try {
            byte[] bytes = new byte[len];
            data.get(bytes);
            String value = new String(bytes);
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            // throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(v);
    }
}
