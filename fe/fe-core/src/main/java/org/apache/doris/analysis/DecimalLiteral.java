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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
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
        init(value);
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

    protected DecimalLiteral(DecimalLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new DecimalLiteral(this);
    }

    private void init(BigDecimal value) {
        this.value = value;
        type = Type.DECIMALV2;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void checkPrecisionAndScale(int precision, int scale) throws AnalysisException {
        Preconditions.checkNotNull(this.value);
        boolean valid = true;
        if (precision != -1 && scale != -1) {
            int realPrecision = this.value.precision();
            int realScale = this.value.scale();
            if (precision < realPrecision || scale < realScale) {
                valid = false;
            }
        } else {
            valid = false;
        }

        if (!valid) {
            throw new AnalysisException("Invalid precision and scale: " + precision + ", " + scale);
        }
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        ByteBuffer buffer;
        // no need to consider the overflow when cast decimal to other type, because this func only be used when querying, not storing.
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
        BigDecimal v = new BigDecimal(value.toBigInteger());
        msg.decimal_literal = new TDecimalLiteral(value.toPlainString());
    }

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = value.negate();
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

    public void roundCeiling() {
        value = value.setScale(0, RoundingMode.CEILING);
    }

    public void roundFloor() {
        value = value.setScale(0, RoundingMode.FLOOR);
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDecimalV2()) {
            return this;
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral(value.doubleValue(), targetType);
        } else if (targetType.isIntegerType()) {
            return new IntLiteral(value.longValue(), targetType);
        } else if (targetType.isStringType()) {
            return new StringLiteral(value.toString());
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
