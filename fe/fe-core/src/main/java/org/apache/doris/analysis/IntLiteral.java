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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TIntLiteral;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(IntLiteral.class);

    public static final long TINY_INT_MIN = Byte.MIN_VALUE; // -2^7 ~ 2^7 - 1
    public static final long TINY_INT_MAX = Byte.MAX_VALUE;
    public static final long SMALL_INT_MIN = Short.MIN_VALUE; // -2^15 ~ 2^15 - 1
    public static final long SMALL_INT_MAX = Short.MAX_VALUE;
    public static final long INT_MIN = Integer.MIN_VALUE; // -2^31 ~ 2^31 - 1
    public static final long INT_MAX = Integer.MAX_VALUE;
    public static final long BIG_INT_MIN = Long.MIN_VALUE; // -2^63 ~ 2^63 - 1
    public static final long BIG_INT_MAX = Long.MAX_VALUE;

    private long value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IntLiteral() {
    }

    public IntLiteral(long value) {
        super();
        init(value);
        analysisDone();
    }

    public IntLiteral(long longValue, Type type) throws AnalysisException {
        super();
        checkValueValid(longValue, type);

        this.value = longValue;
        this.type = type;
        analysisDone();
    }

    public IntLiteral(String value, Type type) throws AnalysisException {
        super();
        long longValue = -1L;
        try {
            longValue = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid number format: " + value);
        }
        checkValueValid(longValue, type);

        this.value = longValue;
        this.type = type;
        analysisDone();
    }

    protected IntLiteral(IntLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        checkValueValid(value, type);
    }

    private void checkValueValid(long longValue, Type type) throws AnalysisException {
        boolean valid = true;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                if (longValue < TINY_INT_MIN || longValue > TINY_INT_MAX) {
                    valid = false;
                }
                break;
            case SMALLINT:
                if (longValue < SMALL_INT_MIN || longValue > SMALL_INT_MAX) {
                    valid = false;
                }
                break;
            case INT:
                if (longValue < INT_MIN || longValue > INT_MAX) {
                    valid = false;
                }
                break;
            case BIGINT:
                if (longValue < BIG_INT_MIN) {
                    valid = false;
                }
                // no need to check upper bound
                break;
            default:
                valid = false;
                break;
        }
        if (!valid) {
            throw new AnalysisException("Number out of range[" + longValue + "]. type: " + type);
        }
    }

    @Override
    public Expr clone() {
        return new IntLiteral(this);
    }

    private void init(long value) {
        this.value = value;
        if (this.value <= TINY_INT_MAX && this.value >= TINY_INT_MIN) {
            type = Type.TINYINT;
        } else if (this.value <= SMALL_INT_MAX && this.value >= SMALL_INT_MIN) {
            type = Type.SMALLINT;
        } else if (this.value <= INT_MAX && this.value >= INT_MIN) {
            type = Type.INT;
        } else if (this.value <= BIG_INT_MAX && this.value >= BIG_INT_MIN) {
            type = Type.BIGINT;
        } else {
            Preconditions.checkState(false, value);
        }
    }

    public static IntLiteral createMinValue(Type type) {
        long value = 0L;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                value = TINY_INT_MIN;
                break;
            case SMALLINT:
                value = SMALL_INT_MIN;
                break;
            case INT:
                value = INT_MIN;
                break;
            case BIGINT:
                value = BIG_INT_MIN;
                break;
            default:
                Preconditions.checkState(false);
        }

        return new IntLiteral(value);
    }

    public static IntLiteral createMaxValue(Type type) {
        long value = 0L;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                value = TINY_INT_MAX;
                break;
            case SMALLINT:
                value = SMALL_INT_MAX;
                break;
            case INT:
                value = INT_MAX;
                break;
            case BIGINT:
                value = BIG_INT_MAX;
                break;
            default:
                Preconditions.checkState(false);
        }

        return new IntLiteral(value);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        //it's so strange, now in write/read function, not write type info
        if (this.type.getPrimitiveType() == Type.INVALID.getPrimitiveType()) {
            if (this.value <= TINY_INT_MAX && this.value >= TINY_INT_MIN) {
                type = Type.TINYINT;
            } else if (this.value <= SMALL_INT_MAX && this.value >= SMALL_INT_MIN) {
                type = Type.SMALLINT;
            } else if (this.value <= INT_MAX && this.value >= INT_MIN) {
                type = Type.INT;
            } else if (this.value <= BIG_INT_MAX && this.value >= BIG_INT_MIN) {
                type = Type.BIGINT;
            } else {
                Preconditions.checkState(false, value);
            }
        }
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return this.value == TINY_INT_MIN;
            case SMALLINT:
                return this.value == SMALL_INT_MIN;
            case INT:
                return this.value == INT_MIN;
            case BIGINT:
                return this.value == BIG_INT_MIN;
            default:
                return false;
        }
    }

    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        switch (type) {
            case TINYINT:
                buffer.put((byte) value);
                break;
            case SMALLINT:
                buffer.putShort((short) value);
                break;
            case INT:
                buffer.putInt((int) value);
                break;
            case BIGINT:
                buffer.putLong(value);
                break;
            default:
                break;
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr instanceof StringLiteral) {
            return ((StringLiteral) expr).compareLiteral(this);
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (value == expr.getLongValue()) {
            return 0;
        } else {
            return value > expr.getLongValue() ? 1 : -1;
        }
    }

    @Override
    public Object getRealValue() {
        return getLongValue();
    }

    public long getValue() {
        return value;
    }

    @Override
    public String getStringValue() {
        return Long.toString(value);
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    public long getLongValue() {
        return value;
    }

    @Override
    public double getDoubleValue() {
        return (double) value;
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(value);
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isNumericType()) {
            if (targetType.isFixedPointType()) {
                if (!targetType.isScalarType(PrimitiveType.LARGEINT)) {
                    if (!type.equals(targetType)) {
                        IntLiteral intLiteral = new IntLiteral(this);
                        intLiteral.setType(targetType);
                        return intLiteral;
                    }
                    return this;
                } else {
                    return new LargeIntLiteral(Long.toString(value));
                }
            } else if (targetType.isFloatingPointType()) {
                return new FloatLiteral(new Double(value), targetType);
            } else if (targetType.isDecimalV2() || targetType.isDecimalV3()) {
                DecimalLiteral res = new DecimalLiteral(new BigDecimal(value));
                res.setType(targetType);
                return res;
            }
            return this;
        } else if (targetType.isDateType()) {
            try {
                //int like 20200101 can be cast to date(2020,01,01)
                DateLiteral res = new DateLiteral("" + value, targetType);
                res.setType(targetType);
                return res;
            } catch (AnalysisException e) {
                if (ConnectContext.get() != null) {
                    ConnectContext.get().getState().reset();
                }
                //invalid date format. leave it to BE to cast it as NULL
            }
        } else if (targetType.isStringType()) {
            StringLiteral res = new StringLiteral("" + value);
            res.setType(targetType);
            return res;
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = -value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = in.readLong();
    }

    public static IntLiteral read(DataInput in) throws IOException {
        IntLiteral literal = new IntLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Long.hashCode(value);
    }

    @Override
    public void setupParamFromBinary(ByteBuffer data) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                value = data.get();
                break;
            case SMALLINT:
                value = data.getChar();
                break;
            case INT:
                value = data.getInt();
                break;
            case BIGINT:
                value = data.getLong();
                break;
            default:
                Preconditions.checkState(false);
        }
    }
}
