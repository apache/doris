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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TLargeIntLiteral;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// large int for the num that native types can not
public class LargeIntLiteral extends LiteralExpr {
    private final static Logger LOG = LogManager.getLogger(LargeIntLiteral.class);

    // -2^127
    public static final BigInteger LARGE_INT_MIN = new BigInteger("-170141183460469231731687303715884105728");
    // 2^127 - 1
    public static final BigInteger LARGE_INT_MAX = new BigInteger("170141183460469231731687303715884105727");
    // 2^127
    public static final BigInteger LARGE_INT_MAX_ABS = new BigInteger("170141183460469231731687303715884105728");

    private BigInteger value;

    public LargeIntLiteral() {
        super();
    }

    public LargeIntLiteral(boolean isMax) throws AnalysisException {
        super();
        type = Type.LARGEINT;
        value = isMax ? LARGE_INT_MAX : LARGE_INT_MIN;
    }

    public LargeIntLiteral(String value) throws AnalysisException {
        super();
        BigInteger bigInt;
        try {
            bigInt = new BigInteger(value);
            // ATTN: value from 'sql_parser.y' is always be positive. for example: '-256' will to be
            // 256, and for int8_t, 256 is invalid, while -256 is valid. So we check the right border
            // is LARGE_INT_MAX_ABS
            if (bigInt.compareTo(LARGE_INT_MIN) < 0 || bigInt.compareTo(LARGE_INT_MAX_ABS) > 0) {
                throw new AnalysisException("Large int literal is out of range: " + value);
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid integer literal: " + value, e);
        }
        this.value = bigInt;
        type = Type.LARGEINT;
    }

    protected LargeIntLiteral(LargeIntLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new LargeIntLiteral(this);
    }

    public static LargeIntLiteral createMinValue() {
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral();
        largeIntLiteral.type = Type.LARGEINT;
        largeIntLiteral.value = LARGE_INT_MIN;
        return largeIntLiteral;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (value.compareTo(LARGE_INT_MIN) < 0 || value.compareTo(LARGE_INT_MAX) > 0) {
            throw new AnalysisException("Number Overflow. literal: " + value);
        }
    }

    @Override
    public boolean isMinValue() {
        return this.value.compareTo(LARGE_INT_MIN) == 0;
    }

    @Override
    public Object getRealValue() {
        return this.value;
    }

    // little endian for hash code
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte[] byteArray = value.toByteArray();
        int len = byteArray.length;
        int end = 0;
        if (len > 16) {
            end = len - 16;
        }

        for (int i = len - 1; i >= end; --i) {
            buffer.put(byteArray[i]);
        }
        if (value.signum() >= 0) {
            while (len++ < 16) {
                buffer.put((byte) 0);
            }
        } else {
            while (len++ < 16) {
                buffer.put((byte) 0xFF);
            }
        }

        buffer.flip();
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr.type == Type.LARGEINT) {
            return value.compareTo(((LargeIntLiteral) expr).value);
        } else {
            BigInteger intValue = new BigInteger(((IntLiteral) expr).getStringValue());
            return value.compareTo(intValue);
        }
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
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.LARGE_INT_LITERAL;
        msg.large_int_literal = new TLargeIntLiteral(value.toString());
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isFloatingPointType()) {
            return new FloatLiteral(new Double(value.doubleValue()), targetType);
        } else if (targetType.isDecimal() || targetType.isDecimalV2()) {
            return new DecimalLiteral(new BigDecimal(value));
        } else if (targetType.isNumericType()) {
            try {
                return new IntLiteral(value.longValueExact(), targetType);
            } catch (ArithmeticException e) {
                throw new AnalysisException("Number out of range[" + value + "]. type: " + targetType);
            }
        }
        return super.uncheckedCastTo(targetType);
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
        value = new BigInteger(Text.readString(in));
    }

    public static LargeIntLiteral read(DataInput in) throws IOException {
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral();
        largeIntLiteral.readFields(in);
        return largeIntLiteral;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
