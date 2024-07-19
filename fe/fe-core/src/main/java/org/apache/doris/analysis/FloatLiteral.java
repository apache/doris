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
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TFloatLiteral;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.NumberFormat;

public class FloatLiteral extends NumericLiteralExpr {
    @SerializedName("v")
    private double value;

    public FloatLiteral() {
    }

    public FloatLiteral(Double value) {
        init(value);
    }

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    public FloatLiteral(Double value, Type type) {
        this.value = value.doubleValue();
        this.type = type;
        analysisDone();
    }

    public FloatLiteral(String value) throws AnalysisException {
        Double floatValue = null;
        try {
            floatValue = new Double(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(floatValue);
    }

    protected FloatLiteral(FloatLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new FloatLiteral(this);
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
                buffer.putLong((long) value);
                break;
            default:
                return super.getHashValue(type);
        }
        buffer.flip();
        return buffer;
    }

    private void init(Double value) {
        this.value = value.doubleValue();
        // Figure out if this will fit in a FLOAT without loosing precision.
        float fvalue;
        fvalue = value.floatValue();
        if (fvalue == this.value) {
            type = Type.FLOAT;
        } else {
            type = Type.DOUBLE;
        }
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return this.compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }
        return Double.compare(value, expr.getDoubleValue());
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        // TODO: Here is weird use float to represent TIME type
        // rethink whether it is reasonable to use this way
        if (type.equals(Type.TIME) || type.equals(Type.TIMEV2)) {
            return timeStrFromFloat(value);
        }
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        if (type == Type.FLOAT) {
            nf.setMaximumFractionDigits(7);
        } else {
            nf.setMaximumFractionDigits(16);
        }
        return nf.format(value);
    }

    @Override
    public String getStringValueInFe(FormatOptions options) {
        if (type == Type.TIME || type == Type.TIMEV2) {
            // FloatLiteral used to represent TIME type, here we need to remove apostrophe from timeStr
            // for example '11:22:33' -> 11:22:33
            String timeStr = getStringValue();
            return timeStr.substring(1, timeStr.length() - 1);
        } else {
            return BigDecimal.valueOf(getValue()).toPlainString();
        }
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        String ret = getStringValue();
        if (type == Type.TIME || type == Type.TIMEV2) {
            // here already wrapped in ''
            ret = ret.substring(1, ret.length() - 1);
        }
        return options.getNestedStringWrapper() + ret + options.getNestedStringWrapper();
    }

    public static Type getDefaultTimeType(Type type) throws AnalysisException {
        switch (type.getPrimitiveType()) {
            case TIME:
                if (Config.enable_date_conversion) {
                    return Type.TIMEV2;
                } else {
                    return Type.TIME;
                }
            case TIMEV2:
                return type;
            default:
                throw new AnalysisException("Invalid time type: " + type);
        }
    }

    @Override
    public long getLongValue() {
        return (long) value;
    }

    @Override
    public double getDoubleValue() {
        return value;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(value);
    }

    public double getValue() {
        return value;
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!(targetType.isFloatingPointType() || targetType.isDecimalV2() || targetType.isDecimalV3())) {
            return super.uncheckedCastTo(targetType);
        }
        if (targetType.isFloatingPointType()) {
            if (!type.equals(targetType)) {
                FloatLiteral floatLiteral = new FloatLiteral(this);
                floatLiteral.setType(targetType);
                return floatLiteral;
            }
            return this;
        } else if (targetType.isDecimalV2()) {
            // the double constructor does an exact translation, use valueOf() instead.
            DecimalLiteral res = new DecimalLiteral(BigDecimal.valueOf(value));
            res.setType(targetType);
            return res;
        } else if (targetType.isDecimalV3()) {
            DecimalLiteral res = new DecimalLiteral(new BigDecimal(value));
            res.setType(ScalarType.createDecimalV3Type(targetType.getPrecision(),
                    ((ScalarType) targetType).decimalScale()));
            return res;
        }
        return this;
    }

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = -value;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = in.readDouble();
    }

    public static FloatLiteral read(DataInput in) throws IOException {
        FloatLiteral literal = new FloatLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Double.hashCode(value);
    }

    private String timeStrFromFloat(double time) {
        String timeStr = "";

        if (time < 0) {
            timeStr += "-";
            time = -time;
        }
        int hour = (int) (time / 60 / 60);
        int minute = (int) ((time / 60)) % 60;
        int second = (int) (time) % 60;

        return "'" + timeStr + String.format("%02d:%02d:%02d", hour, minute, second) + "'";
    }

    @Override
    public void setupParamFromBinary(ByteBuffer data, boolean isUnsigned) {
        if (type.getPrimitiveType() == PrimitiveType.FLOAT) {
            value = data.getFloat();
            return;
        }
        if (type.getPrimitiveType() == PrimitiveType.DOUBLE) {
            value = data.getDouble();
            return;
        }
    }
}
