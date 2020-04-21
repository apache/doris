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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TStringLiteral;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Objects;

public class StringLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(StringLiteral.class);
    private String value;
    
    public StringLiteral() {
        super();
        type = Type.VARCHAR;
    }

    public StringLiteral(String value) {
        super();
        this.value = value;
        type = Type.VARCHAR;
        analysisDone();
    }

    protected StringLiteral(StringLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new StringLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }

        // compare string with utf-8 byte array, same with DM,BE,StorageEngine
        byte[] thisBytes = null;
        byte[] otherBytes = null;
        try {
            thisBytes = value.getBytes("UTF-8");
            otherBytes = expr.getStringValue().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            Preconditions.checkState(false);
        }

        int minLength = Math.min(thisBytes.length, otherBytes.length);
        int i = 0;
        for (i = 0; i < minLength; i++) {
            if (thisBytes[i] < otherBytes[i]) {
                return -1;
            } else if (thisBytes[i] > otherBytes[i]) {
                return 1;
            }
        }
        if (thisBytes.length > otherBytes.length) {
            if (thisBytes[i] == 0x00) {
                return 0;
            } else {
                return 1;
            }
        } else if (thisBytes.length < otherBytes.length) {
            if (otherBytes[i] == 0x00) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public String toSqlImpl() {
        return "'" + value + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.STRING_LITERAL;
        msg.string_literal = new TStringLiteral(getUnescapedValue());
    }

    // FIXME: modify by zhaochun
    public String getUnescapedValue() {
        // Unescape string exactly like Hive does. Hive's method assumes
        // quotes so we add them here to reuse Hive's code.
        return value;
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        return Long.valueOf(value);
    }

    @Override
    public double getDoubleValue() {
        return Double.valueOf(value);
    }

    /**
     * Convert a string literal to a date literal
     *
     * @param targetType is the desired type
     * @return new converted literal (not null)
     * @throws AnalysisException when entire given string cannot be transformed into a date
     */
    private LiteralExpr convertToDate(Type targetType) throws AnalysisException {
        LiteralExpr newLiteral = null;
        try {
            newLiteral = new DateLiteral(value, targetType);
        } catch (AnalysisException e) {
            if (targetType.isScalarType(PrimitiveType.DATETIME)) {
                newLiteral = new DateLiteral(value, Type.DATE);
                newLiteral.setType(Type.DATETIME);
            } else {
                throw e;
            }
        }
        return newLiteral;
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isNumericType()) {
            switch (targetType.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return new IntLiteral(value, targetType);
                case LARGEINT:
                    return new LargeIntLiteral(value);
                case FLOAT:
                case DOUBLE:
                    try {
                        return new FloatLiteral(Double.valueOf(value), targetType);
                    } catch (NumberFormatException e) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_NUMBER, value);
                    }
                    break;
                case DECIMAL:
                case DECIMALV2:
                    return new DecimalLiteral(value);
                default:
                    break;
            }
        } else if (targetType.isDateType()) {
            // FE only support 'yyyy-MM-dd hh:mm:ss' && 'yyyy-MM-dd' format
            // so if FE unchecked cast fail, we also build CastExpr for BE
            // BE support other format suck as 'yyyyMMdd'...
            try {
                return convertToDate(targetType);
            } catch (AnalysisException e) {
                // pass;
            }
        } else if (targetType.equals(type)) {
            return this;
        } else if (targetType.isStringType()) {
            StringLiteral stringLiteral = new StringLiteral(this);
            stringLiteral.setType(targetType);
            return stringLiteral;
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = Text.readString(in);
    }
    
    public static StringLiteral read(DataInput in) throws IOException {
        StringLiteral literal = new StringLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
