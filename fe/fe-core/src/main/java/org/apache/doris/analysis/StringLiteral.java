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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/StringLiteral.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.io.Text;
import org.apache.doris.qe.VariableVarConverters;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TStringLiteral;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class StringLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(StringLiteral.class);
    @SerializedName("v")
    private String value;
    // Means the converted session variable need to be cast to int, such as "cast 'STRICT_TRANS_TABLES' to Integer".
    private String beConverted = "";

    private StringLiteral() {
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

    public void setBeConverted(String val) {
        this.beConverted = val;
    }

    @Override
    public Expr clone() {
        return new StringLiteral(this);
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
            if (Byte.toUnsignedInt(thisBytes[i]) < Byte.toUnsignedInt(otherBytes[i])) {
                return -1;
            } else if (Byte.toUnsignedInt(thisBytes[i]) > Byte.toUnsignedInt(otherBytes[i])) {
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
        return "'" + value.replaceAll("'", "''") + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        if (value == null) {
            msg.node_type = TExprNodeType.NULL_LITERAL;
        } else {
            msg.string_literal = new TStringLiteral(value);
            msg.node_type = TExprNodeType.STRING_LITERAL;
        }
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValue() + options.getNestedStringWrapper();
    }

    @Override
    public long getLongValue() {
        return Long.valueOf(value);
    }

    @Override
    public double getDoubleValue() {
        return Double.parseDouble(value);
    }

    @Override
    public String getRealValue() {
        return getStringValue();
    }

    /**
     * Convert a string literal to a IPv4 literal
     *
     * @return new converted literal (not null)
     * @throws AnalysisException when entire given string cannot be transformed into a date
     */
    public LiteralExpr convertToIPv4() throws AnalysisException {
        LiteralExpr newLiteral;
        newLiteral = new IPv4Literal(value);
        try {
            newLiteral.checkValueValid();
        } catch (AnalysisException e) {
            return NullLiteral.create(newLiteral.getType());
        }
        return newLiteral;
    }

    /**
     * Convert a string literal to a IPv6 literal
     *
     * @return new converted literal (not null)
     * @throws AnalysisException when entire given string cannot be transformed into a date
     */
    public LiteralExpr convertToIPv6() throws AnalysisException {
        LiteralExpr newLiteral;
        newLiteral = new IPv6Literal(value);
        try {
            newLiteral.checkValueValid();
        } catch (AnalysisException e) {
            return NullLiteral.create(newLiteral.getType());
        }
        return newLiteral;
    }

    /**
     * Convert a string literal to a date literal
     *
     * @param targetType is the desired type
     * @return new converted literal (not null)
     * @throws AnalysisException when entire given string cannot be transformed into a date
     */
    public LiteralExpr convertToDate(Type targetType) throws AnalysisException {
        LiteralExpr newLiteral = null;
        try {
            newLiteral = new DateLiteral(value, targetType);
        } catch (AnalysisException e) {
            if (targetType.isScalarType(PrimitiveType.DATETIME)) {
                newLiteral = new DateLiteral(value, Type.DATE);
                newLiteral.setType(Type.DATETIME);
            } else if (targetType.isScalarType(PrimitiveType.DATETIMEV2)) {
                newLiteral = new DateLiteral(value, Type.DATEV2);
                newLiteral.setType(targetType);
            } else {
                throw e;
            }
        }
        try {
            newLiteral.checkValueValid();
        } catch (AnalysisException e) {
            return NullLiteral.create(newLiteral.getType());
        }
        return newLiteral;
    }

    public boolean canConvertToDateType(Type targetType) {
        try {
            Preconditions.checkArgument(targetType.isDateType());
            new DateLiteral(value, targetType);
            return true;
        } catch (AnalysisException e) {
            return false;
        }
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isNumericType()) {
            switch (targetType.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    if (VariableVarConverters.hasConverter(beConverted)) {
                        try {
                            return new IntLiteral(VariableVarConverters.encode(beConverted, value), targetType);
                        } catch (DdlException e) {
                            throw new AnalysisException(e.getMessage());
                        }
                    }
                    return new IntLiteral(value, targetType);
                case LARGEINT:
                    if (VariableVarConverters.hasConverter(beConverted)) {
                        try {
                            return new LargeIntLiteral(String.valueOf(
                                    VariableVarConverters.encode(beConverted, value)));
                        } catch (DdlException e) {
                            throw new AnalysisException(e.getMessage());
                        }
                    }
                    return new LargeIntLiteral(value);
                case FLOAT:
                case DOUBLE:
                    try {
                        return new FloatLiteral(Double.valueOf(value), targetType);
                    } catch (NumberFormatException e) {
                        // consistent with CastExpr's getResultValue() method
                        return new NullLiteral();
                    }
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    try {
                        DecimalLiteral res = new DecimalLiteral(new BigDecimal(value).stripTrailingZeros());
                        res.setType(targetType);
                        return res;
                    } catch (Exception e) {
                        throw new AnalysisException(
                                String.format("input value can't parse to decimal, value=%s", value));
                    }
                default:
                    break;
            }
        } else if (targetType.isDateType()) {
            // FE only support 'yyyy-MM-dd hh:mm:ss' && 'yyyy-MM-dd' format
            // so if FE unchecked cast fail, we also build CastExpr for BE
            // BE support other format such as 'yyyyMMdd'...
            try {
                return convertToDate(targetType);
            } catch (AnalysisException e) {
                // pass;
            }
        } else if (targetType.isIPv4()) {
            return convertToIPv4();
        } else if (targetType.isIPv6()) {
            return convertToIPv6();
        } else if (targetType.equals(type)) {
            return this;
        } else if (targetType.isStringType()) {
            StringLiteral stringLiteral = new StringLiteral(this);
            stringLiteral.setType(targetType);
            return stringLiteral;
        } else if (targetType.isJsonbType()) {
            return new JsonLiteral(value);
        }
        return super.uncheckedCastTo(targetType);
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

    @Override
    public void setupParamFromBinary(ByteBuffer data, boolean isUnsigned) {
        int strLen = getParmLen(data);
        if (strLen > data.remaining()) {
            strLen = data.remaining();
        }
        byte[] bytes = new byte[strLen];
        data.get(bytes);
        // ATTN: use fixed StandardCharsets.UTF_8 to avoid unexpected charset in
        // different environment
        value = new String(bytes, StandardCharsets.UTF_8);
        if (LOG.isDebugEnabled()) {
            LOG.debug("parsed value '{}'", value);
        }
        type = Type.VARCHAR;
    }
}
