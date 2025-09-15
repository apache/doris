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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TVarBinaryLiteral;

import com.google.common.io.BaseEncoding;
import com.google.gson.annotations.SerializedName;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VarBinaryLiteral extends LiteralExpr {

    @SerializedName("v")
    private byte[] value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private VarBinaryLiteral() {
    }

    public VarBinaryLiteral(byte[] value) throws AnalysisException {
        super();
        this.value = value;
        this.type = Type.VARBINARY;
        analysisDone();
    }

    protected VarBinaryLiteral(VarBinaryLiteral other) {
        super(other);
        this.value = other.value;
    }


    @Override
    protected String toSqlImpl() {
        return toHexLiteral();
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return toHexLiteral();
    }

    private String toHexLiteral() {
        String hex = BaseEncoding.base16().encode(value); // upper-case hex
        return "X'" + hex + "'";
    }

    @Override
    public void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.VARBINARY_LITERAL;
        msg.varbinary_literal = new TVarBinaryLiteral(ByteBuffer.wrap(this.value));
    }

    @Override
    public Expr clone() {
        return new VarBinaryLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr other) {
        if (other instanceof VarBinaryLiteral) {
            byte[] thisBytes = this.value;
            byte[] otherBytes = ((VarBinaryLiteral) other).value;

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
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + this.type + ") vs " + other + " (" + ((LiteralExpr) other).type + ")");
    }

    @Override
    public String getStringValue() {
        return new String(value, StandardCharsets.ISO_8859_1);
    }

    @Override
    public String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValueForQuery(options) + options.getNestedStringWrapper();
    }
}
