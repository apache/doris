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
import org.apache.doris.foundation.format.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TIPv4Literal;

import com.google.gson.annotations.SerializedName;

public class IPv4Literal extends LiteralExpr {

    public static final long IPV4_MIN = 0L;             // 0.0.0.0
    public static final long IPV4_MAX = (2L << 31) - 1; // 255.255.255.255

    @SerializedName("v")
    private long value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IPv4Literal() {
    }

    public IPv4Literal(long value) {
        super();
        this.value = value;
        this.type = Type.IPV4;
        analysisDone();
    }

    public IPv4Literal(String value) throws AnalysisException {
        super();
        this.value = parseIPv4toLong(value);
        this.type = Type.IPV4;
        analysisDone();
    }

    protected IPv4Literal(IPv4Literal other) {
        super(other);
        this.value = other.value;
    }

    private static long parseIPv4toLong(String ipv4) throws AnalysisException {
        // Use limit -1 so a trailing dot keeps its empty part: "1.2.3.4." stays 5 parts and is
        // rejected by the count check below (plain split("\\.") drops the trailing "" and would
        // wrongly yield 4). An IPv4 address must be exactly 4 dot-separated parts.
        String[] parts = ipv4.split("\\.", -1);
        if (parts.length != 4) {
            throw new AnalysisException("Invalid IPv4 format: " + ipv4);
        }

        long value = 0L;
        for (int i = 0; i < 4; ++i) {
            String part = parts[i];
            // Each octet must be 1-3 ASCII digits. Short.parseShort alone is too lax for a DDL
            // validator: it accepts a leading '+' and Unicode digits (fullwidth, Arabic-Indic,
            // etc.), which BE's ASCII-only parse_ipv4 later rejects, turning the stored default
            // into a late failure at load time. So check ASCII digits explicitly here. Leading
            // zeros stay decimal ("010" -> 10), consistent with the BE parser and Nereids.
            if (part.isEmpty() || part.length() > 3) {
                throw new AnalysisException("Invalid IPv4 format: " + ipv4);
            }
            for (int j = 0; j < part.length(); ++j) {
                char c = part.charAt(j);
                if (c < '0' || c > '9') {
                    throw new AnalysisException("Invalid IPv4 format: " + ipv4);
                }
            }
            // Safe now: 1-3 ASCII digits never overflow short and never throw.
            short octet = Short.parseShort(part);
            if (octet > 255) {
                throw new AnalysisException("Invalid IPv4 format: " + ipv4);
            }
            value = (value << 8) | octet;
        }

        return value;
    }

    private static String parseLongToIPv4(long ipv4) {
        StringBuilder sb = new StringBuilder();
        for (int i = 3; i >= 0; i--) {
            short octet = (short) ((ipv4 >> (i * 8)) & 0xFF);
            sb.append(octet);
            if (i > 0) {
                sb.append(".");
            }
        }
        return sb.toString();
    }


    @Override
    public Expr clone() {
        return new IPv4Literal(this);
    }

    @Override
    protected String toSqlImpl() {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.IPV4_LITERAL;
        msg.ipv4_literal = new TIPv4Literal(this.value);
    }

    @Override
    public boolean isMinValue() {
        return this.value == IPV4_MIN;
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
        if (expr instanceof IPv4Literal) {
            return Long.compare(this.value, ((IPv4Literal) expr).value);
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + this.type + ") vs " + expr + " (" + expr.type + ")");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IPv4Literal)) {
            return false;
        }
        return this.value == ((IPv4Literal) obj).value;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Long.hashCode(value);
    }

    @Override
    public String getStringValue() {
        return parseLongToIPv4(this.value);
    }

    @Override
    protected String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValueForQuery(options) + options.getNestedStringWrapper();
    }
}
