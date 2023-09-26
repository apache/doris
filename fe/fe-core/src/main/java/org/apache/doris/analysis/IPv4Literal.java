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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TIPv4Literal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

public class IPv4Literal extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(IPv4Literal.class);

    public static final long IPV4_MIN = 0L;             // 0.0.0.0
    public static final long IPV4_MAX = (2L << 31) - 1; // 255.255.255.255
    private static final Pattern IPV4_STD_REGEX =
            Pattern.compile("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

    private long value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IPv4Literal() {
    }

    public IPv4Literal(String value) throws AnalysisException {
        super();
        checkValueValid(value);
        this.value = parseIPv4toLong(value);
        this.type = Type.IPV4;
        analysisDone();
    }

    protected IPv4Literal(IPv4Literal other) {
        super(other);
        this.value = other.value;
    }

    private static long parseIPv4toLong(String ipv4) {
        String[] parts = ipv4.split("\\.");
        if (parts.length != 4) {
            return 0L;
        }

        long value = 0L;
        for (int i = 0; i < 4; ++i) {
            short octet;
            try {
                octet = Short.parseShort(parts[i]);
            } catch (NumberFormatException e) {
                return 0L;
            }
            if (octet < 0 || octet > 255) {
                return 0L;
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

    private void checkValueValid(String ipv4) throws AnalysisException {
        if (ipv4.length() > 15) {
            throw new AnalysisException("The length of IPv4 must not exceed 15. type: " + Type.IPV4);
        } else if (!IPV4_STD_REGEX.matcher(ipv4).matches()) {
            throw new AnalysisException("Invalid IPv4 format: " + ipv4 + ". type: " + Type.IPV4);
        }
    }


    @Override
    public Expr clone() {
        return new IPv4Literal(this);
    }

    @Override
    protected String toSqlImpl() {
        return getStringValue();
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
        return 0;
    }

    @Override
    public String getStringValue() {
        return parseLongToIPv4(this.value);
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
    }
}
