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
import org.apache.doris.thrift.TIPv6Literal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

public class IPv6Literal extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(IPv6Literal.class);

    public static final String IPV6_MIN = "0:0:0:0:0:0:0:0";
    public static final String IPV6_MAX = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
    private static final Pattern IPV6_STD_REGEX =
            Pattern.compile("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");
    private static final Pattern IPV6_COMPRESS_REGEX =
            Pattern.compile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4})*)?)::((([0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4})?)$");

    private String value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IPv6Literal() {
    }

    public IPv6Literal(String value) throws AnalysisException {
        super();
        checkValueValid(value);
        this.value = value;
        this.type = Type.IPV6;
        analysisDone();
    }

    protected IPv6Literal(IPv6Literal other) {
        super(other);
        this.value = other.value;
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        checkValueValid(this.value);
    }

    private void checkValueValid(String ipv6) throws AnalysisException {
        if (ipv6.length() > 39) {
            throw new AnalysisException("The length of IPv6 must not exceed 39. type: " + Type.IPV6);
        } else if (!IPV6_STD_REGEX.matcher(ipv6).matches() && !IPV6_COMPRESS_REGEX.matcher(ipv6).matches()) {
            throw new AnalysisException("Invalid IPv6 format: " + ipv6 + ". type: " + Type.IPV6);
        }
    }

    @Override
    protected String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.IPV6_LITERAL;
        msg.ipv6_literal = new TIPv6Literal(this.value);
    }

    @Override
    public Expr clone() {
        return new IPv6Literal(this);
    }

    @Override
    public boolean isMinValue() {
        return IPV6_MIN.equals(this.value);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public String getStringValue() {
        return this.value;
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
    }
}
