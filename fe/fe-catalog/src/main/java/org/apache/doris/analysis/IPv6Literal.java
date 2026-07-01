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

import com.google.gson.annotations.SerializedName;
import com.googlecode.ipv6.IPv6Address;

import java.util.regex.Pattern;

public class IPv6Literal extends LiteralExpr {

    public static final String IPV6_MIN = "0:0:0:0:0:0:0:0";
    public static final String IPV6_MAX = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
    private static final Pattern IPV6_STD_REGEX =
            Pattern.compile("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");
    private static final Pattern IPV6_COMPRESS_REGEX =
            Pattern.compile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4})*)?)::((([0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4})?)$");
    private static final String IPV4_PART = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final Pattern IPV6_MAPPED_REGEX =
            Pattern.compile("^::[fF]{4}:(" + IPV4_PART + "\\.){3}" + IPV4_PART + "$");

    @SerializedName("v")
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
        this.nullable = false;
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
        } else if (!IPV6_STD_REGEX.matcher(ipv6).matches() && !IPV6_COMPRESS_REGEX.matcher(ipv6).matches()
                && !IPV6_MAPPED_REGEX.matcher(ipv6).matches()) {
            throw new AnalysisException("Invalid IPv6 format: " + ipv6 + ". type: " + Type.IPV6);
        }
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitIPv6Literal(this, context);
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
        if (expr instanceof PlaceHolderExpr) {
            return this.compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr instanceof IPv6Literal && type.equals(expr.type)) {
            return parseAddress(this.value).compareTo(parseAddress(((IPv6Literal) expr).value));
        }
        return -1;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IPv6Literal)) {
            return false;
        }
        return parseAddress(this.value).equals(parseAddress(((IPv6Literal) obj).value));
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + parseAddress(this.value).hashCode();
    }

    // IPv6Address keeps the full 128-bit value for IPv4-mapped literals
    // (e.g. ::ffff:0.0.0.1, ::ffff:0:1) and matches the canonicalization used by
    // the Nereids IPv6Literal, so dedup/range logic stays consistent across both
    // planners. InetAddress.getByName would otherwise collapse mapped forms to a
    // 4-byte Inet4Address and hash-collide with addresses like ::1.
    private static IPv6Address parseAddress(String ipv6) {
        try {
            return IPv6Address.fromString(ipv6);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid IPv6 literal: " + ipv6, e);
        }
    }

    @Override
    public String getStringValue() {
        return this.value;
    }

    public String getValue() {
        return value;
    }

}
