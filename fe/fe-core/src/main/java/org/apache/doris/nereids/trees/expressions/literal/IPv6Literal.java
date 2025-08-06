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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv6Type;

import com.googlecode.ipv6.IPv6Address;

import java.util.regex.Pattern;

/**
 * Represents IPv6 literal
 */
public class IPv6Literal extends Literal implements ComparableLiteral {

    private static final Pattern IPV6_STD_REGEX =
            Pattern.compile("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");
    private static final Pattern IPV6_COMPRESS_REGEX =
            Pattern.compile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4})*)?)::((([0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4})?)$");
    private static final String IPV4_PART = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final Pattern IPV6_MAPPED_REGEX =
            Pattern.compile("^::[fF]{4}:(" + IPV4_PART + "\\.){3}" + IPV4_PART + "$");

    private final IPv6Address value;

    public IPv6Literal(String ipv6) throws AnalysisException {
        super(IPv6Type.INSTANCE);
        checkValueValid(ipv6);
        this.value = IPv6Address.fromString(ipv6);
    }

    @Override
    public IPv6Address getValue() {
        return value;
    }

    @Override
    public double getDouble() {
        return value.toBigInteger().doubleValue();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIPv6Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.IPv6Literal(value.toString());
        } catch (Exception e) {
            throw new AnalysisException("Invalid IPv6 format.");
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof IPv6Literal) {
            return value.compareTo(((IPv6Literal) other).value);
        }
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + dataType + ") vs " + other + " (" + ((Literal) other).dataType + ")");
    }

    /**
     * check IPv6 is valid
     */
    public void checkValueValid(String ipv6) throws AnalysisException {
        if (ipv6.length() > 39) {
            throw new AnalysisException("The length of IPv6 must not exceed 39.");
        } else if (!IPV6_STD_REGEX.matcher(ipv6).matches() && !IPV6_COMPRESS_REGEX.matcher(ipv6).matches()
                && !IPV6_MAPPED_REGEX.matcher(ipv6).matches()) {
            throw new AnalysisException("Invalid IPv6 format.");
        }
    }
}
