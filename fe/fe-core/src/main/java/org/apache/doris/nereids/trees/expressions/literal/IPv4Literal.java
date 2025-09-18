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
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv4Type;

import java.net.Inet4Address;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents IPv4 literal
 */
public class IPv4Literal extends Literal implements ComparableLiteral {

    private static final Pattern IPV4_STD_REGEX =
            Pattern.compile("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

    /**
     * Add a class Inet4Addr wrap in Inet4Address,
     * When cast ipv4 literal to string, it will call `new StringLiteral(ipv4Literal.getValue().toString())`,
     * but Inet4Address.toString() contains a prefix "/", like "/192.168.1.10".
     * Use Inet4Addr can solve this problem.
     */
    public static class Inet4Addr {
        final Inet4Address address;

        public Inet4Addr(Inet4Address addr) {
            this.address = addr;
        }

        public Inet4Address getAddress() {
            return this.address;
        }

        public long toLong() {
            return NetUtils.inet4AddressToLong(address);
        }

        @Override
        public String toString() {
            return address.getHostAddress();
        }

        @Override
        public int hashCode() {
            return Objects.hash(address);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Inet4Addr)) {
                return false;
            }
            Inet4Addr otherAddr = (Inet4Addr) other;
            return address.equals(otherAddr.address);
        }
    }

    private Inet4Addr value;

    public IPv4Literal(String ipv4) throws AnalysisException {
        super(IPv4Type.INSTANCE);
        init(ipv4);
    }

    protected IPv4Literal(long value) throws AnalysisException {
        super(IPv4Type.INSTANCE);
        Inet4Address address;
        try {
            address = NetUtils.longToInet4Address(value);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
        this.value = new Inet4Addr(address);
    }

    @Override
    public Inet4Addr getValue() {
        return value;
    }

    @Override
    public double getDouble() {
        return (double) value.toLong();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIPv4Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.IPv4Literal(value.toLong());
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof IPv4Literal) {
            return Long.compare(value.toLong(), ((IPv4Literal) other).value.toLong());
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

    void init(String ipv4) throws AnalysisException {
        checkValueValid(ipv4);

        String[] parts = ipv4.split("\\.");
        if (parts.length != 4) {
            return;
        }

        long value = 0L;
        for (int i = 0; i < 4; ++i) {
            short octet;
            try {
                octet = Short.parseShort(parts[i]);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid IPv4 format.");
            }
            if (octet < 0 || octet > 255) {
                throw new AnalysisException("Invalid IPv4 format.");
            }
            value = (value << 8) | octet;
        }
        Inet4Address address;
        try {
            address = NetUtils.longToInet4Address(value);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
        this.value = new Inet4Addr(address);
    }

    private void checkValueValid(String ipv4) throws AnalysisException {
        if (ipv4.length() > 15) {
            throw new AnalysisException("The length of IPv4 must not exceed 15.");
        } else if (!IPV4_STD_REGEX.matcher(ipv4).matches()) {
            throw new AnalysisException("Invalid IPv4 format.");
        }
    }
}
