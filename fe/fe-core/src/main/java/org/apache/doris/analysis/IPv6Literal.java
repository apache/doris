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

import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPv6Literal extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(IPv6Literal.class);

    public static final BigInteger IPV6_MIN = new BigInteger("0");
    public static final BigInteger IPV6_MAX = new BigInteger("340282366920938463463374607431768211455"); // 2^128 - 1

    private BigInteger value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IPv6Literal() {
    }

    public IPv6Literal(String value) throws AnalysisException {
        super();
        BigInteger result = parseIPv6toBigInteger(value);
        if (result != null) {
            this.value = result;
            this.type = Type.IPV6;
        } else {
            throw new AnalysisException("Invalid IPv6 format: " + value + ". type: " + Type.IPV6);
        }
        analysisDone();
    }

    public IPv6Literal(BigInteger value) {
        super();
        this.value = value;
        this.type = Type.IPV6;
        analysisDone();
    }

    protected IPv6Literal(IPv6Literal other) {
        super(other);
        this.value = other.value;
    }

    private static BigInteger parseIPv6toBigInteger(String ipv6) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ipv6);
            if (inetAddress instanceof Inet6Address) {
                byte[] bytes = inetAddress.getAddress();
                return new BigInteger(1, bytes);
            }
        } catch (UnknownHostException e) {
            return null;
        }
        return null;
    }

    private static String parseBigIntegerToIPv6(BigInteger ipv6) {
        byte[] bytes = ipv6.toByteArray();
        try {
            InetAddress inetAddress = Inet6Address.getByAddress(bytes);
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            return null;
            // throw new AnalysisException("Invalid IPv6 format: " + ipv6 + ". type: " + Type.IPV6);
        }
    }

    @Override
    protected String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.IPV6_LITERAL;
        msg.ipv6_literal = new TIPv6Literal(parseBigIntegerToIPv6(value));
    }

    @Override
    public Expr clone() {
        return new IPv6Literal(this);
    }

    @Override
    public boolean isMinValue() {
        return this.value == IPV6_MIN;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public String getStringValue() {
        return parseBigIntegerToIPv6(value);
    }

    @Override
    public String getStringValueForArray() {
        return "\"" + getStringValue() + "\"";
    }
}
