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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IPv4Type;

/**
 * Represents IPv4 literal
 */
public class IPv4Literal extends Literal {

    private long value;

    public IPv4Literal(String ipv4) {
        super(IPv4Type.INSTANCE);
        init(ipv4);
    }

    protected IPv4Literal(long value) {
        super(IPv4Type.INSTANCE);
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIPv4Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.IPv4Literal(value);
    }

    void init(String ipv4) {
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
                return;
            }
            if (octet < 0 || octet > 255) {
                return;
            }
            value = (value << 8) | octet;
        }
        this.value = value;
    }
}
