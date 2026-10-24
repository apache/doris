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
import org.apache.doris.nereids.types.UuidType;

import java.math.BigInteger;
import java.util.UUID;
import java.util.regex.Pattern;

/** UUID literal stored and rendered in canonical lowercase form. */
public class UuidLiteral extends Literal implements ComparableLiteral {
    private static final Pattern CANONICAL_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern COMPACT_PATTERN = Pattern.compile("^[0-9a-fA-F]{32}$");

    private final UUID value;

    public UuidLiteral(String text) {
        super(UuidType.INSTANCE);
        value = parse(text);
    }

    @Override
    public UUID getValue() {
        return value;
    }

    @Override
    public double getDouble() {
        return asUnsignedInteger(value).doubleValue();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUuidLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.UuidLiteral(value.toString());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException("Invalid UUID format: " + value, e);
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof UuidLiteral) {
            UuidLiteral that = (UuidLiteral) other;
            int high = Long.compareUnsigned(value.getMostSignificantBits(), that.value.getMostSignificantBits());
            return high != 0 ? high
                    : Long.compareUnsigned(value.getLeastSignificantBits(), that.value.getLeastSignificantBits());
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

    private static UUID parse(String text) {
        String canonical = text;
        if (COMPACT_PATTERN.matcher(text).matches()) {
            canonical = text.substring(0, 8) + "-" + text.substring(8, 12) + "-"
                    + text.substring(12, 16) + "-" + text.substring(16, 20) + "-" + text.substring(20);
        } else if (!CANONICAL_PATTERN.matcher(text).matches()) {
            throw new AnalysisException("Invalid UUID format: " + text);
        }
        return UUID.fromString(canonical);
    }

    private static BigInteger asUnsignedInteger(UUID uuid) {
        return new BigInteger(uuid.toString().replace("-", ""), 16);
    }
}
