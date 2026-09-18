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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.gson.annotations.SerializedName;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.regex.Pattern;

/** Legacy planner UUID literal. */
public class UuidLiteral extends LiteralExpr {
    public static final String UUID_MIN = "00000000-0000-0000-0000-000000000000";
    public static final String UUID_MAX = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    private static final Pattern CANONICAL_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern COMPACT_PATTERN = Pattern.compile("^[0-9a-fA-F]{32}$");

    @SerializedName("v")
    private String value;

    private UuidLiteral() {
    }

    public UuidLiteral(String text) throws AnalysisException {
        value = normalize(text);
        type = Type.UUID;
        nullable = false;
    }

    protected UuidLiteral(UuidLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        value = normalize(value);
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitUuidLiteral(this, context);
    }

    @Override
    public Expr clone() {
        return new UuidLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return UUID_MIN.equals(value);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr instanceof UuidLiteral) {
            UUID left = UUID.fromString(value);
            UUID right = UUID.fromString(((UuidLiteral) expr).value);
            int high = Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
            return high != 0 ? high
                    : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + type + ") vs " + expr + " (" + expr.type + ")");
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof UuidLiteral && value.equals(((UuidLiteral) other).value);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + value.hashCode();
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public String getRealValue() {
        return value;
    }

    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        UUID uuid = UUID.fromString(value);
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.flip();
        return buffer;
    }

    /** Return the next UUID in unsigned 128-bit order, saturating at the maximum UUID. */
    public UuidLiteral successor() throws AnalysisException {
        UUID uuid = UUID.fromString(value);
        long high = uuid.getMostSignificantBits();
        long low = uuid.getLeastSignificantBits();
        if (low != -1L) {
            low++;
        } else if (high != -1L) {
            high++;
            low = 0;
        }
        return new UuidLiteral(new UUID(high, low).toString());
    }

    public String getValue() {
        return value;
    }

    private static String normalize(String text) throws AnalysisException {
        String canonical = text;
        if (COMPACT_PATTERN.matcher(text).matches()) {
            canonical = text.substring(0, 8) + "-" + text.substring(8, 12) + "-"
                    + text.substring(12, 16) + "-" + text.substring(16, 20) + "-" + text.substring(20);
        } else if (!CANONICAL_PATTERN.matcher(text).matches()) {
            throw new AnalysisException("Invalid UUID format: " + text);
        }
        return UUID.fromString(canonical).toString();
    }
}
