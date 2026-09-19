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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;
import java.util.Locale;

/**
 * Represents varbinary literal
 */
public class VarBinaryLiteral extends Literal implements ComparableLiteral {

    public static final VarBinaryLiteral INSTANCE = new VarBinaryLiteral();
    private byte[] byteValues;

    public VarBinaryLiteral() {
        super(VarBinaryType.INSTANCE);
    }

    public VarBinaryLiteral(DataType dataType) {
        super(dataType);
    }

    public VarBinaryLiteral(byte[] byteValues) {
        super(VarBinaryType.INSTANCE);
        this.byteValues = byteValues;
    }

    public VarBinaryLiteral(DataType dataType, byte[] byteValues) {
        super(dataType);
        Preconditions.checkArgument(dataType instanceof VarBinaryType,
                "VarBinaryLiteral data type should be VarBinaryType, but is %s", dataType);
        this.byteValues = byteValues;
    }

    /**
     * Construct VarBinaryLiteral from hex string.
     */
    public VarBinaryLiteral(String hex) throws IllegalArgumentException {
        super(VarBinaryType.INSTANCE);
        if (hex == null) {
            throw new IllegalArgumentException("hex is null");
        }
        String s = hex.trim();
        if ((s.length() & 1) == 1) {
            s = "0" + s;
        }
        s = s.toUpperCase(Locale.ROOT);
        this.byteValues = BaseEncoding.base16().decode(s);
    }

    @Override
    public Object getValue() {
        return byteValues;
    }

    protected String castValueToString() {
        return toString();
    }

    @Override
    public String getStringValue() {
        return toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVarBinaryLiteral(this, context);
    }

    @Override
    public String toString() {
        return BaseEncoding.base16().encode(byteValues);
    }

    @Override
    public String computeToSql() {
        // Persisted view definitions must retain the binary literal's type, not just its hex digits.
        return "X'" + toString() + "'";
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            org.apache.doris.analysis.VarBinaryLiteral literal
                    = new org.apache.doris.analysis.VarBinaryLiteral(byteValues);
            literal.setType(dataType.toCatalogDataType());
            return literal;
        } catch (Exception e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("Invalid VarBinary format.");
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof VarBinaryLiteral) {
            // Match BE byte ordering: trailing zero bytes are data, not padding.
            return UnsignedBytes.lexicographicalComparator().compare(byteValues, ((VarBinaryLiteral) other).byteValues);
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

    @Override
    public double getDouble() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VarBinaryLiteral)) {
            return false;
        }
        return Arrays.equals(byteValues, ((VarBinaryLiteral) o).byteValues);
    }

    @Override
    protected int computeHashCode() {
        // equals compares content regardless of the byte array's identity or declared length.
        return Arrays.hashCode(byteValues);
    }

    @Override
    public int fastChildrenHashCode() {
        // Parent expressions cache this hash before comparing children, so array identity is unsafe.
        return Arrays.hashCode(byteValues);
    }
}
