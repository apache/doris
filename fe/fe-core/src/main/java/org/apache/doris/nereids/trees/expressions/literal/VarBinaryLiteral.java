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

import com.google.common.io.BaseEncoding;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

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

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVarBinaryLiteral(this, context);
    }

    @Override
    public String toString() {
        return BaseEncoding.base16().encode(byteValues);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            return new org.apache.doris.analysis.VarBinaryLiteral(byteValues);
        } catch (Exception e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("Invalid VarBinary format.");
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof VarBinaryLiteral) {
            byte[] thisBytes = this.byteValues;
            byte[] otherBytes = ((VarBinaryLiteral) other).byteValues;

            int minLength = Math.min(thisBytes.length, otherBytes.length);
            int i = 0;
            for (i = 0; i < minLength; i++) {
                if (Byte.toUnsignedInt(thisBytes[i]) < Byte.toUnsignedInt(otherBytes[i])) {
                    return -1;
                } else if (Byte.toUnsignedInt(thisBytes[i]) > Byte.toUnsignedInt(otherBytes[i])) {
                    return 1;
                }
            }
            if (thisBytes.length > otherBytes.length) {
                if (thisBytes[i] == 0x00) {
                    return 0;
                } else {
                    return 1;
                }
            } else if (thisBytes.length < otherBytes.length) {
                if (otherBytes[i] == 0x00) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                return 0;
            }
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
        return Objects.hash(super.computeHashCode(), byteValues);
    }
}
