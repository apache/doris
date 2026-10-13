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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.io.BaseEncoding;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

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
        this(VarBinaryType.INSTANCE, byteValues);
    }

    public VarBinaryLiteral(VarBinaryType type, byte[] byteValues) {
        super(type);
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

    /**
     * BE casts VARBINARY to a string type by keeping the bytes as they are, so CAST(X'616263' AS STRING) is
     * 'abc', not the hex text '616263'. A Java string carries those bytes only when they are valid UTF-8; for
     * other bytes, such as X'FF', this throws, and FoldConstantRuleOnFE.visitCast then leaves the cast for BE
     * to evaluate (it turns only a CastException into NULL).
     */
    @Override
    protected String castValueToString() {
        return toUtf8String().orElseThrow(() -> new AnalysisException(
                "Cannot cast VARBINARY " + computeToSql() + " to a string type on FE: it is not valid UTF-8"));
    }

    /** The bytes decoded as UTF-8, or empty when they are not valid UTF-8 and only BE can cast them. */
    public Optional<String> toUtf8String() {
        try {
            return Optional.of(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(byteValues)).toString());
        } catch (CharacterCodingException e) {
            return Optional.empty();
        }
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
        // Persisted views must retain the literal's binary type instead of only its hex digits.
        return "X'" + toString() + "'";
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        try {
            LiteralExpr literal = new org.apache.doris.analysis.VarBinaryLiteral(byteValues);
            literal.setType(dataType.toCatalogDataType());
            return literal;
        } catch (Exception e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("Invalid VarBinary format.");
        }
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof VarBinaryLiteral) {
            // Binary partition keys have no padding: a prefix and its zero extension are distinct.
            return Arrays.compareUnsigned(byteValues, ((VarBinaryLiteral) other).byteValues);
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
        // equals compares byte contents independently of the declared length, never array identity.
        return Arrays.hashCode(byteValues);
    }
}
