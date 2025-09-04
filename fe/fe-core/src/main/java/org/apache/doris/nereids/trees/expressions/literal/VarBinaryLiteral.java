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

import java.util.Arrays;
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
        return new String(byteValues);
        // return BaseEncoding.base16().encode(byteValues);
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
            return 0;
        }
        return -1;
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
