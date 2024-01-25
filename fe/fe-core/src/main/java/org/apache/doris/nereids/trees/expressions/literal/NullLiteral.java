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
import org.apache.doris.nereids.types.NullType;

import java.util.Objects;

/**
 * Represents Null literal
 */
public class NullLiteral extends Literal {

    public static final NullLiteral INSTANCE = new NullLiteral();

    public NullLiteral() {
        super(NullType.INSTANCE);
    }

    public NullLiteral(DataType dataType) {
        super(dataType);
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNullLiteral(this, context);
    }

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return org.apache.doris.analysis.NullLiteral.create(dataType.toCatalogDataType());
    }

    @Override
    public double getDouble() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        NullLiteral that = (NullLiteral) o;
        return Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }
}
