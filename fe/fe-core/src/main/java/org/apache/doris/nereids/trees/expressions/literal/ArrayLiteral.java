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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/** ArrayLiteral */
public class ArrayLiteral extends Literal {

    private final List<Literal> items;

    /**
     * construct array literal
     */
    public ArrayLiteral(List<Literal> items) {
        super(computeDataType(items));
        this.items = items.stream()
                .map(i -> {
                    if (i instanceof NullLiteral) {
                        DataType type = ((ArrayType) (this.getDataType())).getItemType();
                        return new NullLiteral(type);
                    }
                    return i;
                }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<Literal> getValue() {
        return items;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        if (items.isEmpty()) {
            return new org.apache.doris.analysis.ArrayLiteral();
        } else {
            LiteralExpr[] itemExprs = items.stream()
                    .map(Literal::toLegacyLiteral)
                    .toArray(LiteralExpr[]::new);
            try {
                return new org.apache.doris.analysis.ArrayLiteral(getDataType().toCatalogDataType(), itemExprs);
            } catch (Throwable t) {
                throw new AnalysisException(t.getMessage(), t);
            }
        }
    }

    @Override
    public String toString() {
        String items = this.items.stream()
                .map(Literal::toString)
                .collect(Collectors.joining(", "));
        return "[" + items + "]";
    }

    @Override
    public String toSql() {
        String items = this.items.stream()
                .map(Literal::toSql)
                .collect(Collectors.joining(", "));
        return "[" + items + "]";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayLiteral(this, context);
    }

    private static DataType computeDataType(List<Literal> items) {
        if (items.isEmpty()) {
            return ArrayType.SYSTEM_DEFAULT;
        }
        DataType dataType = NullType.INSTANCE;
        for (Literal item : items) {
            if (!item.dataType.isNullType()) {
                dataType = item.dataType;
            }
        }
        return ArrayType.of(dataType);
    }
}
