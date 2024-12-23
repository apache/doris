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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ArrayLiteral
 */
public class ArrayLiteral extends Literal {

    private final List<Literal> items;

    /**
     * construct array literal
     */
    public ArrayLiteral(List<Literal> items) {
        this(items, ArrayType.of(CollectionUtils.isEmpty(items) ? NullType.INSTANCE : items.get(0).getDataType()));
    }

    /**
     * when items is empty, we could not get dataType from items, so we need pass dataType explicitly.
     */
    public ArrayLiteral(List<Literal> items, DataType dataType) {
        super(dataType);
        Preconditions.checkArgument(dataType instanceof ArrayType,
                "dataType should be ArrayType, but we meet %s", dataType);
        this.items = ImmutableList.copyOf(Objects.requireNonNull(items, "items should not null"));
    }

    @Override
    public List<Literal> getValue() {
        return items;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        LiteralExpr[] itemExprs = items.stream()
                .map(Literal::toLegacyLiteral)
                .toArray(LiteralExpr[]::new);
        return new org.apache.doris.analysis.ArrayLiteral(getDataType().toCatalogDataType(), itemExprs);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        } else if (targetType instanceof ArrayType) {
            // we should pass dataType to constructor because arguments maybe empty
            return new ArrayLiteral(items.stream()
                    .map(i -> i.uncheckedCastTo(((ArrayType) targetType).getItemType()))
                    .map(Literal.class::cast)
                    .collect(ImmutableList.toImmutableList()), targetType);
        } else {
            return super.uncheckedCastTo(targetType);
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
    public String computeToSql() {
        String items = this.items.stream()
                .map(Literal::toSql)
                .collect(Collectors.joining(", "));
        return "[" + items + "]";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayLiteral(this, context);
    }
}
