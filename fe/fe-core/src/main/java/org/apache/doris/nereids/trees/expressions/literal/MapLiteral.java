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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** MapLiteral */
public class MapLiteral extends Literal {

    private final List<Literal> keys;
    private final List<Literal> values;

    public MapLiteral() {
        super(MapType.SYSTEM_DEFAULT);
        this.keys = ImmutableList.of();
        this.values = ImmutableList.of();
    }

    public MapLiteral(List<Literal> keys, List<Literal> values) {
        this(keys, values, computeDataType(keys, values));
    }

    /**
     * create MAP Literal with keys, values and datatype
     */
    public MapLiteral(List<Literal> keys, List<Literal> values, DataType dataType) {
        super(dataType);
        this.keys = ImmutableList.copyOf(Objects.requireNonNull(keys, "keys should not be null"));
        this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values should not be null"));
        Preconditions.checkArgument(dataType instanceof MapType,
                "dataType should be MapType, but we meet %s", dataType);
        Preconditions.checkArgument(keys.size() == values.size(),
                "key size %s is not equal to value size %s", keys.size(), values.size());
    }

    @Override
    public List<List<Literal>> getValue() {
        return ImmutableList.of(keys, values);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        } else if (targetType instanceof MapType) {
            // we should pass dataType to constructor because arguments maybe empty
            return new MapLiteral(
                    keys.stream()
                            .map(k -> k.uncheckedCastTo(((MapType) targetType).getKeyType()))
                            .map(Literal.class::cast)
                            .collect(ImmutableList.toImmutableList()),
                    values.stream()
                            .map(v -> v.uncheckedCastTo(((MapType) targetType).getValueType()))
                            .map(Literal.class::cast)
                            .collect(ImmutableList.toImmutableList()),
                    targetType
            );
        } else {
            return super.uncheckedCastTo(targetType);
        }
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        List<LiteralExpr> keyExprs = keys.stream()
                .map(Literal::toLegacyLiteral)
                .collect(Collectors.toList());
        List<LiteralExpr> valueExprs = values.stream()
                .map(Literal::toLegacyLiteral)
                .collect(Collectors.toList());
        return new org.apache.doris.analysis.MapLiteral(getDataType().toCatalogDataType(), keyExprs, valueExprs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("map(");
        if (!keys.isEmpty()) {
            sb.append(keys.get(0).toString()).append(", ").append(values.get(0).toString());
        }
        for (int i = 1; i < keys.size(); i++) {
            sb.append(", ").append(keys.get(i).toString()).append(",").append(values.get(i).toString());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("map(");
        if (!keys.isEmpty()) {
            sb.append(keys.get(0).toSql()).append(", ").append(values.get(0).toSql());
        }
        for (int i = 1; i < keys.size(); i++) {
            sb.append(", ").append(keys.get(i).toSql()).append(",").append(values.get(i).toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapLiteral(this, context);
    }

    private static DataType computeDataType(List<Literal> keys, List<Literal> values) {
        DataType keyType = NullType.INSTANCE;
        DataType valueType = NullType.INSTANCE;
        if (!keys.isEmpty()) {
            keyType = keys.get(0).dataType;
        }
        if (!values.isEmpty()) {
            valueType = values.get(0).dataType;
        }
        return MapType.of(keyType, valueType);
    }
}
