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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

/** MapLiteral */
public class MapLiteral extends Literal {

    private final Map<Literal, Literal> map;

    public MapLiteral() {
        super(MapType.SYSTEM_DEFAULT);
        this.map = ImmutableMap.of();
    }

    public MapLiteral(Map<Literal, Literal> map) {
        this(map, computeDataType(map));
    }

    /**
     * create MAP Literal with keys, values and datatype
     */
    public MapLiteral(Map<Literal, Literal> map, DataType dataType) {
        super(dataType);
        this.map = ImmutableMap.copyOf(Objects.requireNonNull(map, "Map should not be null"));
        Preconditions.checkArgument(dataType instanceof MapType,
                "dataType should be MapType, but we meet %s", dataType);
    }

    @Override
    public Map<Literal, Literal> getValue() {
        return map;
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        } else if (targetType instanceof MapType) {
            // we should pass dataType to constructor because arguments maybe empty
            return new MapLiteral(
                    map.entrySet().stream()
                            .collect(ImmutableMap.toImmutableMap(
                                    entry -> (Literal) entry.getKey().checkedCastWithFallback(((MapType) targetType)
                                            .getKeyType()),
                                    entry -> (Literal) entry.getValue()
                                            .checkedCastWithFallback(((MapType) targetType).getValueType())
                            )),
                    targetType
            );
        } else {
            return super.uncheckedCastTo(targetType);
        }
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        List<LiteralExpr> keyExprs = map.keySet().stream()
                .map(Literal::toLegacyLiteral)
                .collect(Collectors.toList());
        List<LiteralExpr> valueExprs = map.values().stream()
                .map(Literal::toLegacyLiteral)
                .collect(Collectors.toList());
        return new org.apache.doris.analysis.MapLiteral(getDataType().toCatalogDataType(), keyExprs, valueExprs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("map(");
        for (Entry<Literal, Literal> entry : map.entrySet()) {
            sb.append(entry.getKey().toString()).append(", ").append(entry.getValue().toString());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String computeToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("map(");
        for (Entry<Literal, Literal> entry : map.entrySet()) {
            sb.append(entry.getKey().toString()).append(", ").append(entry.getValue().toString());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMapLiteral(this, context);
    }

    private static DataType computeDataType(Map<Literal, Literal> map) {
        DataType keyType = NullType.INSTANCE;
        DataType valueType = NullType.INSTANCE;
        if (!map.isEmpty()) {
            Map.Entry<Literal, Literal> firstEntry = map.entrySet().iterator().next();
            keyType = firstEntry.getKey().dataType;
            valueType = firstEntry.getValue().dataType;
        }
        return MapType.of(keyType, valueType);
    }
}
