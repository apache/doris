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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Query with alias and column alias
 */
public class AliasQuery extends Expression {

    private final String name;
    private final LogicalPlan queryPlan;
    private final Optional<List<String>> columnAliases;

    public AliasQuery(String name, LogicalPlan queryPlan, Optional<List<String>> columnAliases) {
        this.name = name;
        this.queryPlan = queryPlan;
        this.columnAliases = columnAliases;
    }

    public String getName() {
        return name;
    }

    public LogicalPlan getQueryPlan() {
        return queryPlan;
    }

    public Optional<List<String>> getColumnAliases() {
        return columnAliases;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        throw new UnboundException("not support");
    }

    @Override
    public boolean nullable() throws UnboundException {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AliasQuery other = (AliasQuery) o;
        return name.equals(other.name) && Objects.equals(queryPlan, other.queryPlan)
            && Objects.equals(columnAliases, other.columnAliases);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(name + " ");
        columnAliases.ifPresent(column -> stringBuilder.append(column.stream()
                .collect(Collectors.joining(", ", "(", ") "))));
        stringBuilder.append("AS (" + queryPlan + ")");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[(name: " + name + "), ");
        columnAliases.ifPresent(column -> stringBuilder.append(column.stream()
                .collect(Collectors.joining(", ", "(columnAliases: ", "), "))));
        stringBuilder.append(queryPlan + "]");
        return stringBuilder.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAliasQuery(this, context);
    }
}
