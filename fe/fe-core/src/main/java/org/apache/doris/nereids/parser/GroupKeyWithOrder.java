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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.Objects;

/**
 * Represents the group by expression with order of a statement.
 */
public class GroupKeyWithOrder {

    private final Expression expr;

    // Order is ascending.
    private final boolean hasOrder;

    private final boolean isAsc;

    /**
     * Constructor of GroupKeyWithOrder.
     */
    public GroupKeyWithOrder(Expression expr, boolean hasOrder, boolean isAsc) {
        this.expr = expr;
        this.hasOrder = hasOrder;
        this.isAsc = isAsc;
    }

    public Expression getExpr() {
        return expr;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public boolean hasOrder() {
        return hasOrder;
    }

    public GroupKeyWithOrder withExpression(Expression expr) {
        return new GroupKeyWithOrder(expr, isAsc, hasOrder);
    }

    public String toSql() {
        return expr.toSql() + (hasOrder ? (isAsc ? " asc" : " desc") : "");
    }

    @Override
    public String toString() {
        return expr.toString() + (hasOrder ? (isAsc ? " asc" : " desc") : "");
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, isAsc, hasOrder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupKeyWithOrder that = (GroupKeyWithOrder) o;
        return isAsc == that.isAsc() && hasOrder == that.hasOrder() && expr.equals(that.getExpr());
    }
}
