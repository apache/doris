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
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Subquery Expression.
 */
public class SubqueryExpr extends Expression {
    private final LogicalPlan query;

    public SubqueryExpr(LogicalPlan plan) {
        super(ExpressionType.SUBQUERY);
        this.query = plan;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        // TODO:
        // Returns the data type of the row on a single line
        // For multiple lines, struct type is returned, in the form of splicing,
        // but it seems that struct type is not currently supported
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        // TODO:
        // Any child is nullable, the whole is nullable
        return false;
    }

    @Override
    public String toSql() {
        return "(" + query.toString() + ")";
    }

    @Override
    public String toString() {
        return "(" + query.toString() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryExpr(this, context);
    }

    public LogicalPlan getQuery() {
        return query;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return children.get(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubqueryExpr other = (SubqueryExpr) o;
        return Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}
