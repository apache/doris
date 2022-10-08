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
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * In predicate expression.
 */
public class InSubquery extends SubqueryExpr {

    private final Expression compareExpr;
    private final ListQuery listQuery;
    private final boolean isNot;

    public InSubquery(Expression compareExpression, ListQuery listQuery, boolean isNot) {
        super(Objects.requireNonNull(listQuery.getQueryPlan(), "subquery can not be null"));
        this.compareExpr = Objects.requireNonNull(compareExpression, "compareExpr can not be null");
        this.listQuery = Objects.requireNonNull(listQuery, "listQuery can not be null");
        this.isNot = Objects.requireNonNull(isNot, "isNot can not be null");
    }

    public InSubquery(Expression compareExpr, ListQuery listQuery, List<Slot> correlateSlots, boolean isNot) {
        super(Objects.requireNonNull(listQuery.getQueryPlan(), "subquery can not be null"),
                Objects.requireNonNull(correlateSlots, "correlateSlots can not be null"));
        this.compareExpr = Objects.requireNonNull(compareExpr, "compareExpr can not be null");
        this.listQuery = Objects.requireNonNull(listQuery, "listQuery can not be null");
        this.isNot = Objects.requireNonNull(isNot, "isNot can not be null");
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return listQuery.getDataType();
    }

    @Override
    public boolean nullable() throws UnboundException {
        return super.nullable() || this.compareExpr.nullable();
    }

    @Override
    public String toSql() {
        return this.compareExpr.toSql() + " IN (INSUBQUERY) " + super.toSql();
    }

    @Override
    public String toString() {
        return this.compareExpr + " IN (INSUBQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInSubquery(this, context);
    }

    public Expression getCompareExpr() {
        return this.compareExpr;
    }

    public boolean isNot() {
        return isNot;
    }

    public ListQuery getListQuery() {
        return listQuery;
    }

    @Override
    public InSubquery withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        Preconditions.checkArgument(children.get(1) instanceof ListQuery);
        return new InSubquery(children.get(0), (ListQuery) children.get(1), isNot);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InSubquery inSubquery = (InSubquery) o;
        return Objects.equals(this.compareExpr, inSubquery.getCompareExpr())
                && checkEquals(this.queryPlan, inSubquery.queryPlan)
                && Objects.equals(this.isNot, inSubquery.isNot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.compareExpr, this.listQuery, this.isNot);
    }
}
