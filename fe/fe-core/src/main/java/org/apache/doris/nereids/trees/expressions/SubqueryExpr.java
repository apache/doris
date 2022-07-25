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

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Subquery Expression.
 */
public class SubqueryExpr extends Expression {
    protected LogicalPlan queryPlan;

    public SubqueryExpr(LogicalPlan subquery) {
        this.queryPlan = Objects.requireNonNull(subquery, "subquery can not be null");
    }

    @Override
    public DataType getDataType() throws UnboundException {
        // TODO:
        // Returns the data type of the row on a single line
        // For multiple lines, struct type is returned, in the form of splicing,
        // but it seems that struct type is not currently supported
        throw new UnboundException("not support");
    }

    @Override
    public boolean nullable() throws UnboundException {
        // TODO:
        // Any child is nullable, the whole is nullable
        throw new UnboundException("not support");
    }

    @Override
    public String toSql() {
        return "(" + queryPlan.toString() + ")";
    }

    @Override
    public String toString() {
        return "(" + queryPlan.toString() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryExpr(this, context);
    }

    public LogicalPlan getQueryPlan() {
        return queryPlan;
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
        return checkEquals(queryPlan, other.queryPlan);
    }

    /**
     * Compare whether all logical nodes under query are the same.
     * @param i original query.
     * @param o compared query.
     * @return equal ? true : false;
     */
    private boolean checkEquals(Object i, Object o) {
        if (!(i instanceof LogicalPlan) || !(o instanceof LogicalPlan)) {
            return false;
        }
        LogicalPlan other = (LogicalPlan) o;
        LogicalPlan input = (LogicalPlan) i;
        if (other.children().size() != input.children().size()) {
            return false;
        }
        boolean equal;
        for (int j = 0; j < input.children().size(); j++) {
            LogicalPlan childInput = (LogicalPlan) input.child(j);
            LogicalPlan childOther = (LogicalPlan) other.child(j);
            equal = Objects.equals(childInput, childOther);
            if (!equal) {
                return false;
            }
            if (childInput.children().size() != childOther.children().size()) {
                return false;
            }
            checkEquals(childInput, childOther);
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryPlan);
    }
}
