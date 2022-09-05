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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Subquery Expression.
 */
public abstract class SubqueryExpr extends Expression {
    protected final LogicalPlan queryPlan;
    protected final List<Slot> correlateSlots;

    public SubqueryExpr(LogicalPlan subquery) {
        this.queryPlan = Objects.requireNonNull(subquery, "subquery can not be null");
        this.correlateSlots = ImmutableList.of();
    }

    public SubqueryExpr(LogicalPlan subquery, List<Slot> correlateSlots) {
        this.queryPlan = Objects.requireNonNull(subquery, "subquery can not be null");
        this.correlateSlots = ImmutableList.copyOf(correlateSlots);
    }

    public List<Slot> getCorrelateSlots() {
        return correlateSlots;
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
    public String toSql() {
        return "(" + queryPlan + ")";
    }

    @Override
    public String toString() {
        return "(QueryPlan: " + queryPlan
                + "), (CorrelatedSlots: " + correlateSlots + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryExpr(this, context);
    }

    public LogicalPlan getQueryPlan() {
        return queryPlan;
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
        return checkEquals(queryPlan, other.queryPlan)
                && Objects.equals(correlateSlots, other.correlateSlots);
    }

    /**
     * Compare whether all logical nodes under query are the same.
     * @param i original query.
     * @param o compared query.
     * @return equal ? true : false;
     */
    protected boolean checkEquals(Object i, Object o) {
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
        return Objects.hash(queryPlan, correlateSlots);
    }

    public List<Slot> getOutput() {
        return queryPlan.getOutput();
    }
}
