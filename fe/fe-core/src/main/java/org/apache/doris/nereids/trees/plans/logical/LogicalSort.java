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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Sort plan.
 * <p>
 * eg: select * from table order by a, b desc;
 * orderKeys: list of column information after order by. eg:[a, asc],[b, desc].
 * OrderKey: Contains order expression information and sorting method. Default is ascending.
 */
public class LogicalSort<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    // Default offset is 0.
    private int offset = 0;

    private final List<OrderKey> orderKeys;


    public LogicalSort(List<OrderKey> orderKeys, CHILD_TYPE child) {
        this(orderKeys, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Constructor for LogicalSort.
     */
    public LogicalSort(List<OrderKey> orderKeys, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SORT, groupExpression, logicalProperties, child);
        this.orderKeys = Objects.requireNonNull(orderKeys, "orderKeys can not be null");
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return input.getOutput();
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "LogicalSort (" + StringUtils.join(orderKeys, ", ") + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSort that = (LogicalSort) o;
        return offset == that.offset && Objects.equals(orderKeys, that.orderKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderKeys, offset);
    }


    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSort((LogicalSort<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return orderKeys.stream()
                .map(OrderKey::getExpr)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSort<>(orderKeys, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSort<>(orderKeys, groupExpression, Optional.of(logicalProperties), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalSort<>(orderKeys, Optional.empty(), logicalProperties, child());
    }
}
