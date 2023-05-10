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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.PartitionTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical partition-top-N plan.
 */
public class LogicalPartitionTopN<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements PartitionTopN {
    private final Expression function;
    private final List<Expression> partitionKeys;
    private final List<OrderExpression> orderKeys;
    private final Boolean hasGlobalLimit;
    private final long partitionLimit;

    public LogicalPartitionTopN(WindowExpression windowExpr, Boolean hasGlobalLimit, long partitionLimit,
                                CHILD_TYPE child) {
        this(windowExpr.getFunction(), windowExpr.getPartitionKeys(), windowExpr.getOrderKeys(),
                hasGlobalLimit, partitionLimit, Optional.empty(),
                Optional.empty(), child);
    }

    public LogicalPartitionTopN(Expression function, List<Expression> partitionKeys, List<OrderExpression> orderKeys,
                                Boolean hasGlobalLimit, long partitionLimit, CHILD_TYPE child) {
        this(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
                Optional.empty(), Optional.empty(), child);
    }

    /**
     * Constructor for LogicalSort.
     */
    public LogicalPartitionTopN(Expression function, List<Expression> partitionKeys, List<OrderExpression> orderKeys,
                                Boolean hasGlobalLimit, long partitionLimit, Optional<GroupExpression> groupExpression,
                                Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_PARTITION_TOP_N, groupExpression, logicalProperties, child);
        this.function = function;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = ImmutableList.copyOf(orderKeys);
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
    }

    // TODO: need to implement the computeOutput for PartitionTopN
    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    public Expression getFunction() {
        return function;
    }

    public List<Expression> getPartitionKeys() {
        return partitionKeys;
    }

    public List<OrderExpression> getOrderKeys() {
        return orderKeys;
    }

    public boolean hasGlobalLimit() {
        return hasGlobalLimit;
    }

    public long getPartitionLimit() {
        return partitionLimit;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalPartitionTopN",
            "function", function,
            "partitionKeys", partitionKeys,
            "orderKeys", orderKeys,
            "hasGlobalLimit", hasGlobalLimit,
            "partitionLimit", partitionLimit
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalPartitionTopN<?> that = (LogicalPartitionTopN<?>) o;
        return Objects.equals(this.function, that.function) && Objects.equals(this.partitionKeys, that.partitionKeys)
            && Objects.equals(this.orderKeys, that.orderKeys) && this.hasGlobalLimit == that.hasGlobalLimit
            && this.partitionLimit == that.partitionLimit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalPartitionTopN(this, context);
    }

    // TODO: implement this function
    @Override
    public List<? extends Expression> getExpressions() {
        return null;
    }

    @Override
    public LogicalPartitionTopN<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
            children.get(0));
    }

    @Override
    public LogicalPartitionTopN<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
            groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalPartitionTopN<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
            Optional.empty(), logicalProperties, child());
    }
}
