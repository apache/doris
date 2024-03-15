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
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
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
public class LogicalPartitionTopN<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements PartitionTopN, PropagateFuncDeps {
    private final WindowFuncType function;
    private final List<Expression> partitionKeys;
    private final List<OrderExpression> orderKeys;
    private final boolean hasGlobalLimit;
    private final long partitionLimit;

    public LogicalPartitionTopN(WindowExpression windowExpr, boolean hasGlobalLimit, long partitionLimit,
                                CHILD_TYPE child) {
        this(windowExpr.getFunction(), windowExpr.getPartitionKeys(), windowExpr.getOrderKeys(),
                hasGlobalLimit, partitionLimit, Optional.empty(),
                Optional.empty(), child);
    }

    public LogicalPartitionTopN(WindowFuncType windowFuncType, List<Expression> partitionKeys,
                                List<OrderExpression> orderKeys, boolean hasGlobalLimit, long partitionLimit,
                                CHILD_TYPE child) {
        this(windowFuncType, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
                Optional.empty(), Optional.empty(), child);
    }

    /**
     * Constructor for LogicalPartitionTopN.
     */
    public LogicalPartitionTopN(WindowFuncType windowFuncType, List<Expression> partitionKeys,
                                List<OrderExpression> orderKeys, boolean hasGlobalLimit,
                                long partitionLimit, Optional<GroupExpression> groupExpression,
                                Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_PARTITION_TOP_N, groupExpression, logicalProperties, child);
        this.function = windowFuncType;
        this.partitionKeys = ImmutableList.copyOf(Objects.requireNonNull(partitionKeys,
            "partitionKeys can not be null"));
        this.orderKeys = ImmutableList.copyOf(Objects.requireNonNull(orderKeys,
            "orderKeys can not be null"));
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
    }

    /**
     * Constructor for LogicalPartitionTopN.
     */
    public LogicalPartitionTopN(Expression expr, List<Expression> partitionKeys, List<OrderExpression> orderKeys,
                                boolean hasGlobalLimit, long partitionLimit, Optional<GroupExpression> groupExpression,
                                Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_PARTITION_TOP_N, groupExpression, logicalProperties, child);
        if (expr instanceof RowNumber) {
            this.function = WindowFuncType.ROW_NUMBER;
        } else if (expr instanceof Rank) {
            this.function = WindowFuncType.RANK;
        } else {
            Preconditions.checkArgument(expr instanceof DenseRank);
            this.function = WindowFuncType.DENSE_RANK;
        }
        this.partitionKeys = ImmutableList.copyOf(
            Objects.requireNonNull(partitionKeys, "partitionKeys can not be null"));
        this.orderKeys = ImmutableList.copyOf(
            Objects.requireNonNull(orderKeys, "orderKeys can not be null"));
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    public WindowFuncType getFunction() {
        return function;
    }

    @Override
    public List<Expression> getPartitionKeys() {
        return partitionKeys;
    }

    public List<OrderExpression> getOrderKeys() {
        return orderKeys;
    }

    @Override
    public boolean hasGlobalLimit() {
        return hasGlobalLimit;
    }

    @Override
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

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
            .addAll(partitionKeys)
            .addAll(orderKeys)
            .build();
    }

    public LogicalPartitionTopN<Plan> withPartitionKeysAndOrderKeys(
            List<Expression> partitionKeys, List<OrderExpression> orderKeys) {
        return new LogicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
                Optional.empty(), Optional.of(getLogicalProperties()), child());
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
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit,
                groupExpression, logicalProperties, children.get(0));
    }
}
