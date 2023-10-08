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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.nereids.trees.plans.algebra.PartitionTopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Physical partition-top-N plan.
 */
public class PhysicalPartitionTopN<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements PartitionTopN {
    private final WindowFuncType function;
    private final List<Expression> partitionKeys;
    private final List<OrderKey> orderKeys;
    private final Boolean hasGlobalLimit;
    private final long partitionLimit;
    private final PartitionTopnPhase phase;

    public PhysicalPartitionTopN(WindowFuncType function, List<Expression> partitionKeys, List<OrderKey> orderKeys,
                                 Boolean hasGlobalLimit, long partitionLimit, PartitionTopnPhase phase,
                                 LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit, phase,
                Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalPartitionTopN.
     */
    public PhysicalPartitionTopN(WindowFuncType function, List<Expression> partitionKeys, List<OrderKey> orderKeys,
                                 Boolean hasGlobalLimit, long partitionLimit, PartitionTopnPhase phase,
                                 Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                                 CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PARTITION_TOP_N, groupExpression, logicalProperties, child);
        this.function = function;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = ImmutableList.copyOf(orderKeys);
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
        this.phase = phase;
    }

    /**
     * Constructor of PhysicalPartitionTopN.
     */
    public PhysicalPartitionTopN(WindowFuncType function, List<Expression> partitionKeys, List<OrderKey> orderKeys,
                                 Boolean hasGlobalLimit, long partitionLimit, PartitionTopnPhase phase,
                                 Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                                 PhysicalProperties physicalProperties, Statistics statistics,
                                 CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PARTITION_TOP_N, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
        this.function = function;
        this.partitionKeys = partitionKeys;
        this.orderKeys = orderKeys;
        this.hasGlobalLimit = hasGlobalLimit;
        this.partitionLimit = partitionLimit;
        this.phase = phase;
    }

    public WindowFuncType getFunction() {
        return function;
    }

    @Override
    public List<Expression> getPartitionKeys() {
        return partitionKeys;
    }

    public List<OrderKey> getOrderKeys() {
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

    public PartitionTopnPhase getPhase() {
        return phase;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalPartitionTopN<?> that = (PhysicalPartitionTopN<?>) o;
        return Objects.equals(this.function, that.function)
            && Objects.equals(this.partitionKeys, that.partitionKeys)
            && Objects.equals(this.orderKeys, that.orderKeys) && this.hasGlobalLimit == that.hasGlobalLimit
            && this.partitionLimit == that.partitionLimit
            && this.phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPartitionTopN(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
            .addAll(partitionKeys)
            .addAll(orderKeys.stream()
                .map(OrderKey::getExpr)
                .collect(Collectors.toList()))
            .build();
    }

    @Override
    public PhysicalPartitionTopN<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit,
                partitionLimit, phase, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, children.get(0));
    }

    @Override
    public PhysicalPartitionTopN<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit, phase,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit, phase,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPartitionTopN<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                                            Statistics statistics) {
        return new PhysicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit, phase,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalPartitionTopN[" + id.asInt() + "]" + getGroupIdWithPrefix(),
            "function", function,
            "partitionKeys", partitionKeys,
            "orderKeys", orderKeys,
            "hasGlobalLimit", hasGlobalLimit,
            "partitionLimit", partitionLimit,
            "stats", statistics,
            "phase", phase
        );
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalPartitionTopN<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalPartitionTopN<>(function, partitionKeys, orderKeys, hasGlobalLimit, partitionLimit, phase,
                groupExpression, null, physicalProperties, statistics, child());
    }
}
