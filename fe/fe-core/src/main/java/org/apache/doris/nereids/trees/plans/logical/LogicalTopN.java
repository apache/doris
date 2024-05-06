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
import org.apache.doris.nereids.properties.ExprFdItem;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical top-N plan.
 */
public class LogicalTopN<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements TopN {

    private final List<OrderKey> orderKeys;
    private final long limit;
    private final long offset;
    private final Supplier<List<Expression>> expressions;

    public LogicalTopN(List<OrderKey> orderKeys, long limit, long offset, CHILD_TYPE child) {
        this(orderKeys, limit, offset, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Constructor for LogicalSort.
     */
    public LogicalTopN(List<OrderKey> orderKeys, long limit, long offset, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_TOP_N, groupExpression, logicalProperties, child);
        this.orderKeys = ImmutableList.copyOf(Objects.requireNonNull(orderKeys, "orderKeys can not be null"));
        this.limit = limit;
        this.offset = offset;
        this.expressions = Suppliers.memoize(() -> {
            ImmutableList.Builder<Expression> exprs = ImmutableList.builderWithExpectedSize(orderKeys.size());
            for (OrderKey orderKey : orderKeys) {
                exprs.add(orderKey.getExpr());
            }
            return exprs.build();
        });
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalTopN",
                "limit", limit,
                "offset", offset,
                "orderKeys", orderKeys
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
        LogicalTopN<?> that = (LogicalTopN<?>) o;
        return this.offset == that.offset && this.limit == that.limit && Objects.equals(this.orderKeys, that.orderKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderKeys, limit, offset);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTopN(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return expressions.get();
    }

    public LogicalTopN<Plan> withOrderKeys(List<OrderKey> orderKeys) {
        return new LogicalTopN<>(orderKeys, limit, offset,
                Optional.empty(), Optional.of(getLogicalProperties()), child());
    }

    public LogicalTopN<Plan> withLimitChild(long limit, long offset, Plan child) {
        return new LogicalTopN<>(orderKeys, limit, offset, child);
    }

    public LogicalTopN<Plan> withLimitOrderKeyAndChild(long limit, long offset, List<OrderKey> orderKeys, Plan child) {
        return new LogicalTopN<>(orderKeys, limit, offset, child);
    }

    @Override
    public LogicalTopN<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalTopN should have 1 child, but input is %s", children.size());
        return new LogicalTopN<>(orderKeys, limit, offset, children.get(0));
    }

    @Override
    public LogicalTopN<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTopN<>(orderKeys, limit, offset, groupExpression, Optional.of(getLogicalProperties()),
                child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalTopN<>(orderKeys, limit, offset, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public void computeUnique(FunctionalDependencies.Builder fdBuilder) {
        if (getLimit() == 1) {
            getOutput().forEach(fdBuilder::addUniqueSlot);
        } else {
            fdBuilder.addUniqueSlot(child(0).getLogicalProperties().getFunctionalDependencies());
        }
    }

    @Override
    public void computeUniform(FunctionalDependencies.Builder fdBuilder) {
        if (getLimit() == 1) {
            getOutput().forEach(fdBuilder::addUniformSlot);
        } else {
            fdBuilder.addUniformSlot(child(0).getLogicalProperties().getFunctionalDependencies());
        }
    }

    @Override
    public void computeEqualSet(FunctionalDependencies.Builder fdBuilder) {
        fdBuilder.addEqualSet(child(0).getLogicalProperties().getFunctionalDependencies());
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        ImmutableSet<FdItem> fdItems = child(0).getLogicalProperties().getFunctionalDependencies().getFdItems();
        if (getLimit() == 1) {
            ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();
            List<Slot> output = getOutput();
            ImmutableSet<SlotReference> slotSet = output.stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
            ExprFdItem fdItem = FdFactory.INSTANCE.createExprFdItem(slotSet, true, slotSet);
            builder.add(fdItem);
            fdItems = builder.build();
        }
        return fdItems;
    }
}
