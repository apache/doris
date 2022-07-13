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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Physical sort plan.
 */
public class PhysicalHeapSort<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {
    private final long limit;
    // Default offset is 0.
    private final int offset;

    private final List<OrderKey> orderKeys;


    public PhysicalHeapSort(List<OrderKey> orderKeys, long limit, int offset,
                            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, limit, offset, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalHeapSort(List<OrderKey> orderKeys, long limit, int offset,
                            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_SORT, groupExpression, logicalProperties, child);
        this.offset = offset;
        this.limit = limit;
        this.orderKeys = orderKeys;
    }

    public int getOffset() {
        return offset;
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public boolean hasLimit() {
        return limit > -1;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHeapSort((PhysicalHeapSort<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return orderKeys.stream()
            .map(OrderKey::getExpr)
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHeapSort<>(orderKeys, limit, offset, logicalProperties, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHeapSort<>(orderKeys, limit, offset, groupExpression, logicalProperties, child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalHeapSort<>(orderKeys, limit, offset, Optional.empty(), logicalProperties.get(), child());
    }
}
