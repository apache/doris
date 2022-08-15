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
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical top-N plan.
 */
public class PhysicalTopN<CHILD_TYPE extends Plan> extends AbstractPhysicalSort<CHILD_TYPE> implements TopN {

    private final int limit;
    private final int offset;

    public PhysicalTopN(List<OrderKey> orderKeys, int limit, int offset,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, limit, offset, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalTopN(List<OrderKey> orderKeys, int limit, int offset,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_TOP_N, orderKeys, groupExpression, logicalProperties, child);
        Objects.requireNonNull(orderKeys, "orderKeys should not be null in PhysicalTopN.");
        this.limit = limit;
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
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
        PhysicalTopN<?> that = (PhysicalTopN<?>) o;
        return limit == that.limit && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), limit, offset);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTopN((PhysicalTopN<Plan>) this, context);
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
        return new PhysicalTopN<>(orderKeys, limit, offset, logicalProperties, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalTopN<>(orderKeys, limit, offset, groupExpression, logicalProperties, child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalTopN<>(orderKeys, limit, offset, Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public String toString() {
        return "PhysicalTopN ("
                + "limit=" + limit
                + ", offset=" + offset
                + ", " + StringUtils.join(orderKeys, ", ") + ")";
    }
}
