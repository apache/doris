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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical top-N plan.
 */
public class PhysicalTopN<CHILD_TYPE extends Plan> extends AbstractPhysicalSort<CHILD_TYPE> implements TopN {

    private final long limit;
    private final long offset;

    public PhysicalTopN(List<OrderKey> orderKeys, long limit, long offset,
            SortPhase phase, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, limit, offset, phase, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalTopN(List<OrderKey> orderKeys, long limit, long offset,
            SortPhase phase,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(orderKeys, limit, offset, phase, groupExpression,
                logicalProperties, null, null, child);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalTopN(List<OrderKey> orderKeys, long limit, long offset,
            SortPhase phase, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_TOP_N, orderKeys, phase, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
        Objects.requireNonNull(orderKeys, "orderKeys should not be null in PhysicalTopN.");
        this.limit = limit;
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
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
        return visitor.visitPhysicalTopN(this, context);
    }

    @Override
    public PhysicalTopN<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalTopN's children size must be 1, but real is %s", children.size());
        return new PhysicalTopN<>(orderKeys, limit, offset, phase, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalTopN<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalTopN<>(orderKeys, limit, offset, phase,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public PhysicalTopN<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalTopN's children size must be 1, but real is %s", children.size());
        return new PhysicalTopN<>(orderKeys, limit, offset, phase,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalTopN<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalTopN<>(orderKeys, limit, offset, phase,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public String shapeInfo() {
        return this.getClass().getSimpleName() + "[" + phase + "]";
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalTopN[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "limit", limit,
                "offset", offset,
                "orderKeys", orderKeys,
                "phase", phase.toString()
        );
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalTopN<Plan> resetLogicalProperties() {
        return new PhysicalTopN<>(orderKeys, limit, offset, phase, groupExpression,
                null, physicalProperties, statistics, child());
    }

}
