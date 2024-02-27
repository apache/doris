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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical limit plan
 */
public class PhysicalLimit<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Limit {
    private final LimitPhase phase;
    private final long limit;
    private final long offset;

    public PhysicalLimit(long limit, long offset,
            LimitPhase phase, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        this(limit, offset, phase, Optional.empty(), logicalProperties, child);
    }

    /**
     * constructor
     * select * from t order by a limit [offset], [limit];
     *
     * @param limit the number of tuples retrieved.
     * @param offset the number of tuples skipped.
     */
    public PhysicalLimit(long limit, long offset,
            LimitPhase phase, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LIMIT, groupExpression, logicalProperties, child);
        this.limit = limit;
        this.offset = offset;
        this.phase = phase;
    }

    /**
     * constructor
     * select * from t order by a limit [offset], [limit];
     *
     * @param limit the number of tuples retrieved.
     * @param offset the number of tuples skipped.
     * @param phase the phase of 2-phase limit.
     */
    public PhysicalLimit(long limit, long offset, LimitPhase phase, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_LIMIT, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.limit = limit;
        this.offset = offset;
        this.phase = phase;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public LimitPhase getPhase() {
        return phase;
    }

    public boolean isGlobal() {
        return phase == LimitPhase.GLOBAL;
    }

    public Plan withLimit(long limit) {
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public PhysicalLimit<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalLimit<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalLimit that = (PhysicalLimit) o;
        return offset == that.offset && limit == that.limit && phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, limit);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLimit(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalLimit[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "limit", limit,
                "offset", offset,
                "phase", phase,
                "stats", statistics
        );
    }

    @Override
    public String shapeInfo() {
        return this.getClass().getSimpleName() + "[" + phase + "]";
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalLimit<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalLimit<>(limit, offset, phase, groupExpression, null, physicalProperties,
                statistics, child());
    }
}
