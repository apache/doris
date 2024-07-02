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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalRepeat.
 */
public class PhysicalRepeat<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Repeat<CHILD_TYPE> {

    private final List<List<Expression>> groupingSets;
    private final List<NamedExpression> outputExpressions;

    /**
     * Desc: Constructor for PhysicalRepeat.
     */
    public PhysicalRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_REPEAT, logicalProperties, child);
        this.groupingSets = Objects.requireNonNull(groupingSets, "groupingSets can not be null")
                .stream()
                .map(ImmutableList::copyOf)
                .collect(ImmutableList.toImmutableList());
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions can not be null"));
    }

    /**
     * Desc: Constructor for PhysicalRepeat.
     */
    public PhysicalRepeat(List<List<Expression>> groupingSets, List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_REPEAT, groupExpression, logicalProperties,
                physicalProperties, statistics, child);
        this.groupingSets = Objects.requireNonNull(groupingSets, "groupingSets can not be null")
                .stream()
                .map(ImmutableList::copyOf)
                .collect(ImmutableList.toImmutableList());
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions can not be null"));
    }

    @Override
    public List<List<Expression>> getGroupingSets() {
        return groupingSets;
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputExpressions;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalRepeat[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "groupingSets", groupingSets,
                "outputExpressions", outputExpressions,
                "stats", statistics
        );
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRepeat(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(ExpressionUtils.flatExpressions(groupingSets))
                .addAll(outputExpressions)
                .build();
    }

    /**
     * Determine the equality with another plan
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRepeat that = (PhysicalRepeat) o;
        return Objects.equals(groupingSets, that.groupingSets)
                && Objects.equals(outputExpressions, that.outputExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupingSets, outputExpressions);
    }

    @Override
    public PhysicalRepeat<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalRepeat<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                logicalProperties.get(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalRepeat<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalRepeat<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalRepeat<>(groupingSets, newOutput, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalRepeat<CHILD_TYPE> withGroupSetsAndOutput(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressionList) {
        return new PhysicalRepeat<>(groupingSets, outputExpressionList, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public PhysicalRepeat<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                null, physicalProperties, statistics, child());
    }
}
