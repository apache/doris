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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalRepeat.
 */
public class LogicalRepeat<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements Repeat<CHILD_TYPE> {

    // max num of distinct sets in grouping sets clause
    public static final int MAX_GROUPING_SETS_NUM = 64;

    private final List<List<Expression>> groupingSets;
    private final List<NamedExpression> outputExpressions;

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    public LogicalRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupingSets, outputExpressions, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    public LogicalRepeat(List<List<Expression>> groupingSets, List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_REPEAT, groupExpression, logicalProperties, child);
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
        return Utils.toSqlString("LogicalRepeat",
                "groupingSets", groupingSets,
                "outputExpressions", outputExpressions
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
        return visitor.visitLogicalRepeat(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(ExpressionUtils.flatExpressions(groupingSets))
                .addAll(outputExpressions)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRepeat<?> that = (LogicalRepeat<?>) o;
        return groupingSets.equals(that.groupingSets) && outputExpressions.equals(that.outputExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupingSets, outputExpressions);
    }

    @Override
    public LogicalRepeat<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalRepeat<>(groupingSets, outputExpressions, children.get(0));
    }

    @Override
    public LogicalRepeat<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupExpression, logicalProperties,
                children.get(0));
    }

    public LogicalRepeat<CHILD_TYPE> withGroupSets(List<List<Expression>> groupingSets) {
        return new LogicalRepeat<>(groupingSets, outputExpressions, child());
    }

    public LogicalRepeat<CHILD_TYPE> withGroupSetsAndOutput(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressionList) {
        return new LogicalRepeat<>(groupingSets, outputExpressionList, child());
    }

    @Override
    public LogicalRepeat<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new LogicalRepeat<>(groupingSets, newOutput, child());
    }

    public LogicalRepeat<Plan> withNormalizedExpr(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressionList, Plan child) {
        return new LogicalRepeat<>(groupingSets, outputExpressionList, child);
    }

    public LogicalRepeat<Plan> withAggOutputAndChild(List<NamedExpression> newOutput, Plan child) {
        return new LogicalRepeat<>(groupingSets, newOutput, child);
    }

    public boolean canBindVirtualSlot() {
        return bound() && outputExpressions.stream()
                .noneMatch(output -> output.containsType(VirtualSlotReference.class));
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        // don't generate unique slot
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<FdItem> childItems = child().getLogicalProperties().getTrait().getFdItems();
        builder.addAll(childItems);

        return builder.build();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
