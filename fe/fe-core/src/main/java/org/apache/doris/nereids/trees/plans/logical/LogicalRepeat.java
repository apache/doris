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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LogicalRepeat.
 */
public class LogicalRepeat<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements Repeat<CHILD_TYPE> {

    // max num of distinct sets in grouping sets clause
    public static final int MAX_GROUPING_SETS_NUM = 64;

    private final List<List<Expression>> groupingSets;
    private final List<NamedExpression> outputExpressions;
    private final Optional<SlotReference> groupingId;
    private final boolean withInProjection;

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    public LogicalRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupingSets, outputExpressions, Optional.empty(), child);
    }

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    public LogicalRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            SlotReference groupingId,
            CHILD_TYPE child) {
        this(groupingSets, outputExpressions, Optional.empty(), Optional.empty(),
                Optional.ofNullable(groupingId), true, child);
    }

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    private LogicalRepeat(
            List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            Optional<SlotReference> groupingId,
            CHILD_TYPE child) {
        this(groupingSets, outputExpressions, Optional.empty(), Optional.empty(), groupingId, true, child);
    }

    /**
     * Desc: Constructor for LogicalRepeat.
     */
    private LogicalRepeat(List<List<Expression>> groupingSets, List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<SlotReference> groupingId, boolean withInProjection, CHILD_TYPE child) {
        super(PlanType.LOGICAL_REPEAT, groupExpression, logicalProperties, child);
        this.groupingSets = Objects.requireNonNull(groupingSets, "groupingSets can not be null")
                .stream()
                .map(ImmutableList::copyOf)
                .collect(ImmutableList.toImmutableList());
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions can not be null"));
        this.groupingId = groupingId;
        this.withInProjection = withInProjection;
    }

    @Override
    public List<List<Expression>> getGroupingSets() {
        return groupingSets;
    }

    @Override
    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public Optional<SlotReference> getGroupingId() {
        return groupingId;
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputExpressions;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalRepeat",
                "groupingSets", groupingSets,
                "outputExpressions", outputExpressions,
                "groupingId", groupingId
        );
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        // org.apache.doris.nereids.parser.LogicalPlanBuilder.withProjection will generate different plan for
        // distinct aggregation so use withInProjection flag to control whether to generate a select statement
        // eg: select distinct log_time from example_tbl_duplicate group by log_time,log_type with rollup;
        // select log_time from example_tbl_duplicate group by log_time,log_type with rollup;
        if (!withInProjection) {
            sb.append("SELECT ");
            sb.append(
                    outputExpressions.stream().map(Expression::toDigest)
                            .collect(Collectors.joining(", "))
            );
            sb.append(" FROM ");
        }
        sb.append(child().toDigest());
        sb.append(" GROUP BY GROUPING SETS (");
        for (int i = 0; i < groupingSets.size(); i++) {
            List<Expression> groupingSet = groupingSets.get(i);
            String subSet = groupingSet.stream().map(Expression::toDigest)
                    .collect(Collectors.joining(",", "(", ")"));
            sb.append(subSet);
            if (i != groupingSets.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public List<Slot> computeOutput() {
        return Stream.concat(outputExpressions.stream(), groupingId.map(Stream::of).orElse(Stream.empty()))
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRepeat(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        builder.addAll(ExpressionUtils.flatExpressions(groupingSets)).addAll(outputExpressions);
        groupingId.ifPresent(builder::add);
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRepeat<?> that = (LogicalRepeat<?>) o;
        return Objects.equals(groupingSets, that.groupingSets) && Objects.equals(outputExpressions,
                that.outputExpressions) && Objects.equals(groupingId, that.groupingId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupingSets, outputExpressions, groupingId);
    }

    @Override
    public LogicalRepeat<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupingId, children.get(0));
    }

    @Override
    public LogicalRepeat<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupExpression,
                Optional.of(getLogicalProperties()), groupingId, withInProjection, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupExpression, logicalProperties,
                groupingId, withInProjection, children.get(0));
    }

    public LogicalRepeat<CHILD_TYPE> withGroupSets(List<List<Expression>> groupingSets) {
        return new LogicalRepeat<>(groupingSets, outputExpressions, groupingId, child());
    }

    public LogicalRepeat<CHILD_TYPE> withGroupSetsAndOutput(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressionList) {
        return new LogicalRepeat<>(groupingSets, outputExpressionList, groupingId, child());
    }

    @Override
    public LogicalRepeat<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new LogicalRepeat<>(groupingSets, newOutput, groupingId, child());
    }

    public LogicalRepeat<Plan> withNormalizedExpr(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressionList, SlotReference groupingId, Plan child) {
        return new LogicalRepeat<>(groupingSets, outputExpressionList, groupingId, child);
    }

    public LogicalRepeat<Plan> withAggOutputAndChild(List<NamedExpression> newOutput, Plan child) {
        return new LogicalRepeat<>(groupingSets, newOutput, groupingId, child);
    }

    public LogicalRepeat<CHILD_TYPE> withInProjection(boolean withInProjection) {
        return new LogicalRepeat<>(groupingSets, outputExpressions,
                Optional.empty(), Optional.empty(), groupingId, withInProjection, child());
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        // don't generate unique slot
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        // don't generate uniform slot
        // TODO: this need be supported later
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        Set<Expression> common = getCommonGroupingSetExpressions();
        Set<Slot> slots = new HashSet<>();
        for (Expression expr : common) {
            if (!(expr instanceof Slot)) {
                return;
            }
            slots.add((Slot) expr);
        }
        builder.addEqualSet(child().getLogicalProperties().getTrait());
        builder.pruneEqualSetSlots(slots);
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate fd
    }
}
