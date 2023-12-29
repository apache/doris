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
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Use this node to display the subquery in the relational algebra tree.
 * @param <LEFT_CHILD_TYPE> input.
 * @param <RIGHT_CHILD_TYPE> subquery.
 */
public class LogicalApply<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements PropagateFuncDeps {

    // correlation column
    private final List<Expression> correlationSlot;
    // original subquery
    private final SubqueryExpr subqueryExpr;
    // correlation Conjunction
    private final Optional<Expression> correlationFilter;
    // The slot replaced by the subquery in MarkJoin
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    // Whether the subquery is in logicalProject
    private final boolean inProject;

    // Whether adding the subquery's output to projects
    private final boolean needAddSubOutputToProjects;

    private LogicalApply(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Expression> correlationSlot,
            SubqueryExpr subqueryExpr, Optional<Expression> correlationFilter,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects,
            boolean inProject,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_APPLY, groupExpression, logicalProperties, leftChild, rightChild);
        this.correlationSlot = correlationSlot == null ? ImmutableList.of() : ImmutableList.copyOf(correlationSlot);
        this.subqueryExpr = Objects.requireNonNull(subqueryExpr, "subquery can not be null");
        this.correlationFilter = correlationFilter;
        this.markJoinSlotReference = markJoinSlotReference;
        this.needAddSubOutputToProjects = needAddSubOutputToProjects;
        this.inProject = inProject;
    }

    public LogicalApply(List<Expression> correlationSlot, SubqueryExpr subqueryExpr,
            Optional<Expression> correlationFilter, Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects, boolean inProject,
            LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery) {
        this(Optional.empty(), Optional.empty(), correlationSlot, subqueryExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, inProject, input,
                subquery);
    }

    public List<Expression> getCorrelationSlot() {
        return correlationSlot;
    }

    public Optional<Expression> getCorrelationFilter() {
        return correlationFilter;
    }

    public SubqueryExpr getSubqueryExpr() {
        return subqueryExpr;
    }

    public boolean isScalar() {
        return this.subqueryExpr instanceof ScalarSubquery;
    }

    public boolean isIn() {
        return this.subqueryExpr instanceof InSubquery;
    }

    public boolean isExist() {
        return this.subqueryExpr instanceof Exists;
    }

    public boolean isCorrelated() {
        return !correlationSlot.isEmpty();
    }

    public boolean alreadyExecutedEliminateFilter() {
        return correlationFilter.isPresent();
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    public boolean isNeedAddSubOutputToProjects() {
        return needAddSubOutputToProjects;
    }

    public boolean isInProject() {
        return inProject;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(left().getOutput())
                .addAll(markJoinSlotReference.isPresent()
                    ? ImmutableList.of(markJoinSlotReference.get()) : ImmutableList.of())
                .addAll(needAddSubOutputToProjects
                    ? ImmutableList.of(right().getOutput().get(0)) : ImmutableList.of())
                .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalApply",
                "correlationSlot", correlationSlot,
                "correlationFilter", correlationFilter,
                "isMarkJoin", markJoinSlotReference.isPresent(),
                "MarkJoinSlotReference", markJoinSlotReference.isPresent() ? markJoinSlotReference.get() : "empty");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalApply that = (LogicalApply) o;
        return Objects.equals(correlationSlot, that.getCorrelationSlot())
                && Objects.equals(subqueryExpr, that.getSubqueryExpr())
                && Objects.equals(correlationFilter, that.getCorrelationFilter())
                && Objects.equals(markJoinSlotReference, that.getMarkJoinSlotReference())
                && needAddSubOutputToProjects == that.needAddSubOutputToProjects
                && inProject == that.inProject;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                correlationSlot, subqueryExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, inProject);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalApply(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        if (correlationFilter.isPresent()) {
            return new ImmutableList.Builder<Expression>()
                    .addAll(correlationSlot)
                    .add(correlationFilter.get())
                    .build();
        }
        return new ImmutableList.Builder<Expression>()
                .addAll(correlationSlot)
                .build();
    }

    public LogicalApply<Plan, Plan> withSubqueryExprAndChildren(SubqueryExpr subqueryExpr, List<Plan> children) {
        return new LogicalApply<>(correlationSlot, subqueryExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, inProject, children.get(0), children.get(1));
    }

    @Override
    public LogicalApply<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(correlationSlot, subqueryExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, inProject,
                children.get(0), children.get(1));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalApply<>(groupExpression, Optional.of(getLogicalProperties()),
                correlationSlot, subqueryExpr, correlationFilter, markJoinSlotReference,
                needAddSubOutputToProjects, inProject, left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(groupExpression, logicalProperties, correlationSlot, subqueryExpr,
                correlationFilter, markJoinSlotReference,
                needAddSubOutputToProjects, inProject, children.get(0), children.get(1));
    }
}
