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
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
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
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    /**
     * SubQueryType
     */
    public enum SubQueryType {
        IN_SUBQUERY,
        EXITS_SUBQUERY,
        SCALAR_SUBQUERY
    }

    private final SubQueryType subqueryType;
    private final boolean isNot;

    // only for InSubquery
    private final Optional<Expression> compareExpr;

    // only for InSubquery
    private final Optional<Expression> typeCoercionExpr;

    // correlation column
    private final List<Slot> correlationSlot;
    // correlation Conjunction
    private final Optional<Expression> correlationFilter;
    // The slot replaced by the subquery in MarkJoin
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    // Whether adding the subquery's output to projects
    private final boolean needAddSubOutputToProjects;

    /*
    * This flag is indicate the mark join slot can be non-null or not
    * in InApplyToJoin rule, if it's semi join with non-null mark slot
    * we can safely change the mark conjunct to hash conjunct
    * see SubqueryToApply rule for more info
    */
    private final boolean isMarkJoinSlotNotNull;

    private LogicalApply(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Slot> correlationSlot, SubQueryType subqueryType, boolean isNot,
            Optional<Expression> compareExpr, Optional<Expression> typeCoercionExpr,
            Optional<Expression> correlationFilter,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects,
            boolean isMarkJoinSlotNotNull,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_APPLY, groupExpression, logicalProperties, leftChild, rightChild);
        if (subqueryType == SubQueryType.IN_SUBQUERY) {
            Preconditions.checkArgument(compareExpr.isPresent(), "InSubquery must have compareExpr");
        }
        this.correlationSlot = correlationSlot == null ? ImmutableList.of() : ImmutableList.copyOf(correlationSlot);
        this.subqueryType = subqueryType;
        this.isNot = isNot;
        this.compareExpr = compareExpr;
        this.typeCoercionExpr = typeCoercionExpr;
        this.correlationFilter = correlationFilter;
        this.markJoinSlotReference = markJoinSlotReference;
        this.needAddSubOutputToProjects = needAddSubOutputToProjects;
        this.isMarkJoinSlotNotNull = isMarkJoinSlotNotNull;
    }

    public LogicalApply(List<Slot> correlationSlot, SubQueryType subqueryType, boolean isNot,
            Optional<Expression> compareExpr, Optional<Expression> typeCoercionExpr,
            Optional<Expression> correlationFilter, Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects, boolean isMarkJoinSlotNotNull,
            LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery) {
        this(Optional.empty(), Optional.empty(), correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr,
                correlationFilter, markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull,
                input, subquery);
    }

    public Optional<Expression> getCompareExpr() {
        return compareExpr;
    }

    public Optional<Expression> getTypeCoercionExpr() {
        return typeCoercionExpr;
    }

    public Expression getSubqueryOutput() {
        return typeCoercionExpr.orElseGet(() -> right().getOutput().get(0));
    }

    public List<Slot> getCorrelationSlot() {
        return correlationSlot;
    }

    public Optional<Expression> getCorrelationFilter() {
        return correlationFilter;
    }

    public boolean isScalar() {
        return subqueryType == SubQueryType.SCALAR_SUBQUERY;
    }

    public boolean isIn() {
        return subqueryType == SubQueryType.IN_SUBQUERY;
    }

    public boolean isExist() {
        return subqueryType == SubQueryType.EXITS_SUBQUERY;
    }

    public SubQueryType getSubqueryType() {
        return subqueryType;
    }

    public boolean isNot() {
        return isNot;
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

    public boolean isMarkJoinSlotNotNull() {
        return isMarkJoinSlotNotNull;
    }

    @Override
    public List<Slot> computeOutput() {
        ImmutableList.Builder<Slot> builder = ImmutableList.builder();
        builder.addAll(left().getOutput());
        if (markJoinSlotReference.isPresent()) {
            builder.add(markJoinSlotReference.get());
        }
        // only scalar apply can be needAddSubOutputToProjects = true
        if (needAddSubOutputToProjects) {
            // correlated apply right child may contain multiple output slots
            // in rule ScalarApplyToJoin, only '(isCorrelated() && correlationFilter.isPresent())'
            // but at analyzed phase, the correlationFilter is empty, only after rule UnCorrelatedApplyAggregateFilter
            // correlationFilter will be set, so we skip check correlationFilter here.
            // correlated apply will change to a left outer join, then all the right child output will be nullable.
            if (isCorrelated()) {
                // for sql:
                // `select t1.a,
                //         (select if(sum(t2.a) > 10, count(t2.b), max(t2.c)) as k from t2 where t1.a = t2.a)
                // from t1`,
                // its plan is:
                // LogicalProject(t1.a, if(sum(t2.a) > 10, count(t2.b), max(t2.c)) as k)
                //     |-- LogicalProject(..., if(sum(t2.a > 10), ifnull(count(t2.b), 0), max(t2.c)) as k)
                //           |-- LogicalApply(correlationSlot = [t1.a])
                //                  |-- LogicalOlapScan(t1)
                //                  |-- LogicalAggregate(output = [sum(t2.a), count(t2.b), max(t2.c)])
                for (Slot slot : right().getOutput()) {
                    // in fact some slots may non-nullable, like count.
                    // but after convert correlated apply to left outer join, all the join right child's slots
                    // will become nullable, so we let all slots be nullable, then they wouldn't change nullable
                    // even after convert to join.
                    builder.add(slot.toSlot().withNullable(true));
                }
            } else {
                // uncorrelated apply right child always contains one output slot.
                // for sql:
                // `select t1.a,
                //         (select if(sum(t2.a) > 10, count(t2.b), max(t2.c)) as k from t2)
                // from t1`,
                // its plan is:
                // LogicalProject(t1.a, k)
                //      |--LogicalApply(correlationSlot = [])
                //          |- LogicalOlapScan(t1)
                //          |- LogicalProject(if(sum(t2.a) > 10, count(t2.b), max(t2.c)) as k)
                //                 |-- LogicalAggregate(output = [sum(t2.a), count(t2.b), max(t2.c)])
                builder.add(ScalarSubquery.getScalarQueryOutputAdjustNullable(right(), correlationSlot));
            }
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalApply",
                "correlationSlot", correlationSlot,
                "correlationFilter", correlationFilter,
                "isMarkJoin", markJoinSlotReference.isPresent(),
                "isMarkJoinSlotNotNull", isMarkJoinSlotNotNull,
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
        LogicalApply<?, ?> that = (LogicalApply<?, ?>) o;
        return Objects.equals(correlationSlot, that.getCorrelationSlot())
                && Objects.equals(subqueryType, that.subqueryType)
                && Objects.equals(compareExpr, that.compareExpr)
                && Objects.equals(typeCoercionExpr, that.typeCoercionExpr)
                && Objects.equals(correlationFilter, that.getCorrelationFilter())
                && Objects.equals(markJoinSlotReference, that.getMarkJoinSlotReference())
                && needAddSubOutputToProjects == that.needAddSubOutputToProjects
                && isMarkJoinSlotNotNull == that.isMarkJoinSlotNotNull
                && isNot == that.isNot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                correlationSlot, subqueryType, compareExpr, typeCoercionExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull, isNot);
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
        } else {
            return new ImmutableList.Builder<Expression>()
                    .addAll(correlationSlot)
                    .build();
        }
    }

    @Override
    public LogicalApply<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr,
                correlationFilter, markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull,
                children.get(0), children.get(1));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalApply<>(groupExpression, Optional.of(getLogicalProperties()),
                correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull, left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(groupExpression, logicalProperties, correlationSlot, subqueryType, isNot,
                compareExpr, typeCoercionExpr, correlationFilter, markJoinSlotReference,
                needAddSubOutputToProjects, isMarkJoinSlotNotNull, children.get(0), children.get(1));
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        builder.addUniqueSlot(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(left().getLogicalProperties().getTrait());
    }
}
