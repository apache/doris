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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical bucketed hash aggregation plan node.
 *
 * Fuses two-phase aggregation (local + global) into a single operator for single-BE deployments.
 * The sink side builds per-instance hash tables from raw input (first-phase agg).
 * The source side merges across instances per-bucket using direct in-memory merge
 * (no serialization/deserialization) and outputs the final result.
 *
 * This node replaces the pattern: GlobalAgg -> PhysicalDistribute -> LocalAgg
 * with a single fused operator, eliminating exchange overhead entirely.
 */
public class PhysicalBucketedHashAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE> {

    private final List<Expression> groupByExpressions;
    private final List<NamedExpression> outputExpressions;

    public PhysicalBucketedHashAggregate(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), logicalProperties, child);
    }

    public PhysicalBucketedHashAggregate(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_BUCKETED_HASH_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
    }

    /**
     * Constructor with group expression, logical properties, physical properties, and statistics.
     */
    public PhysicalBucketedHashAggregate(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_BUCKETED_HASH_AGGREGATE, groupExpression, logicalProperties,
                physicalProperties, statistics, child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
    }

    @Override
    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
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
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalBucketedHashAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalBucketedHashAggregate[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "groupByExpr", groupByExpressions,
                "outputExpr", outputExpressions
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
        PhysicalBucketedHashAggregate<?> that = (PhysicalBucketedHashAggregate<?>) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions);
    }

    @Override
    public PhysicalBucketedHashAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, outputExpressions,
                groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalBucketedHashAggregate<CHILD_TYPE> withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, outputExpressions,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, outputExpressions,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalBucketedHashAggregate<CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, outputExpressions,
                groupExpression, getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    @Override
    public PhysicalBucketedHashAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, newOutput,
                Optional.empty(), getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    @Override
    public String shapeInfo() {
        return "bucketedHashAgg";
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalBucketedHashAggregate<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalBucketedHashAggregate<>(groupByExpressions, outputExpressions,
                groupExpression, null,
                physicalProperties, statistics, child());
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        DataTrait childFd = child(0).getLogicalProperties().getTrait();

        if (groupByExpressions.stream().anyMatch(Expression::containsUniqueFunction)) {
            return;
        }

        ImmutableSet.Builder<Slot> groupByKeysBuilder = ImmutableSet.builder();
        for (Expression expr : groupByExpressions) {
            groupByKeysBuilder.addAll(expr.getInputSlots());
        }
        ImmutableSet<Slot> groupByKeys = groupByKeysBuilder.build();

        if (groupByExpressions.isEmpty() || childFd.isUniformAndNotNull(groupByKeys)) {
            getOutput().forEach(builder::addUniqueSlot);
            return;
        }

        builder.addUniqueSlot(childFd);
        builder.addUniqueSlot(groupByKeys);

        if (childFd.isUniqueAndNotNull(groupByKeys)) {
            for (NamedExpression namedExpression : getOutputExpressions()) {
                if (isUniqueGroupByUnique(namedExpression)) {
                    builder.addUniqueSlot(namedExpression.toSlot());
                }
            }
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        DataTrait childFd = child(0).getLogicalProperties().getTrait();
        builder.addUniformSlot(childFd);

        if (groupByExpressions.stream().anyMatch(Expression::containsUniqueFunction)) {
            return;
        }

        ImmutableSet.Builder<Slot> groupByKeysBuilder = ImmutableSet.builder();
        for (Expression expr : groupByExpressions) {
            groupByKeysBuilder.addAll(expr.getInputSlots());
        }
        ImmutableSet<Slot> groupByKeys = groupByKeysBuilder.build();

        if (groupByExpressions.isEmpty() || childFd.isUniformAndNotNull(groupByKeys)) {
            getOutput().forEach(builder::addUniformSlot);
            return;
        }

        if (childFd.isUniqueAndNotNull(groupByKeys)) {
            for (NamedExpression namedExpression : getOutputExpressions()) {
                if (isUniformGroupByUnique(namedExpression)) {
                    builder.addUniformSlot(namedExpression.toSlot());
                }
            }
        }
    }

    private boolean isUniqueGroupByUnique(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1) {
            return false;
        }
        Expression agg = namedExpression.child(0);
        return ExpressionUtils.isInjectiveAgg(agg)
                && child().getLogicalProperties().getTrait().isUniqueAndNotNull(agg.getInputSlots());
    }

    private boolean isUniformGroupByUnique(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1) {
            return false;
        }
        Expression agg = namedExpression.child(0);
        return agg instanceof Count || agg instanceof Ndv;
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
