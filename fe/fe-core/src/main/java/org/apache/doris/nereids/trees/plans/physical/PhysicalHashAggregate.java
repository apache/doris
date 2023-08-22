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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.properties.RequirePropertiesSupplier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical hash aggregation plan.
 */
public class PhysicalHashAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE>, RequirePropertiesSupplier<PhysicalHashAggregate<CHILD_TYPE>> {

    private final List<Expression> groupByExpressions;

    private final List<NamedExpression> outputExpressions;

    private final Optional<List<Expression>> partitionExpressions;

    private final AggregateParam aggregateParam;

    private final boolean maybeUsingStream;

    private final RequireProperties requireProperties;

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            AggregateParam aggregateParam, boolean maybeUsingStream, LogicalProperties logicalProperties,
            RequireProperties requireProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), aggregateParam,
                maybeUsingStream, Optional.empty(), logicalProperties, requireProperties, child);
    }

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam,
            boolean maybeUsingStream, LogicalProperties logicalProperties, RequireProperties requireProperties,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, aggregateParam,
                maybeUsingStream, Optional.empty(), logicalProperties, requireProperties, child);
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param maybeUsingStream whether it's stream agg.
     * @param requireProperties the request physical properties
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam, boolean maybeUsingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            RequireProperties requireProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_HASH_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregate param cannot be null");
        this.maybeUsingStream = maybeUsingStream;
        this.requireProperties = Objects.requireNonNull(requireProperties, "requireProperties cannot be null");
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param maybeUsingStream whether it's stream agg.
     * @param requireProperties the request physical properties
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam, boolean maybeUsingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            RequireProperties requireProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_HASH_AGGREGATE, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregate param cannot be null");
        this.maybeUsingStream = maybeUsingStream;
        this.requireProperties = Objects.requireNonNull(requireProperties, "requireProperties cannot be null");
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return outputExpressions;
    }

    public Optional<List<Expression>> getPartitionExpressions() {
        return partitionExpressions;
    }

    public AggregateParam getAggregateParam() {
        return aggregateParam;
    }

    public AggPhase getAggPhase() {
        return aggregateParam.aggPhase;
    }

    public AggMode getAggMode() {
        return aggregateParam.aggMode;
    }

    public boolean isMaybeUsingStream() {
        return maybeUsingStream;
    }

    @Override
    public RequireProperties getRequireProperties() {
        return requireProperties;
    }

    @Override
    public PhysicalHashAggregate<Plan> withRequireAndChildren(
            RequireProperties requireProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return withRequirePropertiesAndChild(requireProperties, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .addAll(partitionExpressions.orElse(ImmutableList.of()))
                .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalHashAggregate[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "aggPhase", aggregateParam.aggPhase,
                "aggMode", aggregateParam.aggMode,
                "maybeUseStreaming", maybeUsingStream,
                "groupByExpr", groupByExpressions,
                "outputExpr", outputExpressions,
                "partitionExpr", partitionExpressions,
                "requireProperties", requireProperties,
                "stats", statistics
        );
    }

    /**
     * Determine the equality with another operator
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalHashAggregate that = (PhysicalHashAggregate) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && Objects.equals(aggregateParam, that.aggregateParam)
                && maybeUsingStream == that.maybeUsingStream
                && Objects.equals(requireProperties, that.requireProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, requireProperties);
    }

    @Override
    public PhysicalHashAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(),
                requireProperties, physicalProperties, statistics,
                children.get(0));
    }

    public PhysicalHashAggregate<CHILD_TYPE> withPartitionExpressions(List<Expression> partitionExpressions) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions,
                Optional.ofNullable(partitionExpressions), aggregateParam, maybeUsingStream,
                Optional.empty(), getLogicalProperties(), requireProperties, child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(), requireProperties, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, logicalProperties.get(),
                requireProperties, children.get(0));
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(),
                requireProperties, physicalProperties, statistics,
                child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalHashAggregate<>(groupByExpressions, newOutput, partitionExpressions,
                aggregateParam, maybeUsingStream, Optional.empty(), getLogicalProperties(),
                requireProperties, physicalProperties, statistics, child());
    }

    public <C extends Plan> PhysicalHashAggregate<C> withRequirePropertiesAndChild(
            RequireProperties requireProperties, C newChild) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, Optional.empty(), getLogicalProperties(),
                requireProperties, physicalProperties, statistics, newChild);
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder("hashAgg[");
        builder.append(getAggPhase()).append("]");
        return builder.toString();
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
            AbstractPhysicalJoin builderNode, Expression src, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditions(builderNode, ctx, probeSlot)) {
            return false;
        }
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        if (!RuntimeFilterGenerator.isCoveredByPlanNode(this, scan)) {
            return false;
        }

        AbstractPhysicalPlan child = (AbstractPhysicalPlan) child(0);
        boolean pushedDown = child.pushDownRuntimeFilter(context, generator, builderNode,
                src, probeExpr, type, buildSideNdv, exprOrder);
        return pushedDown;
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, null,
                requireProperties, physicalProperties, statistics,
                child());
    }
}
