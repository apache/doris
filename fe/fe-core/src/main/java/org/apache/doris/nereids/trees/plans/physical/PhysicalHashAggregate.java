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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Physical hash aggregation plan.
 */
public class PhysicalHashAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE> {

    private final List<Expression> groupByExpressions;

    private final List<NamedExpression> outputExpressions;

    private final Optional<List<Expression>> partitionExpressions;

    private final AggregateParam aggregateParam;

    private final boolean maybeUsingStream;

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            AggregateParam aggregateParam, boolean maybeUsingStream, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), aggregateParam,
                maybeUsingStream, Optional.empty(), logicalProperties, child);
    }

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam,
            boolean maybeUsingStream, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, aggregateParam,
                maybeUsingStream, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param maybeUsingStream whether it's stream agg.
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam, boolean maybeUsingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_HASH_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = adjustNullableForOutputs(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"),
                groupByExpressions.isEmpty());
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregate param cannot be null");
        // this.aggregateParam = aggregateParam;
        this.maybeUsingStream = maybeUsingStream;
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param maybeUsingStream whether it's stream agg.
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggregateParam aggregateParam, boolean maybeUsingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_HASH_AGGREGATE, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = adjustNullableForOutputs(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"),
                groupByExpressions.isEmpty());
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggregateParam = Objects.requireNonNull(aggregateParam, "aggregate param cannot be null");
        this.maybeUsingStream = maybeUsingStream;
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
        TopnPushInfo topnPushInfo = (TopnPushInfo) getMutableState(
                MutableState.KEY_PUSH_TOPN_TO_AGG).orElse(null);
        return Utils.toSqlString("PhysicalHashAggregate[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics,
                "aggPhase", aggregateParam.aggPhase,
                "aggMode", aggregateParam.aggMode,
                "maybeUseStreaming", maybeUsingStream,
                "groupByExpr", groupByExpressions,
                "outputExpr", outputExpressions,
                "partitionExpr", partitionExpressions,
                "topnFilter", topnPushInfo != null,
                "topnPushDown", getMutableState(MutableState.KEY_PUSH_TOPN_TO_AGG).isPresent()
        );
    }

    @Override
    public String getFingerprint() {
        StringBuilder builder = new StringBuilder();
        String aggPhase = "Aggregate(" + this.aggregateParam.aggPhase.toString() + ")";
        List<Object> groupByExpressionsArgs = Lists.newArrayList(
                "groupByExpr", groupByExpressions);
        builder.append(Utils.toSqlString(aggPhase, groupByExpressionsArgs.toArray()));

        builder.append("outputExpr=");
        for (NamedExpression expr : outputExpressions) {
            if (expr instanceof Alias) {
                if (expr.child(0) instanceof AggregateExpression) {
                    builder.append(((AggregateExpression) expr.child(0)).getFunction().getName());
                } else if (expr.child(0) instanceof AggregateFunction) {
                    builder.append(((AggregateFunction) expr.child(0)).getName());
                } else {
                    builder.append(Utils.toStringOrNull(expr));
                }
            } else {
                builder.append(Utils.toStringOrNull(expr));
            }
        }

        return builder.toString();
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
                && maybeUsingStream == that.maybeUsingStream;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream);
    }

    @Override
    public PhysicalHashAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggregateParam, maybeUsingStream, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalHashAggregate<>(groupByExpressions, newOutput, partitionExpressions,
                aggregateParam, maybeUsingStream, Optional.empty(), getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder("hashAgg[");
        builder.append(getAggPhase());
        ConnectContext context = ConnectContext.get();
        if (context != null
                && context.getSessionVariable().getDetailShapePlanNodesSet().contains(getClass().getSimpleName())) {
            // sort the output to make it stable
            builder.append(", groupByExpr=");
            builder.append(groupByExpressions.stream().map(Expression::shapeInfo)
                    .collect(Collectors.joining(", ", "(", ")")));
            builder.append(", outputExpr=");
            builder.append(outputExpressions.stream().map(Expression::shapeInfo).sorted()
                    .collect(Collectors.joining(", ", "(", ")")));
        }
        builder.append(']');
        return builder.toString();
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
                physicalProperties, statistics, child());
    }

    /**
     * used to push limit down to localAgg
     */
    public static class TopnPushInfo {
        public List<OrderKey> orderkeys;
        public long limit;

        public TopnPushInfo(List<OrderKey> orderkeys, long limit) {
            this.orderkeys = ImmutableList.copyOf(orderkeys);
            this.limit = limit;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("[");
            builder.append("orderkeys=").append(orderkeys);
            builder.append(", limit=").append(limit);
            builder.append("]");
            return builder.toString();
        }
    }

    public TopnPushInfo getTopnPushInfo() {
        Optional<Object> obj = getMutableState(MutableState.KEY_PUSH_TOPN_TO_AGG);
        if (obj.isPresent() && obj.get() instanceof TopnPushInfo) {
            return (TopnPushInfo) obj.get();
        }
        return null;
    }

    public PhysicalHashAggregate<CHILD_TYPE> setTopnPushInfo(TopnPushInfo topnPushInfo) {
        setMutableState(MutableState.KEY_PUSH_TOPN_TO_AGG, topnPushInfo);
        return this;
    }

    /**
     * sql: select sum(distinct c1) from t;
     * assume c1 is not null, because there is no group by
     * sum(distinct c1)'s nullable is alwasNullable in rewritten phase.
     * But in implementation phase, we may create 3 phase agg with group by key c1.
     * And the sum(distinct c1)'s nullability should be changed depending on if there is any group by expressions.
     * This pr update the agg function's nullability accordingly
     */
    private List<NamedExpression> adjustNullableForOutputs(List<NamedExpression> outputs, boolean alwaysNullable) {
        return ExpressionUtils.rewriteDownShortCircuit(outputs, output -> {
            if (output instanceof AggregateExpression) {
                AggregateFunction function = ((AggregateExpression) output).getFunction();
                if (function instanceof NullableAggregateFunction
                        && ((NullableAggregateFunction) function).isAlwaysNullable() != alwaysNullable) {
                    AggregateParam param = ((AggregateExpression) output).getAggregateParam();
                    Expression child = ((AggregateExpression) output).child();
                    AggregateFunction newFunction = ((NullableAggregateFunction) function)
                            .withAlwaysNullable(alwaysNullable);
                    if (function == child) {
                        // function is also child
                        child = newFunction;
                    }
                    return new AggregateExpression(newFunction, param, child);
                }
            }
            return output;
        });
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
    public void computeUnique(DataTrait.Builder builder) {
        if (groupByExpressions.stream().anyMatch(s -> s instanceof VirtualSlotReference)) {
            // roll up may generate new data
            return;
        }
        DataTrait childFd = child(0).getLogicalProperties().getTrait();
        ImmutableSet<Slot> groupByKeys = groupByExpressions.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // when group by all tuples, the result only have one row
        if (groupByExpressions.isEmpty() || childFd.isUniformAndNotNull(groupByKeys)) {
            getOutput().forEach(builder::addUniqueSlot);
            return;
        }

        // propagate all unique slots
        builder.addUniqueSlot(childFd);

        // group by keys is unique
        builder.addUniqueSlot(groupByKeys);

        // group by unique may has unique aggregate result
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
        // always propagate uniform
        DataTrait childFd = child(0).getLogicalProperties().getTrait();
        builder.addUniformSlot(childFd);

        if (groupByExpressions.stream().anyMatch(s -> s instanceof VirtualSlotReference)) {
            // roll up may generate new data
            return;
        }
        ImmutableSet<Slot> groupByKeys = groupByExpressions.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // when group by all tuples, the result only have one row
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

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
