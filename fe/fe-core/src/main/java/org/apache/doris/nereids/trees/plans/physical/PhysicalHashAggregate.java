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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestProperties;
import org.apache.doris.nereids.properties.RequestPropertiesSupplier;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Physical hash aggregation plan.
 */
public class PhysicalHashAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE>, RequestPropertiesSupplier {

    private final List<Expression> groupByExpressions;

    private final List<NamedExpression> outputExpressions;

    private final Optional<List<Expression>> partitionExpressions;

    private final AggMode aggMode;

    private final AggPhase aggPhase;

    private final boolean usingStream;

    private final RequestProperties requestProperties;

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            AggPhase aggPhase, AggMode aggMode, boolean usingStream, LogicalProperties logicalProperties,
            RequestProperties requestProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), aggPhase, aggMode,
                usingStream, Optional.empty(), logicalProperties, requestProperties, child);
    }

    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggPhase aggPhase, AggMode aggMode,
            boolean usingStream, LogicalProperties logicalProperties, RequestProperties requestProperties,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, aggPhase, aggMode,
                usingStream, Optional.empty(), logicalProperties, requestProperties, child);
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param usingStream whether it's stream agg.
     * @param requestProperties the request physical properties
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggPhase aggPhase, AggMode aggMode, boolean usingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            RequestProperties requestProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggPhase = Objects.requireNonNull(aggPhase, "aggPhase cannot be null");
        this.aggMode = Objects.requireNonNull(aggMode, "aggMode cannot be null");
        this.usingStream = usingStream;
        this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties cannot be null");
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions hash distribute expr list
     * @param usingStream whether it's stream agg.
     * @param requestProperties the request physical properties
     */
    public PhysicalHashAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions, AggPhase aggPhase, AggMode aggMode, boolean usingStream,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            RequestProperties requestProperties, PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_AGGREGATE, groupExpression, logicalProperties, physicalProperties, statsDeriveResult,
                child);
        this.groupByExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(groupByExpressions, "groupByExpressions cannot be null"));
        this.outputExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(outputExpressions, "outputExpressions cannot be null"));
        this.partitionExpressions = Objects.requireNonNull(
                partitionExpressions, "partitionExpressions cannot be null");
        this.aggPhase = Objects.requireNonNull(aggPhase, "aggPhase cannot be null");
        this.aggMode = Objects.requireNonNull(aggMode, "aggMode cannot be null");
        this.usingStream = usingStream;
        this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties cannot be null");
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    public AggMode getAggMode() {
        return aggMode;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public Optional<List<Expression>> getPartitionExpressions() {
        return partitionExpressions;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    @Override
    public RequestProperties getRequestProperties() {
        return requestProperties;
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
        return Utils.toSqlString("PhysicalHashAggregate",
                "phase", aggPhase,
                "mode", aggMode,
                "groupByExpr", groupByExpressions,
                "outputExpr", outputExpressions,
                "partitionExpr", partitionExpressions,
                "requestProperties", requestProperties,
                "stats", statsDeriveResult
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
                && usingStream == that.usingStream
                && aggPhase == that.aggPhase
                && aggMode == that.aggMode
                && requestProperties == that.requestProperties;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions, aggPhase,
                aggMode, usingStream, requestProperties);
    }

    @Override
    public PhysicalHashAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, aggMode, usingStream, getLogicalProperties(), requestProperties, children.get(0));
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions, aggPhase,
                aggMode, usingStream, groupExpression, getLogicalProperties(), requestProperties, child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, aggMode, usingStream, Optional.empty(), logicalProperties.get(),
                requestProperties, child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalHashAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, aggMode, usingStream, Optional.empty(), getLogicalProperties(),
                requestProperties, physicalProperties, statsDeriveResult,
                child());
    }

    @Override
    public PhysicalHashAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalHashAggregate<>(groupByExpressions, newOutput, partitionExpressions,
                aggPhase, aggMode, usingStream, Optional.empty(), getLogicalProperties(),
                requestProperties, physicalProperties, statsDeriveResult, child());
    }

    /** localPhaseRequestProperties */
    public static RequestProperties localPhaseRequestProperties(AggMode aggMode) {
        if (aggMode.isFinalPhase) {
            // INPUT_TO_RESULT should gather to one instance
            return RequestProperties.of(PhysicalProperties.GATHER);
        } else {
            // INPUT_TO_BUFFER
            return RequestProperties.of(PhysicalProperties.ANY);
        }
    }

    /** globalPhaseRequestProperties */
    public static RequestProperties globalPhaseRequestProperties(AggMode aggMode) {
        if (aggMode.isFinalPhase) {
            // BUFFER_TO_RESULT should distribute by group by keys
            return RequestProperties.of(PhysicalProperties.GATHER);
        } else {
            // BUFFER_TO_BUFFER
            // local/global distinct stage exists, follow the distinct aggregate's request
            return RequestProperties.followParent();
        }
    }

    /** distinctLocalPhaseRequestProperties */
    public static RequestProperties distinctLocalPhaseRequestProperties(
            AggMode aggMode, LogicalAggregate<? extends Plan> logicalAggregate) {
        if (aggMode.isFinalPhase) {
            // BUFFER_TO_RESULT
            // 3 phase aggregate
            // local distinct: group by originGroupByExpressions
            // global aggregate: group by distinct columns + originGroupByExpressions
            // local aggregate: group by distinct columns + originGroupByExpressions
            //
            // the local distinct and global aggregate has the same request:
            // distribute by hash(originGroupByExpressions), so they must exist in the same instance.
            // and the local aggregate request any request, maybe exist in another instance.
            List<Expression> groupByExpressions = logicalAggregate.getGroupByExpressions();
            if (groupByExpressions.isEmpty()) {
                Set<Expression> distinctArguments = logicalAggregate.getDistinctArguments();
                return RequestProperties.of(PhysicalProperties.createHash(distinctArguments, ShuffleType.AGGREGATE));
            } else {
                return RequestProperties.of(PhysicalProperties.createHash(groupByExpressions, ShuffleType.AGGREGATE));
            }
        } else {
            // BUFFER_TO_BUFFER
            // global distinct stage exists, follow the distinct aggregate's request
            return RequestProperties.followParent();
        }
    }

    /** distinctGlobalPhaseRequestProperties */
    public static RequestProperties distinctGlobalPhaseRequestProperties(
            AggMode aggMode, LogicalAggregate<? extends Plan> logicalAggregate) {
        if (aggMode.isFinalPhase) {
            // BUFFER_TO_RESULT
            List<Expression> groupByExpressions = logicalAggregate.getGroupByExpressions();
            if (groupByExpressions.isEmpty()) {
                Set<Expression> distinctArguments = logicalAggregate.getDistinctArguments();
                return RequestProperties.of(PhysicalProperties.createHash(distinctArguments, ShuffleType.AGGREGATE));
            } else {
                return RequestProperties.of(PhysicalProperties.createHash(groupByExpressions, ShuffleType.AGGREGATE));
            }
        } else {
            throw new AnalysisException("DISTINCT_GLOBAL should be the final stage");
        }
    }
}
