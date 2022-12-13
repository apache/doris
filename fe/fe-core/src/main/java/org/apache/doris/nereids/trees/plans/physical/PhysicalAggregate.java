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
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical aggregation plan.
 * TODO: change class name to PhysicalHashAggregate
 */
public class PhysicalAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE>
        implements Aggregate<CHILD_TYPE> {

    private final ImmutableList<Expression> groupByExpressions;

    private final ImmutableList<NamedExpression> outputExpressions;

    private final ImmutableList<Expression> partitionExpressions;

    private final AggPhase aggPhase;

    private final boolean usingStream;

    // use for scenes containing distinct agg
    // 1. If there are LOCAL and GLOBAL phases, global is the final phase
    // 2. If there are LOCAL, GLOBAL and DISTINCT_LOCAL phases, DISTINCT_LOCAL is the final phase
    // 3. If there are LOCAL, GLOBAL, DISTINCT_LOCAL, DISTINCT_GLOBAL phases,
    // DISTINCT_GLOBAL is the final phase
    private final boolean isFinalPhase;

    public PhysicalAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            List<Expression> partitionExpressions, AggPhase aggPhase, boolean usingStream,
            boolean isFinalPhase, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, aggPhase, usingStream,
                isFinalPhase, Optional.empty(), logicalProperties, child);
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions partition expr list, used for analytic agg.
     * @param usingStream whether it's stream agg.
     */
    public PhysicalAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            List<Expression> partitionExpressions, AggPhase aggPhase, boolean usingStream, boolean isFinalPhase,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.aggPhase = aggPhase;
        this.partitionExpressions = ImmutableList.copyOf(partitionExpressions);
        this.usingStream = usingStream;
        this.isFinalPhase = isFinalPhase;
    }

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExpressions group by expr list.
     * @param outputExpressions agg expr list.
     * @param partitionExpressions partition expr list, used for analytic agg.
     * @param usingStream whether it's stream agg.
     */
    public PhysicalAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
            List<Expression> partitionExpressions, AggPhase aggPhase, boolean usingStream, boolean isFinalPhase,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_AGGREGATE, groupExpression, logicalProperties, physicalProperties, statsDeriveResult,
                child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.aggPhase = aggPhase;
        this.partitionExpressions = ImmutableList.copyOf(partitionExpressions);
        this.usingStream = usingStream;
        this.isFinalPhase = isFinalPhase;
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public boolean isFinalPhase() {
        return isFinalPhase;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .addAll(partitionExpressions).build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalAggregate",
                "phase", aggPhase,
                "outputExpr", outputExpressions,
                "groupByExpr", groupByExpressions,
                "partitionExpr", partitionExpressions,
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
        PhysicalAggregate that = (PhysicalAggregate) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && usingStream == that.usingStream
                && aggPhase == that.aggPhase
                && isFinalPhase == that.isFinalPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, usingStream, isFinalPhase);
    }

    @Override
    public PhysicalAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, usingStream, isFinalPhase, getLogicalProperties(), children.get(0));
    }

    @Override
    public PhysicalAggregate<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, usingStream, isFinalPhase, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public PhysicalAggregate<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, usingStream, isFinalPhase, Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public PhysicalAggregate<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, usingStream, isFinalPhase,
                Optional.empty(), getLogicalProperties(), physicalProperties, statsDeriveResult, child());
    }

    @Override
    public PhysicalAggregate<CHILD_TYPE> withAggOutput(List<NamedExpression> newOutput) {
        return new PhysicalAggregate<>(groupByExpressions, newOutput, partitionExpressions,
                aggPhase, usingStream, isFinalPhase, Optional.empty(), getLogicalProperties(),
                physicalProperties, statsDeriveResult, child());
    }
}
