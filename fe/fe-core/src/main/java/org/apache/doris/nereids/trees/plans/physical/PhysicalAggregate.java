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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical aggregation plan.
 */
public class PhysicalAggregate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    private final List<Expression> groupByExpressions;

    private final List<NamedExpression> outputExpressions;

    private final List<Expression> partitionExpressions;

    private final AggPhase aggPhase;

    private final boolean usingStream;


    public PhysicalAggregate(List<Expression> groupByExpressions, List<NamedExpression> outputExpressions,
                             List<Expression> partitionExpressions, AggPhase aggPhase, boolean usingStream,
                             LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, aggPhase, usingStream,
                Optional.empty(), logicalProperties, child);
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
                             List<Expression> partitionExpressions, AggPhase aggPhase, boolean usingStream,
                             Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
                             CHILD_TYPE child) {
        super(PlanType.PHYSICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = groupByExpressions;
        this.outputExpressions = outputExpressions;
        this.aggPhase = aggPhase;
        this.partitionExpressions = partitionExpressions;
        this.usingStream = usingStream;
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

    public boolean isUsingStream() {
        return usingStream;
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions == null ? groupByExpressions : partitionExpressions;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAggregate((PhysicalAggregate<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        // TODO: partitionExprList maybe null.
        return new ImmutableList.Builder<Expression>().addAll(groupByExpressions).addAll(outputExpressions)
                .addAll(partitionExpressions).build();
    }

    @Override
    public String toString() {
        return "PhysicalAggregate (phase: [" + aggPhase.name() + "], output: ["
                + StringUtils.join(outputExpressions, ", ")
                + "], groupBy: [" + StringUtils.join(groupByExpressions, ", ") + "])";
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
                && aggPhase == that.aggPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions, aggPhase, usingStream);
    }

    @Override
    public PhysicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions, aggPhase,
            usingStream, logicalProperties, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions, aggPhase,
            usingStream, groupExpression, logicalProperties, child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions, aggPhase,
            usingStream, Optional.empty(), logicalProperties.get(), child());
    }
}
