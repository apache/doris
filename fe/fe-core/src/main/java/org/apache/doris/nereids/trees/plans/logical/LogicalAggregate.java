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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Aggregate plan.
 * <p>
 * For example SQL:
 * <p>
 * select a, sum(b), c from table group by a, c;
 * <p>
 * groupByExpressions: Column field after group by. eg: a, c;
 * outputExpressions: Column field after select. eg: a, sum(b), c;
 * <p>
 * Each agg node only contains the select statement field of the same layer,
 * and other agg nodes in the subquery contain.
 * Note: In general, the output of agg is a subset of the group by column plus aggregate column.
 * In special cases. this relationship does not hold. for example, select k1+1, sum(v1) from table group by k1.
 */
public class LogicalAggregate<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Aggregate {

    private final boolean disassembled;
    private final boolean normalized;
    private final AggPhase aggPhase;
    private final ImmutableList<Expression> groupByExpressions;
    private final ImmutableList<NamedExpression> outputExpressions;
    // TODO: we should decide partition expression according to cost.
    private final Optional<ImmutableList<Expression>> partitionExpressions;

    // use for scenes containing distinct agg
    // 1. If there is LOCAL only, LOCAL is the final phase
    // 2. If there are LOCAL and GLOBAL phases, global is the final phase
    // 3. If there are LOCAL, GLOBAL and DISTINCT_LOCAL phases, DISTINCT_LOCAL is the final phase
    // 4. If there are LOCAL, GLOBAL, DISTINCT_LOCAL, DISTINCT_GLOBAL phases,
    // DISTINCT_GLOBAL is the final phase
    private final boolean isFinalPhase;

    /**
     * Desc: Constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), false,
                false, true, AggPhase.LOCAL, child);
    }

    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            boolean disassembled,
            boolean normalized,
            boolean isFinalPhase,
            AggPhase aggPhase,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, Optional.empty(), disassembled, normalized,
                isFinalPhase, aggPhase, Optional.empty(), Optional.empty(), child);
    }

    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions,
            boolean disassembled,
            boolean normalized,
            boolean isFinalPhase,
            AggPhase aggPhase,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, partitionExpressions, disassembled, normalized, isFinalPhase,
                aggPhase, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Whole parameters constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<List<Expression>> partitionExpressions,
            boolean disassembled,
            boolean normalized,
            boolean isFinalPhase,
            AggPhase aggPhase,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.partitionExpressions = partitionExpressions.map(ImmutableList::copyOf);
        this.disassembled = disassembled;
        this.normalized = normalized;
        this.isFinalPhase = isFinalPhase;
        this.aggPhase = aggPhase;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions.orElse(groupByExpressions);
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalAggregate",
                "phase", aggPhase,
                "outputExpr", outputExpressions,
                "groupByExpr", groupByExpressions
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
        return visitor.visitLogicalAggregate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .build();
    }

    public boolean isDisassembled() {
        return disassembled;
    }

    public boolean isNormalized() {
        return normalized;
    }

    public boolean isFinalPhase() {
        return isFinalPhase;
    }

    /**
     * Determine the equality with another plan
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalAggregate that = (LogicalAggregate) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && aggPhase == that.aggPhase
                && disassembled == that.disassembled
                && normalized == that.normalized
                && isFinalPhase == that.isFinalPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, partitionExpressions,
                aggPhase, normalized, disassembled, isFinalPhase);
    }

    @Override
    public LogicalAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions.map(List.class::cast),
                disassembled, normalized, isFinalPhase, aggPhase, children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions.map(List.class::cast),
                disassembled, normalized, isFinalPhase, aggPhase,
                groupExpression, Optional.of(getLogicalProperties()), children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalAggregate<>(groupByExpressions, outputExpressions, partitionExpressions.map(List.class::cast),
                disassembled, normalized, isFinalPhase, aggPhase,
                Optional.empty(), logicalProperties, children.get(0));
    }

    public LogicalAggregate<Plan> withGroupByAndOutput(List<Expression> groupByExprList,
            List<NamedExpression> outputExpressionList) {
        return new LogicalAggregate<>(groupByExprList, outputExpressionList, partitionExpressions.map(List.class::cast),
                disassembled, normalized, isFinalPhase, aggPhase, child());
    }
}
