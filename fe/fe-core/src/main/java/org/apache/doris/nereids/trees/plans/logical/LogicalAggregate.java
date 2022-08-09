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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Aggregate plan.
 * <p>
 * eg:select a, sum(b), c from table group by a, c;
 * groupByExprList: Column field after group by. eg: a, c;
 * outputExpressionList: Column field after select. eg: a, sum(b), c;
 * partitionExprList: Column field after partition by.
 * <p>
 * Each agg node only contains the select statement field of the same layer,
 * and other agg nodes in the subquery contain.
 * Note: In general, the output of agg is a subset of the group by column plus aggregate column.
 * In special cases. this relationship does not hold. for example, select k1+1, sum(v1) from table group by k1.
 */
public class LogicalAggregate<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Aggregate {

    private final boolean disassembled;
    private final List<Expression> groupByExpressions;
    private final List<NamedExpression> outputExpressions;
    private final AggPhase aggPhase;

    /**
     * Desc: Constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, false, AggPhase.GLOBAL, child);
    }

    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            boolean disassembled,
            AggPhase aggPhase,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, disassembled, aggPhase, Optional.empty(), Optional.empty(), child);
    }

    /**
     * Whole parameters constructor for LogicalAggregate.
     */
    public LogicalAggregate(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            boolean disassembled,
            AggPhase aggPhase,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_AGGREGATE, groupExpression, logicalProperties, child);
        this.groupByExpressions = groupByExpressions;
        this.outputExpressions = outputExpressions;
        this.disassembled = disassembled;
        this.aggPhase = aggPhase;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    @Override
    public String toString() {
        return "LogicalAggregate (phase: [" + aggPhase.name() + "], output: ["
                + StringUtils.join(outputExpressions, ", ")
                + "], groupBy: [" + StringUtils.join(groupByExpressions, ", ") + "])";
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAggregate((LogicalAggregate<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .build();
    }

    public boolean isDisassembled() {
        return disassembled;
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
                && aggPhase == that.aggPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, aggPhase);
    }

    @Override
    public LogicalAggregate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalAggregate(groupByExpressions, outputExpressions, disassembled, aggPhase, children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalAggregate(groupByExpressions, outputExpressions, disassembled, aggPhase, groupExpression,
            Optional.of(logicalProperties), children.get(0));
    }

    @Override
    public LogicalAggregate<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalAggregate(groupByExpressions, outputExpressions, disassembled, aggPhase, Optional.empty(),
            logicalProperties, children.get(0));
    }

    public LogicalAggregate<Plan> withGroupByAndOutput(List<Expression> groupByExprList,
                                                 List<NamedExpression> outputExpressionList) {
        return new LogicalAggregate(groupByExprList, outputExpressionList, disassembled, aggPhase, child());
    }
}
