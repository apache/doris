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

package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanOperatorVisitor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Physical aggregation plan operator.
 */
public class PhysicalAggregate extends PhysicalUnaryOperator {

    private final List<Expression> groupByExprList;

    private final List<NamedExpression> outputExpressionList;

    private final List<Expression> partitionExprList;

    private final AggPhase aggPhase;

    private final boolean usingStream;

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExprList group by expr list.
     * @param outputExpressionList agg expr list.
     * @param partitionExprList partition expr list, used for analytic agg.
     * @param usingStream whether it's stream agg.
     */
    public PhysicalAggregate(List<Expression> groupByExprList, List<NamedExpression> outputExpressionList,
            List<Expression> partitionExprList, AggPhase aggPhase, boolean usingStream) {
        super(OperatorType.PHYSICAL_AGGREGATION);
        this.groupByExprList = groupByExprList;
        this.outputExpressionList = outputExpressionList;
        this.aggPhase = aggPhase;
        this.partitionExprList = partitionExprList;
        this.usingStream = usingStream;
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    public List<Expression> getGroupByExprList() {
        return groupByExprList;
    }

    public List<NamedExpression> getOutputExpressionList() {
        return outputExpressionList;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    public List<Expression> getPartitionExprList() {
        return partitionExprList;
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalAggregate((PhysicalUnaryPlan<PhysicalAggregate, Plan>) plan, context);
    }

    @Override
    public List<Expression> getExpressions() {
        // TODO: partitionExprList maybe null.
        return new ImmutableList.Builder<Expression>().addAll(groupByExprList).addAll(outputExpressionList)
                .addAll(partitionExprList).build();
    }

    @Override
    public String toString() {
        return "PhysicalAggregate([key=" + groupByExprList
                + "], [output=" + outputExpressionList + "])";
    }
}
