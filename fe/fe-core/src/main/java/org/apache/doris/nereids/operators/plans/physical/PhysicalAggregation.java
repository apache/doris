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

import org.apache.doris.nereids.PlanOperatorVisitor;
import org.apache.doris.nereids.operators.PlanType;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

import java.util.List;

/**
 * Physical aggregation plan operator.
 */
public class PhysicalAggregation extends PhysicalUnaryOperator {

    private final List<Expression> groupByExprList;

    private final List<Expression> aggExprList;

    private final List<Expression> partitionExprList;

    private final AggPhase aggPhase;

    private final boolean usingStream;

    /**
     * Constructor of PhysicalAggNode.
     *
     * @param groupByExprList group by expr list.
     * @param aggExprList agg expr list.
     * @param partitionExprList  partition expr list, used for analytic agg.
     * @param usingStream whether it's stream agg.
     */
    public PhysicalAggregation(List<Expression> groupByExprList, List<Expression> aggExprList,
            List<Expression> partitionExprList, AggPhase aggPhase, boolean usingStream) {
        super(PlanType.PHYSICAL_AGGREGATION);
        this.groupByExprList = groupByExprList;
        this.aggExprList = aggExprList;
        this.partitionExprList = partitionExprList;
        this.aggPhase = aggPhase;
        this.usingStream = usingStream;
    }

    public List<Expression> getGroupByExprList() {
        return groupByExprList;
    }

    public List<Expression> getAggExprList() {
        return aggExprList;
    }

    public AggPhase getAggPhase() {
        return aggPhase;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    public List<Expression> getPartitionExprList() {
        return partitionExprList;
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalAggregationPlan((PhysicalUnaryPlan<PhysicalAggregation, Plan>) plan, context);
    }
}
