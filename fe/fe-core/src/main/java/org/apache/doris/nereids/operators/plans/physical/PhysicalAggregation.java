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

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.List;

public class PhysicalAggregation extends PhysicalUnaryOperator<PhysicalAggregation, PhysicalPlan> {

    private List<Expression> groupByExprList;

    private List<Expression> aggExprList;

    private List<Expression> partitionExprList;

    private AggPhase aggPhase;

    private boolean needFinalize;

    private boolean usingStream;

    public PhysicalAggregation() {
        super(OperatorType.PHYSICAL_AGGREGATION);
    }

    public PhysicalAggregation(OperatorType type, List<Expression> groupByExprList, List<Expression> aggExprList,
            AggPhase aggPhase) {
        super(type);
        this.groupByExprList = groupByExprList;
        this.aggExprList = aggExprList;
        this.aggPhase = aggPhase;
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

    public boolean isNeedFinalize() {
        return needFinalize;
    }

    public boolean isUsingStream() {
        return usingStream;
    }

    public void setGroupByExprList(List<Expression> groupByExprList) {
        this.groupByExprList = groupByExprList;
    }

    public void setAggExprList(List<Expression> aggExprList) {
        this.aggExprList = aggExprList;
    }

    public List<Expression> getPartitionExprList() {
        return partitionExprList;
    }

    public void setPartitionExprList(List<Expression> partitionExprList) {
        this.partitionExprList = partitionExprList;
    }

    public void setAggPhase(AggPhase aggPhase) {
        this.aggPhase = aggPhase;
    }

    public void setNeedFinalize(boolean needFinalize) {
        this.needFinalize = needFinalize;
    }

    public void setUsingStream(boolean usingStream) {
        this.usingStream = usingStream;
    }
}
