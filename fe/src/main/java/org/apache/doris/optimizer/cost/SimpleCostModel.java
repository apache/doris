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

package org.apache.doris.optimizer.cost;

import org.apache.doris.optimizer.base.OptDistributionSpec;
import org.apache.doris.optimizer.operator.OptExpressionHandle;

public class SimpleCostModel extends CostModel {

    public SimpleCostModel() {
        super(1);
    }

    public SimpleCostModel(int hostNum) {
        super(hostNum);
    }

    @Override
    protected OptCost costHashAgg(OptExpressionHandle exprHandle, CostingInfo info) {
        final long value = (info.getChildRowCount(0) + info.getRowCount()) / 2;
        return new OptCost(value);
    }

    @Override
    protected OptCost costHashJoin(OptExpressionHandle exprHandle, CostingInfo info) {
        final long outerRows = info.getChildRowCount(0);
        final long innerRows = info.getChildRowCount(1);
        return new OptCost(innerRows + outerRows);
    }

    @Override
    protected OptCost costSort(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getRowCount());
    }

    @Override
    protected OptCost costScan(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getRowCount());
    }

    @Override
    protected OptCost costDistribute(OptExpressionHandle exprHandle, CostingInfo info) {
        final OptDistributionSpec distributionSpec = exprHandle.getChildPhysicalProperty(0).getDistributionSpec();
        OptCost cost;
        if (distributionSpec.isBroadcast()) {
            cost = new OptCost(info.getRowCount() * getHostNum());
        } else {
            cost = new OptCost(info.getRowCount());
        }
        return cost;
    }

    @Override
    protected OptCost costUnion(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getChildRowCount(0) + info.getChildRowCount(1));
    }

    @Override
    protected OptCost costLimit(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getRowCount());
    }

    @Override
    protected OptCost costProject(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getRowCount());
    }

    @Override
    protected OptCost costFilter(OptExpressionHandle exprHandle, CostingInfo info) {
        return new OptCost(info.getRowCount());
    }
}
