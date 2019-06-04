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

package org.apache.doris.optimizer.base;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.cost.CostModel;
import org.apache.doris.optimizer.cost.CostingInfo;
import org.apache.doris.optimizer.cost.OptCost;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.List;

// Context used to compute cost for an MultiExpression
public final class OptCostContext {
    private MultiExpression multiExpr;
    private OptimizationContext optCtx;
    private OptCost cost;
    private Statistics statistics;
    private List<OptimizationContext> childrenCtxs;

    private OptPhysicalProperty derivedPhysicalProperty;

    public OptCostContext(MultiExpression multiExpr,
                          OptimizationContext optCtx) {
        this.multiExpr = multiExpr;
        this.optCtx = optCtx;
        this.childrenCtxs = Lists.newArrayList();
    }

    public MultiExpression getMultiExpr() { return multiExpr; }

    public OptCost getCost() { return cost; }

    public OptimizationContext getOptCtx() {
        return optCtx;
    }

    public void addChildrenOptContext(OptimizationContext childOptContext) {
        this.childrenCtxs.add(childOptContext);
    }

    public OptPhysicalProperty getPhysicalProperty() {
        return derivedPhysicalProperty;
    }

    public void setDerivedPhysicalProperty(OptPhysicalProperty property) {
        this.derivedPhysicalProperty = property;
    }

    public OptimizationContext getInput(int idx) {
        return childrenCtxs.get(idx);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public boolean isBetterThan(OptCostContext costContext) {
        return cost.compareTo(costContext.getCost()) == 1;
    }

    private void deriveStats() {
        if (statistics != null) {
            return;
        }
        final OptExpressionHandle exprHandle = new OptExpressionHandle(this);
        exprHandle.deriveCostCtxStats();
        statistics = exprHandle.getStatistics();
    }

    public OptCost compute(CostModel costModel) {
        deriveStats();
        // Current expr's rowcount and cost.
        final CostingInfo costInfo = new CostingInfo();
        costInfo.setStat(statistics);

        final OptExpressionHandle expressionHandle = new OptExpressionHandle(this);
        expressionHandle.derivePhysicalProperty();
        derivedPhysicalProperty = expressionHandle.getPhysicalProperty();

        long rows = 0;
        if (derivedPhysicalProperty.getDistributionSpec().isSingle()) {
            rows = statistics.getRowCount();
        } else {
            rows = getRowCountPerHost(statistics.getRowCount(), costModel);
        }
        costInfo.setRowCount(rows);

        // Children's rowcount and cost.
        for (OptimizationContext context : childrenCtxs) {
            final OptCostContext costCtx = context.getBestCostCtx();
            final Statistics childStatistcs = costCtx.statistics;
            costInfo.addChildrenStat(childStatistcs);
            if (context.getBestCostCtx().getPhysicalProperty().getDistributionSpec().isSingle()) {
                costInfo.addChildCount(childStatistcs.getRowCount());
            } else {
                costInfo.addChildCount(costCtx.getRowCountPerHost(
                        childStatistcs.getRowCount(), costModel));
            }
            costInfo.addChildCost(context.getBestCostCtx().getCost());
        }
        final OptExpressionHandle exprHandle = new OptExpressionHandle(this);
        cost = costModel.cost(exprHandle, costInfo);
        return cost;
    }

    private long getRowCountPerHost(long rowCount, CostModel costModel) {
        return rowCount / costModel.getHostNum();
    }
}
