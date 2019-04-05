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

package org.apache.doris.optimizer.operator;

import com.sleepycat.je.utilint.Stat;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.base.OptCost;
import org.apache.doris.optimizer.base.OptCostContext;
import org.apache.doris.optimizer.base.OptPhysicalProperty;
import org.apache.doris.optimizer.base.OptProperty;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.RequiredProperty;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.List;

public class OptExpressionHandle {
    private OptExpression expression;
    private MultiExpression multiExpr;
    private OptCostContext costContext;

    private OptProperty deriveProperty;
    private Statistics statistics;
    private RequiredProperty requiredProperty;

    private List<OptProperty> childrenProperty;
    private List<Statistics> childrenStatistics;
    private List<RequiredProperty> requiredProperties;

    public OptExpressionHandle(OptExpression expression) {
        this.expression = expression;
    }
    public OptExpressionHandle(MultiExpression multiExpr) {
        this.multiExpr = multiExpr;
    }
    public OptExpressionHandle(OptCostContext costContext) {
        this.costContext = costContext;
        this.multiExpr = costContext.getMultiExpr();
    }

    public int arity() {
        if (expression != null) {
            return expression.arity();
        }
        return multiExpr.arity();
    }

    public boolean isItemChild(int idx) {
        if (expression != null) {
            return expression.getInput(idx).getOp().isItem();
        }
        return multiExpr.getInput(idx).isItem();
    }

    public MultiExpression getMultiExpr() { return multiExpr; }
    public OptOperator getOp() {
        if (expression != null) {
            return expression.getOp();
        }
        return multiExpr.getOp();
    }

    public OptProperty getChildProperty(int idx) { return childrenProperty.get(idx); }
    public OptPhysicalProperty getChildPhysicalProperty(int idx) { return (OptPhysicalProperty) childrenProperty.get(idx); }

    private boolean isStatsDerived() {
        Statistics stats;
        if (expression != null) {
            stats = expression.getStatistics();
        } else {
            stats = multiExpr.getGroup().getStatistics();
        }
        if (stats == null) {
            return false;
        }
        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            if (isItemChild(i)) {
                continue;
            }
            Statistics childStats;
            if (expression != null) {
                childStats = expression.getInput(i).getStatistics();
            } else {
                childStats = multiExpr.getInput(i).getStatistics();
            }
            if (childStats == null) {
                return false;
            }
        }
        return true;
    }

    private void copyStats() {
        if (!isStatsDerived()) {
            return;
        }

        if (expression != null) {
            statistics = expression.getStatistics();
        } else {
            statistics = multiExpr.getGroup().getStatistics();
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            if (isItemChild(i)) {
                continue;
            }
            if (expression != null) {
                childrenStatistics.add(expression.getInput(i).getStatistics());
            } else {
                childrenStatistics.add(multiExpr.getInput(i).getStatistics());
            }
        }
    }

    private void copyCostCtxProperty() {
        deriveProperty = costContext.getDerivedPhysicalProperty();
        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            OptGroup group = multiExpr.getInput(i);
            if (group.isItem()) {
                continue;
            }
            OptimizationContext childOptCtx = costContext.getInput(i);
            OptCostContext childCostCtx = childOptCtx.getBestCostCtx();
            childrenProperty.add(childCostCtx.getDerivedPhysicalProperty());
        }
    }

    public void derivePhysicalProperty() {
        // get statistics
        copyStats();

        // get derived property
        if (costContext.getDerivedPhysicalProperty() != null) {
            copyCostCtxProperty();
            return;
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            OptimizationContext childOptCtx = costContext.getInput(i);
            OptPhysicalProperty childProperty = childOptCtx.getBestCostCtx().getDerivedPhysicalProperty();
            childrenProperty.add(childProperty);
        }
        OptProperty property = getOp().createProperty();
        property.derive(this);
    }

}
