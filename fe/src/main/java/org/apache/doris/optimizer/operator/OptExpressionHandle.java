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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.List;

public class OptExpressionHandle {
    private OptExpression expression;
    private MultiExpression multiExpr;
    private OptCostContext costContext;

    private OptProperty deriveProperty;
    private Statistics statistics;
    private RequiredProperty requiredProperty;

    private List<OptProperty> childrenProperty = Lists.newArrayList();
    private List<Statistics> childrenStatistics = Lists.newArrayList();
    private List<RequiredProperty> requiredProperties = Lists.newArrayList();

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

    public OptExpression getExpression() { return expression; }
    public MultiExpression getMultiExpr() { return multiExpr; }
    public OptOperator getOp() {
        if (expression != null) {
            return expression.getOp();
        }
        return multiExpr.getOp();
    }

    public OptProperty getChildProperty(int idx) { return childrenProperty.get(idx); }
    public OptPhysicalProperty getChildPhysicalProperty(int idx) { return (OptPhysicalProperty) childrenProperty.get(idx); }
    public OptLogicalProperty getChildLogicalProperty(int idx) { return (OptLogicalProperty) childrenProperty.get(idx); }
    public OptItemProperty getChildItemProperty(int idx) { return (OptItemProperty) childrenProperty.get(idx); }

    public int getChildPropertySize() {
        return childrenProperty.size();
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public List<Statistics> getChildrenStatistics() {
        return childrenStatistics;
    }

    public OptLogicalProperty getLogicalProperty() {
        return (OptLogicalProperty) deriveProperty;
    }

    public OptPhysicalProperty getPhysicalProperty() {
        return (OptPhysicalProperty) deriveProperty;
    }

    private void copyStats() {
        if (expression != null) {
            statistics = expression.getStatistics();
        } else {
            Preconditions.checkNotNull(multiExpr,
                    "multiExpr can't be null.");
            statistics = multiExpr.getGroup().getStatistics();
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            if (isItemChild(i)) {
                continue;
            }
            Statistics statistics;
            if (expression != null) {
                statistics = expression.getInput(i).getStatistics();
            } else {
                statistics = multiExpr.getInput(i).getStatistics();
            }
            Preconditions.checkNotNull(statistics,
                    "Children group's statistics can't be null.");
            childrenStatistics.add(statistics);
        }
    }

    private void copyProperties() {
        if (expression != null) {
            deriveProperty = expression.getProperty();
        } else {
            Preconditions.checkNotNull(multiExpr,
                    "multiExpr can't be null.");
            deriveProperty = multiExpr.getGroup().getProperty();
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            if (isItemChild(i)) {
                continue;
            }
            OptProperty childProperty;
            if (expression != null) {
                childProperty = expression.getInput(i).getProperty();
            } else {
                childProperty = multiExpr.getInput(i).getProperty();
            }
            Preconditions.checkNotNull(childProperty,
                    "Children group's logical property can't be null.");
            childrenProperty.add(childProperty);
        }
    }

    private void copyStatsAndProperties() {
        copyProperties();
        copyStats();
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

    /**
     * 1. Derive current operator and children's physical property.
     * 2. save operator and direct children's logical properties,
     * physical properties, and statistics.
     */
    public void derivePhysicalProperty() {
        Preconditions.checkNotNull(multiExpr,
                "Only support MultiExpression to derive physical property.");
        Preconditions.checkState(multiExpr.getOp().isPhysical(),
                "The MultiExpression derived must be physical.");

        copyStatsAndProperties();

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

        OptPhysicalProperty property = (OptPhysicalProperty)getOp().createProperty();
        property.derive(this);
        costContext.setDerivedPhysicalProperty(property);
    }

    public OptExpression getItemChild(int idx) {
        if (multiExpr != null) {
            return multiExpr.getInput(idx).getItemExpression();
        }
        if (expression != null && expression.getInput(idx).getMExpr() != null) {
            return expression.getInput(idx).getMExpr().getGroup().getItemExpression();
        }
        return expression.getInput(idx);
    }

    /**
     * 1. Derive current operator and children's logical property
     * 2. save current operator and direct children's logical property.
     */
    public void deriveProperty() {
        if (multiExpr != null) {
            Preconditions.checkArgument(multiExpr.getOp().isLogical());
            copyProperties();
            return;
        }

        Preconditions.checkArgument(expression.getOp().isLogical(),
                "Expression must be logcial.");
        Preconditions.checkNotNull(expression,
                "expression is null.");

        for (OptExpression input : expression.getInputs()) {
            final OptProperty property = input.deriveProperty();
            childrenProperty.add(property);
        }

        final OptProperty property = expression.getOp().createProperty();
        deriveProperty = property;
        property.derive(this);
        expression.setProperty(property);
    }

    /**
     * 1. Derive current operator and children's logical statistics.
     * 2. save current operator and direct children's statistics.
     * @param parentReqProperty
     * @return
     */
    public void deriveStat(RequiredLogicalProperty parentReqProperty) {
        Preconditions.checkNotNull(multiExpr,
                "Only support MultiExpression to derive statistics.");
        Preconditions.checkState(multiExpr.getOp().isLogical(),
                "multiExpr derived must be logical.");

        if (multiExpr.getGroup().getStatistics() != null) {
            copyStats();
            return;
        }

        for (int i = 0; i < multiExpr.getInputs().size(); i++) {
            final OptGroup childGroup = multiExpr.getInput(i);
            final RequiredLogicalProperty childReqProperty = new RequiredLogicalProperty();
            childReqProperty.compute(this, parentReqProperty, i);
            childGroup.deriveStat(childReqProperty);
            childrenStatistics.add(childGroup.getStatistics());
        }

        statistics = multiExpr.deriveStat(this, parentReqProperty);
    }
}
