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

public final class OptExpressionHandle {
    private OptExpression expression;
    private MultiExpression multiExpr;
    private OptCostContext costContext;

    private OptProperty deriveProperty;
    private Statistics statistics;
    private RequiredProperty requiredProperty;

    private List<OptProperty> childrenProperty = Lists.newArrayList();
    private List<Statistics> childrenStatistics = Lists.newArrayList();
    private List<RequiredProperty> childrenRequiredProperties = Lists.newArrayList();

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
        return multiExpr.getInput(idx).isItemGroup();
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

    public OptProperty getProperty() { return deriveProperty; }

    public OptLogicalProperty getLogicalProperty() {
        return (OptLogicalProperty) deriveProperty;
    }

    public OptPhysicalProperty getPhysicalProperty() {
        return (OptPhysicalProperty) deriveProperty;
    }

    public RequiredProperty getRequiredProperty() {
        return requiredProperty;
    }

    public RequiredProperty getChildrenRequiredProperty(int childIndex) {
        return childrenRequiredProperties.get(childIndex);
    }

    /**
     * Is Expression or Multi Expression's statistics derived.
     */
    private boolean isStatsDerived() {
        Statistics statistics;
        if (expression != null) {
            statistics = expression.getStatistics();
        } else {
            Preconditions.checkNotNull(multiExpr,
                    "multiExpr can't be null.");
            statistics = multiExpr.getGroup().getStatistics();
        }

        if (statistics == null) {
            return false;
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            if (isItemChild(i)) {
                continue;
            }
            if (expression != null) {
                statistics = expression.getInput(i).getStatistics();
            } else {
                statistics = multiExpr.getInput(i).getStatistics();
            }
            if (statistics == null) {
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

    private void copyLogicalProperty() {
        if (expression != null) {
            deriveProperty = expression.getProperty();
        } else {
            Preconditions.checkNotNull(multiExpr,
                    "multiExpr can't be null.");
            deriveProperty = multiExpr.getGroup().getProperty();
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
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

    private void copyStatsAndLogicalProperty() {
        copyLogicalProperty();
        copyStats();
    }

    private void copyCostCtxPhysicalProperty() {
        deriveProperty = costContext.getPhysicalProperty();
        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            OptGroup group = multiExpr.getInput(i);
            if (group.isItemGroup()) {
                continue;
            }
            OptimizationContext childOptCtx = costContext.getInput(i);
            OptCostContext childCostCtx = childOptCtx.getBestCostCtx();
            childrenProperty.add(childCostCtx.getPhysicalProperty());
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
        copyStats();

        // get derived property
        if (costContext.getPhysicalProperty() != null) {
            copyCostCtxPhysicalProperty();
            return;
        }

        int arity = arity();
        for (int i = 0; i < arity; ++i) {
            OptimizationContext childOptCtx = costContext.getInput(i);
            OptPhysicalProperty childProperty = childOptCtx.getBestCostCtx().getPhysicalProperty();
            childrenProperty.add(childProperty);
        }

        final OptPhysicalProperty property = (OptPhysicalProperty) getOp().createProperty();
        property.derive(this);
        deriveProperty = property;
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
     * For logical and item expression deriving property.
     */
    public void deriveExpressionLogicalOrItemProperty() {
        Preconditions.checkNotNull(expression, "Expression can't be null.");
        if (expression.getProperty() != null) {
            copyLogicalProperty();
            return;
        }

//        copyStats();

        for (OptExpression input : expression.getInputs()) {
            final OptProperty property = input.deriveProperty();
            childrenProperty.add(property);
        }

        deriveProperty = expression.getOp().createProperty();
        deriveProperty.derive(this);
    }

    public void deriveCostCtxStats() {
        Preconditions.checkNotNull(costContext, "CostContext can't be null");
        Preconditions.checkArgument(statistics == null, "Statistics is null.");

        copyStatsAndLogicalProperty();
        if (statistics != null) {
            return;
        }

        for (int i = 0; i < arity(); i++) {
            final OptGroup childGroup = multiExpr.getInput(i);
            childrenStatistics.add(childGroup.getStatistics());
        }

        multiExpr.getGroup().deriveStat(costContext.getOptCtx().getRequiredLogicalProperty());
        statistics = multiExpr.getGroup().getStatistics();
    }

    public void computeExpressionRequiredProperty(RequiredProperty requiredProperty) {
        if (getOp().isItem()) {
            return;
        }

        for (int i = 0; i < multiExpr.getInputs().size(); i++) {
            if (multiExpr.getInput(i).isItemGroup()) {
                continue;
            }
            RequiredProperty childRequiredProperty;
            if (multiExpr.getOp().isLogical()) {
                childRequiredProperty = new RequiredLogicalProperty();
            } else {
                childRequiredProperty = new RequiredPhysicalProperty();
            }
            childRequiredProperty.compute(this, requiredProperty, i);
            childrenRequiredProperties.add(childRequiredProperty);
        }
        this.requiredProperty = requiredProperty;
    }

    /**
     * For logical and item expression deriving stats.
     */
    public void deriveExpressionStats(RequiredLogicalProperty parentReqProperty) {
        Preconditions.checkArgument(expression != null && expression.getOp().isLogical(),
                " Expression can't be null and can only be logical operator.");
        if (expression.getStatistics() != null) {
            copyStats();
            return;
        }

        for (int i = 0; i < expression.getInputs().size(); i++) {
            final OptExpression childExpression = expression.getInput(i);
            final RequiredLogicalProperty childRequiredProperty = new RequiredLogicalProperty();
            childRequiredProperty.compute(this, parentReqProperty, i);
            childExpression.deriveStat(childRequiredProperty);
        }

        final OptLogical logical = (OptLogical) expression.getOp();
        statistics = logical.deriveStat(this, parentReqProperty);
    }

    public void deriveMultiExpressionLogicalOrItemProperty() {
        Preconditions.checkNotNull(multiExpr, "Multi Expression can't be null.");
        copyLogicalProperty();
    }

    public void deriveMultiExpressionStats(RequiredLogicalProperty parentReqProperty) {
        Preconditions.checkArgument(multiExpr != null && multiExpr.getOp().isLogical(),
                "Multi Expression can't be null.");
        if (multiExpr.getGroup().getStatistics() != null) {
            copyStats();
            return;
        }

        for (int i = 0; i < multiExpr.getInputs().size(); i++) {
            final OptGroup childGroup = multiExpr.getInput(i);
            if (childGroup.isItemGroup()) {
                continue;
            }
            final RequiredLogicalProperty childReqProperty = new RequiredLogicalProperty();
            childReqProperty.compute(this, parentReqProperty, i);
            if (childReqProperty.isEmpty()) {
                // Only a placeholder
                childrenStatistics.add(new Statistics(childReqProperty));
                continue;
            }
            childGroup.deriveStat(childReqProperty);
            childrenStatistics.add(childGroup.getStatistics());
        }

        final OptLogical logical = (OptLogical) multiExpr.getOp();
        statistics = logical.deriveStat(this, parentReqProperty);
    }
}
