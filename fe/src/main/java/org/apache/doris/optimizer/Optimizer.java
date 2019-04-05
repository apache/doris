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

package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.base.EnforceProperty;
import org.apache.doris.optimizer.base.OptCostContext;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.QueryContext;
import org.apache.doris.optimizer.base.RequiredPhysicalProperty;
import org.apache.doris.optimizer.base.SearchVariable;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptPatternLeaf;
import org.apache.doris.optimizer.operator.OptPhysical;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.search.*;

import java.util.List;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
    private static OptExpression ENFORCE_PATTERN = OptExpression.create(new OptPatternLeaf());
    private QueryContext queryContext;
    private OptMemo memo;
    private List<OptRule> rules = Lists.newArrayList();
    private SearchVariable variables;

    public Optimizer(QueryContext queryContext) {
        this.queryContext = queryContext;
        this.memo = new OptMemo();
    }

    public OptMemo getMemo() { return memo; }
    public List<OptRule> getRules() { return rules; }
    public void addRule(OptRule rule) { rules.add(rule); }

    public void init() {
        memo.init(queryContext.getExpression());
    }

    public void optimize() {

        OptimizationContext optCtx = new OptimizationContext(
                memo.getRoot(),
                queryContext.getReqdProp());

        final Scheduler scheduler = DefaultScheduler.create();
        final SearchContext sContext = SearchContext.create(this, memo.getRoot(), optCtx, scheduler, variables);
        scheduler.run(sContext);
    }

    public boolean checkEnforceProperty(
            MultiExpression multiExpression,
            OptimizationContext optCtx,
            List<OptimizationContext> childrenCtxs) {
        for (OptimizationContext childOptCtx : childrenCtxs) {
            if (childOptCtx.getBestMultiExpr() != null) {
                return false;
            }
        }
        // new one CostContext
        OptCostContext costContext = new OptCostContext(multiExpression, optCtx);
        costContext.setChildrenCtxs(childrenCtxs);
        // get its derive physical property
        OptExpressionHandle exprHandle = new OptExpressionHandle(costContext);
        exprHandle.derivePhysicalProperty();

        OptPhysical physicalOp = (OptPhysical) multiExpression.getOp();
        RequiredPhysicalProperty physicalProperty = physicalOp.getPhysicalProperty();

        EnforceProperty.EnforceType orderEnforceType = EnforceProperty.EnforceType.UNNECESSARY;
        if (!physicalProperty.getReqdOrder().getPropertySpec().isEmpty()) {
            orderEnforceType = physicalOp.getOrderEnforceType(exprHandle, physicalProperty.getReqdOrder());
        }

        OptExpression expr = OptBinding.bind(ENFORCE_PATTERN, exprHandle.getMultiExpr(), null);

        List<OptExpression> enforcedExpressions = Lists.newArrayList();

        physicalProperty.getReqdOrder().appendEnforcers(
                orderEnforceType, physicalProperty, expr, exprHandle, enforcedExpressions);
        if (enforcedExpressions.size() > 0) {
        }

        return EnforceProperty.isOptimized(orderEnforceType);
    }
}
