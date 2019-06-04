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
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.QueryContext;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.cost.CostModel;
import org.apache.doris.optimizer.operator.ExpressionPreprocessor;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.search.DefaultScheduler;
import org.apache.doris.optimizer.search.Scheduler;
import org.apache.doris.optimizer.search.SearchContext;

import java.util.List;

/**
 * Optimizer's entrance class
 */
public final class Optimizer {
    private final QueryContext queryContext;
    private final OptMemo memo;
    private final CostModel costModel;
    private final List<OptRule> rules = Lists.newArrayList();
    private final OptColumnRefFactory columnRefFactory;
    private OptimizationContext rootOptContext;

    public Optimizer(QueryContext queryContext, CostModel costModel, OptColumnRefFactory columnRefFactory) {
        this.queryContext = queryContext;
        this.memo = new OptMemo();
        this.costModel = costModel;
        this.columnRefFactory = columnRefFactory;
    }

    public OptMemo getMemo() { return memo; }

    public CostModel getCostModel() { return costModel; }

    public OptGroup getRoot() { return memo.getRoot(); }

    public List<OptRule> getRules() { return rules; }

    public OptColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public void addRule(OptRule rule) { rules.add(rule); }

    public OptimizationContext getRootOptContext() {
        return rootOptContext;
    }

    public void preOptimize() {
        final OptExpression newExpr = ExpressionPreprocessor.preprocess(queryContext.getExpression(), queryContext.getColumnRefs());
        memo.init(newExpr);
    }

    public void optimize() {
        if(memo.getRoot() == null) {
            memo.init(queryContext.getExpression());
        }
        rootOptContext = new OptimizationContext(
                memo.getRoot(),
                RequiredLogicalProperty.createEmptyProperty(),
                queryContext.getReqdProp());
        final Scheduler scheduler = DefaultScheduler.create();
        final SearchContext sContext = SearchContext.create(this, memo.getRoot(),
                rootOptContext, scheduler, queryContext.getVariables(), columnRefFactory);
        scheduler.run(sContext);
    }
}
