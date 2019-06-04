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

package org.apache.doris.optimizer.search;

import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.OptMemo;
import org.apache.doris.optimizer.Optimizer;
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.optimizer.base.OptimizationContext;
import org.apache.doris.optimizer.base.SearchVariable;
import org.apache.doris.optimizer.cost.CostModel;
import org.apache.doris.optimizer.rule.OptRule;

import java.util.List;

public class SearchContext {

    private final Optimizer optimizer;
    private final Scheduler scheduler;
    private final SearchVariable variables;
    private final OptColumnRefFactory columnRefFactory;

    private SearchContext(Optimizer optimizer, Scheduler scheduler, SearchVariable variables, OptColumnRefFactory factory) {
        this.optimizer = optimizer;
        this.scheduler = scheduler;
        this.variables = variables;
        this.columnRefFactory = factory;
    }

    public static SearchContext create(Optimizer optimizer, OptGroup firstGroup, OptimizationContext oContext,
                                       Scheduler scheduler, SearchVariable variables, OptColumnRefFactory factory) {
        final SearchContext sContext = new SearchContext(optimizer, scheduler, variables, factory);
        TaskGroupOptimization.schedule(sContext, firstGroup, firstGroup.getFirstMultiExpression(), oContext, null);
        return sContext;
    }

    public void schedule(Task task) {
        scheduler.add(task);
    }

    public OptMemo getMemo() { return optimizer.getMemo(); }
    public CostModel getCostModel() { return optimizer.getCostModel(); }
    public List<OptRule> getRules() { return optimizer.getRules(); }
    public SearchVariable getSearchVariables() { return variables; }
    public OptColumnRefFactory getColumnRefFactory() { return columnRefFactory; }
}
