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


package org.apache.doris.nereids;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.cost.statistics.StatisticsEstimate;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Context for plan.
 * Abstraction for group expressions and stand-alone expressions/DAGs.
 * A ExpressionHandle is attached to either {@link Plan} or {@link GroupExpression}.
 * Inspired by GPORCA-CExpressionHandle.
 */
public class PlanContext {
    // array of children's derived stats
    private final List<StatisticsEstimate> childrenStats = Lists.newArrayList();
    // attached plan
    private Plan plan;
    // attached group expression
    private GroupExpression groupExpression;
    // statistics of attached plan/gexpr
    private StatisticsEstimate statistics;

    public PlanContext(Plan plan) {
        this.plan = plan;
    }

    public PlanContext(GroupExpression groupExpression) {
        this.groupExpression = groupExpression;
    }

    public Plan getPlan() {
        return plan;
    }

    public GroupExpression getGroupExpression() {
        return groupExpression;
    }

    public List<StatisticsEstimate> getChildrenStats() {
        return childrenStats;
    }

    public StatisticsEstimate getStatistics() {
        return statistics;
    }

    public void setStatistics(StatisticsEstimate stats) {
        this.statistics = stats;
    }

    public StatisticsEstimate getStatisticsWithCheck() {
        Preconditions.checkNotNull(statistics);
        return statistics;
    }

    public StatisticsEstimate getChildStatistics(int index) {
        StatisticsEstimate statistics = childrenStats.get(index);
        Preconditions.checkNotNull(statistics);
        return statistics;
    }
}
