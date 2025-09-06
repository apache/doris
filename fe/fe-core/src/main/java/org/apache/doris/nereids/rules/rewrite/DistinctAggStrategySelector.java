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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.DistinctAggStrategySelector.DistinctSelectorContext;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Chooses the optimal execution strategy for queries with multiple DISTINCT aggregations.
 *
 * Handles queries like "SELECT COUNT(DISTINCT c1), COUNT(DISTINCT c2) FROM t" by selecting between:
 * - CTE decomposition: Splits into multiple CTEs, each computing one DISTINCT aggregate
 * - Multi-DISTINCT function: Processes all distinct function use multi distinct function
 *
 * Selection criteria includes:
 * - Number of distinct aggregates
 * - Estimated cardinality of distinct values
 * - Available memory resources
 * - Query complexity
 */
public class DistinctAggStrategySelector extends DefaultPlanRewriter<DistinctSelectorContext>
        implements CustomRewriter {
    public static DistinctAggStrategySelector INSTANCE = new DistinctAggStrategySelector();

    /**DistinctSplitContext*/
    public static class DistinctSelectorContext {
        List<LogicalCTEProducer<? extends Plan>> cteProducerList;
        StatementContext statementContext;
        CascadesContext cascadesContext;

        public DistinctSelectorContext(StatementContext statementContext, CascadesContext cascadesContext) {
            this.statementContext = statementContext;
            this.cteProducerList = new ArrayList<>();
            this.cascadesContext = cascadesContext;
        }
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        DistinctSelectorContext ctx = new DistinctSelectorContext(
                jobContext.getCascadesContext().getStatementContext(), jobContext.getCascadesContext());
        plan = plan.accept(this, ctx);
        for (int i = ctx.cteProducerList.size() - 1; i >= 0; i--) {
            LogicalCTEProducer<? extends Plan> producer = ctx.cteProducerList.get(i);
            plan = new LogicalCTEAnchor<>(producer.getCteId(), producer, plan);
        }
        return plan;
    }

    @Override
    public Plan visitLogicalCTEAnchor(
            LogicalCTEAnchor<? extends Plan, ? extends Plan> anchor, DistinctSelectorContext ctx) {
        Plan child1 = anchor.child(0).accept(this, ctx);
        DistinctSelectorContext consumerContext =
                new DistinctSelectorContext(ctx.statementContext, ctx.cascadesContext);
        Plan child2 = anchor.child(1).accept(this, consumerContext);
        for (int i = consumerContext.cteProducerList.size() - 1; i >= 0; i--) {
            LogicalCTEProducer<? extends Plan> producer = consumerContext.cteProducerList.get(i);
            child2 = new LogicalCTEAnchor<>(producer.getCteId(), producer, child2);
        }
        return anchor.withChildren(ImmutableList.of(child1, child2));
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, DistinctSelectorContext ctx) {
        Plan newChild = agg.child().accept(this, ctx);
        agg = agg.withChildren(ImmutableList.of(newChild));
        // not process：
        // count(distinct a,b);
        // count(distinct a), sum(distinct a);
        // count(distinct a)
        // process:
        // count(distinct a,b), count(distinct a,c)
        // count(distinct a), sum(distinct b)
        if (agg.distinctFuncNum() < 2 || agg.getDistinctArguments().size() < 2) {
            return agg;
        }
        if (shouldUseMultiDistinct(agg)) {
            return MultiDistinctFunctionStrategy.rewrite(agg);
        } else {
            return SplitMultiDistinctStrategy.rewrite(agg, ctx);
        }
    }

    private boolean shouldUseMultiDistinct(LogicalAggregate<? extends Plan> agg) {
        if (AggregateUtils.containsCountDistinctMultiExpr(agg)) {
            return false;
        }
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return true;
        }
        switch (ctx.getSessionVariable().multiDistinctStrategy) {
            case 1:
                return true;
            case 2:
                return false;
            default:
                break;
        }
        if (agg.getStats() == null || agg.child().getStats() == null) {
            StatsDerive derive = new StatsDerive(false);
            agg.accept(derive, new DeriveContext());
        }
        Statistics aggStats = agg.getStats();
        Statistics childStats = agg.child().getStats();
        double row = childStats.getRowCount();
        if (agg.getGroupByExpressions().isEmpty()) {
            for (Expression distinctArgument : agg.getDistinctArguments()) {
                ColumnStatistic columnStatistic = childStats.findColumnStatistics(distinctArgument);
                if (columnStatistic == null || columnStatistic.isUnKnown) {
                    return false;
                }
                if (columnStatistic.ndv >= row * AggregateUtils.MID_CARDINALITY_THRESHOLD) {
                    // If there is a distinct key with high ndv, then do not use multi distinct
                    return false;
                }
            }
        } else {
            if (AggregateUtils.hasUnknownStatistics(agg.getGroupByExpressions(), childStats)) {
                return false;
            }
            // The joint ndv of Group by key is high, so multi_distinct is not selected;
            if (aggStats.getRowCount() >= row * AggregateUtils.LOW_CARDINALITY_THRESHOLD) {
                return false;
            }
            // // TODO:Also need to consider the size of the group by key.
            // //If the group by key size is larger than
            // //a certain threshold, the network distribution of CTE will be very slow,
            // so using multi_distinct will be better.
            // double groupByKeyByte = 0;
            // for (Expression groupByKey : agg.getGroupByExpressions()) {
            //     ColumnStatistic columnStatistic = childStats.findColumnStatistics(groupByKey);
            //     groupByKeyByte += columnStatistic.avgSizeByte;
            // }
            // // If ndv is large and group by key size is also large, should I use cte or multi distinct?
            // if (groupByKeyByte < 20) {
            //     return false;
            // }
        }
        return true;
    }
}
