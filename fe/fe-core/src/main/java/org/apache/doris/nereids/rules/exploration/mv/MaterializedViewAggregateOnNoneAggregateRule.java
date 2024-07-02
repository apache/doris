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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewAggregateRule.AggregateExpressionRewriteContext.ExpressionRewriteMode;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MaterializedViewAggregateOnNoAggregateRule
 */
public class MaterializedViewAggregateOnNoneAggregateRule extends AbstractMaterializedViewAggregateRule {

    public static final MaterializedViewAggregateOnNoneAggregateRule INSTANCE =
            new MaterializedViewAggregateOnNoneAggregateRule();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter(logicalProject(logicalAggregate(any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            LogicalFilter<LogicalProject<LogicalAggregate<Plan>>> root = ctx.root;
                            return rewrite(root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_AGGREGATE_ON_NONE_AGGREGATE),
                logicalAggregate(any().when(LogicalPlan.class::isInstance)).thenApplyMultiNoThrow(ctx -> {
                    LogicalAggregate<Plan> root = ctx.root;
                    return rewrite(root, ctx.cascadesContext);
                }).toRule(RuleType.MATERIALIZED_VIEW_ONLY_AGGREGATE_ON_NONE_AGGREGATE),
                logicalProject(logicalFilter(logicalAggregate(any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            LogicalProject<LogicalFilter<LogicalAggregate<Plan>>> root = ctx.root;
                            return rewrite(root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_AGGREGATE_ON_NONE_AGGREGATE),
                logicalProject(logicalAggregate(any().when(LogicalPlan.class::isInstance))).thenApplyMultiNoThrow(
                        ctx -> {
                            LogicalProject<LogicalAggregate<Plan>> root = ctx.root;
                            return rewrite(root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_AGGREGATE_ON_NONE_AGGREGATE),
                logicalFilter(logicalAggregate(any().when(LogicalPlan.class::isInstance))).thenApplyMultiNoThrow(
                        ctx -> {
                            LogicalFilter<LogicalAggregate<Plan>> root = ctx.root;
                            return rewrite(root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_AGGREGATE_ON_NONE_AGGREGATE));
    }

    @Override
    protected boolean checkMaterializationPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        // any check result of join or scan is true, then return true
        PlanCheckContext joinCheckContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        boolean joinCheckResult = structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, joinCheckContext)
                && !joinCheckContext.isContainsTopAggregate();
        if (joinCheckResult) {
            return true;
        }
        PlanCheckContext scanCheckContext = PlanCheckContext.of(ImmutableSet.of());
        return structInfo.getTopPlan().accept(StructInfo.SCAN_PLAN_PATTERN_CHECKER, scanCheckContext)
                && !scanCheckContext.isContainsTopAggregate();
    }

    @Override
    protected Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> calcInvalidPartitions(
            Plan queryPlan, Plan rewrittenPlan, AsyncMaterializationContext materializationContext,
            CascadesContext cascadesContext) throws AnalysisException {
        Pair<Map<BaseTableInfo, Set<String>>, Map<BaseTableInfo, Set<String>>> invalidPartitions
                = super.calcInvalidPartitions(queryPlan, rewrittenPlan, materializationContext, cascadesContext);
        if (needUnionRewrite(invalidPartitions, cascadesContext)) {
            // if query use some invalid partition in mv, bail out
            return null;
        }
        return invalidPartitions;
    }

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        // check the expression used in group by and group out expression in query
        Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair = splitToTopPlanAndAggregate(queryStructInfo);
        if (queryTopPlanAndAggPair == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Split query to top plan and agg fail",
                    () -> String.format("query plan = %s\n", queryStructInfo.getOriginalPlan().treeString()));
            return null;
        }
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        boolean queryContainsGroupSets = queryAggregate.getSourceRepeat().isPresent();
        if (queryContainsGroupSets) {
            // doesn't support group sets momentarily
            materializationContext.recordFailReason(queryStructInfo,
                    "Query function roll up fail",
                    () -> String.format("query aggregate = %s", queryAggregate.treeString()));
            return null;
        }
        return doRewriteQueryByView(queryStructInfo,
                viewToQuerySlotMapping,
                queryTopPlanAndAggPair,
                tempRewritedPlan,
                materializationContext,
                ExpressionRewriteMode.EXPRESSION_DIRECT_ALL,
                ExpressionRewriteMode.EXPRESSION_DIRECT_ALL);
    }
}
