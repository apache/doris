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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AbstractMaterializedViewWindowRule
 * This is responsible for common window function rewriting
 */
public class MaterializedViewWindowAggregateRule extends AbstractMaterializedViewWindowRule {

    public static MaterializedViewWindowAggregateRule INSTANCE = new MaterializedViewWindowAggregateRule();

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        return super.rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                tempRewritedPlan, materializationContext, cascadesContext);
    }

    /**
     * Check window pattern is valid or not
     */
    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        PlanCheckContext checkContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        return structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext)
                && checkContext.isContainsTopAggregate() && checkContext.isContainsTopWindow()
                && checkContext.getTopAggregateNum() <= 1 && checkContext.getTopWindowNum() <= 1
                && !checkContext.isWindowUnderAggregate()
                && !checkContext.isContainsTopTopN() && !checkContext.isContainsTopLimit();
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalWindow(logicalAggregate(any().when(LogicalPlan.class::isInstance)))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_ONLY_WINDOW_AGGREGATE),
                logicalProject(logicalWindow(logicalAggregate(any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_WINDOW_AGGREGATE),
                logicalFilter(logicalWindow(logicalAggregate(any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_WINDOW_AGGREGATE),
                logicalProject(logicalFilter(logicalWindow(logicalAggregate(
                        any().when(LogicalPlan.class::isInstance)))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_WINDOW_AGGREGATE),
                logicalFilter(logicalProject(logicalWindow(logicalAggregate(
                        any().when(LogicalPlan.class::isInstance)))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_WINDOW_AGGREGATE)
        );
    }
}
