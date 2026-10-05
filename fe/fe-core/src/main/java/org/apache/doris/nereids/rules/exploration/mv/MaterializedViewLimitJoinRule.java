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
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * MaterializedViewLimitJoinRule
 */
public class MaterializedViewLimitJoinRule extends AbstractMaterializedViewJoinRule
        implements AbstractMaterializedViewLimitOrTopNRule {

    public static MaterializedViewLimitJoinRule INSTANCE = new MaterializedViewLimitJoinRule();

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        Plan tempRewritePlan = super.rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo,
                viewToQuerySlotMapping, tempRewritedPlan, materializationContext, cascadesContext);
        if (!StructInfo.checkLimitTmpRewrittenPlanIsValid(tempRewritePlan)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Limit join rewriteQueryByView fail because tempRewritePlan is invalid",
                    () -> String.format("tempRewrittenPlan is %s", tempRewritePlan));
            return null;
        }
        Optional<LogicalLimit<Plan>> queryLimit
                = queryStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalLimit
                && ((LogicalLimit<Plan>) node).getPhase() == LimitPhase.GLOBAL);
        Optional<LogicalLimit<Plan>> viewLimit
                = viewStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalLimit
                && ((LogicalLimit<Plan>) node).getPhase() == LimitPhase.GLOBAL);
        return tryRewriteLimit(queryLimit.orElse(null), viewLimit.orElse(null), tempRewritePlan,
                queryStructInfo, materializationContext);
    }

    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        PlanCheckContext checkContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        return structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext)
                && !checkContext.isContainsTopAggregate()
                && !checkContext.isContainsTopTopN()
                && !checkContext.isContainsTopWindow()
                && checkContext.isContainsTopLimit() && checkContext.getTopLimitNum() == 1;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // because limit spit to two phases
                logicalLimit(logicalLimit(logicalUnary(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter)))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_LIMIT_UNARY_JOIN),
                logicalLimit(logicalLimit(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_LIMIT_JOIN)
        );
    }
}
