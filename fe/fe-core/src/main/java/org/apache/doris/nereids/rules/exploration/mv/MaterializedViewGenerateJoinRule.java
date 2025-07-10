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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * MaterializedViewGenerateJoinRule
 */
public class MaterializedViewGenerateJoinRule extends AbstractMaterializedViewJoinRule {

    public static MaterializedViewGenerateJoinRule INSTANCE = new MaterializedViewGenerateJoinRule();

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        if (!checkTmpRewrittenPlanIsValid(tempRewritedPlan)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Explode rewriteQueryByView fail",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s, tempRewrittenPlan is %s",
                            queryStructInfo.getExpressions(), materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping, tempRewritedPlan.treeString()));
            return null;
        }
        Optional<LogicalGenerate<Plan>> queryGenerate
                = queryStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalGenerate);
        Optional<LogicalGenerate<Plan>> viewGenerate
                = viewStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalGenerate);
        if (!checkGenerate(queryGenerate.orElse(null), viewGenerate.orElse(null),
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, materializationContext)) {
            return null;
        }
        return super.rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                tempRewritedPlan, materializationContext, cascadesContext);
    }

    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        PlanCheckContext checkContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        Boolean accept = structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext);
        return accept
                && !checkContext.isContainsTopAggregate()
                && checkContext.isContainsTopGenerate() && checkContext.getTopGenerateNum() == 1;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalGenerate(logicalUnary(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_GENERATE_UNARY_JOIN),
                logicalProject(logicalGenerate(logicalUnary(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter)))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_GENERATE_UNARY_JOIN),
                logicalFilter(logicalGenerate(logicalUnary(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter)))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_GENERATE_UNARY_JOIN),
                logicalFilter(logicalProject(logicalGenerate(logicalUnary(logicalJoin(
                        any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_GENERATE_UNARY_JOIN),
                logicalProject(logicalFilter(logicalGenerate(logicalUnary(logicalJoin(
                        any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .when(node -> node instanceof LogicalProject || node instanceof LogicalFilter))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_GENERATE_UNARY_JOIN),
                logicalGenerate(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_GENERATE_JOIN),
                logicalProject(logicalGenerate(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_GENERATE_JOIN),
                logicalFilter(logicalGenerate(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_GENERATE_JOIN),
                logicalProject(logicalFilter(logicalGenerate(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_GENERATE_JOIN),
                logicalFilter(logicalProject(logicalGenerate(logicalJoin(any().when(LogicalPlan.class::isInstance),
                        any().when(LogicalPlan.class::isInstance)))))
                        .thenApplyMultiNoThrow(ctx -> {
                            return rewrite(ctx.root, ctx.cascadesContext);
                        }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_GENERATE_JOIN)
        );
    }
}
