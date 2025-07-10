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
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * MaterializedViewExplodeRule this is used to rewrite the explode operator in materialized view.
 */
public class MaterializedViewGenerateScanRule extends AbstractMaterializedViewScanRule {

    public static final MaterializedViewGenerateScanRule INSTANCE = new MaterializedViewGenerateScanRule();

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping targetToSourceMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        Optional<LogicalGenerate<Plan>> queryGenerate
                = queryStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalGenerate);
        Optional<LogicalGenerate<Plan>> viewGenerate
                = viewStructInfo.getTopPlan().collectFirst(node -> node instanceof LogicalGenerate);
        if (!checkGenerate(queryGenerate.orElse(null), viewGenerate.orElse(null),
                queryStructInfo, viewStructInfo, targetToSourceMapping, materializationContext)) {
            return null;
        }
        return super.rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, targetToSourceMapping,
                tempRewritedPlan, materializationContext, cascadesContext);
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalGenerate(
                        subTree(LogicalProject.class, LogicalFilter.class, LogicalCatalogRelation.class))
                        .thenApplyMultiNoThrow(
                                ctx -> {
                                    return rewrite(ctx.root, ctx.cascadesContext);
                                }).toRule(RuleType.MATERIALIZED_VIEW_EXPLODE_SCAN),
                logicalProject(logicalGenerate(
                        subTree(LogicalProject.class, LogicalFilter.class, LogicalCatalogRelation.class)))
                        .thenApplyMultiNoThrow(
                                ctx -> {
                                    return rewrite(ctx.root, ctx.cascadesContext);
                                }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_EXPLODE_SCAN),
                logicalFilter(logicalGenerate(
                        subTree(LogicalProject.class, LogicalFilter.class, LogicalCatalogRelation.class)))
                        .thenApplyMultiNoThrow(
                                ctx -> {
                                    return rewrite(ctx.root, ctx.cascadesContext);
                                }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_EXPLODE_SCAN),
                logicalProject(logicalFilter(logicalGenerate(
                        subTree(LogicalProject.class, LogicalFilter.class, LogicalCatalogRelation.class))))
                        .thenApplyMultiNoThrow(
                                ctx -> {
                                    return rewrite(ctx.root, ctx.cascadesContext);
                                }).toRule(RuleType.MATERIALIZED_VIEW_PROJECT_FILTER_EXPLODE_SCAN),
                logicalFilter(logicalProject(logicalGenerate(
                        subTree(LogicalProject.class, LogicalFilter.class, LogicalCatalogRelation.class))))
                        .thenApplyMultiNoThrow(
                                ctx -> {
                                    return rewrite(ctx.root, ctx.cascadesContext);
                                }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_EXPLODE_SCAN));
    }
}
