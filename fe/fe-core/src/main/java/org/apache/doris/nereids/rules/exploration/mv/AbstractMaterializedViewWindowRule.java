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
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewWindowRule
 */
public abstract class AbstractMaterializedViewWindowRule extends AbstractMaterializedViewRule {


    /**
     * compensatePredicates should be pulled up through window
     */
    @Override
    protected boolean rewriteQueryByViewPreCheck(MatchMode matchMode, StructInfo queryStructInfo,
            StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan,
            MaterializationContext materializationContext, ComparisonResult comparisonResult) {
        boolean superCheck = super.rewriteQueryByViewPreCheck(matchMode, queryStructInfo, viewStructInfo,
                viewToQuerySlotMapping, tempRewritedPlan, materializationContext, comparisonResult);
        Optional<LogicalWindow<Plan>> logicalWindow =
                queryStructInfo.getTopPlan().collectFirst(LogicalWindow.class::isInstance);
        if (!logicalWindow.isPresent()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Window rewriteQueryByViewPreCheck fail, logical window is not present",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s, tempRewrittenPlan is %s",
                            queryStructInfo.getExpressions(), materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping, tempRewritedPlan.treeString()));
            return false;
        }
        // if compensatePredicates exists, should be pulled up through window
        Set<SlotReference> queryCommonPartitionKeySet = logicalWindow.get()
                .getCommonPartitionKeyFromWindowExpressions();
        for (Expression conjuct : comparisonResult.getQueryExpressions()) {
            if (!queryCommonPartitionKeySet.containsAll(conjuct.getInputSlots())) {
                return false;
            }
        }
        return superCheck;
    }

    /**
     * Rewrite query by view
     *
     * @param matchMode match mode
     * @param queryStructInfo query struct info
     * @param viewStructInfo view struct info
     * @param viewToQuerySlotMapping slot mapping from view to query
     * @param tempRewrittenPlan temporary rewritten plan
     * @param materializationContext materialization context
     * @param cascadesContext cascades context
     * @return rewritten plan
     */
    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewrittenPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        if (!StructInfo.checkWindowTmpRewrittenPlanIsValid(tempRewrittenPlan)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Window rewriteQueryByView fail",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s, tempRewrittenPlan is %s",
                            queryStructInfo.getExpressions(), materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping, tempRewrittenPlan.treeString()));
            return null;
        }
        // Rewrite top projects, represent the query projects by view
        List<Expression> expressionsRewritten = rewriteExpression(
                queryStructInfo.getExpressions(),
                queryStructInfo.getTopPlan(),
                materializationContext.getShuttledExprToScanExprMapping(),
                viewToQuerySlotMapping,
                ImmutableMap.of(), cascadesContext
        );
        // Can not rewrite, bail out
        if (expressionsRewritten.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Rewrite expressions by view in window scan fail",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s", queryStructInfo.getExpressions(),
                            materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping));
            return null;
        }
        return new LogicalProject<>(
                expressionsRewritten.stream()
                        .map(expression -> expression instanceof NamedExpression ? expression : new Alias(expression))
                        .map(NamedExpression.class::cast)
                        .collect(Collectors.toList()), tempRewrittenPlan);
    }
}
