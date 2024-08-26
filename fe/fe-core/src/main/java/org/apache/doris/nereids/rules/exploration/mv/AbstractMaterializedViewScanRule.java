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
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This is responsible for single table rewriting according to different pattern
 * */
public abstract class AbstractMaterializedViewScanRule extends AbstractMaterializedViewRule {

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping targetToSourceMapping,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        // Rewrite top projects, represent the query projects by view
        List<Expression> expressionsRewritten = rewriteExpression(
                queryStructInfo.getExpressions(),
                queryStructInfo.getTopPlan(),
                materializationContext.getShuttledExprToScanExprMapping(),
                targetToSourceMapping,
                queryStructInfo.getTableBitSet()
        );
        // Can not rewrite, bail out
        if (expressionsRewritten.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Rewrite expressions by view in scan fail",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s", queryStructInfo.getExpressions(),
                            materializationContext.getShuttledExprToScanExprMapping(),
                            targetToSourceMapping));
            return null;
        }
        return new LogicalProject<>(
                expressionsRewritten.stream()
                        .map(expression -> expression instanceof NamedExpression ? expression : new Alias(expression))
                        .map(NamedExpression.class::cast)
                        .collect(Collectors.toList()),
                tempRewritedPlan);
    }

    /**
     * Check scan is whether valid or not. Support join's input only support project, filter, join,
     * logical relation, simple aggregate node. Con not have aggregate above on join.
     * Join condition should be slot reference equals currently.
     */
    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        PlanCheckContext checkContext = PlanCheckContext.of(ImmutableSet.of());
        return structInfo.getTopPlan().accept(StructInfo.SCAN_PLAN_PATTERN_CHECKER, checkContext)
                && !checkContext.isContainsTopAggregate();
    }
}
