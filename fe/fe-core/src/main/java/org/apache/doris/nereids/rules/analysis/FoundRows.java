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
package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.Set;

public class FoundRows extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalResultSink(logicalProject(logicalAggregate(logicalSubQueryAlias())))
                .thenApply(ctx -> {
                    if (!ctx.cascadesContext.getConnectContext().getSessionVariable().isEnableFoundRows()) {
                        return null;
                    }
                    LogicalResultSink<LogicalProject<LogicalAggregate<LogicalSubQueryAlias<Plan>>>> rs = ctx.root;
                    LogicalProject<LogicalAggregate<LogicalSubQueryAlias<Plan>>> project = rs.child();
                    if (project.getProjects().size() != 1) {
                        return null;
                    }
                    LogicalAggregate<LogicalSubQueryAlias<Plan>> aggr = project.child();
                    Set<AggregateFunction> funcs = aggr.getAggregateFunctions();
                    if (funcs.size() != 1) {
                        return null;
                    }
                    AggregateFunction function = funcs.stream().iterator().next();
                    if (!(function instanceof Count) || !((Count) function).isCountStar() || function.isDistinct()) {
                        return null;
                    }
                    LogicalSubQueryAlias<Plan> subQueryAlias = aggr.child();
                    LogicalPlan innerPlan = (LogicalPlan) subQueryAlias.child();
                    LogicalPlan restoredPlan = ctx.cascadesContext.getConnectContext().getRootPlan();
                    if (equalsTree(innerPlan, restoredPlan)) {
                        long foundRows = ctx.cascadesContext.getConnectContext().getTotalReturnRows();
                        // TODO: return foundRows
                        return null;
                    }
                    return null;
                }).toRule(RuleType.FOUND_ROWS);
    }

    private boolean equalsTree(LogicalPlan plan, LogicalPlan other) {
        if (!plan.equals(other)) {
            return false;
        } else if (plan.children().size() != other.children().size()) {
            return false;
        } else {
            int childSize = plan.children().size();
            for (int i = 0; i < childSize; i ++) {
                LogicalPlan planChild = (LogicalPlan) plan.children().get(i);
                LogicalPlan otherChild = (LogicalPlan) other.children().get(i);
                if (!equalsTree(planChild, otherChild)) {
                    return false;
                }
            }
        }
        return true;
    }
}
