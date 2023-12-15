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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * FoundRows support.
 */
public class FoundRows extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalResultSink(logicalProject(logicalAggregate(logicalSubQueryAlias())))
                .thenApply(ctx -> {
                    String user = ctx.cascadesContext.getConnectContext().getQualifiedUser();
                    if (user == null || !Env.getCurrentEnv().getAuth().isEnableFoundRows(user)
                            || ctx.cascadesContext.getConnectContext().getFoundRowsPlan() == null) {
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
                    LogicalPlan currentPlan = (LogicalPlan) subQueryAlias.child();
                    LogicalPlan foundRowsPlan = ctx.cascadesContext.getConnectContext().getFoundRowsPlan();
                    if (checkPlanTreeEquals(currentPlan, foundRowsPlan)) {
                        ctx.cascadesContext.getConnectContext().setFoundRowsPlan(null);
                        long foundRows = ctx.cascadesContext.getConnectContext().getFoundRows();
                        List<NamedExpression> newProjects = new ArrayList<>();
                        RelationId id = StatementScopeIdGenerator.newRelationId();
                        BigIntLiteral literal = new BigIntLiteral(foundRows);
                        Alias aliasExpr = new Alias(literal, literal.toString());
                        newProjects.add(aliasExpr);

                        LogicalOneRowRelation relation = new LogicalOneRowRelation(id, newProjects);
                        LogicalProject newProject = new LogicalProject(relation.getProjects(), relation);

                        return new LogicalResultSink<>(newProject.getOutputs(), newProject);
                    } else {
                        ctx.cascadesContext.getConnectContext().setFoundRowsPlan(null);
                        return null;
                    }
                }).toRule(RuleType.FOUND_ROWS);
    }

    private boolean checkPlanTreeEquals(LogicalPlan plan, LogicalPlan other) {
        if (!plan.equals(other)) {
            return false;
        } else if (plan.children().size() != other.children().size()) {
            return false;
        } else {
            int childSize = plan.children().size();
            for (int i = 0; i < childSize; i++) {
                LogicalPlan planChild = (LogicalPlan) plan.children().get(i);
                LogicalPlan otherChild = (LogicalPlan) other.children().get(i);
                if (!checkPlanTreeEquals(planChild, otherChild)) {
                    return false;
                }
            }
        }
        return true;
    }
}
