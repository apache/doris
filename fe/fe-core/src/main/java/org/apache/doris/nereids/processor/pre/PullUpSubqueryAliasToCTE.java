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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO turnoff pipeline for any dml temporary, remove this pre-process when pipeline-sink is ok.
 */
public class PullUpSubqueryAliasToCTE extends PlanPreprocessor {
    private List<LogicalSubQueryAlias<Plan>> aliasQueries = new ArrayList<>();

    @Override
    public Plan visitUnboundResultSink(UnboundResultSink<? extends Plan> unboundResultSink,
                                       StatementContext context) {
        Plan topPlan = visitChildren(this, unboundResultSink, context);
        if (!aliasQueries.isEmpty()) {
            if (((UnboundResultSink) topPlan).child() instanceof LogicalCTE) {
                LogicalCTE logicalCTE = (LogicalCTE) ((UnboundResultSink) topPlan).child();
                List<LogicalSubQueryAlias<Plan>> subQueryAliases = new ArrayList<>();
                subQueryAliases.addAll(logicalCTE.getAliasQueries());
                subQueryAliases.addAll(aliasQueries);
                return topPlan.withChildren(
                        new LogicalCTE<>(subQueryAliases, (LogicalPlan) ((UnboundResultSink) topPlan).child()));
            }
            return topPlan.withChildren(
                    new LogicalCTE<>(aliasQueries, (LogicalPlan) ((UnboundResultSink) topPlan).child()));
        }
        return topPlan;
    }

    @Override
    public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
                                          StatementContext context) {
        if (alias.child() instanceof LogicalSelectHint
                && ((LogicalSelectHint) alias.child()).isIncludeHint("Leading")) {
            aliasQueries.add((LogicalSubQueryAlias<Plan>) alias);
            List<String> tableName = new ArrayList<>();
            tableName.add(alias.getAlias());
            return new UnboundRelation(StatementScopeIdGenerator.newRelationId(), tableName);
        }
        return alias;
    }

    @Override
    public Plan visitLogicalCTE(LogicalCTE<? extends Plan> logicalCTE, StatementContext context) {
        List<LogicalSubQueryAlias<Plan>> subQueryAliases = logicalCTE.getAliasQueries();
        for (LogicalSubQueryAlias<Plan> subQueryAlias : subQueryAliases) {
            Plan newSubQueryAlias = subQueryAlias.accept(new PullUpSubqueryAliasToCTE(), context);
            if (newSubQueryAlias instanceof LogicalSubQueryAlias) {
                subQueryAlias = (LogicalSubQueryAlias<Plan>) newSubQueryAlias;
            } else {
                subQueryAlias = new LogicalSubQueryAlias<>(subQueryAlias.getAlias(), newSubQueryAlias);
            }
        }
        Plan cte = visitChildren(this, logicalCTE, context);
        if (!aliasQueries.isEmpty()) {
            LogicalCTE newLogicalCTE = (LogicalCTE) cte;
            List<LogicalSubQueryAlias<Plan>> subQueryAliasesOfCte = new ArrayList<>();
            subQueryAliasesOfCte.addAll(logicalCTE.getAliasQueries());
            subQueryAliasesOfCte.addAll(aliasQueries);
            aliasQueries = new ArrayList<>();
            return new LogicalCTE<>(subQueryAliasesOfCte, (LogicalPlan) newLogicalCTE.child());
        }
        return cte;
    }
}
