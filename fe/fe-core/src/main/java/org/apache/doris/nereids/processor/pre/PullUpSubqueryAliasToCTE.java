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
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
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
        return createCteForRootNode(unboundResultSink, context);
    }

    @Override
    public Plan visitUnboundTableSink(UnboundTableSink<? extends Plan> unboundTableSink,
            StatementContext context) {
        return createCteForRootNode(unboundTableSink, context);
    }

    @Override
    public Plan visitLogicalFileSink(LogicalFileSink<? extends Plan> logicalFileSink,
            StatementContext context) {
        return createCteForRootNode(logicalFileSink, context);
    }

    @Override
    public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
            StatementContext context) {
        if (findLeadingHintIgnoreSortAndLimit(alias.child())) {
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
            subQueryAlias.accept(new PullUpSubqueryAliasToCTE(), context);
        }
        Plan cte = visitChildren(this, logicalCTE, context);
        if (!aliasQueries.isEmpty()) {
            LogicalCTE newLogicalCTE = (LogicalCTE) cte;
            List<LogicalSubQueryAlias<Plan>> subQueryAliasesOfCte = new ArrayList<>();
            subQueryAliasesOfCte.addAll(logicalCTE.getAliasQueries());
            subQueryAliasesOfCte.addAll(aliasQueries);
            aliasQueries = new ArrayList<>();
            return new LogicalCTE<>(newLogicalCTE.isRecursive(), subQueryAliasesOfCte,
                    (LogicalPlan) newLogicalCTE.child());
        }
        return cte;
    }

    private Plan createCteForRootNode(Plan plan, StatementContext context) {
        Plan topPlan = visitChildren(this, plan, context);
        if (!aliasQueries.isEmpty()) {
            if (topPlan.child(0) instanceof LogicalCTE) {
                LogicalCTE logicalCTE = (LogicalCTE) topPlan.child(0);
                List<LogicalSubQueryAlias<Plan>> subQueryAliases = new ArrayList<>();
                subQueryAliases.addAll(logicalCTE.getAliasQueries());
                subQueryAliases.addAll(aliasQueries);
                return topPlan.withChildren(
                        new LogicalCTE<>(logicalCTE.isRecursive(), subQueryAliases,
                                (LogicalPlan) topPlan.child(0)));
            }
            return topPlan.withChildren(
                    new LogicalCTE<>(false, aliasQueries, (LogicalPlan) topPlan.child(0)));
        }
        return topPlan;
    }

    private boolean findLeadingHintIgnoreSortAndLimit(Plan plan) {
        if (plan instanceof LogicalSelectHint
                && ((LogicalSelectHint) plan).isIncludeHint("Leading")) {
            return true;
        } else if (plan instanceof LogicalLimit || plan instanceof LogicalSort) {
            return findLeadingHintIgnoreSortAndLimit(plan.child(0));
        } else {
            return false;
        }
    }
}
