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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Check search expression usage - search() can only be used in WHERE filters on single-table OLAP scans.
 * This rule validates that search() expressions only appear in supported contexts.
 * Must run in analysis phase before search() gets optimized away.
 */
public class CheckSearchUsage implements AnalysisRuleFactory {
    private static final Logger LOG = LogManager.getLogger(CheckSearchUsage.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                any().thenApply(ctx -> {
                    Plan plan = ctx.root;
                    checkPlanRecursively(plan);
                    return plan;
                }).toRule(RuleType.CHECK_SEARCH_USAGE)
        );
    }

    private void checkPlanRecursively(Plan plan) {
        // Check if current plan node contains search expressions
        if (containsSearchInPlanExpressions(plan)) {
            validateSearchUsage(plan);
        }

        // Check aggregate nodes specifically for GROUP BY usage
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> agg = (LogicalAggregate<?>) plan;
            for (Expression expr : agg.getGroupByExpressions()) {
                if (containsSearchExpression(expr)) {
                    throw new AnalysisException("search() cannot appear in GROUP BY expressions; "
                            + "search predicates are only supported in WHERE filters on single-table scans");
                }
            }
            for (Expression expr : agg.getOutputExpressions()) {
                if (containsSearchExpression(expr)) {
                    throw new AnalysisException("search() cannot appear in aggregate output expressions; "
                            + "search predicates are only supported in WHERE filters on single-table scans");
                }
            }
        }

        // Check project nodes
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            for (Expression expr : project.getProjects()) {
                if (containsSearchExpression(expr)) {
                    // Only allow if it's the project directly above a filter->scan pattern
                    throw new AnalysisException("search() can only appear in WHERE filters on OLAP scans; "
                            + "projection of search() is not supported");
                }
            }
        }

        // Recursively check children
        for (Plan child : plan.children()) {
            checkPlanRecursively(child);
        }
    }

    private void validateSearchUsage(Plan plan) {
        LOG.debug("validateSearchUsage: {}", plan.treeString());
        if (plan instanceof LogicalFilter) {
            Plan child = plan.child(0);
            if (!isSingleTableScanPipeline(child)) {
                throw new AnalysisException("search() predicate only supports filtering directly on a single "
                        + "table scan; remove joins, subqueries, or additional operators between search() "
                        + "and the target table");
            }
        } else if (!(plan instanceof LogicalProject)) {
            // search() can only appear in LogicalFilter or specific LogicalProject nodes
            throw new AnalysisException("search() predicates are only supported inside WHERE filters on "
                    + "single-table scans");
        }
    }

    private boolean containsSearchInPlanExpressions(Plan plan) {
        for (Expression expr : plan.getExpressions()) {
            if (containsSearchExpression(expr)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSearchExpression(Expression expression) {
        if (expression instanceof Search || expression instanceof SearchExpression) {
            return true;
        }
        for (Expression child : expression.children()) {
            if (containsSearchExpression(child)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSingleTableScanPipeline(Plan plan) {
        Plan current = plan;
        while (true) {
            if (current instanceof LogicalOlapScan) {
                return true;
            }
            if (current.arity() != 1) {
                return false;
            }
            current = current.child(0);
        }
    }
}
