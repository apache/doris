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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reject score() calls outside a supported scan scoring context.
 * Aggregates cannot consume raw score() calls.
 */
public class CheckScoreUsage implements RewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(CheckScoreUsage.class);
    private static final String SCORE_CONTEXT_ERROR =
            "score() function requires WHERE clause with MATCH function, ORDER BY and LIMIT for optimization, "
                    + "or a supported minimum score filter with MATCH or SEARCH";
    private static final String SCORE_FILTER_CONTEXT_ERROR =
            "score() filter must be in the WHERE clause of a query on one table with a MATCH or SEARCH "
                    + "predicate. It cannot filter the result of an inner, cross or full join, an aggregation, "
                    + "a window or another operator; put the MATCH or SEARCH predicate and the score() filter "
                    + "in a subquery on that table";

    /**
     * Fail a score() filter whose input combines or recomputes rows. score() has no input slot, so
     * predicate pushdown would treat it as a constant and move it below a window, an aggregation
     * or an inner join, changing the result or filtering another scan. Called after analysis,
     * before any pushdown or CTE inlining.
     */
    public static void checkScoreFilterContext(Plan plan, StatementContext statementContext) {
        if (!(plan instanceof LogicalFilter || plan instanceof LogicalHaving || plan instanceof LogicalQualify)) {
            return;
        }
        if (plan.getExpressions().stream().noneMatch(expr -> expr.containsType(Score.class))) {
            return;
        }
        if (!producesScanRows(plan.child(0), statementContext, new HashSet<>())) {
            throw new AnalysisException(SCORE_FILTER_CONTEXT_ERROR);
        }
    }

    // Whether every output row is a row of one scan, so that a filter above can move down to it.
    // Outer, semi and anti joins keep every row of one side.
    private static boolean producesScanRows(Plan plan, StatementContext statementContext,
            Set<CTEId> visitingCtes) {
        if (plan instanceof LogicalCTEConsumer) {
            // The producer decides the rows: a single-use CTE is inlined into its consumer later.
            CTEId cteId = ((LogicalCTEConsumer) plan).getCteId();
            LogicalCTEProducer<? extends Plan> producer = statementContext.getCteProducerByCteId(cteId);
            Preconditions.checkState(producer != null, "CTE producer %s is not analyzed", cteId);
            // A recursive CTE reads itself and has no single scan behind its rows.
            if (!visitingCtes.add(cteId)) {
                return false;
            }
            boolean result = producesScanRows(producer.child(), statementContext, visitingCtes);
            visitingCtes.remove(cteId);
            return result;
        }
        if (plan instanceof LogicalRelation) {
            return true;
        }
        if (plan instanceof LogicalJoin) {
            JoinType joinType = ((LogicalJoin<?, ?>) plan).getJoinType();
            if (joinType.isLeftJoin()) {
                return producesScanRows(plan.child(0), statementContext, visitingCtes);
            }
            return joinType.isRightJoin() && producesScanRows(plan.child(1), statementContext, visitingCtes);
        }
        if (plan instanceof LogicalUnion) {
            // UNION DISTINCT merges equal rows of different scans, which can have different scores.
            return ((LogicalUnion) plan).getQualifier() == Qualifier.ALL
                    && plan.children().stream()
                    .allMatch(child -> producesScanRows(child, statementContext, visitingCtes));
        }
        if (plan instanceof LogicalProject) {
            // Analysis keeps window functions in the project; they depend on the other rows.
            return ((LogicalProject<?>) plan).getProjects().stream()
                    .noneMatch(project -> project.containsType(WindowExpression.class))
                    && producesScanRows(plan.child(0), statementContext, visitingCtes);
        }
        if (plan instanceof LogicalFilter || plan instanceof LogicalSort || plan instanceof LogicalSubQueryAlias
                || plan instanceof LogicalView || plan instanceof LogicalCheckPolicy
                || plan instanceof LogicalSelectHint) {
            return producesScanRows(plan.child(0), statementContext, visitingCtes);
        }
        return false;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalProject(any())
                .when(project -> {
                    boolean hasScore = hasScoreFunction(project);
                    boolean isOptimized = isScoreAlreadyOptimized(project);
                    LOG.debug("Project check - hasScore: " + hasScore + ", isOptimized: " + isOptimized);
                    return hasScore && !isOptimized;
                })
                .then(project -> {
                    throw new AnalysisException(SCORE_CONTEXT_ERROR);
                }).toRule(RuleType.CHECK_SCORE_USAGE),

            // A score() filter on a scan was bound by PushDownScoreTopNIntoOlapScan. One left here
            // filters another operator, e.g. an inner join, and would evaluate the scalar placeholder
            // as zero.
            logicalFilter(any())
                .when(filter -> filter.getConjuncts().stream().anyMatch(this::containsScoreFunction))
                .then(filter -> {
                    throw new AnalysisException(SCORE_FILTER_CONTEXT_ERROR);
                }).toRule(RuleType.CHECK_SCORE_USAGE),

            logicalAggregate(any())
                .when(this::hasScoreInAggregate)
                .then(agg -> {
                    throw new AnalysisException("score() function cannot be used in aggregate functions.");
                }).toRule(RuleType.CHECK_SCORE_USAGE)
        );
    }

    private boolean hasScoreFunction(LogicalProject<?> project) {
        return project.getProjects().stream()
                .anyMatch(projection -> {
                    if (projection instanceof Alias) {
                        return containsScoreFunction(((Alias) projection).child());
                    }
                    return false;
                });
    }

    private boolean hasScoreInAggregate(LogicalAggregate<?> agg) {
        boolean hasScoreInOutput = agg.getOutputExpressions().stream()
                .anyMatch(this::containsScoreFunction);

        boolean hasScoreInGroupBy = agg.getGroupByExpressions().stream()
                .anyMatch(this::containsScoreFunction);

        if (hasScoreInOutput || hasScoreInGroupBy) {
            return true;
        }
        // Aggregate normalization can move score() arguments into the child projection.
        Plan child = agg.child();
        return child instanceof LogicalProject && hasScoreFunction((LogicalProject<?>) child);
    }

    private boolean containsScoreFunction(Expression expr) {
        return expr.containsType(Score.class);
    }

    private boolean isScoreAlreadyOptimized(LogicalProject<?> project) {
        Plan child = project.child();
        if (child instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) child;
            return scan.getVirtualColumns().stream()
                    .anyMatch(virtualCol -> {
                        if (virtualCol instanceof Alias) {
                            Expression childExpr = ((Alias) virtualCol).child();
                            return containsScoreFunction(childExpr);
                        }
                        return false;
                    });
        }
        return false;
    }
}
