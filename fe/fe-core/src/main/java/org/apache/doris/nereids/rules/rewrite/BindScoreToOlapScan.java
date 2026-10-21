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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Bind every score() call to the score column of the scan whose rows its MATCH and SEARCH
 * predicates filter.
 *
 * <p>CheckScoreUsage marked each score() call and the MATCH and SEARCH predicates of the same
 * SELECT with one score id. This rewriter runs after views and CTEs are inlined, and before window
 * expressions are extracted and filters are pushed down. For each score id it finds the one olap
 * scan whose columns those predicates read, adds to the scan a hidden score column computed for
 * every row the scan returns, and replaces the score() calls with that column, passing it through
 * the projections in between. From then on the score is an ordinary column for windows, joins and
 * filter push down. PushDownScoreRangeIntoOlapScan and PushDownScoreTopNIntoOlapScan only reduce
 * how much of it the scan has to compute.
 */
public class BindScoreToOlapScan implements CustomRewriter {
    public static final String UNRESOLVED_SCAN_ERROR = "score() requires the MATCH and SEARCH predicates "
            + "of its WHERE clause to filter the columns of one table, not of a set operation, an aggregation "
            + "or a CTE that is used more than once";
    public static final String SHARED_SCAN_ERROR = "score() is called by two SELECTs whose MATCH or SEARCH "
            + "predicates filter the same table, which computes only one score: call score() in one of them";
    public static final String SCORE_NOT_AVAILABLE_ERROR = "score() is not available above GROUP BY: "
            + "use it in an aggregate function or as a grouping key";
    // Thrown by ExpressionTranslator, which finds where each predicate ends up.
    public static final String UNEVALUATED_PREDICATE_ERROR = "score() requires the scan of its table to "
            + "evaluate each MATCH and SEARCH predicate of its WHERE clause, which it cannot for a predicate in an "
            + "OR with conditions on other tables, or on the result of a LIMIT, a window function or the nullable "
            + "side of an outer join";

    private final Map<RelationId, ExprId> scanScoreIds = new HashMap<>();
    private final Map<RelationId, Alias> scoreColumns = new HashMap<>();

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!CheckScoreUsage.containsScore(plan)) {
            return plan;
        }
        return bind(plan, ImmutableMap.of());
    }

    // scope maps every score id that an ancestor calls to the scan that computes it.
    private Plan bind(Plan plan, Map<ExprId, RelationId> scope) {
        Map<ExprId, RelationId> childScope = scope;
        for (ExprId scoreId : scoreIds(plan)) {
            if (!childScope.containsKey(scoreId)) {
                RelationId scan = resolveScan(plan, scoreId);
                ExprId boundScoreId = scanScoreIds.putIfAbsent(scan, scoreId);
                if (boundScoreId != null && !boundScoreId.equals(scoreId)) {
                    throw new AnalysisException(SHARED_SCAN_ERROR);
                }
                Map<ExprId, RelationId> newScope = new HashMap<>(childScope);
                newScope.put(scoreId, scan);
                childScope = newScope;
            }
        }
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (Plan child : plan.children()) {
            Plan newChild = bind(child, childScope);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        Plan newPlan = changed ? plan.withChildren(newChildren.build()) : plan;
        if (newPlan instanceof LogicalOlapScan) {
            return addScoreColumn((LogicalOlapScan) newPlan);
        }
        if (newPlan instanceof LogicalProject) {
            newPlan = passScoreColumns((LogicalProject<?>) newPlan, scope);
        }
        return replaceScore(newPlan, childScope);
    }

    private static Set<ExprId> scoreIds(Plan plan) {
        Set<ExprId> scoreIds = new LinkedHashSet<>();
        for (Expression expression : plan.getExpressions()) {
            for (Score score : expression.<Score>collect(Score.class::isInstance)) {
                scoreIds.add(score.getScoreId().get());
            }
        }
        return scoreIds;
    }

    // The olap scan whose rows the MATCH and SEARCH predicates with the score id filter.
    private static RelationId resolveScan(Plan plan, ExprId scoreId) {
        Set<RelationId> scans = new HashSet<>();
        collectScoredScans(plan, scoreId, scans);
        if (scans.size() != 1) {
            throw new AnalysisException(UNRESOLVED_SCAN_ERROR);
        }
        return scans.iterator().next();
    }

    private static void collectScoredScans(Plan plan, ExprId scoreId, Set<RelationId> scans) {
        if (plan instanceof LogicalFilter) {
            Plan input = plan.child(0);
            for (Expression conjunct : ((LogicalFilter<?>) plan).getConjuncts()) {
                for (Expression predicate : conjunct.<Expression>collect(e -> definesScore((Expression) e, scoreId))) {
                    if (predicate instanceof Search) {
                        // search() reads its fields from the scan below it, see RewriteSearchToSlots.
                        collectOlapScans(input, scans);
                    } else {
                        for (Slot slot : predicate.getInputSlots()) {
                            scans.add(findScan(input, slot.getExprId())
                                    .orElseThrow(() -> new AnalysisException(UNRESOLVED_SCAN_ERROR)));
                        }
                    }
                }
            }
        }
        for (Plan child : plan.children()) {
            collectScoredScans(child, scoreId, scans);
        }
    }

    private static boolean definesScore(Expression expression, ExprId scoreId) {
        Optional<ExprId> predicateScoreId = Optional.empty();
        if (expression instanceof Match) {
            predicateScoreId = ((Match) expression).getScoreId();
        } else if (expression instanceof Search) {
            predicateScoreId = ((Search) expression).getScoreId();
        }
        return predicateScoreId.isPresent() && predicateScoreId.get().equals(scoreId);
    }

    private static void collectOlapScans(Plan plan, Set<RelationId> scans) {
        if (plan instanceof LogicalOlapScan) {
            scans.add(((LogicalOlapScan) plan).getRelationId());
        }
        for (Plan child : plan.children()) {
            collectOlapScans(child, scans);
        }
    }

    // The olap scan that outputs the slot as one of its columns, following projections that pass
    // or rename it. Empty when another operator computes the slot, e.g. an aggregation.
    private static Optional<RelationId> findScan(Plan plan, ExprId slotId) {
        if (plan instanceof LogicalOlapScan) {
            return plan.getOutputExprIdSet().contains(slotId)
                    ? Optional.of(((LogicalOlapScan) plan).getRelationId()) : Optional.empty();
        }
        if (plan instanceof LogicalProject) {
            for (NamedExpression project : ((LogicalProject<?>) plan).getProjects()) {
                if (!project.getExprId().equals(slotId)) {
                    continue;
                }
                if (project instanceof Slot) {
                    return findScan(plan.child(0), slotId);
                }
                Expression producer = ((Alias) project).child();
                return producer instanceof Slot
                        ? findScan(plan.child(0), ((Slot) producer).getExprId()) : Optional.empty();
            }
            return Optional.empty();
        }
        if (plan instanceof LogicalAggregate) {
            return Optional.empty();
        }
        for (Plan child : plan.children()) {
            if (child.getOutputExprIdSet().contains(slotId)) {
                return findScan(child, slotId);
            }
        }
        return Optional.empty();
    }

    private LogicalOlapScan addScoreColumn(LogicalOlapScan scan) {
        ExprId scoreId = scanScoreIds.get(scan.getRelationId());
        if (scoreId == null) {
            return scan;
        }
        Alias scoreColumn = new Alias(new Score().withScoreId(scoreId), "__score");
        scoreColumns.put(scan.getRelationId(), scoreColumn);
        // A zero score limit scores every row the scan returns.
        return scan.appendVirtualColumnsAndTopN(ImmutableList.of(scoreColumn),
                scan.getAnnOrderKeys(), scan.getAnnLimit(),
                ImmutableList.of(new OrderKey(scoreColumn.toSlot(), false, false)), Optional.of(0L),
                Optional.empty());
    }

    // A projection between a score column and the score() calls above it passes the column on.
    private Plan passScoreColumns(LogicalProject<?> project, Map<ExprId, RelationId> scope) {
        Set<Slot> input = project.child().getOutputSet();
        Set<Slot> output = project.getOutputSet();
        List<NamedExpression> projects = new ArrayList<>(project.getProjects());
        for (RelationId scan : new LinkedHashSet<>(scope.values())) {
            Alias scoreColumn = scoreColumns.get(scan);
            if (scoreColumn != null && input.contains(scoreColumn.toSlot())
                    && !output.contains(scoreColumn.toSlot())) {
                projects.add(scoreColumn.toSlot());
            }
        }
        return projects.size() == project.getProjects().size() ? project : project.withProjects(projects);
    }

    private Plan replaceScore(Plan plan, Map<ExprId, RelationId> scope) {
        if (plan.getExpressions().stream().noneMatch(expression -> expression.containsType(Score.class))) {
            return plan;
        }
        Set<Slot> input = new HashSet<>();
        plan.children().forEach(child -> input.addAll(child.getOutputSet()));
        Function<Expression, Expression> replace = expression -> expression.rewriteDownShortCircuit(node -> {
            if (!(node instanceof Score)) {
                return node;
            }
            Slot scoreColumn = scoreColumns.get(scope.get(((Score) node).getScoreId().get())).toSlot();
            if (!input.contains(scoreColumn)) {
                throw new AnalysisException(SCORE_NOT_AVAILABLE_ERROR);
            }
            return scoreColumn;
        });
        if (plan instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            return filter.withConjuncts(filter.getConjuncts().stream().map(replace)
                    .collect(ImmutableSet.toImmutableSet()));
        }
        if (plan instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            return project.withProjects(replaceNamed(project.getProjects(), replace));
        }
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> aggregate = (LogicalAggregate<?>) plan;
            List<Expression> groupBy = aggregate.getGroupByExpressions().stream().map(replace)
                    .collect(ImmutableList.toImmutableList());
            return aggregate.withGroupByAndOutput(groupBy, replaceNamed(aggregate.getOutputExpressions(), replace));
        }
        if (plan instanceof LogicalSort) {
            LogicalSort<?> sort = (LogicalSort<?>) plan;
            return sort.withOrderKeys(sort.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(replace.apply(orderKey.getExpr())))
                    .collect(ImmutableList.toImmutableList()));
        }
        throw new AnalysisException(CheckScoreUsage.UNSUPPORTED_CLAUSE_ERROR);
    }

    private static List<NamedExpression> replaceNamed(List<NamedExpression> expressions,
            Function<Expression, Expression> replace) {
        return expressions.stream()
                .map(expression -> (NamedExpression) replace.apply(expression))
                .collect(ImmutableList.toImmutableList());
    }
}
