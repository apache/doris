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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Bind score() to the olap scan that computes it.
 *
 * <p>score() is the relevance of a row for the MATCH and SEARCH predicates in the WHERE clause of
 * its SELECT, and only the olap scan that evaluates those predicates computes it. So score() is
 * supported in a SELECT that reads one olap table and has a MATCH or SEARCH predicate in its WHERE
 * clause:
 * <ul>
 *   <li>in the WHERE clause, as one minimum score: score() > x or score() >= x;</li>
 *   <li>in the select list, window functions and ORDER BY of a SELECT that does not aggregate.</li>
 * </ul>
 * A SELECT in a subquery, view, CTE or set operation has to ORDER BY score() with a LIMIT, which keeps
 * the predicates of the outer query away from its scan. A correlated subquery cannot call score(), because
 * decorrelation can remove that LIMIT. Any other use of score() is rejected.
 *
 * <p>This rule runs before subquery aliases are removed. It adds a score column to the scan, which
 * scores every row the scan returns, and replaces the score() calls of the SELECT with it.
 * PushDownScoreRangeIntoOlapScan and PushDownScoreTopNIntoOlapScan push the minimum score and a
 * TopN on the score into the scan.
 */
public class BindScoreToOlapScan implements CustomRewriter {
    public static final String UNSUPPORTED_SELECT_ERROR = "score() can only be used in a SELECT that reads one "
            + "table and has a MATCH or SEARCH predicate in its WHERE clause";
    public static final String NESTED_SELECT_ERROR = "score() in a subquery, view, CTE or set operation "
            + "requires ORDER BY score() with LIMIT";
    public static final String CORRELATED_SUBQUERY_ERROR = "score() cannot be used in a correlated subquery";
    public static final String AGGREGATE_ERROR = "score() can only be used in the WHERE clause of a SELECT "
            + "with GROUP BY, aggregate functions or DISTINCT";
    public static final String HAVING_OR_QUALIFY_ERROR = "score(), MATCH and SEARCH cannot be used in HAVING "
            + "or QUALIFY of a SELECT that calls score()";
    public static final String ONE_RANGE_ERROR = "Only one score() range predicate is supported in WHERE clause";
    public static final String RANGE_FORM_ERROR = "score() predicate in WHERE clause must be in the form of "
            + "'score() > literal' or 'score() >= literal'";
    public static final String TOP_LEVEL_RANGE_ERROR = "score() predicate must be a top-level AND condition "
            + "in WHERE clause";

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!plan.anyMatch(node -> callsScore((Plan) node))) {
            return plan;
        }
        return bindSelect(plan, true);
    }

    /** The score column that this rule added to the scan. */
    public static Optional<Slot> findScoreSlot(LogicalOlapScan scan) {
        return scan.getVirtualColumns().stream()
                .filter(column -> column instanceof Alias && ((Alias) column).child() instanceof Score)
                .map(NamedExpression::toSlot)
                .findFirst();
    }

    private Plan bindSelect(Plan root, boolean outermost) {
        Plan plan = bindNestedSelects(root);
        if (!selectCallsScore(plan)) {
            return plan;
        }
        List<Plan> path = pathToScan(plan);
        if (path.isEmpty()) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
        check(path, outermost);
        return bind(path);
    }

    private Plan bindNestedSelects(Plan plan) {
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (int i = 0; i < plan.arity(); i++) {
            Plan child = plan.child(i);
            // Decorrelation can remove the LIMIT and ORDER BY of a SELECT in a correlated subquery.
            if (plan instanceof LogicalApply && i == 1 && ((LogicalApply<?, ?>) plan).isCorrelated()
                    && child.anyMatch(node -> callsScore((Plan) node))) {
                throw new AnalysisException(CORRELATED_SUBQUERY_ERROR);
            }
            Plan newChild = startsSelect(plan, i) ? bindSelect(child, false) : bindNestedSelects(child);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? plan.withChildren(newChildren.build()) : plan;
    }

    // Subqueries, views, CTEs and the inputs of set operations are SELECTs of their own, a table
    // alias is not.
    private static boolean startsSelect(Plan parent, int childIndex) {
        if (parent instanceof LogicalSubQueryAlias) {
            return !isTable(parent.child(0));
        }
        return parent instanceof LogicalView || parent instanceof LogicalCTEProducer
                || parent instanceof LogicalSetOperation || parent instanceof LogicalRecursiveUnionAnchor
                || parent instanceof LogicalRecursiveUnionProducer
                || parent instanceof LogicalCTEAnchor && childIndex == 0
                || parent instanceof LogicalApply && childIndex == 1;
    }

    // A relation, also under the delete sign filter that analysis adds to a UNIQUE table.
    private static boolean isTable(Plan plan) {
        if (plan instanceof LogicalFilter && plan.child(0) instanceof LogicalOlapScan) {
            Set<Expression> conjuncts = ((LogicalFilter<?>) plan).getConjuncts();
            Expression conjunct = conjuncts.iterator().next();
            return conjuncts.size() == 1 && conjunct instanceof EqualTo && conjunct.child(0) instanceof Slot
                    && ((Slot) conjunct.child(0)).getName().equals(Column.DELETE_SIGN);
        }
        return plan instanceof LogicalRelation;
    }

    private static boolean selectCallsScore(Plan plan) {
        if (callsScore(plan)) {
            return true;
        }
        for (int i = 0; i < plan.arity(); i++) {
            if (!startsSelect(plan, i) && selectCallsScore(plan.child(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean callsScore(Plan plan) {
        return plan.getExpressions().stream().anyMatch(expression -> expression.containsType(Score.class));
    }

    // The plan nodes from the root of the SELECT down to its olap scan; empty when the SELECT reads
    // anything else than one olap table, e.g. a join, a subquery, a lateral view or a table stream,
    // which is read by separate scans of its table and of its changes.
    private static List<Plan> pathToScan(Plan root) {
        List<Plan> path = new ArrayList<>();
        Plan node = root;
        while (!(node instanceof LogicalOlapScan)) {
            path.add(node);
            if (node instanceof LogicalCTEAnchor) {
                node = node.child(1);
            } else if (node instanceof LogicalSink || node instanceof LogicalLimit || node instanceof LogicalSort
                    || node instanceof LogicalProject || node instanceof LogicalFilter
                    || node instanceof LogicalAggregate || node instanceof LogicalRepeat
                    || node instanceof LogicalSubQueryAlias && !startsSelect(node, 0)) {
                node = node.child(0);
            } else {
                return ImmutableList.of();
            }
        }
        if (node instanceof LogicalOlapTableStreamScan) {
            return ImmutableList.of();
        }
        path.add(node);
        return path;
    }

    private static void check(List<Plan> path, boolean outermost) {
        // The WHERE clause is the filters right above the scan, around its table alias.
        int clause = path.size() - 2;
        List<Expression> where = new ArrayList<>();
        for (; clause >= 0 && (path.get(clause) instanceof LogicalFilter
                || path.get(clause) instanceof LogicalSubQueryAlias); clause--) {
            if (path.get(clause) instanceof LogicalFilter) {
                where.addAll(((LogicalFilter<?>) path.get(clause)).getConjuncts());
            }
        }
        if (where.stream().noneMatch(conjunct -> conjunct.containsType(Match.class, Search.class))) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
        List<Expression> ranges = where.stream()
                .filter(conjunct -> conjunct.containsType(Score.class))
                .collect(Collectors.toList());
        if (ranges.size() > 1) {
            throw new AnalysisException(ONE_RANGE_ERROR);
        }
        ranges.forEach(BindScoreToOlapScan::checkRange);

        List<Plan> aboveWhere = path.subList(0, clause + 1);
        boolean aggregates = aboveWhere.stream().anyMatch(LogicalAggregate.class::isInstance);
        // HAVING and QUALIFY can also read score(), MATCH and SEARCH through aliases of the SELECT.
        Set<Slot> scoringAliases = new HashSet<>();
        for (int i = aboveWhere.size() - 1; i >= 0; i--) {
            for (Expression expression : aboveWhere.get(i).getExpressions()) {
                if (expression instanceof NamedExpression
                        && (expression.containsType(Score.class, Match.class, Search.class)
                                || expression.getInputSlots().stream().anyMatch(scoringAliases::contains))) {
                    scoringAliases.add(((NamedExpression) expression).toSlot());
                }
            }
        }
        for (Plan node : aboveWhere) {
            if (node instanceof LogicalFilter && ((LogicalFilter<?>) node).getConjuncts().stream().anyMatch(
                    conjunct -> conjunct.containsType(Score.class, Match.class, Search.class)
                            || conjunct.getInputSlots().stream().anyMatch(scoringAliases::contains))) {
                throw new AnalysisException(HAVING_OR_QUALIFY_ERROR);
            }
            if (aggregates && callsScore(node)) {
                throw new AnalysisException(AGGREGATE_ERROR);
            }
        }
        if (!outermost && !ordersByScoreWithLimit(aboveWhere)) {
            throw new AnalysisException(NESTED_SELECT_ERROR);
        }
    }

    // score() > x or score() >= x, also written x < score() or x <= score(). A cast of score() to
    // FLOAT or DOUBLE keeps its value.
    private static void checkRange(Expression conjunct) {
        if (!(conjunct instanceof ComparisonPredicate)) {
            throw new AnalysisException(TOP_LEVEL_RANGE_ERROR);
        }
        ComparisonPredicate comparison = (ComparisonPredicate) conjunct;
        boolean scoreAbove = (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual)
                && isScore(comparison.left()) && isConstant(comparison.right());
        boolean scoreBelow = (comparison instanceof LessThan || comparison instanceof LessThanEqual)
                && isScore(comparison.right()) && isConstant(comparison.left());
        if (!scoreAbove && !scoreBelow) {
            throw new AnalysisException(RANGE_FORM_ERROR);
        }
    }

    private static boolean isScore(Expression expression) {
        if (expression instanceof Cast) {
            return expression.getDataType().isFloatLikeType() && isScore(((Cast) expression).child());
        }
        return expression instanceof Score;
    }

    private static boolean isConstant(Expression expression) {
        return expression.getInputSlots().isEmpty() && !expression.containsType(Score.class);
    }

    // A TopN on the score above the SELECT, so that no outer predicate is pushed into the scan. Analysis
    // restores the select list between LIMIT and ORDER BY when HAVING or ORDER BY reads another column.
    private static boolean ordersByScoreWithLimit(List<Plan> aboveWhere) {
        for (int i = 0; i < aboveWhere.size(); i++) {
            if (!(aboveWhere.get(i) instanceof LogicalLimit)) {
                continue;
            }
            int sort = i + 1;
            while (sort < aboveWhere.size() && aboveWhere.get(sort) instanceof LogicalProject
                    && ((LogicalProject<?>) aboveWhere.get(sort)).isAllSlots()) {
                sort++;
            }
            if (sort < aboveWhere.size() && aboveWhere.get(sort) instanceof LogicalSort) {
                List<OrderKey> orderKeys = ((LogicalSort<?>) aboveWhere.get(sort)).getOrderKeys();
                return orderKeys.size() == 1 && isScoreOrAlias(orderKeys.get(0).getExpr(), aboveWhere);
            }
        }
        return false;
    }

    private static boolean isScoreOrAlias(Expression expression, List<Plan> aboveWhere) {
        if (expression instanceof Score) {
            return true;
        }
        return aboveWhere.stream()
                .filter(LogicalProject.class::isInstance)
                .flatMap(project -> ((LogicalProject<?>) project).getProjects().stream())
                .anyMatch(project -> project instanceof Alias && project.toSlot().equals(expression)
                        && ((Alias) project).child() instanceof Score);
    }

    // Adds the score column to the scan and replaces the score() calls above it with that column.
    private static Plan bind(List<Plan> path) {
        LogicalOlapScan scan = (LogicalOlapScan) path.get(path.size() - 1);
        Alias scoreColumn = new Alias(new Score(), "__score");
        Slot scoreSlot = scoreColumn.toSlot();
        // A zero score limit scores every row the scan returns.
        Plan newPlan = scan.appendVirtualColumnsAndTopN(ImmutableList.of(scoreColumn),
                scan.getAnnOrderKeys(), scan.getAnnLimit(),
                ImmutableList.of(new OrderKey(scoreSlot, false, false)), Optional.of(0L), Optional.empty());
        Function<Expression, Expression> replace = expression -> expression.rewriteDownShortCircuit(
                node -> node instanceof Score ? scoreSlot : node);
        for (int i = path.size() - 2; i >= 0; i--) {
            Plan node = path.get(i);
            List<Plan> children = new ArrayList<>(node.children());
            children.set(node instanceof LogicalCTEAnchor ? 1 : 0, newPlan);
            newPlan = node.withChildren(children);
            if (newPlan instanceof LogicalFilter) {
                LogicalFilter<?> filter = (LogicalFilter<?>) newPlan;
                newPlan = filter.withConjuncts(filter.getConjuncts().stream().map(replace)
                        .collect(ImmutableSet.toImmutableSet()));
            } else if (newPlan instanceof LogicalSort) {
                LogicalSort<?> sort = (LogicalSort<?>) newPlan;
                newPlan = sort.withOrderKeys(sort.getOrderKeys().stream()
                        .map(orderKey -> orderKey.withExpression(replace.apply(orderKey.getExpr())))
                        .collect(ImmutableList.toImmutableList()));
            } else if (newPlan instanceof LogicalProject) {
                LogicalProject<?> project = (LogicalProject<?>) newPlan;
                List<NamedExpression> projects = project.getProjects().stream()
                        .map(expression -> (NamedExpression) replace.apply(expression))
                        .collect(Collectors.toCollection(ArrayList::new));
                // A node above the projection reads the score, e.g. ORDER BY score().
                if (path.subList(0, i).stream().anyMatch(BindScoreToOlapScan::callsScore)
                        && !projects.contains(scoreSlot)) {
                    projects.add(scoreSlot);
                }
                newPlan = project.withProjects(projects);
            }
        }
        return newPlan;
    }
}
