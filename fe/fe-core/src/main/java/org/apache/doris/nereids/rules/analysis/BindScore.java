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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPreAggOnHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

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
 * the predicates of the outer query away from its scan. A correlated subquery cannot call score(), see
 * CheckScoreInCorrelatedSubquery. Any other use of score() is rejected.
 *
 * <p>This rule runs first on each plan the analyzer binds: a statement, and on their own its views, CTEs
 * and subqueries. The plan nodes of each SELECT are still its clauses then. The rule asks the table of a
 * SELECT that calls score() for a score column, which BindRelation adds to the olap scan, and replaces
 * the score() calls of the SELECT with that column. The scan scores every row it returns, so the minimum
 * score is a filter on the column. PushDownScoreTopNIntoOlapScan pushes a TopN on the score into the
 * scan.
 */
public class BindScore implements CustomRewriter {
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
    public static final String NUMERIC_COMPARISON_ERROR = "score() function can only be compared with numeric types";

    // No column of a table can have a name with the hidden column prefix.
    private static final String SCORE_COLUMN = Column.HIDDEN_COLUMN_PREFIX + "SCORE__";

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (!plan.anyMatch(node -> callsScore((Plan) node))) {
            return plan;
        }
        FunctionRegistry functions = jobContext.getCascadesContext().getConnectContext().getEnv()
                .getFunctionRegistry();
        // A view, a CTE and a subquery are analyzed without the sink of their statement.
        Plan newPlan = bindQuery(plan, plan instanceof LogicalSink, functions);
        // Outside the clauses of a SELECT, e.g. in a join condition.
        if (newPlan.anyMatch(node -> callsScore((Plan) node))) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
        return newPlan;
    }

    /** The score column that BindRelation added to the scan. */
    public static Optional<Slot> findScoreSlot(LogicalOlapScan scan) {
        return scan.getVirtualColumns().stream()
                .filter(column -> column instanceof Alias && ((Alias) column).child() instanceof Score)
                .map(NamedExpression::toSlot)
                .findFirst();
    }

    /** Make the scan of a table that a SELECT asked for its score column score every row it returns. */
    public static LogicalOlapScan addScoreColumn(LogicalOlapScan scan, Alias scoreColumn) {
        // A zero score limit scores every row.
        return scan.appendVirtualColumnsAndTopN(ImmutableList.of(scoreColumn), scan.getAnnOrderKeys(),
                scan.getAnnLimit(), ImmutableList.of(new OrderKey(scoreColumn.toSlot(), false, false)),
                Optional.of(0L), Optional.empty());
    }

    /** Only the olap scan of a table can compute the score, not a view, a CTE or another relation. */
    public static void checkScoreColumn(Plan relation, Alias scoreColumn) {
        if (!relation.getOutputSet().contains(scoreColumn.toSlot())) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
    }

    // A query: a SELECT, a set operation of queries or a query with a WITH clause, also under the sink of
    // the statement.
    private static Plan bindQuery(Plan plan, boolean outermost, FunctionRegistry functions) {
        if (plan instanceof LogicalSink || plan instanceof LogicalCTE) {
            return withChildren(plan, ImmutableList.of(bindQuery(plan.child(0), outermost, functions)));
        }
        if (plan instanceof LogicalSetOperation) {
            return withChildren(plan, plan.children().stream()
                    .map(child -> bindQuery(child, false, functions))
                    .collect(ImmutableList.toImmutableList()));
        }
        return bindSelect(plan, outermost, functions);
    }

    private static Plan bindSelect(Plan top, boolean outermost, FunctionRegistry functions) {
        // The parser builds a SELECT as the chain of its clauses, from its hints down to its WHERE clause.
        List<Plan> clauses = new ArrayList<>();
        Plan from = top;
        for (; isClause(from); from = from.child(0)) {
            clauses.add(from);
        }
        Plan newFrom = bindFrom(from, functions);
        if (clauses.stream().anyMatch(BindScore::callsScore)) {
            check(clauses, from, outermost, functions);
            Alias scoreColumn = new Alias(new Score(), SCORE_COLUMN);
            newFrom = askForScoreColumn(from, scoreColumn);
            clauses = clauses.stream().map(clause -> bindScore(clause, scoreColumn.toSlot()))
                    .collect(Collectors.toList());
        }
        Plan plan = newFrom;
        for (int i = clauses.size() - 1; i >= 0; i--) {
            plan = withChildren(clauses.get(i), ImmutableList.of(plan));
        }
        return plan;
    }

    private static boolean isClause(Plan plan) {
        return plan instanceof LogicalPreAggOnHint || plan instanceof LogicalSelectHint
                || plan instanceof LogicalLimit || plan instanceof LogicalSort || plan instanceof LogicalQualify
                || plan instanceof LogicalHaving || plan instanceof LogicalProject
                || plan instanceof LogicalAggregate || plan instanceof LogicalRepeat || plan instanceof LogicalFilter;
    }

    // The FROM clause: tables, joins and lateral views of them, and subqueries, which are queries of
    // their own.
    private static Plan bindFrom(Plan from, FunctionRegistry functions) {
        if (from.arity() == 0 || isTable(from)) {
            return from;
        }
        if (from instanceof LogicalSubQueryAlias) {
            return withChildren(from, ImmutableList.of(bindQuery(from.child(0), false, functions)));
        }
        // E.g. a set operation under ORDER BY and LIMIT.
        if (isClause(from) || from instanceof LogicalSetOperation || from instanceof LogicalCTE) {
            return bindQuery(from, false, functions);
        }
        return withChildren(from, from.children().stream()
                .map(child -> bindFrom(child, functions))
                .collect(ImmutableList.toImmutableList()));
    }

    // A table, also with an alias.
    private static boolean isTable(Plan plan) {
        Plan relation = plan instanceof LogicalSubQueryAlias ? plan.child(0) : plan;
        return relation instanceof LogicalCheckPolicy && relation.child(0) instanceof UnboundRelation;
    }

    private static Plan askForScoreColumn(Plan table, Alias scoreColumn) {
        if (table instanceof LogicalSubQueryAlias) {
            return table.withChildren(askForScoreColumn(table.child(0), scoreColumn));
        }
        return table.withChildren(((UnboundRelation) table.child(0)).withScoreColumn(scoreColumn));
    }

    private static void check(List<Plan> clauses, Plan from, boolean outermost, FunctionRegistry functions) {
        // A subquery in a clause reads another table.
        if (!isTable(from) || clauses.stream().anyMatch(clause -> clause.getExpressions().stream()
                .anyMatch(expression -> expression.containsType(SubqueryExpr.class)))) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
        Plan last = clauses.get(clauses.size() - 1);
        List<Expression> where = last instanceof LogicalFilter
                ? ((LogicalFilter<?>) last).getConjuncts().stream()
                        .flatMap(conjunct -> ExpressionUtils.extractConjunction(conjunct).stream())
                        .collect(Collectors.toList())
                : ImmutableList.of();
        if (where.stream().noneMatch(conjunct -> conjunct.anyMatch(BindScore::matchesText))) {
            throw new AnalysisException(UNSUPPORTED_SELECT_ERROR);
        }
        clauses.forEach(clause -> clause.getExpressions().forEach(expression -> expression.foreach(node -> {
            if (node instanceof ComparisonPredicate) {
                checkComparedWithNumber((ComparisonPredicate) node);
            }
        })));
        List<Expression> ranges = where.stream()
                .filter(conjunct -> conjunct.anyMatch(BindScore::isScoreCall))
                .collect(Collectors.toList());
        if (ranges.size() > 1) {
            throw new AnalysisException(ONE_RANGE_ERROR);
        }
        ranges.forEach(BindScore::checkRange);

        List<Plan> aboveWhere = last instanceof LogicalFilter ? clauses.subList(0, clauses.size() - 1) : clauses;
        boolean aggregates = aboveWhere.stream().anyMatch(clause -> aggregates(clause, functions));
        // HAVING and QUALIFY can also read score(), MATCH and SEARCH through aliases of the select list.
        Set<String> scoringAliases = new HashSet<>();
        for (int i = aboveWhere.size() - 1; i >= 0; i--) {
            for (Expression expression : aboveWhere.get(i).getExpressions()) {
                if (expression instanceof UnboundAlias && ((UnboundAlias) expression).getAlias().isPresent()
                        && scores(expression, scoringAliases)) {
                    scoringAliases.add(((UnboundAlias) expression).getAlias().get().toLowerCase());
                }
            }
        }
        for (Plan clause : aboveWhere) {
            if ((clause instanceof LogicalHaving || clause instanceof LogicalQualify)
                    && clause.getExpressions().stream().anyMatch(expression -> scores(expression, scoringAliases))) {
                throw new AnalysisException(HAVING_OR_QUALIFY_ERROR);
            }
            if (aggregates && callsScore(clause)) {
                throw new AnalysisException(AGGREGATE_ERROR);
            }
        }
        if (!outermost && !ordersByScoreWithLimit(aboveWhere)) {
            throw new AnalysisException(NESTED_SELECT_ERROR);
        }
    }

    // score() is a FLOAT. The type of a literal is known before binding, e.g. score() > 'abc' is rejected.
    private static void checkComparedWithNumber(ComparisonPredicate comparison) {
        for (int i = 0; i < 2; i++) {
            Expression other = comparison.child(1 - i);
            if (isScoreCall(comparison.child(i)) && other instanceof Literal
                    && !other.getDataType().isNumericType() && !other.getDataType().isNullType()) {
                throw new AnalysisException(NUMERIC_COMPARISON_ERROR + ", but found: " + other.getDataType()
                        + " in " + comparison.toSql());
            }
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
        return isScoreCall(expression);
    }

    private static boolean isConstant(Expression expression) {
        return expression.getInputSlots().isEmpty() && !expression.anyMatch(BindScore::isScoreCall);
    }

    // GROUP BY, DISTINCT or an aggregate function, which aggregates all rows of a SELECT without GROUP BY.
    private static boolean aggregates(Plan clause, FunctionRegistry functions) {
        return clause instanceof LogicalAggregate || clause instanceof LogicalRepeat
                || clause instanceof LogicalProject && ((LogicalProject<?>) clause).isDistinct()
                || clause.getExpressions().stream().anyMatch(expression -> callsAggregate(expression, functions));
    }

    private static boolean callsAggregate(Expression expression, FunctionRegistry functions) {
        // A window function aggregates the rows of its window.
        if (expression instanceof WindowExpression) {
            return false;
        }
        if (expression instanceof UnboundFunction && functions.isAggregateFunction(
                ((UnboundFunction) expression).getDbName(), ((UnboundFunction) expression).getName())) {
            return true;
        }
        return expression.children().stream().anyMatch(child -> callsAggregate(child, functions));
    }

    private static boolean scores(Expression expression, Set<String> scoringAliases) {
        return expression.anyMatch(node -> isScoreCall((Expression) node) || matchesText((Expression) node)
                || node instanceof UnboundSlot && ((UnboundSlot) node).getNameParts().size() == 1
                        && scoringAliases.contains(((UnboundSlot) node).getName().toLowerCase()));
    }

    // A TopN on the score at the top of the SELECT, which no outer predicate passes.
    private static boolean ordersByScoreWithLimit(List<Plan> aboveWhere) {
        for (int i = 0; i + 1 < aboveWhere.size(); i++) {
            if (aboveWhere.get(i) instanceof LogicalLimit && aboveWhere.get(i + 1) instanceof LogicalSort) {
                List<OrderKey> orderKeys = ((LogicalSort<?>) aboveWhere.get(i + 1)).getOrderKeys();
                return orderKeys.size() == 1 && isScoreOrAlias(orderKeys.get(0).getExpr(), aboveWhere);
            }
        }
        return false;
    }

    private static boolean isScoreOrAlias(Expression expression, List<Plan> aboveWhere) {
        if (isScoreCall(expression)) {
            return true;
        }
        return expression instanceof UnboundSlot && ((UnboundSlot) expression).getNameParts().size() == 1
                && aboveWhere.stream().flatMap(clause -> clause.getExpressions().stream())
                        .anyMatch(item -> item instanceof UnboundAlias && isScoreCall(item.child(0))
                                && ((UnboundAlias) item).getAlias()
                                        .filter(((UnboundSlot) expression).getName()::equalsIgnoreCase).isPresent());
    }

    // Replaces the score() calls of a clause with the score column, which * leaves out because it is not
    // a column of the table.
    private static Plan bindScore(Plan clause, Slot scoreSlot) {
        Function<Expression, Expression> bind = expression -> expression.rewriteDownShortCircuit(
                node -> isScoreCall(node) ? scoreSlot : node);
        if (clause instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) clause;
            return filter.withConjuncts(filter.getConjuncts().stream().map(bind)
                    .collect(ImmutableSet.toImmutableSet()));
        } else if (clause instanceof LogicalSort) {
            LogicalSort<?> sort = (LogicalSort<?>) clause;
            return sort.withOrderKeys(sort.getOrderKeys().stream()
                    .map(orderKey -> orderKey.withExpression(bind.apply(orderKey.getExpr())))
                    .collect(ImmutableList.toImmutableList()));
        } else if (clause instanceof LogicalProject) {
            LogicalProject<?> project = (LogicalProject<?>) clause;
            return project.withProjects(project.getProjects().stream()
                    .map(item -> exceptScore((NamedExpression) bind.apply(item), scoreSlot))
                    .collect(ImmutableList.toImmutableList()));
        } else if (clause instanceof LogicalAggregate) {
            LogicalAggregate<?> aggregate = (LogicalAggregate<?>) clause;
            return aggregate.withAggOutput(aggregate.getOutputExpressions().stream()
                    .map(item -> exceptScore(item, scoreSlot))
                    .collect(ImmutableList.toImmutableList()));
        } else if (clause instanceof LogicalRepeat) {
            LogicalRepeat<?> repeat = (LogicalRepeat<?>) clause;
            return repeat.withAggOutput(repeat.getOutputExpressions().stream()
                    .map(item -> exceptScore(item, scoreSlot))
                    .collect(ImmutableList.toImmutableList()));
        }
        return clause;
    }

    private static NamedExpression exceptScore(NamedExpression item, Slot scoreSlot) {
        if (!(item instanceof UnboundStar)) {
            return item;
        }
        UnboundStar star = (UnboundStar) item;
        return new UnboundStar(star.getQualifier(), ImmutableList.<NamedExpression>builder()
                .addAll(star.getExceptedSlots()).add(scoreSlot).build(),
                star.getReplacedAlias(), star.getIndexInSqlString());
    }

    private static boolean callsScore(Plan plan) {
        return plan.getExpressions().stream().anyMatch(expression -> expression.anyMatch(BindScore::isScoreCall));
    }

    private static boolean isScoreCall(Object expression) {
        return isBuiltinCall(expression, "score") && ((UnboundFunction) expression).arity() == 0;
    }

    private static boolean matchesText(Object expression) {
        return expression instanceof Match || isBuiltinCall(expression, "search");
    }

    // Analysis binds the function calls of the parsed plan later.
    private static boolean isBuiltinCall(Object expression, String name) {
        return expression instanceof UnboundFunction && ((UnboundFunction) expression).getDbName() == null
                && ((UnboundFunction) expression).getName().equalsIgnoreCase(name);
    }

    private static Plan withChildren(Plan plan, List<Plan> children) {
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) != plan.child(i)) {
                return plan.withChildren(children);
            }
        }
        return plan;
    }
}
