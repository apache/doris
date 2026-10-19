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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
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
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Materialize score virtual columns for score TopN and minimum score filters.
 * A standalone range uses a zero score limit to compute all candidate scores
 * without requiring a SQL TopN operator.
 */
public class PushDownScoreTopNIntoOlapScan implements RewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(PushDownScoreTopNIntoOlapScan.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFilter(logicalOlapScan())))
                        .then(topN -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = topN.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(topN, project, filter, scan);
                        }).toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN),
                logicalProject(logicalFilter(logicalOlapScan()))
                        .then(project -> pushDownScoreRange(project))
                        .toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN),
                logicalFilter(logicalOlapScan())
                        .then(filter -> pushDownScoreRange(filter))
                        .toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN));
    }

    private LogicalProject<Plan> pushDownScoreRange(
            LogicalProject<LogicalFilter<LogicalOlapScan>> project) {
        LogicalFilter<Plan> filter = pushDownScoreRange(project.child());
        if (filter == null) {
            return null;
        }
        LogicalOlapScan scan = (LogicalOlapScan) filter.child();
        Expression scoreSlot = scan.getScoreOrderKeys().get(0).getExpr();
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        for (NamedExpression projection : project.getProjects()) {
            for (Expression score : projection.<Expression>collect(e -> e instanceof Score)) {
                replaceMap.put(score, scoreSlot);
            }
        }
        return project.withProjectsAndChild(
                ExpressionUtils.replaceNamedExpressions(project.getProjects(), replaceMap), filter);
    }

    private LogicalFilter<Plan> pushDownScoreRange(LogicalFilter<LogicalOlapScan> filter) {
        Optional<Expression> scorePredicateOpt = findScoreRangePredicate(filter.getConjuncts());
        if (!scorePredicateOpt.isPresent()) {
            return null;
        }
        boolean hasMatchOrSearch = filter.getConjuncts().stream()
                .anyMatch(conjunct -> !conjunct.collect(
                        e -> e instanceof Match || e instanceof SearchExpression).isEmpty());
        if (!hasMatchOrSearch) {
            throw new AnalysisException("WHERE clause must contain at least one MATCH or SEARCH function"
                    + " for score() filtering");
        }
        Expression scorePredicate = scorePredicateOpt.get();
        Optional<ScoreRangeInfo> scoreRangeInfo = Optional.of(extractScoreRangeInfo(scorePredicate));
        Set<Expression> scoreFunctions = scorePredicate.collect(e -> e instanceof Score);
        Alias scoreAlias = new Alias(scoreFunctions.iterator().next(), "__score");
        // A zero score limit enables scoring without discarding candidates before other filters.
        LogicalOlapScan scan = filter.child().appendVirtualColumnsAndTopN(ImmutableList.of(scoreAlias),
                ImmutableList.of(), Optional.empty(),
                ImmutableList.of(new OrderKey(scoreAlias.toSlot(), false, false)), Optional.of(0L),
                scoreRangeInfo);
        Set<Expression> remainingConjuncts = filter.getConjuncts().stream()
                .filter(conjunct -> !conjunct.equals(scorePredicate))
                .collect(ImmutableSet.toImmutableSet());
        return filter.withConjunctsAndChild(remainingConjuncts, scan);
    }

    private Optional<Expression> findScoreRangePredicate(Set<Expression> conjuncts) {
        List<Expression> scorePredicates = conjuncts.stream()
                .filter(conjunct -> !conjunct.collect(e -> e instanceof Score).isEmpty())
                .collect(ImmutableList.toImmutableList());
        if (scorePredicates.size() > 1) {
            throw new AnalysisException(
                    "Only one score() range predicate is supported in WHERE clause. "
                            + "Found " + scorePredicates.size() + " predicates: " + scorePredicates);
        }
        return scorePredicates.stream().findFirst();
    }

    private Plan pushDown(
            LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN,
            LogicalProject<LogicalFilter<LogicalOlapScan>> project,
            LogicalFilter<LogicalOlapScan> filter,
            LogicalOlapScan scan) {
        // 1. Requirement: Project must contain a score() function.
        boolean hasScoreFunction = project.getProjects().stream()
                .anyMatch(projection -> {
                    if (projection instanceof Alias) {
                        return ((Alias) projection).child() instanceof Score;
                    }
                    return false;
                });
        if (!hasScoreFunction) {
            return null;
        }

        // 2. Requirement: WHERE clause must contain a MATCH or SEARCH function.
        boolean hasMatchOrSearchPredicate = filter.getConjuncts().stream()
                .anyMatch(conjunct -> !conjunct.collect(
                        e -> e instanceof Match || e instanceof SearchExpression).isEmpty());
        if (!hasMatchOrSearchPredicate) {
            throw new AnalysisException(
                    "WHERE clause must contain at least one MATCH or SEARCH function"
                            + " for score() push down optimization");
        }

        // 3. Check for score() predicates in WHERE clause and extract score range info
        Optional<Expression> scorePredicate = findScoreRangePredicate(filter.getConjuncts());
        Optional<ScoreRangeInfo> scoreRangeInfo = scorePredicate.map(this::extractScoreRangeInfo);
        Expression extractedScorePredicate = scorePredicate.orElse(null);

        // 4. Requirement: TopN must have exactly one ordering expression.
        if (topN.getOrderKeys().size() != 1) {
            if (scoreRangeInfo.isPresent()) {
                return null;
            }
            throw new AnalysisException(
                    "TopN must have exactly one ordering expression for score() push down optimization");
        }

        // 5. Requirement: ORDER BY expression must refer to a SELECT score() alias.
        Expression orderKey = topN.getOrderKeys().get(0).getExpr();
        if (!(orderKey instanceof SlotReference)) {
            if (scoreRangeInfo.isPresent()) {
                return null;
            }
            throw new AnalysisException(
                    "ORDER BY expression must be a slot reference (not a function call or complex expression)"
                            + " for score() push down optimization");
        }

        SlotReference orderSlot = (SlotReference) orderKey;
        Expression scoreExpr = null;
        Alias scoreAlias = null;

        // Find the 'score()' expression in the project list that matches the order by
        // slot.
        for (NamedExpression projection : project.getProjects()) {
            if (projection.toSlot().equals(orderSlot) && projection instanceof Alias) {
                Expression childExpr = ((Alias) projection).child();
                if (childExpr instanceof Score) {
                    scoreExpr = childExpr;
                    scoreAlias = (Alias) projection;
                    break;
                }
            }
        }

        if (scoreAlias == null) {
            if (scoreRangeInfo.isPresent()) {
                return null;
            }
            throw new AnalysisException(
                    "ORDER BY expression must reference a score() function from SELECT clause"
                            + " for push down optimization");
        }

        // When limit + offset overflows the long range, the pushed scan limit would wrap to a
        // negative value. Fail with the same error as ordinary TopN instead of leaving score()
        // unmaterialized and reporting an unrelated score() usage error.
        if (Utils.addOverflows(topN.getLimit(), topN.getOffset())) {
            throw new AnalysisException("limit + offset overflows the long range");
        }

        long scoreLimit = topN.getLimit() + topN.getOffset();
        long pushedScoreLimit = shouldDisableSearchTopN(filter.getConjuncts(), extractedScorePredicate)
                ? 0L : scoreLimit;

        // All conditions met, perform the push down.
        // This is the core action: push score() as a virtual column and also push the
        // topN info.
        Plan newScan = scan.appendVirtualColumnsAndTopN(ImmutableList.of(scoreAlias),
                ImmutableList.of(), Optional.empty(),
                topN.getOrderKeys(), Optional.of(pushedScoreLimit),
                scoreRangeInfo);

        // Rebuild the plan tree above the new scan.
        // We need to replace the original score() function with a reference to the new
        // virtual column slot.
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        replaceMap.put(scoreExpr, scoreAlias.toSlot());
        replaceMap.put(scoreAlias, scoreAlias.toSlot());

        // If we extracted a score predicate, remove it from the filter
        // as it will be pushed down to the scan node
        Set<Expression> newConjuncts;
        if (extractedScorePredicate != null) {
            final Expression predicateToRemove = extractedScorePredicate;
            newConjuncts = filter.getConjuncts().stream()
                    .filter(c -> !c.equals(predicateToRemove))
                    .collect(ImmutableSet.toImmutableSet());
        } else {
            newConjuncts = filter.getConjuncts();
        }

        // The filter node remains, as the MATCH predicate is still needed.
        Plan newFilter;
        if (newConjuncts.isEmpty()) {
            newFilter = newScan;
        } else {
            newFilter = filter.withConjunctsAndChild(newConjuncts, newScan);
        }

        // Rebuild project list with the replaced expressions.
        List<NamedExpression> newProjections = ExpressionUtils
                .replaceNamedExpressions(project.getProjects(), replaceMap);
        Plan newProject = project.withProjectsAndChild(newProjections, newFilter);

        // Rebuild the TopN node on top of the new project.
        return topN.withChildren(newProject);
    }

    private boolean shouldDisableSearchTopN(Set<Expression> conjuncts, Expression extractedScorePredicate) {
        List<Expression> nonScoreConjuncts = conjuncts.stream()
                .filter(conjunct -> extractedScorePredicate == null || !conjunct.equals(extractedScorePredicate))
                .collect(ImmutableList.toImmutableList());

        boolean hasSearchPredicate = nonScoreConjuncts.stream()
                .anyMatch(conjunct -> !conjunct.collect(e -> e instanceof SearchExpression).isEmpty());
        if (!hasSearchPredicate) {
            return false;
        }
        return nonScoreConjuncts.size() > 1 || !(nonScoreConjuncts.get(0) instanceof SearchExpression);
    }

    private ScoreRangeInfo extractScoreRangeInfo(Expression predicate) {
        if (!(predicate instanceof ComparisonPredicate)) {
            throw new AnalysisException(
                    "score() predicate must be a top-level AND condition in WHERE clause. "
                            + "Nesting score() inside OR or other compound expressions is not supported. "
                            + "Invalid expression: " + predicate.toSql());
        }

        ComparisonPredicate comp = (ComparisonPredicate) predicate;
        Expression left = comp.left();
        Expression right = comp.right();

        if (isScoreExpression(left) && isNumericLiteral(right)) {
            TExprOpcode op = getMinScoreOpcode(comp);
            if (op != null) {
                return new ScoreRangeInfo(op, extractNumericValue(right));
            }
        }

        if (isScoreExpression(right) && isNumericLiteral(left)) {
            TExprOpcode op = getReversedMinScoreOpcode(comp);
            if (op != null) {
                return new ScoreRangeInfo(op, extractNumericValue(left));
            }
        }

        throw new AnalysisException(
                "score() predicate in WHERE clause must be in the form of 'score() > literal' "
                        + "or 'score() >= literal' (min_score semantics). "
                        + "Operators <, <=, and = are not supported. "
                        + "Only FLOAT and DOUBLE casts of score() are supported.");
    }

    /** Accept score() and FLOAT/DOUBLE casts that preserve its value. */
    private boolean isScoreExpression(Expression expr) {
        if (expr instanceof Score) {
            return true;
        }
        if (expr instanceof Cast) {
            return expr.getDataType().isFloatLikeType() && isScoreExpression(((Cast) expr).child());
        }
        return false;
    }

    private boolean isNumericLiteral(Expression expr) {
        return expr instanceof DoubleLiteral
                || expr instanceof FloatLiteral
                || expr instanceof IntegerLikeLiteral
                || expr instanceof DecimalV3Literal;
    }

    private double extractNumericValue(Expression expr) {
        if (expr instanceof DoubleLiteral) {
            return ((DoubleLiteral) expr).getValue();
        } else if (expr instanceof FloatLiteral) {
            return ((FloatLiteral) expr).getValue();
        } else if (expr instanceof IntegerLikeLiteral) {
            return ((IntegerLikeLiteral) expr).getLongValue();
        } else if (expr instanceof DecimalV3Literal) {
            return ((DecimalV3Literal) expr).getDouble();
        }
        throw new IllegalArgumentException("Not a numeric literal: " + expr);
    }

    /**
     * Get opcode for min_score patterns: score() > X or score() >= X
     * Returns null for unsupported operators (< and <=)
     */
    private TExprOpcode getMinScoreOpcode(ComparisonPredicate comp) {
        if (comp instanceof GreaterThan) {
            return TExprOpcode.GT;
        } else if (comp instanceof GreaterThanEqual) {
            return TExprOpcode.GE;
        }
        return null;
    }

    /**
     * Get the reversed opcode for min_score patterns like "0.5 < score()" (equivalent to "score() > 0.5")
     * Returns null for unsupported operators
     */
    private TExprOpcode getReversedMinScoreOpcode(ComparisonPredicate comp) {
        if (comp instanceof LessThan) {
            return TExprOpcode.GT;
        } else if (comp instanceof LessThanEqual) {
            return TExprOpcode.GE;
        }
        return null;
    }
}
