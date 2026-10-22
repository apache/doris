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
import org.apache.doris.nereids.rules.analysis.BindScore;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
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
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Limit the scoring of a scan to the rows that a TopN on its score keeps.
 *
 * <p>BindScore makes the scan score every row it returns. When a TopN orders the rows of
 * the scan by the score alone, the scan can keep the best rows of each segment instead, and SEARCH
 * can skip documents that cannot reach them. The scan keeps the best rows only when no filter
 * follows it, so it also takes over a minimum score of the filter, as before score() could be used
 * without a TopN.
 */
public class PushDownScoreTopNIntoOlapScan implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFilter(logicalOlapScan())))
                        .then(topN -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = topN.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            return pushDown(topN, Optional.of(project), filter).map(newFilter ->
                                    topN.withChildren(project.withChildren(newFilter))).orElse(null);
                        }).toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN),
                logicalTopN(logicalFilter(logicalOlapScan()))
                        .then(topN -> pushDown(topN, Optional.empty(), topN.child())
                                .map(topN::withChildren).orElse(null))
                        .toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN));
    }

    private Optional<Plan> pushDown(LogicalTopN<?> topN, Optional<LogicalProject<?>> project,
            LogicalFilter<LogicalOlapScan> filter) {
        LogicalOlapScan scan = filter.child();
        Optional<Slot> scoreSlot = BindScore.findScoreSlot(scan);
        if (!scoreSlot.isPresent() || !scan.getScoreLimit().equals(Optional.of(0L))
                || scan.getScoreRangeInfo().isPresent() || topN.getOrderKeys().size() != 1) {
            return Optional.empty();
        }
        OrderKey orderKey = topN.getOrderKeys().get(0);
        if (!ordersByScore(orderKey.getExpr(), project, scoreSlot.get())) {
            return Optional.empty();
        }
        // When limit + offset overflows the long range, the pushed scan limit would wrap to a
        // negative value. Fail with the same error as ordinary TopN.
        if (Utils.addOverflows(topN.getLimit(), topN.getOffset())) {
            throw new AnalysisException("limit + offset overflows the long range");
        }
        Optional<Expression> minimum = filter.getConjuncts().stream()
                .filter(conjunct -> extractScoreRange(conjunct, scoreSlot.get()).isPresent())
                .findFirst();
        Set<Expression> conjuncts = filter.getConjuncts().stream()
                .filter(conjunct -> !minimum.equals(Optional.of(conjunct)))
                .collect(ImmutableSet.toImmutableSet());
        // A zero limit keeps scoring every row, and the scan applies the minimum score.
        long limit = searchNeedsAllScores(conjuncts) ? 0 : topN.getLimit() + topN.getOffset();
        if (limit == 0 && !minimum.isPresent()) {
            return Optional.empty();
        }
        LogicalOlapScan newScan = scan.appendVirtualColumnsAndTopN(ImmutableList.of(),
                scan.getAnnOrderKeys(), scan.getAnnLimit(),
                ImmutableList.of(new OrderKey(scoreSlot.get(), orderKey.isAsc(), orderKey.isNullFirst())),
                Optional.of(limit), minimum.flatMap(conjunct -> extractScoreRange(conjunct, scoreSlot.get())));
        return Optional.of(conjuncts.isEmpty() ? newScan : filter.withConjunctsAndChild(conjuncts, newScan));
    }

    private static boolean ordersByScore(Expression orderExpr, Optional<LogicalProject<?>> project, Slot scoreSlot) {
        if (orderExpr.equals(scoreSlot)) {
            return true;
        }
        if (!project.isPresent() || !(orderExpr instanceof Slot)) {
            return false;
        }
        for (NamedExpression projection : project.get().getProjects()) {
            if (projection.toSlot().equals(orderExpr)) {
                return projection instanceof Alias && ((Alias) projection).child().equals(scoreSlot);
            }
        }
        return false;
    }

    // SEARCH skips documents that cannot reach the top only when it is the only predicate of the
    // scan: another predicate could drop the documents it keeps.
    private static boolean searchNeedsAllScores(Set<Expression> conjuncts) {
        if (conjuncts.stream().noneMatch(conjunct -> conjunct.containsType(SearchExpression.class))) {
            return false;
        }
        return conjuncts.size() > 1 || !(conjuncts.iterator().next() instanceof SearchExpression);
    }

    // A minimum score: score > x or score >= x, also written x < score or x <= score.
    private static Optional<ScoreRangeInfo> extractScoreRange(Expression predicate, Slot scoreSlot) {
        if (!(predicate instanceof ComparisonPredicate)) {
            return Optional.empty();
        }
        ComparisonPredicate comparison = (ComparisonPredicate) predicate;
        if (isScore(comparison.left(), scoreSlot) && isNumericLiteral(comparison.right())) {
            if (comparison instanceof GreaterThan) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GT, numericValue(comparison.right())));
            } else if (comparison instanceof GreaterThanEqual) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GE, numericValue(comparison.right())));
            }
        } else if (isScore(comparison.right(), scoreSlot) && isNumericLiteral(comparison.left())) {
            if (comparison instanceof LessThan) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GT, numericValue(comparison.left())));
            } else if (comparison instanceof LessThanEqual) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GE, numericValue(comparison.left())));
            }
        }
        return Optional.empty();
    }

    // The score column, or a FLOAT or DOUBLE cast of it, which keeps its value.
    private static boolean isScore(Expression expression, Slot scoreSlot) {
        if (expression instanceof Cast) {
            return expression.getDataType().isFloatLikeType() && isScore(((Cast) expression).child(), scoreSlot);
        }
        return expression.equals(scoreSlot);
    }

    private static boolean isNumericLiteral(Expression expression) {
        return expression instanceof DoubleLiteral || expression instanceof FloatLiteral
                || expression instanceof IntegerLikeLiteral || expression instanceof DecimalV3Literal;
    }

    private static double numericValue(Expression literal) {
        if (literal instanceof DoubleLiteral) {
            return ((DoubleLiteral) literal).getValue();
        } else if (literal instanceof FloatLiteral) {
            return ((FloatLiteral) literal).getValue();
        } else if (literal instanceof IntegerLikeLiteral) {
            return ((IntegerLikeLiteral) literal).getLongValue();
        }
        return ((DecimalV3Literal) literal).getDouble();
    }
}
