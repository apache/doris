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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Push a minimum score filter, score > x or score >= x, into the scan that computes the score.
 *
 * <p>The scan then drops the rows below the threshold while it scores them. Any other filter on
 * the score, e.g. score < x or a second threshold, stays an ordinary filter on the score column
 * that BindScoreToOlapScan added. Runs before PushDownScoreTopNIntoOlapScan, which limits the
 * scoring only when no such filter is left.
 */
public class PushDownScoreRangeIntoOlapScan extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan().when(scan -> !scan.getScoreRangeInfo().isPresent()
                && Score.findScoreColumn(scan.getVirtualColumns()).isPresent()))
                .thenApply(ctx -> {
                    LogicalFilter<LogicalOlapScan> filter = ctx.root;
                    LogicalOlapScan scan = filter.child();
                    Slot scoreColumn = Score.findScoreColumn(scan.getVirtualColumns()).get().toSlot();
                    Map<Expression, ScoreRangeInfo> ranges = new LinkedHashMap<>();
                    for (Expression conjunct : filter.getConjuncts()) {
                        extractScoreRange(conjunct, scoreColumn).ifPresent(range -> ranges.put(conjunct, range));
                    }
                    // The scan applies one threshold.
                    if (ranges.size() != 1) {
                        return null;
                    }
                    Map.Entry<Expression, ScoreRangeInfo> range = ranges.entrySet().iterator().next();
                    LogicalOlapScan newScan = scan.appendVirtualColumnsAndTopN(ImmutableList.of(),
                            scan.getAnnOrderKeys(), scan.getAnnLimit(), scan.getScoreOrderKeys(),
                            scan.getScoreLimit(), Optional.of(range.getValue()));
                    Set<Expression> remaining = filter.getConjuncts().stream()
                            .filter(conjunct -> !conjunct.equals(range.getKey()))
                            .collect(ImmutableSet.toImmutableSet());
                    return remaining.isEmpty() ? newScan : filter.withConjunctsAndChild(remaining, newScan);
                }).toRule(RuleType.PUSH_DOWN_SCORE_RANGE_INTO_OLAP_SCAN);
    }

    private static Optional<ScoreRangeInfo> extractScoreRange(Expression predicate, Slot scoreColumn) {
        if (!(predicate instanceof ComparisonPredicate)) {
            return Optional.empty();
        }
        ComparisonPredicate comparison = (ComparisonPredicate) predicate;
        if (isScore(comparison.left(), scoreColumn) && isNumericLiteral(comparison.right())) {
            if (comparison instanceof GreaterThan) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GT, numericValue(comparison.right())));
            } else if (comparison instanceof GreaterThanEqual) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GE, numericValue(comparison.right())));
            }
        } else if (isScore(comparison.right(), scoreColumn) && isNumericLiteral(comparison.left())) {
            if (comparison instanceof LessThan) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GT, numericValue(comparison.left())));
            } else if (comparison instanceof LessThanEqual) {
                return Optional.of(new ScoreRangeInfo(TExprOpcode.GE, numericValue(comparison.left())));
            }
        }
        return Optional.empty();
    }

    // The score column, or a FLOAT or DOUBLE cast of it, which keeps its value.
    private static boolean isScore(Expression expression, Slot scoreColumn) {
        if (expression instanceof Cast) {
            return expression.getDataType().isFloatLikeType() && isScore(((Cast) expression).child(), scoreColumn);
        }
        return expression.equals(scoreColumn);
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
