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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.stats.FilterEstimation.EstimationContext;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.statistics.Bucket;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.HistogramBuilder;
import org.apache.doris.statistics.StatisticRange;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Calculate selectivity of expression that produces boolean value.
 * TODO: Should consider the distribution of data.
 */
public class FilterEstimation extends ExpressionVisitor<Statistics, EstimationContext> {
    public static final double DEFAULT_INEQUALITY_COEFFICIENT = 0.5;
    public static final double DEFAULT_IN_COEFFICIENT = 1.0 / 3.0;

    public static final double DEFAULT_HAVING_COEFFICIENT = 0.01;

    public static final double DEFAULT_EQUALITY_COMPARISON_SELECTIVITY = 0.1;
    public static final double DEFAULT_LIKE_COMPARISON_SELECTIVITY = 0.2;
    private Set<Slot> aggSlots;

    public FilterEstimation() {
    }

    public FilterEstimation(Set<Slot> aggSlots) {
        this.aggSlots = aggSlots;
    }

    /**
     * This method will update the stats according to the selectivity.
     */
    public Statistics estimate(Expression expression, Statistics statistics) {
        // For a comparison predicate, only when it's left side is a slot and right side is a literal, we would
        // consider is a valid predicate.
        return expression.accept(this, new EstimationContext(statistics));
    }

    @Override
    public Statistics visit(Expression expr, EstimationContext context) {
        return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
    }

    @Override
    public Statistics visitCompoundPredicate(CompoundPredicate predicate, EstimationContext context) {
        Expression leftExpr = predicate.child(0);
        Expression rightExpr = predicate.child(1);
        Statistics leftStats = leftExpr.accept(this, context);
        Statistics andStats = rightExpr.accept(new FilterEstimation(),
                new EstimationContext(leftStats));
        if (predicate instanceof And) {
            return andStats;
        } else if (predicate instanceof Or) {
            Statistics rightStats = rightExpr.accept(this, context);
            double rowCount = leftStats.getRowCount() + rightStats.getRowCount() - andStats.getRowCount();
            Statistics orStats = context.statistics.withRowCount(rowCount);
            for (Map.Entry<Expression, ColumnStatistic> entry : orStats.columnStatistics().entrySet()) {
                ColumnStatistic leftColStats = leftStats.findColumnStatistics(entry.getKey());
                ColumnStatistic rightColStats = rightStats.findColumnStatistics(entry.getKey());
                ColumnStatisticBuilder estimatedColStatsBuilder = new ColumnStatisticBuilder(entry.getValue());
                if (leftColStats.minValue <= rightColStats.minValue) {
                    estimatedColStatsBuilder.setMinValue(leftColStats.minValue);
                    estimatedColStatsBuilder.setMinExpr(leftColStats.minExpr);
                } else {
                    estimatedColStatsBuilder.setMinValue(rightColStats.minValue);
                    estimatedColStatsBuilder.setMinExpr(rightColStats.minExpr);
                }
                if (leftColStats.maxValue >= rightColStats.maxValue) {
                    estimatedColStatsBuilder.setMaxValue(leftColStats.maxValue);
                    estimatedColStatsBuilder.setMaxExpr(leftColStats.maxExpr);
                } else {
                    estimatedColStatsBuilder.setMaxValue(rightColStats.maxValue);
                    estimatedColStatsBuilder.setMaxExpr(rightColStats.maxExpr);
                }
            }
            return orStats;
        }
        return context.statistics;
    }

    @Override
    public Statistics visitComparisonPredicate(ComparisonPredicate cp, EstimationContext context) {
        Expression left = cp.left();
        if (left instanceof SlotReference && ((SlotReference) left).getColumn().isPresent()) {
            if ("__DORIS_DELETE_SIGN__".equals(((SlotReference) left).getColumn().get().getName())) {
                return context.statistics;
            }
        }
        Expression right = cp.right();
        if (right instanceof SlotReference && ((SlotReference) right).getColumn().isPresent()) {
            if ("__DORIS_DELETE_SIGN__".equals(((SlotReference) right).getColumn().get().getName())) {
                return context.statistics;
            }
        }
        ColumnStatistic statsForLeft = ExpressionEstimation.estimate(left, context.statistics);
        ColumnStatistic statsForRight = ExpressionEstimation.estimate(right, context.statistics);
        if (aggSlots != null) {
            Predicate<TreeNode<Expression>> containsAggSlot = e -> {
                if (e instanceof SlotReference) {
                    SlotReference slot = (SlotReference) e;
                    return aggSlots.contains(slot);
                }
                return false;
            };
            boolean leftAgg = left.anyMatch(containsAggSlot);
            boolean rightAgg = right.anyMatch(containsAggSlot);
            // It means this predicate appears in HAVING clause.
            if (leftAgg || rightAgg) {
                double rowCount = context.statistics.getRowCount();
                double newRowCount = Math.max(rowCount * DEFAULT_HAVING_COEFFICIENT,
                        Math.max(statsForLeft.ndv, statsForRight.ndv));
                return context.statistics.withRowCount(newRowCount);
            }
        }
        if (!left.isConstant() && !right.isConstant()) {
            return calculateWhenBothColumn(cp, context, statsForLeft, statsForRight);
        } else {
            // For literal, it's max min is same value.
            return calculateWhenLiteralRight(cp,
                    statsForLeft,
                    statsForRight,
                    context);
        }
    }

    private Statistics updateLessThanLiteral(Expression leftExpr, ColumnStatistic statsForLeft,
            double val, EstimationContext context, boolean contains) {
        if (statsForLeft.hasHistogram()) {
            return estimateLessThanLiteralWithHistogram(leftExpr, statsForLeft, val, context, contains);
        }
        //rightRange.distinctValues should not be used
        StatisticRange rightRange = new StatisticRange(statsForLeft.minValue, val, statsForLeft.ndv,
                leftExpr.getDataType());
        return estimateBinaryComparisonFilter(leftExpr,
                statsForLeft,
                rightRange, context);
    }

    private Statistics updateGreaterThanLiteral(Expression leftExpr, ColumnStatistic statsForLeft,
            double val, EstimationContext context, boolean contains) {
        if (statsForLeft.hasHistogram()) {
            return estimateGreaterThanLiteralWithHistogram(leftExpr, statsForLeft, val, context, contains);
        }
        //rightRange.distinctValues should not be used
        StatisticRange rightRange = new StatisticRange(val, statsForLeft.maxValue,
                statsForLeft.ndv, leftExpr.getDataType());
        return estimateBinaryComparisonFilter(leftExpr, statsForLeft, rightRange, context);
    }

    private Statistics calculateWhenLiteralRight(ComparisonPredicate cp,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight, EstimationContext context) {
        if (statsForLeft.isUnKnown) {
            return context.statistics.withSel(DEFAULT_INEQUALITY_COEFFICIENT);
        }

        if (cp instanceof EqualTo || cp instanceof NullSafeEqual) {
            return estimateEqualTo(cp, statsForLeft, statsForRight, context);
        } else {
            double val = statsForRight.maxValue;
            if (cp instanceof LessThan || cp instanceof LessThanEqual) {
                return updateLessThanLiteral(cp.left(), statsForLeft, val, context, cp instanceof LessThanEqual);
            } else if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {

                return updateGreaterThanLiteral(cp.left(), statsForLeft, val, context,
                        cp instanceof GreaterThanEqual);
            } else {
                throw new RuntimeException(String.format("Unexpected expression : %s", cp.toSql()));
            }
        }
    }

    private Statistics estimateEqualTo(ComparisonPredicate cp, ColumnStatistic statsForLeft,
            ColumnStatistic statsForRight,
            EstimationContext context) {
        double selectivity;
        double ndv = statsForLeft.ndv;
        double val = statsForRight.maxValue;
        if (val > statsForLeft.maxValue || val < statsForLeft.minValue) {
            selectivity = 0.0;
        } else {
            selectivity = StatsMathUtil.minNonNaN(1.0, 1.0 / ndv);
        }
        if (statsForLeft.hasHistogram()) {
            return estimateEqualToWithHistogram(cp.left(), statsForLeft, val, context);
        }

        Statistics equalStats = context.statistics.withSel(selectivity);
        Expression left = cp.left();
        if (left instanceof Cast) {
            left = ((Cast) left).child();
        }
        if (left instanceof SlotReference) {
            Slot leftSlot = (SlotReference) left;
            //update min/max of cp.left
            ColumnStatistic columnStats = equalStats.findColumnStatistics(leftSlot);
            ColumnStatisticBuilder colStatsBuilder = new ColumnStatisticBuilder(columnStats);
            colStatsBuilder.setMaxValue(val);
            colStatsBuilder.setMinValue(val);
            colStatsBuilder.setNdv(1);
            colStatsBuilder.setNumNulls(0);
            equalStats.addColumnStats(leftSlot, colStatsBuilder.build());
        }
        return equalStats;
    }

    private Statistics calculateWhenBothColumn(ComparisonPredicate cp, EstimationContext context,
            ColumnStatistic statsForLeft, ColumnStatistic statsForRight) {
        Expression left = cp.left();
        Expression right = cp.right();
        if (cp instanceof EqualTo || cp instanceof NullSafeEqual) {
            return estimateColumnEqualToColumn(left, statsForLeft, right, statsForRight, context);
        }
        if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
            return estimateColumnLessThanColumn(right, statsForRight, left, statsForLeft, context);
        }
        if (cp instanceof LessThan || cp instanceof LessThanEqual) {
            return estimateColumnLessThanColumn(left, statsForLeft, right, statsForRight, context);
        }
        return context.statistics;
    }

    @Override
    public Statistics visitInPredicate(InPredicate inPredicate, EstimationContext context) {
        Expression compareExpr = inPredicate.getCompareExpr();
        ColumnStatistic compareExprStats = ExpressionEstimation.estimate(compareExpr, context.statistics);
        if (compareExprStats.isUnKnown || compareExpr instanceof Function) {
            return context.statistics.withSel(DEFAULT_IN_COEFFICIENT);
        }
        List<Expression> options = inPredicate.getOptions();
        double maxOption = 0;
        double minOption = Double.MAX_VALUE;
        /* suppose A.(min, max) = (0, 10), A.ndv=10
         A in ( 1, 2, 5, 100):
              validInOptCount = 3, that is (1, 2, 5)
              table selectivity = 3/10
              A.min = 1, A.max=5
              A.selectivity = 3/5
              A.ndv = 3
         A not in (1, 2, 3, 100):
              validInOptCount = 10 - 3
              we assume that 1, 2, 3 exist in A
              A.ndv = 10 - 3 = 7
              table selectivity = 7/10
              A.(min, max) not changed
              A.selectivity = 7/10
        */
        double validInOptCount = 0;
        double selectivity = 1.0;
        ColumnStatisticBuilder compareExprStatsBuilder = new ColumnStatisticBuilder(compareExprStats);

        for (Expression option : options) {
            ColumnStatistic optionStats = ExpressionEstimation.estimate(option, context.statistics);
            double validOptionNdv = compareExprStats.ndvIntersection(optionStats);
            if (validOptionNdv > 0.0) {
                validInOptCount += validOptionNdv;
                maxOption = Math.max(optionStats.maxValue, maxOption);
                minOption = Math.min(optionStats.minValue, minOption);
            }
        }
        maxOption = Math.min(maxOption, compareExprStats.maxValue);
        minOption = Math.max(minOption, compareExprStats.minValue);
        compareExprStatsBuilder.setMaxValue(maxOption);
        compareExprStatsBuilder.setMinValue(minOption);

        selectivity = StatsMathUtil.minNonNaN(1.0, validInOptCount / compareExprStats.ndv);
        compareExprStatsBuilder.setNdv(validInOptCount);
        Statistics estimated = new Statistics(context.statistics);
        estimated = estimated.withSel(selectivity);
        if (compareExpr instanceof SlotReference) {
            estimated.addColumnStats(compareExpr,
                    compareExprStatsBuilder.build());
        }
        return estimated;
    }

    @Override
    public Statistics visitNot(Not not, EstimationContext context) {
        Statistics childStats = new FilterEstimation().estimate(not.child(), context.statistics);
        //if estimated rowCount is 0, adjust to 1 to make upper join reorder reasonable.
        double rowCount = Math.max(context.statistics.getRowCount() - childStats.getRowCount(), 1);
        StatisticsBuilder statisticsBuilder = new StatisticsBuilder(context.statistics).setRowCount(rowCount);
        for (Entry<Expression, ColumnStatistic> entry : context.statistics.columnStatistics().entrySet()) {
            Expression expr = entry.getKey();
            ColumnStatistic originColStats = entry.getValue();
            ColumnStatistic childColStats = childStats.findColumnStatistics(expr);
            double originNonNullCount = Math.max(originColStats.count - originColStats.numNulls, 0);
            double childNonNullCount = Math.max(childColStats.count - childColStats.numNulls, 0);
            double supersetValuesPerDistinctValue = StatsMathUtil.divide(originNonNullCount, originColStats.ndv);
            double subsetValuesPerDistinctValue = StatsMathUtil.divide(childNonNullCount, childColStats.ndv);
            double ndv;
            if (supersetValuesPerDistinctValue <= subsetValuesPerDistinctValue) {
                ndv = Math.max(originColStats.ndv - childColStats.ndv, 0);
            } else {
                ndv = originColStats.ndv;
            }
            double nullCount = Math.max(originColStats.numNulls - childColStats.numNulls, 0);
            ColumnStatistic columnStatistic = new ColumnStatisticBuilder(originColStats)
                    .setNdv(ndv)
                    .setNumNulls(nullCount)
                    .build();
            statisticsBuilder.putColumnStatistics(expr, columnStatistic);
        }
        return statisticsBuilder.build();
    }

    static class EstimationContext {
        private final Statistics statistics;

        public EstimationContext(Statistics statistics) {
            this.statistics = statistics;
        }
    }

    private Statistics estimateBinaryComparisonFilter(Expression leftExpr, ColumnStatistic leftStats,
            StatisticRange rightRange, EstimationContext context) {
        StatisticRange leftRange =
                new StatisticRange(leftStats.minValue, leftStats.maxValue, leftStats.ndv, leftExpr.getDataType());
        StatisticRange intersectRange = leftRange.cover(rightRange);
        ColumnStatisticBuilder leftColumnStatisticBuilder = new ColumnStatisticBuilder(leftStats)
                .setMinValue(intersectRange.getLow())
                .setMaxValue(intersectRange.getHigh())
                .setNdv(intersectRange.getDistinctValues());
        double sel = leftRange.overlapPercentWith(rightRange);
        Statistics updatedStatistics = context.statistics.withSel(sel);
        leftColumnStatisticBuilder.setCount(updatedStatistics.getRowCount());
        updatedStatistics.addColumnStats(leftExpr, leftColumnStatisticBuilder.build());
        leftExpr.accept(new ColumnStatsAdjustVisitor(), updatedStatistics);
        return updatedStatistics;
    }

    private Statistics estimateColumnEqualToColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, EstimationContext context) {
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        StatisticRange leftIntersectRight = leftRange.intersect(rightRange);
        StatisticRange rightIntersectLeft = rightRange.intersect(leftIntersectRight);
        ColumnStatisticBuilder leftBuilder = new ColumnStatisticBuilder(leftStats);
        leftBuilder.setNdv(leftIntersectRight.getDistinctValues());
        leftBuilder.setMinValue(leftIntersectRight.getLow());
        leftBuilder.setMaxValue(leftIntersectRight.getHigh());
        ColumnStatisticBuilder rightBuilder = new ColumnStatisticBuilder(rightStats);
        rightBuilder.setNdv(rightIntersectLeft.getDistinctValues());
        rightBuilder.setMinValue(rightIntersectLeft.getLow());
        rightBuilder.setMaxValue(rightIntersectLeft.getDistinctValues());
        double sel;
        double reduceRatio = 0.25;
        double bothSideReducedRatio = 0.9;
        if (!leftStats.rangeChanged() && !rightStats.rangeChanged()
                && leftStats.ndv < leftStats.getOriginalNdv() * bothSideReducedRatio
                && rightStats.ndv < rightStats.getOriginalNdv() * bothSideReducedRatio) {
            double sel1;
            if (leftStats.ndv > rightStats.ndv) {
                sel1 = 1 / StatsMathUtil.nonZeroDivisor(leftStats.ndv);
            } else {
                sel1 = 1 / StatsMathUtil.nonZeroDivisor(rightStats.ndv);
            }
            double sel2 = Math.min(rightStats.ndv / rightStats.getOriginalNdv(),
                    leftStats.ndv / leftStats.getOriginalNdv());
            sel = sel1 * Math.pow(sel2, reduceRatio);
        } else {
            sel = 1 / StatsMathUtil.nonZeroDivisor(Math.max(leftStats.ndv, rightStats.ndv));
        }
        Statistics updatedStatistics = context.statistics.withSel(sel);
        updatedStatistics.addColumnStats(leftExpr, leftBuilder.build());
        updatedStatistics.addColumnStats(rightExpr, rightBuilder.build());
        return updatedStatistics;
    }

    private Statistics estimateColumnLessThanColumn(Expression leftExpr, ColumnStatistic leftStats,
            Expression rightExpr, ColumnStatistic rightStats, EstimationContext context) {
        StatisticRange leftRange = StatisticRange.from(leftStats, leftExpr.getDataType());
        StatisticRange rightRange = StatisticRange.from(rightStats, rightExpr.getDataType());
        Statistics statistics = null;
        // Left always less than Right
        if (leftRange.getHigh() < rightRange.getLow()) {
            statistics =
                    context.statistics.withRowCount(Math.min(context.statistics.getRowCount() - leftStats.numNulls,
                            context.statistics.getRowCount() - rightStats.numNulls));
            statistics.addColumnStats(leftExpr, new ColumnStatisticBuilder(leftStats).setNumNulls(0.0).build());
            statistics.addColumnStats(rightExpr, new ColumnStatisticBuilder(rightStats).setNumNulls(0.0).build());
            return statistics;
        }
        double leftOverlapPercent = leftRange.overlapPercentWith(rightRange);
        // Left always greater than right
        if (leftOverlapPercent == 0) {
            return context.statistics.withRowCount(0.0);
        }
        StatisticRange leftAlwaysLessThanRightRange = new StatisticRange(leftStats.minValue,
                rightStats.minValue, Double.NaN, leftExpr.getDataType());
        double leftAlwaysLessThanRightPercent = 0;
        if (leftRange.getLow() < rightRange.getLow()) {
            leftAlwaysLessThanRightPercent = leftRange.overlapPercentWith(leftAlwaysLessThanRightRange);
        }
        ColumnStatistic leftColumnStatistic = new ColumnStatisticBuilder(leftStats)
                .setMaxValue(Math.min(leftRange.getHigh(), rightRange.getHigh()))
                .setMinValue(leftRange.getLow())
                .setNdv(leftStats.ndv * (leftAlwaysLessThanRightPercent + leftOverlapPercent))
                .setNumNulls(0)
                .build();
        double rightOverlappingRangeFraction = rightRange.overlapPercentWith(leftRange);
        double rightAlwaysGreaterRangeFraction = 0;
        if (leftRange.getHigh() < rightRange.getHigh()) {
            rightAlwaysGreaterRangeFraction = rightRange.overlapPercentWith(new StatisticRange(leftRange.getHigh(),
                    rightRange.getHigh(), Double.NaN, rightExpr.getDataType()));
        }
        ColumnStatistic rightColumnStatistic = new ColumnStatisticBuilder(rightStats)
                .setMinValue(Math.max(leftRange.getLow(), rightRange.getLow()))
                .setMaxValue(rightRange.getHigh())
                .setNdv(rightStats.ndv * (rightAlwaysGreaterRangeFraction + rightOverlappingRangeFraction))
                .setNumNulls(0)
                .build();
        double sel = leftAlwaysLessThanRightPercent
                + leftOverlapPercent * rightOverlappingRangeFraction * DEFAULT_INEQUALITY_COEFFICIENT
                + leftOverlapPercent * rightAlwaysGreaterRangeFraction;
        return context.statistics.withSel(sel)
                .addColumnStats(leftExpr, leftColumnStatistic)
                .addColumnStats(rightExpr, rightColumnStatistic);
    }

    private Statistics estimateLessThanLiteralWithHistogram(Expression leftExpr, ColumnStatistic leftStats,
            double numVal, EstimationContext context, boolean contains) {
        Histogram leftHist = leftStats.histogram;

        for (int i = 0; i < leftHist.buckets.size(); i++) {
            Bucket bucket = leftHist.buckets.get(i);
            if (bucket.upper >= numVal && bucket.lower <= numVal) {
                double overlapPercentInBucket;
                if (numVal == bucket.upper && numVal == bucket.lower) {
                    if (contains) {
                        overlapPercentInBucket = 1;
                    } else {
                        overlapPercentInBucket = 0;
                    }
                } else {
                    overlapPercentInBucket = StatsMathUtil.minNonNaN(1, (numVal - bucket.lower)
                            / (bucket.upper - bucket.lower));
                }
                double overlapCountInBucket = overlapPercentInBucket * bucket.count;
                double sel = StatsMathUtil.minNonNaN(1, (bucket.preSum + overlapCountInBucket)
                        / StatsMathUtil.nonZeroDivisor(context.statistics.getRowCount()));
                List<Bucket> updatedBucketList = leftHist.buckets.subList(0, i + 1);
                updatedBucketList.add(new Bucket(bucket.lower, numVal, overlapCountInBucket,
                        bucket.preSum, overlapPercentInBucket * bucket.ndv));
                ColumnStatistic columnStatistic = new ColumnStatisticBuilder(leftStats)
                        .setMaxValue(numVal)
                        .setHistogram(new HistogramBuilder(leftHist).setBuckets(updatedBucketList).build())
                        .build();
                return context.statistics.withSel(sel).addColumnStats(leftExpr, columnStatistic);
            }
        }
        return context.statistics.withSel(0);
    }

    private Statistics estimateGreaterThanLiteralWithHistogram(Expression leftExpr, ColumnStatistic leftStats,
            double numVal, EstimationContext context, boolean contains) {
        Histogram leftHist = leftStats.histogram;

        for (int i = 0; i < leftHist.buckets.size(); i++) {
            Bucket bucket = leftHist.buckets.get(i);
            if (bucket.upper >= numVal && bucket.lower <= numVal) {
                double overlapPercentInBucket;
                if (numVal == bucket.upper && numVal == bucket.lower) {
                    if (contains) {
                        overlapPercentInBucket = 1;
                    } else {
                        overlapPercentInBucket = 0;
                    }
                } else {
                    overlapPercentInBucket = StatsMathUtil.minNonNaN(1, ((bucket.upper - numVal)
                            / (bucket.upper - bucket.lower)));
                }
                double overlapCountInBucket = overlapPercentInBucket * bucket.count;
                double sel = StatsMathUtil.minNonNaN(1,
                        (leftHist.size() - bucket.preSum - (bucket.count - overlapCountInBucket))
                        / context.statistics.getRowCount());
                List<Bucket> updatedBucketList = new ArrayList<>();
                updatedBucketList.add(new Bucket(numVal, bucket.upper, overlapPercentInBucket * bucket.count,
                        0, overlapPercentInBucket * bucket.ndv));
                updatedBucketList.addAll(leftHist.buckets.subList(i, leftHist.buckets.size()));
                ColumnStatistic columnStatistic = new ColumnStatisticBuilder(leftStats)
                        .setMaxValue(numVal)
                        .setHistogram(new HistogramBuilder(leftHist).setBuckets(updatedBucketList).build())
                        .build();
                return context.statistics.withSel(sel).addColumnStats(leftExpr, columnStatistic);
            }
        }
        return context.statistics.withSel(0);
    }

    private Statistics estimateEqualToWithHistogram(Expression leftExpr, ColumnStatistic leftStats,
            double numVal, EstimationContext context) {
        Histogram histogram = leftStats.histogram;

        double sel = 0;
        for (int i = 0; i < histogram.buckets.size(); i++) {
            Bucket bucket = histogram.buckets.get(i);
            if (bucket.upper >= numVal && bucket.lower <= numVal) {
                sel = (bucket.count / bucket.ndv) / histogram.size();
            }
        }
        if (sel == 0) {
            return Statistics.zero(context.statistics);
        }
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(leftStats)
                .setHistogram(null)
                .setNdv(1)
                .setNumNulls(0)
                .setMaxValue(numVal)
                .setMinValue(numVal)
                .build();
        return context.statistics.withSel(sel).addColumnStats(leftExpr, columnStatistic);
    }

    @Override
    public Statistics visitLike(Like like, EstimationContext context) {
        return context.statistics.withSel(DEFAULT_LIKE_COMPARISON_SELECTIVITY);
    }
}
