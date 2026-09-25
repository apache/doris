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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.statistics.model.Bucket;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Histogram;
import org.apache.doris.statistics.model.HistogramBuilder;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters rewrite the histogram of the column so that it describes the rows they keep: the buckets
 * are cut to the range, negated values are taken out, an equality or IN list becomes the matched
 * points. The join reads the result as is.
 */
public class FilterEstimationHistogramTest {

    private final SlotReference a = new SlotReference("a", IntegerType.INSTANCE);

    // value 0 in half of the rows, 1..1004 evenly in four buckets of 251 values, 500 rows each
    private Histogram histogram() {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        List<Bucket> cold = new ArrayList<>();
        for (int b = 0; b < 4; b++) {
            cold.add(new Bucket(1 + b * 251, 251 + b * 251, 125500, b * 125500, 251));
        }
        return new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(4).setBuckets(cold)
                .setMcv(mcv).setMcvBuckets(cold).build();
    }

    private Statistics statistics(Histogram histogram) {
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(a, new ColumnStatisticBuilder(1000000).setNdv(1005).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(0).setMaxValue(1004).setMinExpr(new IntegerLiteral(0).toLegacyLiteral())
                .setMaxExpr(new IntegerLiteral(1004).toLegacyLiteral()).setHistogram(histogram).build());
        return new Statistics(1000000, columnStats);
    }

    private Histogram filtered(Expression predicate) {
        Statistics result = new FilterEstimation().estimate(predicate, statistics(histogram()));
        return result.findColumnStatistics(a).histogram;
    }

    // value 0 in half of the rows, the other half skewed to the low end: 400000, 60000, 30000, 12000
    // rows in four buckets of 251 values
    private Histogram skewed() {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        List<Bucket> cold = new ArrayList<>();
        double[] counts = {400000, 60000, 30000, 12000};
        double preSum = 0;
        for (int b = 0; b < 4; b++) {
            cold.add(new Bucket(1 + b * 251, 251 + b * 251, counts[b], preSum, 251));
            preSum += counts[b];
        }
        return new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(4).setBuckets(cold)
                .setMcv(mcv).setMcvBuckets(cold).build();
    }

    private double rows(Expression predicate, Histogram histogram) {
        return new FilterEstimation().estimate(predicate, statistics(histogram)).getRowCount();
    }

    @Test
    public void testRowCountFollowsTheHistogram() {
        // k <= 251: the hot value and the whole first bucket, 0.5 + 0.5 * 400000 / 502000 of the rows
        Assertions.assertEquals(1000000 * (0.5 + 0.5 * 400000 / 502000.0),
                rows(new LessThanEqual(a, new IntegerLiteral(251)), skewed()), 1);
        // without a histogram the range is linear: 251 / 1004 of the rows
        Assertions.assertEquals(1000000 * 251 / 1004.0,
                rows(new LessThanEqual(a, new IntegerLiteral(251)), null), 1);
        // k > 753: the last bucket only
        Assertions.assertEquals(1000000 * 0.5 * 12000 / 502000.0,
                rows(new GreaterThan(a, new IntegerLiteral(753)), skewed()), 1);
        // the ndv comes from the histogram left: 251 values and the hot value
        Statistics result = new FilterEstimation().estimate(new LessThanEqual(a, new IntegerLiteral(251)),
                statistics(skewed()));
        Assertions.assertEquals(252, result.findColumnStatistics(a).ndv, 1e-6);
    }

    @Test
    public void testEqualityAndInFollowTheHistogram() {
        // a cold value: its share of the bucket holding it, without the rows of the hot value
        double five = 0.5 * 400000 / 502000.0 / 251;
        Assertions.assertEquals(1000000 * five, rows(new EqualTo(a, new IntegerLiteral(5)), skewed()), 1);
        Assertions.assertEquals(1000000 / 1005.0, rows(new EqualTo(a, new IntegerLiteral(5)), null), 1);
        // the hot value
        Assertions.assertEquals(500000, rows(new EqualTo(a, new IntegerLiteral(0)), skewed()), 1);
        // IN: hot value, a value of the first bucket, a value of the third
        double sixHundred = 0.5 * 30000 / 502000.0 / 251;
        InPredicate in = new InPredicate(a,
                Lists.newArrayList(new IntegerLiteral(0), new IntegerLiteral(5), new IntegerLiteral(600)));
        Assertions.assertEquals(1000000 * (0.5 + five + sixHundred), rows(in, skewed()), 1);
        // a value in no bucket and not hot: nothing
        Assertions.assertEquals(1, rows(new EqualTo(a, new IntegerLiteral(1005)), skewed()), 1);
    }

    private static double sum(Map<Literal, Float> ratios) {
        return ratios.values().stream().mapToDouble(r -> r).sum();
    }

    @Test
    public void testLessThan() {
        Histogram open = filtered(new LessThan(a, new IntegerLiteral(502)));
        Assertions.assertEquals(2, open.mcvBuckets.size());
        Assertions.assertEquals(501, open.mcvBuckets.get(1).upper, 0, "k < 502 stops at 501");
        Assertions.assertEquals(125500 * 250.0 / 251, open.mcvBuckets.get(1).count, 1e-6);
        Assertions.assertEquals(1, open.mcv.size());
        // the hot value is 0.5 of the rows against 0.5 * (501 / 1004) in the buckets left
        double bucketShare = 0.5 * (125500 + 125500 * 250.0 / 251) / (4 * 125500);
        Assertions.assertEquals(0.5 / (0.5 + bucketShare), open.mcv.get(new BigIntLiteral(0)), 1e-6);

        Histogram closed = filtered(new LessThanEqual(a, new IntegerLiteral(502)));
        Assertions.assertEquals(502, closed.mcvBuckets.get(1).upper, 0);
        Assertions.assertEquals(125500, closed.mcvBuckets.get(1).count, 1e-6);
    }

    @Test
    public void testGreaterThan() {
        Histogram open = filtered(new GreaterThan(a, new IntegerLiteral(0)));
        Assertions.assertTrue(open.mcv.isEmpty(), "k > 0 drops the hot value 0");
        Assertions.assertEquals(4, open.buckets.size());
        Assertions.assertEquals(125500, open.buckets.get(0).count, 1e-6, "the buckets keep their shape");

        Histogram closed = filtered(new GreaterThanEqual(a, new IntegerLiteral(0)));
        Assertions.assertEquals(0.5, closed.mcv.get(new BigIntLiteral(0)), 1e-9, "k >= 0 keeps everything");

        Histogram tail = filtered(new GreaterThan(a, new IntegerLiteral(1000)));
        Assertions.assertEquals(1, tail.buckets.size());
        Assertions.assertEquals(1001, tail.buckets.get(0).lower, 0);
        Assertions.assertEquals(125500 * 4.0 / 251, tail.buckets.get(0).count, 1e-6);
        Assertions.assertEquals(4, tail.buckets.get(0).ndv, 1e-6);
    }

    @Test
    public void testEqual() {
        Histogram hot = filtered(new EqualTo(a, new IntegerLiteral(0)));
        Assertions.assertTrue(hot.mcvBuckets.isEmpty());
        Assertions.assertEquals(1.0, hot.mcv.get(new BigIntLiteral(0)), 1e-9, "all rows are the hot value");

        Histogram cold = filtered(new EqualTo(a, new IntegerLiteral(5)));
        Assertions.assertTrue(cold.mcvBuckets.isEmpty());
        Assertions.assertEquals(1, cold.mcv.size());
        Assertions.assertEquals(1.0, cold.mcv.get(new IntegerLiteral(5)), 1e-9);

        Assertions.assertNull(filtered(new EqualTo(a, new IntegerLiteral(5000))), "a value outside the histogram");
    }

    @Test
    public void testNotEqual() {
        Histogram noHot = filtered(new Not(new EqualTo(a, new IntegerLiteral(0))));
        Assertions.assertTrue(noHot.mcv.isEmpty(), "k <> 0 drops the hot value");
        Assertions.assertEquals(4, noHot.buckets.size());
        Assertions.assertEquals(125500, noHot.buckets.get(0).count, 1e-6);

        Histogram noCold = filtered(new Not(new EqualTo(a, new IntegerLiteral(5))));
        Assertions.assertEquals(125500 - 125500.0 / 251, noCold.mcvBuckets.get(0).count, 1e-6);
        Assertions.assertEquals(250, noCold.mcvBuckets.get(0).ndv, 1e-6);
        Assertions.assertEquals(125500, noCold.mcvBuckets.get(1).count, 1e-6, "other buckets are untouched");
        Assertions.assertTrue(noCold.mcv.get(new BigIntLiteral(0)) > 0.5, "the hot value gains a little");
    }

    @Test
    public void testIn() {
        Histogram points = filtered(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(0), new IntegerLiteral(5), new IntegerLiteral(7))));
        Assertions.assertTrue(points.mcvBuckets.isEmpty());
        Assertions.assertEquals(3, points.mcv.size());
        Assertions.assertEquals(1.0, sum(points.mcv), 1e-6);
        // a cold value holds 0.5 / 1004 of the rows
        double cold = 0.5 / 1004;
        Assertions.assertEquals(0.5 / (0.5 + 2 * cold), points.mcv.get(new BigIntLiteral(0)), 1e-6);
        Assertions.assertEquals(cold / (0.5 + 2 * cold), points.mcv.get(new IntegerLiteral(5)), 1e-6);

        Histogram coldOnly = filtered(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(5), new IntegerLiteral(7), new IntegerLiteral(5000))));
        Assertions.assertEquals(2, coldOnly.mcv.size(), "5000 is outside");
        Assertions.assertEquals(0.5, coldOnly.mcv.get(new IntegerLiteral(5)), 1e-6);
    }

    @Test
    public void testNotIn() {
        Histogram rest = filtered(new Not(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(0), new IntegerLiteral(5)))));
        Assertions.assertTrue(rest.mcv.isEmpty());
        Assertions.assertEquals(4, rest.buckets.size());
        Assertions.assertEquals(250, rest.buckets.get(0).ndv, 1e-6);
        Assertions.assertEquals(251, rest.buckets.get(1).ndv, 1e-6);
    }

    @Test
    public void testAndAppliesBothCuts() {
        Histogram band = filtered(new And(new GreaterThan(a, new IntegerLiteral(0)),
                new LessThan(a, new IntegerLiteral(300))));
        Assertions.assertTrue(band.mcv.isEmpty());
        Assertions.assertEquals(2, band.buckets.size());
        Assertions.assertEquals(1, band.buckets.get(0).lower, 0);
        Assertions.assertEquals(299, band.buckets.get(1).upper, 0);
        Assertions.assertEquals(125500 * 48.0 / 251, band.buckets.get(1).count, 1e-6);
    }

    @Test
    public void testOr() {
        Assertions.assertNull(filtered(new Or(new EqualTo(a, new IntegerLiteral(0)),
                new EqualTo(a, new IntegerLiteral(5)))), "the points of the two branches are not merged");
        // range predicates do not register the column as a key, so OR leaves its column stats,
        // histogram included, as they came in
        Statistics input = statistics(histogram());
        Statistics result = new FilterEstimation().estimate(new Or(new LessThan(a, new IntegerLiteral(5)),
                new GreaterThan(a, new IntegerLiteral(995))), input);
        Assertions.assertSame(input.findColumnStatistics(a).histogram, result.findColumnStatistics(a).histogram);
        Assertions.assertEquals(1005, result.findColumnStatistics(a).ndv, 0);
    }

    @Test
    public void testIsNotNullKeepsTheHistogram() {
        Statistics input = statistics(histogram());
        Statistics result = new FilterEstimation().estimate(new Not(new IsNull(a)), input);
        Assertions.assertSame(input.findColumnStatistics(a).histogram, result.findColumnStatistics(a).histogram);
        Assertions.assertNull(new FilterEstimation().estimate(new IsNull(a), input).findColumnStatistics(a).histogram);
    }

    /** the hot values of the column are kept in step too: the matched ones as ratios of the rows kept. */
    @Test
    public void testHotValuesRescaled() {
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(new IntegerLiteral(0), 0.5f);
        hotValues.put(new IntegerLiteral(999), 5e-4f);
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(a, new ColumnStatisticBuilder(1000000).setNdv(1005).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(0).setMaxValue(1004).setMinExpr(new IntegerLiteral(0).toLegacyLiteral())
                .setMaxExpr(new IntegerLiteral(1004).toLegacyLiteral()).setHotValues(hotValues).build());
        Statistics input = new Statistics(1000000, columnStats);
        FilterEstimation estimation = new FilterEstimation();

        // k > 0 keeps the other half of the rows: 999 doubles
        Map<Literal, Float> gt0 = estimation.estimate(new GreaterThan(a, new IntegerLiteral(0)), input)
                .findColumnStatistics(a).getHotValues();
        Assertions.assertNull(gt0.get(new IntegerLiteral(0)));
        Assertions.assertEquals(1e-3, gt0.get(new IntegerLiteral(999)), 1e-6);

        // k IN (0, 1, 2) keeps 0 and two cold values of 1003
        Map<Literal, Float> in = estimation.estimate(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(0), new IntegerLiteral(1), new IntegerLiteral(2))), input)
                .findColumnStatistics(a).getHotValues();
        Assertions.assertEquals(0.5 / (0.5 + 0.4995 * 2 / 1003), in.get(new IntegerLiteral(0)), 1e-4);

        // k = 0 is all the rows
        Map<Literal, Float> eq = estimation.estimate(new EqualTo(a, new IntegerLiteral(0)), input)
                .findColumnStatistics(a).getHotValues();
        Assertions.assertEquals(1.0, eq.get(new IntegerLiteral(0)), 0);

        // NOT (k = 0) keeps the other half of the rows: 999 doubles
        Map<Literal, Float> ne = estimation.estimate(new Not(new EqualTo(a, new IntegerLiteral(0))), input)
                .findColumnStatistics(a).getHotValues();
        Assertions.assertNull(ne.get(new IntegerLiteral(0)));
        Assertions.assertEquals(1e-3, ne.get(new IntegerLiteral(999)), 1e-6);
    }

    @Test
    public void testInWithNonLiteralOptionDropsTheHistogram() {
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(a, statistics(histogram()).findColumnStatistics(a));
        columnStats.put(b, new ColumnStatisticBuilder(1000000).setNdv(10).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(0).setMaxValue(9).build());
        Statistics result = new FilterEstimation().estimate(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(0), new Add(b, new IntegerLiteral(1)))), new Statistics(1000000, columnStats));
        Assertions.assertNull(result.findColumnStatistics(a).histogram);
    }

    @Test
    public void testNotInOnPlainHistogram() {
        // no section: the buckets themselves lose the values
        List<Bucket> plain = new ArrayList<>();
        plain.add(new Bucket(0, 0, 500000, 0, 1));
        plain.add(new Bucket(1, 1004, 500000, 500000, 1004));
        Histogram h = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(2)
                .setBuckets(plain).build();
        Statistics result = new FilterEstimation().estimate(new Not(new InPredicate(a, Lists.newArrayList(
                new IntegerLiteral(0), new IntegerLiteral(5)))), statistics(h));
        Histogram rest = result.findColumnStatistics(a).histogram;
        Assertions.assertTrue(rest.mcv.isEmpty());
        Assertions.assertEquals(1, rest.buckets.size(), "the single value bucket of 0 is gone");
        Assertions.assertEquals(1003, rest.buckets.get(0).ndv, 1e-6);
    }

    @Test
    public void testDateRange() throws Exception {
        SlotReference d = new SlotReference("d", DateType.INSTANCE);
        DateLiteral jan01 = new DateLiteral("2025-01-01");
        DateLiteral dec31 = new DateLiteral("2025-12-31");
        DateLiteral jul01 = new DateLiteral("2025-07-01");
        // one bucket over the year, 365 days, 1000 rows a day
        List<Bucket> one = new ArrayList<>();
        one.add(new Bucket(jan01.getDouble(), dec31.getDouble(), 365000, 0, 365));
        Histogram h = new HistogramBuilder().setDataType(Type.DATE).setSampleRate(0).setNumBuckets(1)
                .setBuckets(one).build();
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(d, new ColumnStatisticBuilder(365000).setNdv(365).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(jan01.getDouble()).setMaxValue(dec31.getDouble())
                .setMinExpr(jan01.toLegacyLiteral()).setMaxExpr(dec31.toLegacyLiteral()).setHistogram(h).build());
        Statistics result = new FilterEstimation().estimate(new GreaterThanEqual(d, jul01),
                new Statistics(365000, columnStats));
        Bucket cut = result.findColumnStatistics(d).histogram.buckets.get(0);
        // July 1 to December 31 are 184 of the 365 days
        Assertions.assertEquals(184000, cut.count, 1e-6);
        Assertions.assertEquals(184, cut.ndv, 1e-6);

        // NOT (d = c) drops the hot value even when the literal is a DATEV2 and the key a DATE
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(jan01, 0.5f);
        columnStats.put(d, new ColumnStatisticBuilder(columnStats.get(d)).setHotValues(hotValues).build());
        Map<Literal, Float> rest = new FilterEstimation()
                .estimate(new Not(new EqualTo(d, new DateV2Literal("2025-01-01"))), new Statistics(365000, columnStats))
                .findColumnStatistics(d).getHotValues();
        Assertions.assertTrue(rest.isEmpty(), "the hot value is gone: " + rest);
    }

    /** the rescaled hot values never sum above 1, however small the row count estimate came out. */
    @Test
    public void testHotValuesRescaledSumStaysBelowOne() {
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(new IntegerLiteral(1), 0.3f);
        hotValues.put(new IntegerLiteral(2), 0.3f);
        hotValues.put(new IntegerLiteral(3), 0.3f);
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        // ndv 3 with hot values 0.9 in total: NOT (k = 4) keeps nearly all rows, k > 0 keeps all
        columnStats.put(a, new ColumnStatisticBuilder(1000).setNdv(3).setAvgSizeByte(4).setNumNulls(0)
                .setMinValue(1).setMaxValue(4).setMinExpr(new IntegerLiteral(1).toLegacyLiteral())
                .setMaxExpr(new IntegerLiteral(4).toLegacyLiteral()).setHotValues(hotValues).build());
        Statistics input = new Statistics(1000, columnStats);
        for (Expression predicate : Lists.<Expression>newArrayList(new Not(new EqualTo(a, new IntegerLiteral(4))),
                new GreaterThan(a, new IntegerLiteral(0)),
                new InPredicate(a, Lists.newArrayList(new IntegerLiteral(1), new IntegerLiteral(2))))) {
            Map<Literal, Float> kept = new FilterEstimation().estimate(predicate, input)
                    .findColumnStatistics(a).getHotValues();
            Assertions.assertTrue(sum(kept) <= 1 + 1e-6, predicate + " -> " + kept);
            for (Float ratio : kept.values()) {
                Assertions.assertTrue(ratio <= 1 + 1e-6, predicate + " -> " + kept);
            }
        }
    }

    @Test
    public void testLikeDropsTheHistogram() {
        SlotReference s = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(s, new ColumnStatisticBuilder(1000000).setNdv(1005).setAvgSizeByte(4).setNumNulls(0)
                .setHistogram(histogram()).build());
        Statistics result = new FilterEstimation().estimate(new Like(s, new VarcharLiteral("v%")),
                new Statistics(1000000, columnStats));
        Assertions.assertNull(result.findColumnStatistics(s).histogram);
    }
}
