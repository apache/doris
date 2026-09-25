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

package org.apache.doris.statistics.model;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HistogramRewriteTest {

    // ndv distinct values from lo split evenly across numBuckets contiguous buckets
    private static Histogram evenHistogram(Type type, double lo, double ndv, int numBuckets, double rowsPerValue) {
        List<Bucket> buckets = new ArrayList<>();
        double perBucket = ndv / numBuckets;
        double preSum = 0;
        for (int b = 0; b < numBuckets; b++) {
            double lower = lo + b * perBucket;
            double count = perBucket * rowsPerValue;
            buckets.add(new Bucket(lower, lower + perBucket - 1, count, preSum, perBucket));
            preSum += count;
        }
        return new HistogramBuilder().setDataType(type).setSampleRate(1.0).setNumBuckets(numBuckets)
                .setBuckets(buckets).build();
    }

    // mcv {0: 0.5, 999: 0.1}, the other rows in [1, 998] (count 400, ndv 998) and a single value bucket [999, 999]
    private static Histogram section() {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        mcv.put(new BigIntLiteral(999), 0.1f);
        List<Bucket> cold = new ArrayList<>();
        cold.add(new Bucket(1, 998, 400, 0, 998));
        cold.add(new Bucket(1000, 1000, 10, 400, 1));
        // the stored buckets cover all rows: the hot values sit in their own buckets
        List<Bucket> all = new ArrayList<>();
        all.add(new Bucket(0, 0, 500, 0, 1));
        all.add(new Bucket(1, 998, 400, 500, 998));
        all.add(new Bucket(999, 999, 100, 900, 1));
        all.add(new Bucket(1000, 1000, 10, 1000, 1));
        return new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(4).setBuckets(all)
                .setMcv(mcv).setMcvBuckets(cold).build();
    }

    private static List<Literal> values(long... values) {
        List<Literal> result = new ArrayList<>();
        for (long value : values) {
            result.add(new BigIntLiteral(value));
        }
        return result;
    }

    private static double sum(Map<Literal, Float> ratios) {
        return ratios.values().stream().mapToDouble(r -> r).sum();
    }

    @Test
    public void testIntersectRangeCutsBoundaryBucketByIntegerWidth() {
        Histogram full = evenHistogram(Type.INT, 0, 1024, 8, 1000);
        Histogram upper = full.intersectRange(500, 1023, true);
        Assertions.assertEquals(5, upper.buckets.size());
        Assertions.assertTrue(upper.mcv.isEmpty());
        // [384, 511] keeps 500..511: 12 of 128 values
        Bucket boundary = upper.buckets.get(0);
        Assertions.assertEquals(500, boundary.lower, 0);
        Assertions.assertEquals(511, boundary.upper, 0);
        Assertions.assertEquals(128000 * 12.0 / 128, boundary.count, 1e-9);
        Assertions.assertEquals(12, boundary.ndv, 1e-9);
        Assertions.assertEquals(128000, upper.buckets.get(1).count, 1e-9);

        // a non integer column with the same ndv per bucket: 128 values 127 gaps apart, the interval
        // [500, 511] spans 11 gaps and holds 12 of them too
        Histogram real = evenHistogram(Type.DOUBLE, 0, 1024, 8, 1000).intersectRange(500, 1023, true);
        Assertions.assertEquals(128000 * 12.0 / 128, real.buckets.get(0).count, 1e-9);
        // one value per unit is what makes it (len + 1) / (length + 1): a bucket holding only every
        // other value of its range keeps a share closer to the plain length ratio
        List<Bucket> sparse = new ArrayList<>();
        sparse.add(new Bucket(0, 126, 64000, 0, 64));
        Histogram sparseHist = new HistogramBuilder().setDataType(Type.INT).setSampleRate(1.0).setNumBuckets(1)
                .setBuckets(sparse).build();
        Assertions.assertEquals(64000 * (63 * 63.0 / 126 + 1) / 64, sparseHist.intersectRange(0, 63, true)
                .buckets.get(0).count, 1e-6);
    }

    @Test
    public void testIntersectRangeOpenInterval() {
        Histogram full = evenHistogram(Type.INT, 0, 1024, 8, 1000);
        // k < 512 stops at 511: four whole buckets
        Histogram open = full.intersectRange(Double.NEGATIVE_INFINITY, 512, false);
        Assertions.assertEquals(4, open.buckets.size());
        Assertions.assertEquals(511, open.buckets.get(3).upper, 0);
        // k <= 512 also keeps one value of the fifth bucket
        Histogram closed = full.intersectRange(Double.NEGATIVE_INFINITY, 512, true);
        Assertions.assertEquals(5, closed.buckets.size());
        Assertions.assertEquals(1000, closed.buckets.get(4).count, 1e-9);
        Assertions.assertEquals(1, closed.buckets.get(4).ndv, 1e-9);
        // k > 1023 on a column ending at 1023 leaves nothing
        Assertions.assertNull(full.intersectRange(1023, Double.POSITIVE_INFINITY, false));
        // a non integer open bound only excludes the bound itself: k < 600 keeps as much of [512, 639]
        // as k <= 600, while on an integer column it keeps one value less
        Histogram real = evenHistogram(Type.DOUBLE, 0, 1024, 8, 1000);
        Assertions.assertEquals(real.intersectRange(Double.NEGATIVE_INFINITY, 600, true).buckets.get(4).count,
                real.intersectRange(Double.NEGATIVE_INFINITY, 600, false).buckets.get(4).count, 1e-6);
        Assertions.assertEquals(128000 * 88.0 / 128, full.intersectRange(Double.NEGATIVE_INFINITY, 600, false)
                .buckets.get(4).count, 1e-9);
        Assertions.assertEquals(128000 * 89.0 / 128, full.intersectRange(Double.NEGATIVE_INFINITY, 600, true)
                .buckets.get(4).count, 1e-9);
    }

    @Test
    public void testIntersectRangeCutsStringBucketsByEncoding() {
        // the first 7 bytes encoded as a number: monotone, and for digit strings of one length within
        // about 10% of the share of values (each digit position uses 10 of 256 codes)
        double c0000 = new VarcharLiteral("c0000").getDouble();
        double c0500 = new VarcharLiteral("c0500").getDouble();
        double c0999 = new VarcharLiteral("c0999").getDouble();
        List<Bucket> one = new ArrayList<>();
        one.add(new Bucket(c0000, c0999, 100000, 0, 1000));
        Histogram h = new HistogramBuilder().setDataType(Type.VARCHAR).setSampleRate(0).setNumBuckets(1)
                .setBuckets(one).build();
        Bucket cut = h.intersectRange(c0500, Double.POSITIVE_INFINITY, false).buckets.get(0);
        double encoded = (c0999 - Math.nextUp(c0500)) / (c0999 - c0000);
        Assertions.assertEquals(100000 * (encoded * 999 + 1) / 1000, cut.count, 1e-3);
        Assertions.assertEquals(100000 * 499.0 / 999, cut.count, 100000 * 0.06, "c0501 .. c0999 of c0000 .. c0999");
        Assertions.assertTrue(cut.lower > c0500);
    }

    @Test
    public void testIntersectRangeMeasuresDatesInDays() throws Exception {
        // a 28 day bucket across a month boundary: the yyyymmdd encoding jumps by 70 days' worth at
        // the boundary, so cutting by encoded width would keep 15 / 96 instead of 16 / 28
        double may20 = new DateLiteral("2025-05-20").getDouble();
        double jun01 = new DateLiteral("2025-06-01").getDouble();
        double jun16 = new DateLiteral("2025-06-16").getDouble();
        List<Bucket> one = new ArrayList<>();
        one.add(new Bucket(may20, jun16, 2800, 0, 28));
        Histogram h = new HistogramBuilder().setDataType(Type.DATE).setSampleRate(0).setNumBuckets(1)
                .setBuckets(one).build();
        Bucket cut = h.intersectRange(jun01, Double.POSITIVE_INFINITY, true).buckets.get(0);
        // a date interval holds one more day than its length: June 1 .. June 16 is 16 of 28 days
        Assertions.assertEquals(2800 * 16.0 / 28, cut.count, 1e-6);
        Assertions.assertEquals(28 * 16.0 / 28, cut.ndv, 1e-6);
        // an open bound on a date is measured like the closed one: the type counts whole days and
        // Doris' own range selectivity does not tell > from >= on dates either
        Bucket open = h.intersectRange(jun01, Double.POSITIVE_INFINITY, false).buckets.get(0);
        Assertions.assertEquals(cut.count, open.count, 1e-9);
        Assertions.assertTrue(open.lower > jun01);
        // a single day bucket is either in or out
        List<Bucket> day = new ArrayList<>();
        day.add(new Bucket(jun01, jun01, 100, 0, 1));
        Histogram single = new HistogramBuilder().setDataType(Type.DATE).setSampleRate(0).setNumBuckets(1)
                .setBuckets(day).build();
        Assertions.assertNull(single.intersectRange(jun01, Double.POSITIVE_INFINITY, false));
        Assertions.assertEquals(100, single.intersectRange(jun01, Double.POSITIVE_INFINITY, true)
                .buckets.get(0).count, 0);
    }

    @Test
    public void testOperationsChain() {
        // IN then a range: the points are hot values, the range drops the ones outside and rescales
        Histogram points = section().retainValues(values(0, 5, 700));
        Histogram cut = points.intersectRange(Double.NEGATIVE_INFINITY, 100, true);
        Assertions.assertEquals(2, cut.mcv.size());
        Assertions.assertTrue(cut.mcvBuckets.isEmpty());
        Assertions.assertEquals(1.0, sum(cut.mcv), 1e-6, "only hot values, they are all the rows");
        // a range then NOT: the removal works on the cut buckets
        Histogram range = evenHistogram(Type.INT, 0, 1024, 8, 1000).intersectRange(100, 900, true);
        Histogram removed = range.removeValues(values(500));
        Bucket holding = range.buckets.get(3);
        Assertions.assertEquals(range.buckets.stream().mapToDouble(b -> b.count).sum() - holding.count / holding.ndv,
                removed.buckets.stream().mapToDouble(b -> b.count).sum(), 1e-6);
        // removing every value of a single value section leaves nothing
        Assertions.assertNull(section().retainValues(values(0)).removeValues(values(0)));
        Assertions.assertNull(evenHistogram(Type.INT, 0, 1, 1, 10).removeValues(values(0)));
    }

    @Test
    public void testFromHotValuesKeepsStoredBuckets() throws Exception {
        // a plain stored histogram: value 0 in its own bucket, 1..1004 in two buckets
        List<Bucket> plain = new ArrayList<>();
        plain.add(new Bucket(0, 0, 500000, 0, 1));
        plain.add(new Bucket(1, 502, 251000, 500000, 502));
        plain.add(new Bucket(503, 1004, 249000, 751000, 502));
        Histogram stored = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(3)
                .setBuckets(plain).build();
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(new BigIntLiteral(0), 0.5f);
        ColumnStatistic colStats = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000).setNdv(1005)
                .setMinValue(0).setMaxValue(1004).setMinExpr(new IntLiteral(0, Type.INT)).setIsUnknown(false)
                .setHotValues(hotValues).setHistogram(stored).build();
        Histogram h = Histogram.fromHotValues(colStats);
        Assertions.assertEquals(0.5, h.mcv.get(new BigIntLiteral(0)), 0);
        Assertions.assertEquals(2, h.mcvBuckets.size(), "the bucket of the hot value is gone, the shape stays");
        // all rows hot: no bucket for the other values
        Map<Literal, Float> whole = new LinkedHashMap<>();
        whole.put(new BigIntLiteral(0), 0.6f);
        whole.put(new BigIntLiteral(1), 0.4f);
        Histogram hotOnly = Histogram.fromHotValues(new ColumnStatisticBuilder(colStats).setNdv(2)
                .setHotValues(whole).setHistogram(null).build());
        Assertions.assertTrue(hotOnly.mcvBuckets.isEmpty());
        Assertions.assertEquals(1, h.mcvBuckets.get(0).lower, 0);
        Assertions.assertEquals(251000, h.mcvBuckets.get(0).count, 0);
        Assertions.assertEquals(249000, h.mcvBuckets.get(1).count, 0);
        Assertions.assertTrue(h.buckets.isEmpty());
        // a stored histogram with a section is not rebuilt from the hot values
        Assertions.assertSame(stored.buckets, colStats.histogram.buckets);
    }

    @Test
    public void testFromHotValuesAfterEquality() throws Exception {
        // k = 5 leaves {5: 1} and ndv 1: the value is all the rows, no bucket for other values
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(new BigIntLiteral(5), 1.0f);
        ColumnStatistic colStats = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 500).setNdv(1)
                .setMinValue(5).setMaxValue(5).setMinExpr(new IntLiteral(5, Type.INT)).setIsUnknown(false)
                .setHotValues(hotValues).build();
        Histogram h = Histogram.fromHotValues(colStats);
        Assertions.assertEquals(1.0, h.mcv.get(new BigIntLiteral(5)), 0);
        Assertions.assertTrue(h.mcvBuckets.isEmpty());
    }

    @Test
    public void testIntersectRangeRescalesHotValues() {
        Histogram cut = section().intersectRange(Double.NEGATIVE_INFINITY, 500, true);
        Assertions.assertEquals(1, cut.mcv.size(), "999 is outside the range");
        Assertions.assertEquals(1, cut.mcvBuckets.size(), "the single value bucket [1000, 1000] is outside");
        Assertions.assertEquals(2, cut.buckets.size(), "the stored buckets [0, 0] and [1, 500] are cut too");
        Bucket bucket = cut.mcvBuckets.get(0);
        Assertions.assertEquals(400 * 500.0 / 998, bucket.count, 1e-9);
        Assertions.assertEquals(500, bucket.ndv, 1e-9);
        // the buckets kept 500/998 of their 410 rows worth 0.4, so the hot value 0.5 becomes 0.5 / (0.5 + 0.4 * share)
        double bucketShare = 0.4 * (400 * 500.0 / 998) / 410;
        Assertions.assertEquals(0.5 / (0.5 + bucketShare), cut.mcv.get(new BigIntLiteral(0)), 1e-6);

        Histogram same = section().intersectRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true);
        Assertions.assertEquals(0.5, same.mcv.get(new BigIntLiteral(0)), 1e-9, "a full range changes nothing");
        Assertions.assertEquals(0.1, same.mcv.get(new BigIntLiteral(999)), 1e-6);
        Assertions.assertEquals(2, same.mcvBuckets.size());

        Assertions.assertNull(section().intersectRange(2000, 3000, true), "nothing left");
        Histogram hotOnly = section().intersectRange(0, 0, true);
        Assertions.assertTrue(hotOnly.mcvBuckets.isEmpty());
        Assertions.assertEquals(1.0, hotOnly.mcv.get(new BigIntLiteral(0)), 1e-9, "all rows are the hot value");
    }

    @Test
    public void testRemoveValues() {
        Histogram noHot = section().removeValues(values(0));
        Assertions.assertEquals(1, noHot.mcv.size());
        Assertions.assertNull(noHot.mcv.get(new BigIntLiteral(0)));
        // the remaining hot value 0.1 against buckets worth 0.4: 0.1 / 0.5
        Assertions.assertEquals(0.2, noHot.mcv.get(new BigIntLiteral(999)), 1e-6);
        Assertions.assertEquals(400, noHot.mcvBuckets.get(0).count, 1e-9, "buckets are untouched");

        Histogram noSingle = section().removeValues(values(1000));
        Assertions.assertEquals(1, noSingle.mcvBuckets.size(), "the single value bucket is dropped");
        Assertions.assertEquals(1, noSingle.mcvBuckets.get(0).lower, 0);
        Assertions.assertEquals(0.5 / (0.6 + 0.4 * 400 / 410.0), noSingle.mcv.get(new BigIntLiteral(0)), 1e-6);

        Histogram noCold = section().removeValues(values(500));
        Bucket bucket = noCold.mcvBuckets.get(0);
        Assertions.assertEquals(400 - 400.0 / 998, bucket.count, 1e-9, "one value's share of the count");
        Assertions.assertEquals(997, bucket.ndv, 1e-9);

        Histogram missing = section().removeValues(values(5000));
        Assertions.assertEquals(0.5, missing.mcv.get(new BigIntLiteral(0)), 1e-9, "unknown values change nothing");
        Assertions.assertEquals(400, missing.mcvBuckets.get(0).count, 1e-9);

        // a plain histogram: single value bucket dropped, mcv stays null
        List<Bucket> plain = new ArrayList<>();
        plain.add(new Bucket(0, 0, 500000, 0, 1));
        plain.add(new Bucket(1, 1004, 500000, 500000, 1004));
        Histogram h = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(2)
                .setBuckets(plain).build();
        Histogram removed = h.removeValues(values(0));
        Assertions.assertTrue(removed.mcv.isEmpty(), "no section, the result stays a plain histogram");
        Assertions.assertEquals(1, removed.buckets.size());
        Assertions.assertNull(h.removeValues(values(0)).removeValues(values(1)).intersectRange(2, 1, true));
    }

    @Test
    public void testRetainValues() {
        Histogram points = section().retainValues(values(0, 5, 5000));
        Assertions.assertTrue(points.buckets.isEmpty());
        Assertions.assertTrue(points.mcvBuckets.isEmpty());
        Assertions.assertEquals(2, points.mcv.size(), "5000 is in no bucket");
        Assertions.assertEquals(1.0, sum(points.mcv), 1e-6, "the retained values are all the rows");
        // 5 gets its share of the bucket: the 0.4 of the rows in buckets, 400/410 of it in its bucket, 1/998 of that
        double five = 0.4 * 400 / 410.0 / 998;
        Assertions.assertEquals(0.5 / (0.5 + five), points.mcv.get(new BigIntLiteral(0)), 1e-6);
        Assertions.assertEquals(five / (0.5 + five), points.mcv.get(new BigIntLiteral(5)), 1e-6);

        Histogram plain = evenHistogram(Type.INT, 0, 1024, 8, 1000).retainValues(values(5, 700));
        Assertions.assertNotNull(plain.mcv, "a plain histogram becomes a section of points");
        Assertions.assertEquals(0.5, plain.mcv.get(new BigIntLiteral(5)), 1e-9, "two uniform values share the rows");
        Assertions.assertEquals(0.5, plain.mcv.get(new BigIntLiteral(700)), 1e-9);

        Assertions.assertNull(section().retainValues(values(5000)), "no value found");
    }

    @Test
    public void testFromHotValues() throws Exception {
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        hotValues.put(new BigIntLiteral(0), 0.5f);
        hotValues.put(new BigIntLiteral(999), 5e-4f);
        ColumnStatistic colStats = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000).setNdv(1005)
                .setMinValue(0).setMaxValue(1004).setMinExpr(new IntLiteral(0, Type.INT)).setIsUnknown(false)
                .setHotValues(hotValues).build();
        Histogram h = Histogram.fromHotValues(colStats);
        Assertions.assertEquals(1, h.mcv.size(), "0.05% of 1005 values is not skew");
        Assertions.assertEquals(0.5, h.mcv.get(new BigIntLiteral(0)), 0);
        Assertions.assertEquals(1, h.mcvBuckets.size());
        Assertions.assertTrue(h.buckets.isEmpty());
        Bucket bucket = h.mcvBuckets.get(0);
        Assertions.assertEquals(0, bucket.lower, 0);
        Assertions.assertEquals(1004, bucket.upper, 0);
        Assertions.assertEquals(0.5, bucket.count, 1e-9, "the rows that are not hot");
        Assertions.assertEquals(1004, bucket.ndv, 1e-9);
        Assertions.assertTrue(h.dataType.isIntegerType());

        Map<Literal, Float> uniform = new LinkedHashMap<>();
        for (long k : new long[] {199, 4, 7, 14, 16, 22, 26, 28, 33, 39}) {
            uniform.put(new BigIntLiteral(k), 0.0033f);
        }
        Assertions.assertNull(Histogram.fromHotValues(new ColumnStatisticBuilder(colStats).setNdv(301)
                .setHotValues(uniform).build()), "the top values of a uniform column are not hot");
        Assertions.assertNull(Histogram.fromHotValues(new ColumnStatisticBuilder(colStats)
                .setHotValues(new LinkedHashMap<>()).build()));
        Assertions.assertNull(Histogram.fromHotValues(new ColumnStatisticBuilder(colStats).setHotValues(null).build()));
    }

    @Test
    public void testRewriteAppliesToStoredBucketsAndSection() {
        // k <= 500: both lists are cut, hot value 999 is dropped
        Histogram cut = section().intersectRange(Double.NEGATIVE_INFINITY, 500, true);
        Assertions.assertTrue(cut.hasMcv());
        Assertions.assertEquals(1, cut.mcv.size());
        Assertions.assertEquals(1, cut.mcvBuckets.size());
        Assertions.assertEquals(500, cut.mcvBuckets.get(0).upper, 0);
        Assertions.assertEquals(2, cut.buckets.size(), "the stored buckets [0, 0] and [1, 500] stay");
        Assertions.assertEquals(500, cut.buckets.get(1).upper, 0);
        // k <> 0: gone from the hot values and from the stored buckets, the section's buckets never held it
        Histogram removed = section().removeValues(values(0));
        Assertions.assertNull(removed.mcv.get(new BigIntLiteral(0)));
        Assertions.assertEquals(3, removed.buckets.size());
        Assertions.assertEquals(1, removed.buckets.get(0).lower, 0);
        Assertions.assertEquals(400, removed.mcvBuckets.get(0).count, 0, "no share taken from a section bucket");
        // a plain histogram has no section
        Assertions.assertFalse(evenHistogram(Type.INT, 0, 1000, 4, 500).hasMcv());
    }

    @Test
    public void testCollapsedBucketsAreDetected() {
        Assertions.assertFalse(section().hasCollapsedBuckets());
        // integers beyond 2^53: 78 values whose bounds are the same double
        List<Bucket> collapsed = new ArrayList<>();
        collapsed.add(new Bucket(5e18, 5e18, 1000, 0, 78));
        Histogram beyond = new HistogramBuilder().setDataType(Type.LARGEINT).setSampleRate(0)
                .setNumBuckets(1).setBuckets(collapsed).build();
        Assertions.assertTrue(beyond.hasCollapsedBuckets());
        // a single value bucket has the same bounds by nature
        List<Bucket> single = new ArrayList<>();
        single.add(new Bucket(7, 7, 1000, 0, 1));
        Histogram singleValue = new HistogramBuilder(beyond).setBuckets(single).build();
        Assertions.assertFalse(singleValue.hasCollapsedBuckets());
    }

    @Test
    public void testFindHotValueKeyComparesNumbersByValue() {
        Map<Literal, Float> hotValues = new LinkedHashMap<>();
        BigIntLiteral five = new BigIntLiteral(5);
        hotValues.put(five, 0.5f);
        hotValues.put(new VarcharLiteral("a"), 0.1f);
        Assertions.assertSame(five, StatisticsUtil.findHotValueKey(hotValues, new IntegerLiteral(5)));
        Assertions.assertEquals(five, StatisticsUtil.findHotValueKey(hotValues, new BigIntLiteral(5)));
        Assertions.assertNull(StatisticsUtil.findHotValueKey(hotValues, new IntegerLiteral(6)));
        Assertions.assertNotNull(StatisticsUtil.findHotValueKey(hotValues, new VarcharLiteral("a")));
        Assertions.assertNull(StatisticsUtil.findHotValueKey(hotValues, new VarcharLiteral("5")),
                "a string is not a number");
        Assertions.assertNotNull(StatisticsUtil.findHotValueKey(hotValues, new StringLiteral("a")),
                "a STRING literal matches a VARCHAR key holding the same text");
        Assertions.assertNull(StatisticsUtil.findHotValueKey(hotValues, new StringLiteral("b")));
        // a DATE key holding the same day as a DATEV2 literal
        Map<Literal, Float> dates = new LinkedHashMap<>();
        DateLiteral day = new DateLiteral("2024-01-01");
        dates.put(day, 0.5f);
        Assertions.assertSame(day, StatisticsUtil.findHotValueKey(dates, new DateV2Literal("2024-01-01")));
        Assertions.assertNull(StatisticsUtil.findHotValueKey(dates, new DateV2Literal("2024-01-02")));
        // exact beyond 2^53: two BIGINT values one apart share a double
        Map<Literal, Float> big = new LinkedHashMap<>();
        big.put(new BigIntLiteral(9007199254740993L), 0.5f);
        Assertions.assertNull(StatisticsUtil.findHotValueKey(big, new BigIntLiteral(9007199254740992L)));
        Assertions.assertNotNull(
                StatisticsUtil.findHotValueKey(big, new LargeIntLiteral(new BigInteger("9007199254740993"))));
        // a retained hot value keeps the key of the map, a bucket value the given literal
        Histogram points = section().retainValues(Arrays.asList(new IntegerLiteral(0), new IntegerLiteral(5)));
        Assertions.assertTrue(points.mcv.containsKey(new BigIntLiteral(0)));
        Assertions.assertTrue(points.mcv.containsKey(new IntegerLiteral(5)));
    }
}
