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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.model.Bucket;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Histogram;
import org.apache.doris.statistics.model.HistogramBuilder;
import org.apache.doris.statistics.model.Statistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Tests for the join key value distribution in JoinEstimation.
 */
public class ValueDistributionTest {

    private static double invokeJ(ColumnStatistic left, ColumnStatistic right) throws Exception {
        return merge(left, right, null);
    }

    // selectivity of the two columns; the column stats of the key after the join are set on out if given
    private static double merge(ColumnStatistic left, ColumnStatistic right, ColumnStatisticBuilder out) {
        ColumnStatisticBuilder joinedKey = out != null ? out : new ColumnStatisticBuilder(left);
        return Deencapsulation.invoke(JoinEstimation.class, "estimateJoinKeySelectivity", left, right, joinedKey);
    }

    private static ColumnStatistic col(double ndv, double min, double max) {
        return new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000)
                .setNdv(ndv)
                .setMinValue(min)
                .setMaxValue(max)
                .setIsUnknown(false)
                .setNumNulls(0)
                .build();
    }

    /**
     * with both sides with ndv only, J must equal 1/max(ndvL, ndvR) exactly.
     *
     * The min/max of the two columns differ in most cases: the ndv-only bucket must be unbounded,
     * otherwise the ndv would be rescaled by the range overlap and the identity would break.
     */
    @Test
    public void testDegenerateIdentity() throws Exception {
        // ndvL, minL, maxL, ndvR, minR, maxR
        double[][] cases = {
                {1000, 0, 999, 1000, 0, 999},        // identical ranges
                {1000, 0, 999, 500, 0, 999},
                {7, 0, 6, 999, 0, 998},              // disjoint sizes, nested ranges
                {301, 0, 299, 1701, 0, 1699},        // the asym_s/asym_b shape
                {1000, 5000, 6000, 1000, 0, 999},    // fully disjoint ranges
                {1, 5, 5, 1, 9, 9},                  // degenerate points, disjoint
                {1005, 0, 1004, 1005, 0, 1004},
        };
        for (double[] c : cases) {
            double j = invokeJ(col(c[0], c[1], c[2]), col(c[3], c[4], c[5]));
            double expected = 1.0 / Math.max(c[0], c[3]);
            Assertions.assertEquals(expected, j, expected * 1e-9,
                    "NDV_ONLY x NDV_ONLY must reproduce 1/max(ndv) for ndv=" + c[0] + "," + c[3]
                            + " ranges [" + c[1] + "," + c[2] + "] [" + c[4] + "," + c[5] + "]");
        }
    }

    /** J stays in (0, 1] with no NaN/Inf across degenerate inputs. */
    @Test
    public void testValueRangeAndEdgeCases() throws Exception {
        List<ColumnStatistic> odd = new ArrayList<>();
        odd.add(col(0, 0, 999));                                    // ndv = 0
        odd.add(col(1, 5, 5));                                      // degenerate point domain
        odd.add(col(1000, 999, 0));                                 // min > max
        odd.add(col(1000, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        odd.add(new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000).build());  // unknown
        for (ColumnStatistic a : odd) {
            for (ColumnStatistic b : odd) {
                double j = invokeJ(a, b);
                Assertions.assertFalse(Double.isNaN(j), "J must not be NaN");
                Assertions.assertFalse(Double.isInfinite(j), "J must not be infinite");
                Assertions.assertTrue(j > 0 && j <= 1.0, "J must be in (0,1], got " + j);
            }
        }
    }

    /** the non-null factor is (1-thetaL)(1-thetaR); nullFraction is the clamped NULL share. */
    @Test
    public void testNotNullFactor() throws Exception {
        Statistics side = new Statistics(1000, new HashMap<>());

        double thetaL = (double) Deencapsulation.invoke(JoinEstimation.class,
                "getNullRatio", colWithNulls(300), side);
        double thetaR = (double) Deencapsulation.invoke(JoinEstimation.class,
                "getNullRatio", colWithNulls(200), side);
        Assertions.assertEquals(0.7 * 0.8, (1 - thetaL) * (1 - thetaR), 1e-9,
                "thetaL=0.3, thetaR=0.2 must yield 0.7*0.8");
        Assertions.assertEquals(0.0, (double) Deencapsulation.invoke(JoinEstimation.class,
                "getNullRatio", colWithNulls(0), side), 1e-9,
                "no nulls must leave J untouched");
        // An all-null key genuinely joins zero rows, so theta = 1 is the correct value here --
        // unlike J, which must stay strictly positive.
        Assertions.assertEquals(1.0, (double) Deencapsulation.invoke(JoinEstimation.class,
                "getNullRatio", colWithNulls(1000), side), 0,
                "an all-null join key must have theta 1");
        // numNulls exceeding rowCount is corrupt input; the clamp must keep theta at 1, never above.
        Assertions.assertEquals(1.0, (double) Deencapsulation.invoke(JoinEstimation.class,
                "getNullRatio", colWithNulls(5000), side), 0,
                "numNulls > rowCount must clamp to 1");
    }

    /**
     * the untrustable branch's old formula must equal crossRows * J.
     *
     * max(N_L, N_R) * (N_small / d_small) * min(d_L, d_R) / d_big equals N_L * N_R / max(d_L, d_R),
     * which is crossRows * J with ndv only.
     */
    @Test
    public void testUntrustableBranchIdentity() throws Exception {
        double[][] cases = {
                {200000, 301, 900000, 1701},     // asymmetric rows and ndv
                {1000000, 1005, 1000000, 1005},  // equal rows and ndv
                {1000000, 701, 1000000, 802},    // equal rows, asymmetric ndv
                {500, 7, 1000000, 999},          // tiny vs huge
        };
        for (double[] c : cases) {
            double rowsL = c[0];
            double ndvL = c[1];
            double rowsR = c[2];
            double ndvR = c[3];
            double rho = rowsL > rowsR
                    ? (rowsR / ndvR) * Math.min(ndvL, ndvR) / ndvL
                    : (rowsL / ndvL) * Math.min(ndvL, ndvR) / ndvR;
            double oldFormula = Math.max(rowsL, rowsR) * rho;
            double newFormula = rowsL * rowsR * invokeJ(col(ndvL, 0, ndvL - 1), col(ndvR, 0, ndvR - 1));
            Assertions.assertEquals(oldFormula, newFormula, oldFormula * 1e-9,
                    "crossRows*J must reproduce max(N)*rho for rows=" + rowsL + "," + rowsR
                            + " ndv=" + ndvL + "," + ndvR);
        }
    }

    /**
     * golden case for the hot value distribution.
     *
     *Two 1e6-row tables, ndv 1005, both dominated by value 0 at 50% plus nine values at
     * 0.05%. Only the 50% value clears the skew threshold; the 0.05% ones sit at half the
     * uniform average and are dropped, so the mcv is {0: 0.5} and the rest stays in the cold
     * block. The ndv-only formula gives 1/1005 = 9.95e-4 and underestimates the join 250x,
     * while the collision probability is dominated by the 0.5 * 0.5 term.
     */
    @Test
    public void testMcvOnlyGoldenCase() throws Exception {
        Map<Literal, Float> hotL = hot(0, 999, 988, 986, 982, 976, 974, 968, 964, 962);
        Map<Literal, Float> hotR = hot(0, 993, 494, 986, 982, 976, 974, 968, 964, 962);
        double j = withMcvEnabled(() ->
                invokeJ(colWithHot(1005, hotL), colWithHot(1005, hotR)));

        // J1: value 0, hot on both sides. J2/J3 vanish because neither side has a surviving
        // hot value the other lacks.
        double j1 = 0.5 * 0.5;
        double j4 = 0.5 * 0.5 / (1005 - 1);
        Assertions.assertEquals(j1 + j4, j, (j1 + j4) * 1e-9,
                "MCV_ONLY must match the 2x2 expansion");
        Assertions.assertEquals(0.250249, j, 1e-6, "golden J for the 50% skew case");
        Assertions.assertTrue(j > 250 * (1.0 / 1005),
                "MCV must lift J far above the ndv-only value; got " + j);
    }

    /**
     * The top-N values of a uniform column are a sampling artifact, not skew, and must not
     * become hot values.
     *
     *ANALYZE keeps any value above a 1e-4 ratio, so a uniform column with ndv 301 reports ten
     * "hot" values at 0.0033 - which is exactly its uniform average 1/301. Promoting those to
     * hot values removes their mass from the bucket while the bucket goes on assuming its
     * remainder is spread evenly across the range, which it then is not.
     */
    @Test
    public void testUniformColumnTopNIsNotSkew() throws Exception {
        Map<Literal, Float> artifact = new LinkedHashMap<>();
        long[] keys = {199, 4, 7, 14, 16, 22, 26, 28, 33, 39};
        for (long k : keys) {
            artifact.put(new BigIntLiteral(k), 0.0033f);
        }
        double j = withMcvEnabled(() ->
                invokeJ(colWithHot(301, artifact), colWithHot(301, artifact)));
        Assertions.assertEquals(1.0 / 301, j, 1e-15,
                "uniform top-N must be ignored, leaving the ndv-only value");
    }

    /** with the switch off, hotValues must be ignored entirely. */
    @Test
    public void testSwitchOffIgnoresHotValues() throws Exception {
        Map<Literal, Float> hotL = hot(0, 999);
        Map<Literal, Float> hotR = hot(0, 993);
        double off = invokeJ(colWithHot(1005, hotL), colWithHot(1005, hotR));
        Assertions.assertEquals(1.0 / 1005, off, 1e-15,
                "switch off must reproduce the ndv-only value even when hotValues exist");

        double on = withMcvEnabled(() -> invokeJ(colWithHot(1005, hotL), colWithHot(1005, hotR)));
        Assertions.assertTrue(on > off, "switch on must change the result for skewed keys");
    }

    /** only one side needs hotValues; the distribution is chosen per side. */
    @Test
    public void testCrossTierAndDegenerateMcv() throws Exception {
        Map<Literal, Float> hotL = hot(0, 999);
        // One-sided MCV against a uniform side collapses back to 1/ndv, exactly:
        //   J = sum_v p_L(v) * (1/d_R) = (1/d_R) * sum_v p_L(v) = 1/d_R.
        // Skew on one side alone cannot change the collision probability, because a uniform
        // partner assigns the same mass to the hot value as to any other. MCV pays off only when
        // both sides concentrate on the same values, or when the partner is itself non-uniform
        // (the histogram distributions). This is a property of the math, not a gap in the distribution.
        double oneSided = withMcvEnabled(() -> invokeJ(colWithHot(1005, hotL), col(1005, 0, 1004)));
        Assertions.assertEquals(1.0 / 1005, oneSided, 1e-15,
                "one-sided MCV against a uniform side must collapse to 1/ndv exactly");

        // An empty map means "collected, none found" and carries no hot value.
        double empty = withMcvEnabled(() ->
                invokeJ(colWithHot(1005, new LinkedHashMap<>()), col(1005, 0, 1004)));
        Assertions.assertEquals(1.0 / 1005, empty, 1e-15, "no hot values must reproduce ndv-only");
    }

    /**
     * the joined column's distribution must be H_L(v) * H_R(v) / J, not min(H_L, H_R).
     *
     *Two tables half of whose rows carry key 0 produce an output that is 99.9% key 0, because
     * the matching rows multiply while the rest barely match at all. The baseline merge reports
     * 50%, which then understates any downstream filter or second join on that column.
     */
    @Test
    public void testOutputDistribution() throws Exception {
        Map<Literal, Float> hotL = hot(0, 999);
        Map<Literal, Float> hotR = hot(0, 993);

        ConnectContext prev = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setEnableMcvJoinEstimation(true);
        ctx.setThreadLocalInfo();
        try {
            ColumnStatisticBuilder out = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000);
            double j = merge(colWithHot(1005, hotL), colWithHot(1005, hotR), out);
            Map<Literal, Float> written = out.build().getHotValues();

            Assertions.assertNotNull(written, "output hot values must be written back");
            Float share = written.get(new BigIntLiteral(0));
            Assertions.assertNotNull(share, "value 0 must survive into the output distribution");
            Assertions.assertEquals(0.5 * 0.5 / j, share, 1e-4,
                    "output share must be H_L * H_R / J");
            Assertions.assertEquals(0.999, share, 1e-3,
                    "a 50%/50% shared key must dominate the output");

            double total = 0;
            for (float f : written.values()) {
                total += f;
            }
            Assertions.assertTrue(total <= 1.0 + 1e-6,
                    "output hot values must not exceed 1; got " + total);
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    /**
     * a ndv-only side against a multi-bucket side must still give exactly 1/max(ndv).
     *
     *With mu_L uniform over an unknown support, J = mu_R(S_L)/d_L and S_L is on average a
     * d_L/d_R share of S_R, so the other side's shape drops out. Taking the width ratio for the
     * unbounded block instead clamped it to 1 against every bucket, inflating J by the bucket
     * count - 128x on a default histogram.
     */
    @Test
    public void testShapelessAgainstBucketsStaysNdvOnly() throws Exception {
        for (int numBuckets : new int[] {1, 8, 128}) {
            for (double[] ndvs : new double[][] {{1024, 1024}, {5000, 1024}, {17, 1024}}) {
                double ndvL = ndvs[0];
                double ndvR = ndvs[1];
                Histogram histR = evenHistogram(0, ndvR, numBuckets, 1000);
                double j = withSwitches(false, true, () -> invokeJ(
                        col(ndvL, 0, ndvL - 1), colWithHist(ndvR, 0, ndvR - 1, histR)));
                double expected = 1.0 / Math.max(ndvL, ndvR);
                Assertions.assertEquals(expected, j, expected * 1e-9,
                        "ndv-only x " + numBuckets + " buckets must stay 1/max(ndv) for ndv="
                                + ndvL + "," + ndvR);
            }
        }
    }

    /** two histograms over the same values reproduce sum_j m_j^2 / k_j; disjoint ones vanish. */
    @Test
    public void testBucketsOnlyGoldenCases() throws Exception {
        Histogram same = evenHistogram(0, 1024, 8, 1000);
        double j = withSwitches(false, true, () -> invokeJ(
                colWithHist(1024, 0, 1023, same), colWithHist(1024, 0, 1023, same)));
        // Eight buckets each holding 1/8 of the mass over 128 values: J = 8 * (1/8)^2 / 128 = 1/1024.
        Assertions.assertEquals(1.0 / 1024, j, 1e-12, "matching uniform histograms must give 1/ndv");

        // Second histogram occupies the upper half of the same range only.
        Histogram upperHalf = evenHistogram(512, 512, 4, 1000);
        double half = withSwitches(false, true, () -> invokeJ(
                colWithHist(1024, 0, 1023, same), colWithHist(512, 512, 1023, upperHalf)));
        // Only L's four upper buckets overlap: each carries 1/8 of L's mass and 1/4 of R's, over
        // 128 shared values: J = 4 * (1/8 * 1/4) / 128 = 1/1024.
        Assertions.assertEquals(1.0 / 1024, half, 1e-12,
                "half-overlapping histograms must count only the shared buckets");
        // ndv-only would say 1/max(1024, 512) = 1/1024 as well here; the shape matters when the
        // ranges are disjoint:
        Histogram farAway = evenHistogram(100000, 1024, 8, 1000);
        double disjoint = withSwitches(false, true, () -> invokeJ(
                colWithHist(1024, 0, 1023, same), colWithHist(1024, 100000, 101023, farAway)));
        Assertions.assertTrue(disjoint < 1e-9,
                "disjoint histograms must produce (near) zero collision, got " + disjoint);
    }

    /** with the histogram switch off, a present histogram must be ignored entirely. */
    @Test
    public void testHistogramSwitchOff() throws Exception {
        Histogram same = evenHistogram(0, 1024, 8, 1000);
        Histogram farAway = evenHistogram(100000, 1024, 8, 1000);
        double off = withSwitches(false, false, () -> invokeJ(
                colWithHist(1024, 0, 1023, same), colWithHist(1024, 100000, 101023, farAway)));
        Assertions.assertEquals(1.0 / 1024, off, 1e-15,
                "switch off must reproduce ndv-only even for disjoint histograms");
    }

    /** mcv_histogram with one dominant value and an even cold histogram. */
    private static Histogram mcvHistogram(double ndv, Map<Literal, Float> mcv, int numBuckets) {
        Histogram cold = evenHistogram(1, ndv - mcv.size(), numBuckets, 500);
        return new HistogramBuilder(cold).setMcv(mcv).setMcvBuckets(cold.buckets).build();
    }

    /**
     * MCV_AND_BUCKETS golden case. mcv {0: 0.5} on both sides, the other half spread evenly
     * over 1..1004 in eight buckets: J = 0.25 + 8 * (0.5/8)^2 / (1004/8) = 0.25 + 0.25/1004.
     */
    @Test
    public void testMcvAndBucketsGoldenCase() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        Histogram h = mcvHistogram(1005, mcv, 8);
        double j = withSwitches(true, true, () -> invokeJ(
                colWithHist(1005, 0, 1004, h), colWithHist(1005, 0, 1004, h)));
        double expected = 0.25 + 0.25 / 1004;
        Assertions.assertEquals(expected, j, expected * 1e-9, "MCV_AND_BUCKETS must sum J1 and J4");
    }

    /**
     * consistency across distributions. MCV_AND_BUCKETS against a ndv-only side collapses to
     * 1/max(ndv) exactly, in both ndv orderings. The ndv-only side's point density has to carry
     * the containment factor min(1, d_this/d_other) for the smaller-ndv case to come out right.
     */
    @Test
    public void testMcvAndBucketsAgainstShapeless() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        Histogram h = mcvHistogram(1005, mcv, 8);
        for (double otherNdv : new double[] {1005, 5000, 300}) {
            double j = withSwitches(true, true, () -> invokeJ(
                    colWithHist(1005, 0, 1004, h), col(otherNdv, 0, otherNdv - 1)));
            double expected = 1.0 / Math.max(1005, otherNdv);
            Assertions.assertEquals(expected, j, expected * 1e-9,
                    "MCV_AND_BUCKETS x NDV_ONLY must be 1/max(ndv) for otherNdv=" + otherNdv);
        }
    }

    /**
     * The histogram switch reads the stored histogram, section included; the hot value switch reads
     * the hot values of the column, not the section. Both off: ndv only.
     */
    @Test
    public void testSectionReadByHistogramSwitchOnly() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        Histogram h = mcvHistogram(1005, mcv, 8);
        ColumnStatistic c = colWithHist(1005, 0, 1004, h);
        double both = withSwitches(true, true, () -> invokeJ(c, c));
        double histOnly = withSwitches(false, true, () -> invokeJ(c, c));
        double mcvOnly = withSwitches(true, false, () -> invokeJ(c, c));
        double none = withSwitches(false, false, () -> invokeJ(c, c));
        Assertions.assertEquals(1.0 / 1005, none, 1e-15, "no switches: ndv-only");
        Assertions.assertEquals(both, histOnly, 1e-15, "the histogram switch alone uses the section");
        Assertions.assertEquals(none, mcvOnly, 1e-15, "the hot value switch reads the column hot values");
        Assertions.assertTrue(both > none, "the section adds the hot value term");
    }

    /** the join output carries a histogram whose mcv_histogram section holds the derived distribution. */
    @Test
    public void testOutputHistogramWrittenBack() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        Histogram h = mcvHistogram(1005, mcv, 8);
        ColumnStatisticBuilder out = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000);
        withSwitches(true, true, () -> merge(colWithHist(1005, 0, 1004, h), colWithHist(1005, 0, 1004, h), out));
        Histogram written = out.build().histogram;
        Assertions.assertNotNull(written, "output must carry a histogram");
        Assertions.assertNotNull(written.mcv);
        Assertions.assertEquals(8, written.mcvBuckets.size(), "buckets: one per overlapping bucket pair");
        Assertions.assertEquals(1, written.mcv.size());
        // Output share of value 0 is 0.25 / J ~ 0.999.
        Assertions.assertEquals(0.25 / (0.25 + 0.25 / 1004), written.mcv.get(new BigIntLiteral(0)), 1e-4);
        // A derived histogram has only the section. The bucket counts stay ratios of the cross product:
        // the cold halves of both sides, 0.5 * 0.5, spread over 1004 values.
        Assertions.assertTrue(written.buckets.isEmpty());
        double bucketTotal = 0;
        for (Bucket b : written.mcvBuckets) {
            bucketTotal += b.count;
        }
        Assertions.assertEquals(0.25 / 1004, bucketTotal, 1e-9, "bucket counts are ratios of the cross product");
    }

    /** a hot value inside a bucket stays in the section, the bucket is not split. */
    @Test
    public void testOutputHistogramKeepsHotValueInSection() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(500), 0.5f);
        List<Bucket> one = new ArrayList<>();
        one.add(new Bucket(0, 1003, 500000, 0, 1004));
        Histogram h = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(1)
                .setBuckets(one).setMcv(mcv).setMcvBuckets(one).build();
        ColumnStatisticBuilder out = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000);
        withSwitches(true, true, () -> merge(colWithHist(1005, 0, 1003, h), colWithHist(1005, 0, 1003, h), out));
        Histogram written = out.build().histogram;
        Assertions.assertEquals(1, written.mcv.size());
        Assertions.assertEquals(1, written.mcvBuckets.size());
        Assertions.assertEquals(0, written.mcvBuckets.get(0).lower, 0);
        Assertions.assertEquals(1003, written.mcvBuckets.get(0).upper, 0);
    }

    /** the join output ndv is the shared distinct count, not min(d_L, d_R) when ranges only partly meet. */
    @Test
    public void testOutputNdvFollowsOverlap() throws Exception {
        Histogram loHist = evenHistogram(0, 1024, 8, 1000);
        Histogram hiHist = evenHistogram(512, 1024, 8, 1000);
        ColumnStatisticBuilder out = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000);
        withSwitches(false, true, () -> merge(colWithHist(1024, 0, 1023, loHist),
                colWithHist(1024, 512, 1535, hiHist), out));
        Assertions.assertEquals(512, out.build().ndv, 1e-6, "only the 512 shared values can appear in the output");

        // Full containment keeps the baseline min(d_L, d_R).
        ColumnStatisticBuilder same = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000);
        withSwitches(false, true, () -> merge(colWithHist(1024, 0, 1023, loHist),
                colWithHist(1024, 0, 1023, loHist), same));
        Assertions.assertEquals(1024, same.build().ndv, 1e-6);
    }

    /**
     * A hot value outside the other side's [min, max] must not collide, even though the other
     * side's NDV_ONLY block is unbounded. Seen on a four-way chain: the dominant key 0 carried
     * through three joins kept matching a table whose values start at 500.
     */
    @Test
    public void testPointMassOutsideOtherRangeDoesNotCollide() throws Exception {
        ColumnStatistic hotAtZero = colWithHot(1005, hot(0, 999));
        ColumnStatistic shifted = col(500, 500, 999);
        double j = withMcvEnabled(() -> invokeJ(hotAtZero, shifted));
        // Only the cold part can meet: 0.5 * 1 / max(1004, 500), and even that is generous.
        Assertions.assertTrue(j <= 0.5 / 1004 + 1e-12, "hot value 0 must contribute nothing, got " + j);
        // Inside the range it does collide: J2 = 0.5 / max(1004, 1005).
        ColumnStatistic covering = col(1005, 0, 1004);
        double in = withMcvEnabled(() -> invokeJ(hotAtZero, covering));
        Assertions.assertEquals(1.0 / 1005, in, 1e-12, "in-range hot value keeps the one-sided identity");
    }

    /**
     * A filter cuts the histogram to the range it keeps, so the join sees only the values that
     * survived. Left column filtered to its upper half: J = 4 * (1/4 * 1/8) / 128 = 1/1024.
     */
    @Test
    public void testHistogramCutToColumnRange() throws Exception {
        Histogram full = evenHistogram(0, 1024, 8, 1000);
        Histogram upper = full.intersectRange(512, 1023, true);
        double j = withSwitches(false, true, () -> invokeJ(
                colWithHist(512, 512, 1023, upper), colWithHist(1024, 0, 1023, full)));
        Assertions.assertEquals(1.0 / 1024, j, 1e-12, "cut histogram must not see values the filter removed");
        double asym = withSwitches(false, true, () -> invokeJ(
                colWithHist(512, 512, 1023, upper), col(2048, 0, 2047)));
        Assertions.assertEquals(1.0 / 2048, asym, 1e-12,
                "cut histogram against a ndv-only side must still give 1/max(ndv)");
    }

    /**
     * An IN list leaves the matched points. Both sides filtered to the same four values out of a
     * 1000-value range: J = 4 * (1/4)^2 = 1/4, exactly. Smearing four points over the clipped
     * buckets instead gave J ~ 1/76, a 19x underestimate on a real query.
     */
    @Test
    public void testPointDistributionAfterInFilter() throws Exception {
        Histogram full = evenHistogram(0, 1024, 128, 1000);
        List<Literal> four = new ArrayList<>();
        for (long v : new long[] {600, 700, 800, 900}) {
            four.add(new BigIntLiteral(v));
        }
        Histogram points = full.retainValues(four);
        double j = withSwitches(false, true, () -> invokeJ(
                colWithHist(4, 600, 900, points), colWithHist(4, 600, 900, points)));
        Assertions.assertEquals(1.0 / 4, j, 1e-12, "four equal points on both sides");

        // A section: mcv {0: 0.5, 999: 5e-4}, cold 1..1003 evenly; IN (0, 1, 2) keeps 0 and two
        // cold values, so value 0 dominates: J ~ (0.5 / 0.501)^2 ~ 0.996.
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        mcv.put(new BigIntLiteral(999), 5e-4f);
        Histogram cold = evenHistogram(1, 1003, 59, 500);
        Histogram h = new HistogramBuilder(cold).setMcv(mcv).setMcvBuckets(cold.buckets).build();
        List<Literal> three = new ArrayList<>();
        for (long v : new long[] {0, 1, 2}) {
            three.add(new BigIntLiteral(v));
        }
        Histogram in = h.retainValues(three);
        ColumnStatistic filtered = colWithHist(3, 0, 2, in);
        double jIn = withSwitches(true, true, () -> invokeJ(filtered, filtered));
        double p0 = 0.5 / (0.5 + 0.4995 * 2 / 1003);
        double expected = p0 * p0 + Math.pow(1 - p0, 2) / 2;
        Assertions.assertEquals(expected, jIn, expected * 1e-6, "the hot value keeps its rows, the cold points theirs");
        Assertions.assertTrue(jIn > 0.99, "value 0 must dominate the filtered join, got " + jIn);
    }

    /**
     * Hot values alone: the filter keeps the matched hot values as ratios of the rows it kept, the
     * join spreads the rest over one bucket. IN (0,1,2) keeps two cold values of 1004: value 0 is
     * 0.5 / (0.5 + 0.5 * 2 / 1004) of the rows; BETWEEN 0 AND 499 keeps half the cold range.
     */
    @Test
    public void testHotValuesRescaledAfterFilter() throws Exception {
        ColumnStatistic base = colWithHot(1005, hot(0, 999));
        Map<Literal, Float> inHot = new LinkedHashMap<>();
        double p0 = 0.5 / (0.5 + 0.5 * 2 / 1004);
        inHot.put(new BigIntLiteral(0), (float) p0);
        ColumnStatistic in = new ColumnStatisticBuilder(base).setNdv(3).setMinValue(0).setMaxValue(2)
                .setHotValues(inHot).build();
        double j = withMcvEnabled(() -> invokeJ(in, in));
        double expected = p0 * p0 + Math.pow(1 - p0, 2) / 2;
        Assertions.assertEquals(expected, j, expected * 1e-6, "IN filter: hot share ~0.998, two cold values");

        Map<Literal, Float> rangeHot = new LinkedHashMap<>();
        double p0r = 0.5 / (0.5 + 0.5 * 499.0 / 1004);
        rangeHot.put(new BigIntLiteral(0), (float) p0r);
        ColumnStatistic range = new ColumnStatisticBuilder(base).setNdv(500).setMinValue(0).setMaxValue(499)
                .setHotValues(rangeHot).build();
        double jr = withMcvEnabled(() -> invokeJ(range, range));
        double expectedR = p0r * p0r + Math.pow(1 - p0r, 2) / 499;
        Assertions.assertEquals(expectedR, jr, expectedR * 1e-6, "range filter: hot share ~2/3, 499 cold values");
        Assertions.assertTrue(p0r > 0.66 && p0r < 0.67);

        double none = withMcvEnabled(() -> invokeJ(base, base));
        Assertions.assertEquals(0.25 + 0.25 / 1004, none, 1e-12, "unfiltered column must be unchanged");
    }

    /**
     * k > 0 or k <> 0 on a key whose value 0 is hot: the hot value and, in a plain histogram, its
     * single value bucket are gone, so the join is the cold part alone.
     */
    @Test
    public void testRemovedHotValueLeavesTheColdPart() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        List<Bucket> cold = new ArrayList<>();
        cold.add(new Bucket(1, 1004, 500000, 0, 1004));
        Histogram section = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(1)
                .setBuckets(cold).setMcv(mcv).setMcvBuckets(cold).build();
        List<Bucket> plainBuckets = new ArrayList<>();
        plainBuckets.add(new Bucket(0, 0, 500000, 0, 1));
        plainBuckets.add(new Bucket(1, 1004, 500000, 500000, 1004));
        Histogram plain = new HistogramBuilder().setDataType(Type.INT).setSampleRate(0).setNumBuckets(2)
                .setBuckets(plainBuckets).build();
        List<Literal> zero = new ArrayList<>();
        zero.add(new BigIntLiteral(0));

        ColumnStatistic gt0 = colWithHist(1004, 1, 1004, section.intersectRange(0, Double.POSITIVE_INFINITY, false));
        Assertions.assertEquals(1.0 / 1004, withSwitches(true, true, () -> invokeJ(gt0, gt0)), 1e-12,
                "section: the hot value is gone");
        ColumnStatistic ne0 = colWithHist(1004, 0, 1004, plain.removeValues(zero));
        Assertions.assertEquals(1.0 / 1004, withSwitches(false, true, () -> invokeJ(ne0, ne0)), 1e-12,
                "plain: the single value bucket is gone");
        ColumnStatistic scan = colWithHist(1005, 0, 1004, section);
        Assertions.assertTrue(withSwitches(true, true, () -> invokeJ(scan, scan)) > 0.25,
                "the unfiltered column keeps the hot value");
    }

    /**
     * After k = c the column is a section of one point with no buckets. Against a plain histogram
     * J is the share of c on the other side; against the same point it is 1; against a column that
     * does not hold c it is (nearly) 0.
     */
    @Test
    public void testPointSectionAgainstBuckets() throws Exception {
        Histogram full = evenHistogram(0, 1024, 8, 1000);
        List<Literal> five = new ArrayList<>();
        five.add(new BigIntLiteral(5));
        ColumnStatistic point = colWithHist(1, 5, 5, full.retainValues(five));
        Assertions.assertTrue(point.histogram.mcvBuckets.isEmpty());
        double j = withSwitches(false, true, () -> invokeJ(point, colWithHist(1024, 0, 1023, full)));
        Assertions.assertEquals(1.0 / 1024, j, 1e-12, "the other side holds 5 in 1 of 1024 shares");
        Assertions.assertEquals(1.0, withSwitches(false, true, () -> invokeJ(point, point)), 1e-12);
        Histogram farAway = evenHistogram(100000, 1024, 8, 1000);
        double none = withSwitches(false, true, () -> invokeJ(point, colWithHist(1024, 100000, 101023, farAway)));
        Assertions.assertTrue(none < 1e-9, "5 is in no bucket of the other side, got " + none);
    }

    /** the hot value switch alone: k = 5 leaves {5: 1} with ndv 1, which is a point section too. */
    @Test
    public void testHotValueFromEqualityFilter() throws Exception {
        Map<Literal, Float> one = new LinkedHashMap<>();
        one.put(new BigIntLiteral(5), 1.0f);
        ColumnStatistic point = new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 500).setNdv(1).setMinValue(5)
                .setMaxValue(5).setIsUnknown(false).setNumNulls(0).setHotValues(one).build();
        double j = withMcvEnabled(() -> invokeJ(point, colWithHot(1005, hot(0, 999))));
        // 5 is a cold value of the other side, whose only skewed hot value is 0: 0.5 / 1004 of its rows
        Assertions.assertEquals(0.5 / 1004, j, 1e-9);
        Assertions.assertEquals(1.0, withMcvEnabled(() -> invokeJ(point, point)), 1e-12);
    }

    /**
     * A join whose key was filtered to points writes a section with hot values and no buckets, so
     * the next join up the chain still sees the point: with the histogram switch alone, the second
     * join of (a filtered to 0) x b x c must match half of c's rows, not 1 / ndv of them.
     */
    @Test
    public void testPointMassWrittenBackForNextJoin() throws Exception {
        Map<Literal, Float> mcv = new LinkedHashMap<>();
        mcv.put(new BigIntLiteral(0), 0.5f);
        Histogram section = mcvHistogram(1005, mcv, 8);
        ColumnStatistic full = colWithHist(1005, 0, 1004, section);
        List<Literal> zero = new ArrayList<>();
        zero.add(new BigIntLiteral(0));
        ColumnStatistic point = colWithHist(1, 0, 0, section.retainValues(zero));
        ColumnStatisticBuilder out = new ColumnStatisticBuilder(point);
        double first = withSwitches(false, true, () -> merge(point, full, out));
        Assertions.assertEquals(0.5, first, 1e-9);
        ColumnStatistic joined = out.build();
        Assertions.assertNotNull(joined.histogram, "the point must be written back");
        Assertions.assertTrue(joined.histogram.mcvBuckets.isEmpty());
        Assertions.assertEquals(1.0, joined.histogram.mcv.get(new BigIntLiteral(0)), 1e-6);
        double second = withSwitches(false, true, () -> invokeJ(joined, full));
        Assertions.assertEquals(0.5, second, 1e-9, "the second join sees the point, not 1 / ndv");
    }

    private static Map<Literal, Float> hot(long dominant, long... rest) {
        Map<Literal, Float> m = new LinkedHashMap<>();
        m.put(new BigIntLiteral(dominant), 0.5f);
        for (long v : rest) {
            m.put(new BigIntLiteral(v), 5e-4f);
        }
        return m;
    }

    private static ColumnStatistic colWithHot(double ndv, Map<Literal, Float> hotValues) {
        return new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000)
                .setNdv(ndv)
                .setMinValue(0)
                .setMaxValue(ndv - 1)
                .setIsUnknown(false)
                .setNumNulls(0)
                .setHotValues(hotValues)
                .build();
    }

    /**
     * Run {@code body} with enable_mcv_join_estimation on.
     *
     *JoinEstimation reads the flag off ConnectContext.get(), so the test has to install one.
     */
    private static double withMcvEnabled(Callable<Double> body) throws Exception {
        return withSwitches(true, false, body);
    }

    private static double withSwitches(boolean mcv, boolean histogram, Callable<Double> body)
            throws Exception {
        ConnectContext prev = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setEnableMcvJoinEstimation(mcv);
        ctx.getSessionVariable().setEnableHistogramJoinEstimation(histogram);
        ctx.setThreadLocalInfo();
        try {
            return body.call();
        } finally {
            ConnectContext.remove();
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    /** {@code ndv} distinct integer values split evenly across {@code numBuckets} contiguous buckets. */
    private static Histogram evenHistogram(double lo, double ndv, int numBuckets, double rowsPerValue) {
        List<Bucket> buckets = new ArrayList<>();
        double perBucket = ndv / numBuckets;
        double preSum = 0;
        for (int b = 0; b < numBuckets; b++) {
            double l = lo + b * perBucket;
            double h = l + perBucket - 1;
            double count = perBucket * rowsPerValue;
            buckets.add(new Bucket(l, h, count, preSum, perBucket));
            preSum += count;
        }
        return new HistogramBuilder().setDataType(Type.INT).setSampleRate(1.0)
                .setNumBuckets(numBuckets).setBuckets(buckets).build();
    }

    private static ColumnStatistic colWithHist(double ndv, double min, double max, Histogram hist) {
        return new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000000)
                .setNdv(ndv)
                .setMinValue(min)
                .setMaxValue(max)
                .setIsUnknown(false)
                .setNumNulls(0)
                .setHistogram(hist)
                .build();
    }

    private static ColumnStatistic colWithNulls(double numNulls) {
        return new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN, 1000)
                .setNdv(1000)
                .setMinValue(0)
                .setMaxValue(999)
                .setIsUnknown(false)
                .setNumNulls(numNulls)
                .build();
    }

}
