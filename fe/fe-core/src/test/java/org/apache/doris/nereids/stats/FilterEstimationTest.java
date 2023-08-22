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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class FilterEstimationTest {

    // a > 500 or b < 100
    // b isNaN
    @Test
    public void testOrNaN() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThan greaterThan1 = new GreaterThan(a, int500);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        IntegerLiteral int100 = new IntegerLiteral(100);
        LessThan lessThan = new LessThan(b, int100);
        Or or = new Or(greaterThan1, lessThan);
        Map<Expression, ColumnStatistic> columnStat = new HashMap<>();
        ColumnStatistic aStats = new ColumnStatisticBuilder().setCount(500).setNdv(500).setAvgSizeByte(4)
                .setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).build();
        ColumnStatistic bStats = new ColumnStatisticBuilder().setCount(500).setNdv(500).setAvgSizeByte(4)
                .setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).setIsUnknown(true).build();
        columnStat.put(a, aStats);
        columnStat.put(b, bStats);

        Statistics stat = new Statistics(1000, columnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(or, stat);
        Assertions.assertTrue(
                Precision.equals(expected.getRowCount(), 750,
                         0.01));
    }

    // a > 500 and b < 100
    // b isNaN
    @Test
    public void testAndNaN() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThan greaterThan1 = new GreaterThan(a, int500);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        IntegerLiteral int100 = new IntegerLiteral(100);
        LessThan lessThan = new LessThan(b, int100);
        And and = new And(greaterThan1, lessThan);
        Map<Expression, ColumnStatistic> columnStat = new HashMap<>();
        ColumnStatistic aStats = new ColumnStatisticBuilder().setCount(500).setNdv(500)
                .setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).build();
        ColumnStatistic bStats = new ColumnStatisticBuilder().setCount(500).setNdv(500)
                .setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).setIsUnknown(true).build();
        columnStat.put(a, aStats);
        columnStat.put(b, bStats);

        Statistics stat = new Statistics(1000, columnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(and, stat);
        Assertions.assertTrue(
                Precision.equals(expected.getRowCount(), 250,
                        0.01));
    }

    @Test
    public void testInNaN() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        InPredicate in = new InPredicate(a, Lists.newArrayList(int500));
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setIsUnknown(true);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(in, stat);
        Assertions.assertTrue(Precision.equals(333.33, expected.getRowCount(), 0.01));
    }

    @Test
    public void testNotInNaN() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        InPredicate in = new InPredicate(a, Lists.newArrayList(int500));
        Not notIn = new Not(in);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setIsUnknown(true);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(notIn, stat);
        Assertions.assertTrue(Precision.equals(666.666, expected.getRowCount(), 0.01));
    }

    /**
     * pre-condition: a in (0,300)
     * predicate: a > 100 and a < 200
     *
     */
    @Test
    public void testRelatedAnd() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int100 = new IntegerLiteral(100);
        IntegerLiteral int200 = new IntegerLiteral(200);
        GreaterThan ge = new GreaterThan(a, int100);
        LessThan le = new LessThan(a, int200);
        And and = new And(ge, le);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatistic aStats = new ColumnStatisticBuilder().setCount(300).setNdv(30)
                .setAvgSizeByte(4).setNumNulls(0).setDataSize(0)
                .setMinValue(0).setMaxValue(300).build();
        slotToColumnStat.put(a, aStats);
        Statistics stats = new Statistics(300, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics result = filterEstimation.estimate(and, stats);
        Assertions.assertEquals(100, result.getRowCount());
        ColumnStatistic aStatsEst = result.findColumnStatistics(a);
        Assertions.assertEquals(100, aStatsEst.minValue);
        Assertions.assertEquals(200, aStatsEst.maxValue);
        Assertions.assertEquals(10, aStatsEst.ndv);
    }

    // a > 500 and b < 100 or a = c
    @Test
    public void test1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThan greaterThan1 = new GreaterThan(a, int500);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        IntegerLiteral int100 = new IntegerLiteral(100);
        LessThan lessThan = new LessThan(b, int100);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, c);
        And and = new And(greaterThan1, lessThan);
        Or or = new Or(and, equalTo);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatistic aStats = new ColumnStatisticBuilder().setCount(500).setNdv(500)
                .setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).build();
        ColumnStatistic bStats = new ColumnStatisticBuilder().setCount(500).setNdv(500)
                .setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).build();
        ColumnStatistic cStats = new ColumnStatisticBuilder().setCount(500).setNdv(500)
                .setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                .setMinValue(0).setMaxValue(1000).setMinExpr(null).build();
        slotToColumnStat.put(a, aStats);
        slotToColumnStat.put(b, bStats);
        slotToColumnStat.put(c, cStats);
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(or, stat);
        Assertions.assertEquals(51, expected.getRowCount(), 0.1);
    }

    // a > 500 and b < 100 or a > c
    @Test
    public void test2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThan greaterThan1 = new GreaterThan(a, int500);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        IntegerLiteral int100 = new IntegerLiteral(100);
        LessThan lessThan = new LessThan(b, int100);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        GreaterThan greaterThan = new GreaterThan(a, c);
        And and = new And(greaterThan1, lessThan);
        Or or = new Or(and, greaterThan);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder aBuilder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(0)
                .setMaxValue(1000);
        slotToColumnStat.put(a, aBuilder.build());
        slotToColumnStat.put(b, aBuilder.build());
        slotToColumnStat.put(c, aBuilder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(or, stat);
        Assertions.assertTrue(
                Precision.equals(512.5,
                        expected.getRowCount(), 0.01));
    }

    // a >= 500
    // a belongs to [0, 500]
    @Test
    public void test3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThanEqual ge = new GreaterThanEqual(a, int500);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(0)
                .setMaxValue(500);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(ge, stat);
        Assertions.assertEquals(1000 * 1.0 / 500, expected.getRowCount());
    }

    // a <= 500
    // a belongs to [500, 1000]
    @Test
    public void test4() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        LessThanEqual le = new LessThanEqual(a, int500);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a, builder1.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(le, stat);
        Assertions.assertEquals(1000 * 1.0 / 500, expected.getRowCount());
    }

    // a < 500
    // a belongs to [500, 1000]
    @Test
    public void test5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        LessThan less = new LessThan(a, int500);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(less, stat);
        Assertions.assertEquals(2, expected.getRowCount());
    }

    // a > 1000
    // a belongs to [500, 1000]
    @Test
    public void test6() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int1000 = new IntegerLiteral(1000);
        GreaterThan ge = new GreaterThan(a, int1000);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(ge, stat);
        Assertions.assertEquals(2, expected.getRowCount());
    }

    // a > b
    // a belongs to [0, 500]
    // b belongs to [501, 100]
    @Test
    public void test7() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        GreaterThan ge = new GreaterThan(a, b);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builder2 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(501)
                .setMaxValue(1000);
        slotToColumnStat.put(a, builder1.build());
        slotToColumnStat.put(b, builder2.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics expected = filterEstimation.estimate(ge, stat);
        Assertions.assertEquals(0, expected.getRowCount());
    }

    // a < b
    // a belongs to [0, 500]
    // b belongs to [501, 100]
    @Test
    public void test8() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        LessThan less = new LessThan(a, b);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builder2 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(501)
                .setMaxValue(1000);
        slotToColumnStat.put(a, builder1.build());
        slotToColumnStat.put(b, builder2.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics esimated = filterEstimation.estimate(less, stat);
        Assertions.assertEquals(1000, esimated.getRowCount());
    }

    // a > b
    // a belongs to [501, 1000]
    // b belongs to [0, 500]
    @Test
    public void test9() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        GreaterThan ge = new GreaterThan(a, b);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(501)
                .setMaxValue(1000);
        ColumnStatisticBuilder builder2 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(0)
                .setMaxValue(500);
        slotToColumnStat.put(a, builder1.build());
        slotToColumnStat.put(b, builder2.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(ge, stat);
        Assertions.assertEquals(500, estimated.getRowCount());
    }

    // a in (1, 3, 5)
    // a belongs to [1, 10]
    @Test
    public void test10() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral i1 = new IntegerLiteral(1);
        IntegerLiteral i3 = new IntegerLiteral(3);
        IntegerLiteral i5 = new IntegerLiteral(5);
        InPredicate inPredicate = new InPredicate(a, Lists.newArrayList(i1, i3, i5));
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(10)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1)
                .setMinExpr(new IntLiteral(1))
                .setMaxValue(10)
                .setMaxExpr(new IntLiteral(10));
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(inPredicate, stat);
        Assertions.assertEquals(1000 * 3.0 / 10.0, estimated.getRowCount());
    }

    // a not in (1, 3, 5)
    // a belongs to [1, 10]
    @Test
    public void test11() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral i1 = new IntegerLiteral(1);
        IntegerLiteral i3 = new IntegerLiteral(3);
        IntegerLiteral i5 = new IntegerLiteral(5);
        InPredicate inPredicate = new InPredicate(a, Lists.newArrayList(i1, i3, i5));
        Not not = new Not(inPredicate);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(10)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1)
                .setMaxValue(10);
        slotToColumnStat.put(a, builder.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(not, stat);
        Assertions.assertEquals(1000 * 7.0 / 10.0, estimated.getRowCount());
    }

    // c>100
    // a is primary-key, a.ndv is reduced
    // b is normal, b.ndv is smaller: newNdv = ndv * (1 - Math.pow(1 - selectivity, rowCount / ndv));
    // c.selectivity is still 1, but its range becomes half
    @Test
    public void test12() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i100 = new IntegerLiteral(100);
        GreaterThan ge = new GreaterThan(c, i100);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(1000)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1000)
                .setMaxValue(10000);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(200);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(ge, stat);
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        Assertions.assertEquals(500, statsA.ndv);
        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        Assertions.assertEquals(100, statsB.ndv, 0.1);
        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        Assertions.assertEquals(50, statsC.ndv);
        Assertions.assertEquals(100, statsC.minValue);
        Assertions.assertEquals(200, statsC.maxValue);
    }

    /**
     * test filter estimation, like 20>c>10, c in (0,40)
     * filter range has intersection with (c.min, c.max)
     *      rows = 100
     *     a primary key, a.ndv reduced by 1/4
     *     b normal field, b.ndv=20 =>
     *     c.ndv = 10/40 * c.ndv
     */
    @Test
    public void testFilterInsideMinMax() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i10 = new IntegerLiteral(10);
        IntegerLiteral i20 = new IntegerLiteral(20);
        GreaterThan ge1 = new GreaterThan(c, i10);
        //GreaterThan ge2 = new GreaterThan(i20, c);
        LessThan le1 = new LessThan(c, i20);
        And and = new And(ge1, le1);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(20)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(40)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(40);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(100, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(and, stat);
        Assertions.assertEquals(25, estimated.getRowCount());
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        Assertions.assertEquals(25, statsA.ndv, 0.1);
        //Assertions.assertEquals(0.25, statsA.selectivity);
        Assertions.assertEquals(0, statsA.minValue);
        Assertions.assertEquals(100, statsA.maxValue);

        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        Assertions.assertEquals(15.6, statsB.ndv, 0.1);
        Assertions.assertEquals(0, statsB.minValue);
        Assertions.assertEquals(500, statsB.maxValue);

        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        Assertions.assertEquals(10, statsC.ndv);
        Assertions.assertEquals(10, statsC.minValue);
        Assertions.assertEquals(20, statsC.maxValue);
    }


    /**
     *  test filter estimation, c > 300, where 300 is out of c's range (0,200)
     *  after filter
     *     c.selectivity=a.selectivity=b.selectivity = 0
     *     c.ndv=a.ndv=b.ndv=0
     *     a.ndv = b.ndv = 0
     */

    @Test
    public void testFilterOutofMinMax() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i300 = new IntegerLiteral(300);
        GreaterThan ge = new GreaterThan(c, i300);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(1000)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(10000)
                .setMaxValue(1000);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(200);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics estimated = filterEstimation.estimate(ge, stat);
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        Assertions.assertEquals(0, statsA.ndv);
        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        Assertions.assertEquals(0, statsB.ndv);
        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        Assertions.assertEquals(0, statsC.ndv);
        Assertions.assertTrue(Double.isNaN(statsC.minValue));
        Assertions.assertTrue(Double.isNaN(statsC.maxValue));
    }

    /**
     * table rows 100
     * before
     * A: ndv 100, (0, 100), selectivity=1.0, primary-key
     * B: ndv 20,  (0, 500), selectivity=1.0,
     * C: ndv 40,  (0, 40),  selectivity=1.0,
     *
     * filter: c in (10 ,20)
     *
     * after
     * A: ndv 5, (0, 100),  selectivity=2/40,  primary-key
     * B: ndv 5,  (0, 500),  selectivity=0.25,
     * C: ndv 2,  (10, 20),   selectivity=0.2,
     *
     * C.selectivity=0.2:
     * before filter, 40 distinct values distributed evenly in range (0, 40),
     * after filter, the range shrinks to (10,20), there are 10 distinct values in (10,20).
     * there are two value, e.g. 10 and 20, are in (10, 20).
     * the selectivity is 2 / 10.
     *
     * A.selectivity = 2/40:
     * the table after filter keeps 2/40 rows
     *
     * B.selectivity = 5/20
     * after filter, there are 5 rows => B.ndv at most is 5. from 20 to 5, B.selectivity at most 5/20
     */
    @Test
    public void testInPredicateEstimationForColumns() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i10 = new IntegerLiteral(10);
        IntegerLiteral i20 = new IntegerLiteral(20);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(20)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(40)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(40);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(100, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();

        InPredicate inPredicate = new InPredicate(c, Lists.newArrayList(i10, i20));
        Statistics estimated = filterEstimation.estimate(inPredicate, stat);
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        Assertions.assertEquals(5, statsA.ndv, 0.1);
        Assertions.assertEquals(0, statsA.minValue);
        Assertions.assertEquals(100, statsA.maxValue);
        Assertions.assertEquals(4.5, statsB.ndv, 0.1);
        Assertions.assertEquals(0, statsB.minValue);
        Assertions.assertEquals(500, statsB.maxValue);
        Assertions.assertEquals(2, statsC.ndv);
        Assertions.assertEquals(10, statsC.minValue);
        Assertions.assertEquals(20, statsC.maxValue);
    }

    /**
     * table rows 100
     * before
     * A: ndv 100, (0, 100), selectivity=1.0, primary-key
     * B: ndv 20,  (0, 500), selectivity=1.0,
     * C: ndv 40,  (0, 40),  selectivity=1.0,
     *
     * filter: c in (10, 15, 200)
     *
     * after
     * A: ndv 5, (0, 100),  selectivity=2/40,  primary-key
     * B: ndv 5,  (0, 500),  selectivity=0.25,
     * C: ndv 2,  (10, 15),  selectivity=0.4,
     *
     * c.selectivity=0.4:
     *      distinct c value count in range (10,15) is 5,
     *      only 2 values are selected, so selectivity is 2 / 5
     */
    @Test
    public void testInPredicateEstimationForColumnsOutofRange() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i10 = new IntegerLiteral(10);
        IntegerLiteral i15 = new IntegerLiteral(15);
        IntegerLiteral i200 = new IntegerLiteral(200);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100)
                .setCount(100);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setCount(100)
                .setNdv(20)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setCount(100)
                .setNdv(40)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(40);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(100, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();

        InPredicate inPredicate = new InPredicate(c, Lists.newArrayList(i10, i15, i200));
        Statistics estimated = filterEstimation.estimate(inPredicate, stat);
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        System.out.println(statsA);
        System.out.println(statsB);
        System.out.println(statsC);
        Assertions.assertEquals(5, statsA.ndv, 0.1);
        Assertions.assertEquals(0, statsA.minValue);
        Assertions.assertEquals(100, statsA.maxValue);
        Assertions.assertEquals(4.5, statsB.ndv, 0.1);
        Assertions.assertEquals(0, statsB.minValue);
        Assertions.assertEquals(500, statsB.maxValue);
        Assertions.assertEquals(2, statsC.ndv);
        Assertions.assertEquals(10, statsC.minValue);
        Assertions.assertEquals(15, statsC.maxValue);
    }

    /**
     * table rows 100
     * before
     * A: ndv 100, (0, 100), selectivity=1.0, primary-key
     * B: ndv 20,  (0, 500), selectivity=1.0,
     * C: ndv 40,  (0, 40),  selectivity=1.0,
     *
     * filter: c > 10
     *
     * after
     * rows = 30
     * A: ndv 75, (0, 100),  selectivity= 30/40,  primary-key
     * B: ndv 20,  (0, 500),  selectivity=1.0,
     * C: ndv 30,  (10, 40),  selectivity=1.0,
     */
    @Test
    public void testFilterEstimationForColumnsNotChanged() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i10 = new IntegerLiteral(10);
        Map<Expression, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100)
                .setCount(100);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setCount(100)
                .setNdv(20)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setCount(100)
                .setNdv(40)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(40);
        slotToColumnStat.put(a, builderA.build());
        slotToColumnStat.put(b, builderB.build());
        slotToColumnStat.put(c, builderC.build());
        Statistics stat = new Statistics(100, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation();

        GreaterThan greaterThan = new GreaterThan(c, i10);
        Statistics estimated = filterEstimation.estimate(greaterThan, stat);
        ColumnStatistic statsA = estimated.findColumnStatistics(a);
        ColumnStatistic statsB = estimated.findColumnStatistics(b);
        ColumnStatistic statsC = estimated.findColumnStatistics(c);
        Assertions.assertEquals(75, statsA.ndv);
        Assertions.assertEquals(0, statsA.minValue);
        Assertions.assertEquals(100, statsA.maxValue);
        Assertions.assertEquals(19.9, statsB.ndv, 0.1);
        Assertions.assertEquals(0, statsB.minValue);
        Assertions.assertEquals(500, statsB.maxValue);
        Assertions.assertEquals(30, statsC.ndv);
        Assertions.assertEquals(10, statsC.minValue);
        Assertions.assertEquals(40, statsC.maxValue);
    }

    @Test
    public void testBetweenCastFilter() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMaxValue(100)
                .setMinValue(0)
                .setCount(100);
        DoubleLiteral begin = new DoubleLiteral(40.0);
        DoubleLiteral end = new DoubleLiteral(50.0);
        LessThan less = new LessThan(new Cast(a, DoubleType.INSTANCE), end);
        GreaterThan greater = new GreaterThan(new Cast(a, DoubleType.INSTANCE), begin);
        And and = new And(less, greater);
        Statistics stats = new Statistics(100, new HashMap<>());
        stats.addColumnStats(a, builder.build());
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics result = filterEstimation.estimate(and, stats);
        Assertions.assertEquals(result.getRowCount(), 10, 0.01);
        ColumnStatistic colStats = result.findColumnStatistics(a);
        Assertions.assertTrue(colStats != null);
        Assertions.assertEquals(10, colStats.ndv, 0.1);
    }

    @Test
    public void testDateRangeSelectivity() {
        DateLiteral from = new DateLiteral("1990-01-01");
        DateLiteral to = new DateLiteral("2000-01-01");
        SlotReference a = new SlotReference("a", DateType.INSTANCE);
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMaxValue(to.getDouble())
                .setMinValue(from.getDouble())
                .setCount(100);
        DateLiteral mid = new DateLiteral("1999-01-01");
        GreaterThan greaterThan = new GreaterThan(a, mid);
        Statistics stats = new Statistics(100, new HashMap<>());
        stats.addColumnStats(a, builder.build());
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics result = filterEstimation.estimate(greaterThan, stats);
        Assertions.assertEquals(result.getRowCount(), 10, 0.1);
    }
}
