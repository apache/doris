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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class FilterEstimationTest {

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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a.getExprId(),
                new ColumnStatisticBuilder().setCount(500).setNdv(500).setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                        .setMinValue(0).setMaxValue(1000).setMinExpr(null).build());
        slotToColumnStat.put(b.getExprId(),
                new ColumnStatisticBuilder().setCount(500).setNdv(500).setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                        .setMinValue(0).setMaxValue(1000).setMinExpr(null).build());
        slotToColumnStat.put(c.getExprId(),
                new ColumnStatisticBuilder().setCount(500).setNdv(500).setAvgSizeByte(4).setNumNulls(500).setDataSize(0)
                        .setMinValue(0).setMaxValue(1000).setMinExpr(null).build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(or);
        Assertions.assertTrue(
                Precision.equals((0.5 * 0.1 + FilterEstimation.DEFAULT_EQUALITY_COMPARISON_SELECTIVITY
                        - 0.5 * 0.1 * FilterEstimation.DEFAULT_EQUALITY_COMPARISON_SELECTIVITY) * 1000,
                        expected.getRowCount(), 0.01));
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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder aBuilder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(0)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), aBuilder.build());
        slotToColumnStat.put(b.getExprId(), aBuilder.build());
        slotToColumnStat.put(c.getExprId(), aBuilder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(or);
        Assertions.assertTrue(
                Precision.equals((0.5 * 0.1
                        + FilterEstimation.DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY
                        - 0.5 * 0.1 * FilterEstimation.DEFAULT_INEQUALITY_COMPARISON_SELECTIVITY) * 1000,
                        expected.getRowCount(), 0.01));
    }

    // a >= 500
    // a belongs to [0, 500]
    @Test
    public void test3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        GreaterThanEqual ge = new GreaterThanEqual(a, int500);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(0)
                .setMaxValue(500);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(ge);
        Assertions.assertEquals(1000 * 1.0 / 500, expected.getRowCount());
    }

    // a <= 500
    // a belongs to [500, 1000]
    @Test
    public void test4() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        LessThanEqual le = new LessThanEqual(a, int500);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder1.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(le);
        Assertions.assertEquals(1000 * 1.0 / 500, expected.getRowCount());
    }

    // a < 500
    // a belongs to [500, 1000]
    @Test
    public void test5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int500 = new IntegerLiteral(500);
        LessThan less = new LessThan(a, int500);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(less);
        Assertions.assertEquals(0, expected.getRowCount());
    }

    // a > 1000
    // a belongs to [500, 1000]
    @Test
    public void test6() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        IntegerLiteral int1000 = new IntegerLiteral(1000);
        GreaterThan ge = new GreaterThan(a, int1000);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(500)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(ge);
        Assertions.assertEquals(0, expected.getRowCount());
    }

    // a > b
    // a belongs to [0, 500]
    // b belongs to [501, 100]
    @Test
    public void test7() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        GreaterThan ge = new GreaterThan(a, b);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
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
        slotToColumnStat.put(a.getExprId(), builder1.build());
        slotToColumnStat.put(b.getExprId(), builder2.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(ge);
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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
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
        slotToColumnStat.put(a.getExprId(), builder1.build());
        slotToColumnStat.put(b.getExprId(), builder2.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult esimated = filterEstimation.estimate(less);
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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
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
        slotToColumnStat.put(a.getExprId(), builder1.build());
        slotToColumnStat.put(b.getExprId(), builder2.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(ge);
        Assertions.assertEquals(1000, estimated.getRowCount());
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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(10)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1)
                .setMaxValue(10);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(inPredicate);
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
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(10)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(1)
                .setMaxValue(10);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(not);
        Assertions.assertEquals(1000 * 7.0 / 10.0, estimated.getRowCount());
    }

    //c>100
    // a is primary-key, a.ndv is reduced
    // b is normal, b.ndv is not changed
    // c.selectivity is still 1, but its range becomes half
    @Test
    public void test12() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i100 = new IntegerLiteral(100);
        GreaterThan ge = new GreaterThan(c, i100);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(1000)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(10000)
                .setMaxValue(1000)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(200)
                .setSelectivity(1.0);
        slotToColumnStat.put(a.getExprId(), builderA.build());
        slotToColumnStat.put(b.getExprId(), builderB.build());
        slotToColumnStat.put(c.getExprId(), builderC.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(ge);
        ColumnStatistic statsA = estimated.getColumnStatsBySlotId(a.getExprId());
        Assertions.assertEquals(500, statsA.ndv);
        Assertions.assertEquals(0.5, statsA.selectivity);
        ColumnStatistic statsB = estimated.getColumnStatsBySlotId(b.getExprId());
        Assertions.assertEquals(100, statsB.ndv);
        Assertions.assertEquals(1.0, statsB.selectivity);
        ColumnStatistic statsC = estimated.getColumnStatsBySlotId(c.getExprId());
        Assertions.assertEquals(50, statsC.ndv);
        Assertions.assertEquals(100, statsC.minValue);
        Assertions.assertEquals(200, statsC.maxValue);
        Assertions.assertEquals(1.0, statsC.selectivity);
    }

    // 20>c>10, c in (0,40)
    // a primary key, a.ndv reduced by 1/4, a.selectivity=0.25
    // b normal field, b.ndv not changed, b.selectivity=1.0
    // c.ndv = 10/40 * c.ndv, c.selectivity=1
    @Test
    public void test13() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i10 = new IntegerLiteral(10);
        IntegerLiteral i20 = new IntegerLiteral(20);
        GreaterThan ge1 = new GreaterThan(c, i10);
        //GreaterThan ge2 = new GreaterThan(i20, c);
        LessThan le1 = new LessThan(c, i20);
        And and = new And(ge1, le1);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(100)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(20)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(40)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(40)
                .setSelectivity(1.0);
        slotToColumnStat.put(a.getExprId(), builderA.build());
        slotToColumnStat.put(b.getExprId(), builderB.build());
        slotToColumnStat.put(c.getExprId(), builderC.build());
        StatsDeriveResult stat = new StatsDeriveResult(100, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(and);
        Assertions.assertEquals(25, estimated.getRowCount());
        ColumnStatistic statsA = estimated.getColumnStatsBySlotId(a.getExprId());
        Assertions.assertEquals(25, statsA.ndv);
        //Assertions.assertEquals(0.25, statsA.selectivity);
        Assertions.assertEquals(0, statsA.minValue);
        Assertions.assertEquals(100, statsA.maxValue);

        ColumnStatistic statsB = estimated.getColumnStatsBySlotId(b.getExprId());
        Assertions.assertEquals(20, statsB.ndv);
        Assertions.assertEquals(0, statsB.minValue);
        Assertions.assertEquals(500, statsB.maxValue);
        Assertions.assertEquals(1.0, statsB.selectivity);

        ColumnStatistic statsC = estimated.getColumnStatsBySlotId(c.getExprId());
        Assertions.assertEquals(10, statsC.ndv);
        Assertions.assertEquals(10, statsC.minValue);
        Assertions.assertEquals(20, statsC.maxValue);
        Assertions.assertEquals(1.0, statsC.selectivity);
    }


    // c in (0,200) filter:c > 300
    // after filter
    // c.selectivity=a.selectivity=b.selectivity = 0
    // c.ndv=a.ndv=b.ndv=0
    @Test
    public void test14() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        IntegerLiteral i300 = new IntegerLiteral(300);
        GreaterThan ge = new GreaterThan(c, i300);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builderA = new ColumnStatisticBuilder()
                .setNdv(1000)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(10000)
                .setMaxValue(1000)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderB = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500)
                .setSelectivity(1.0);
        ColumnStatisticBuilder builderC = new ColumnStatisticBuilder()
                .setNdv(100)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(200)
                .setSelectivity(1.0);
        slotToColumnStat.put(a.getExprId(), builderA.build());
        slotToColumnStat.put(b.getExprId(), builderB.build());
        slotToColumnStat.put(c.getExprId(), builderC.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(ge);
        ColumnStatistic statsA = estimated.getColumnStatsBySlotId(a.getExprId());
        Assertions.assertEquals(0, statsA.ndv);
        Assertions.assertEquals(0, statsA.selectivity);
        ColumnStatistic statsB = estimated.getColumnStatsBySlotId(b.getExprId());
        Assertions.assertEquals(0, statsB.ndv);
        Assertions.assertEquals(1.0, statsB.selectivity);
        ColumnStatistic statsC = estimated.getColumnStatsBySlotId(c.getExprId());
        Assertions.assertEquals(0, statsC.ndv);
        Assertions.assertEquals(300, statsC.minValue);
        Assertions.assertEquals(300, statsC.maxValue);
        Assertions.assertEquals(1.0, statsC.selectivity);
    }
}
