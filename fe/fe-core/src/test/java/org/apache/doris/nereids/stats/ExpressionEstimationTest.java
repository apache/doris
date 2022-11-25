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
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.StatsDeriveResult;

import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ExpressionEstimationTest {

    // MAX(a)
    // a belongs to [0, 500]
    @Test
    public void test1() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Max max = new Max(a);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);

        //min/max not changed. select min(A) as X from T group by B. X.max is A.max, not A.min
        ColumnStatistic estimated = ExpressionEstimation.estimate(max, stat);
        Assertions.assertEquals(0, estimated.minValue);
        Assertions.assertEquals(500, estimated.maxValue);
        Assertions.assertEquals(1, estimated.ndv);
    }

    // MIN(a)
    // a belongs to [0, 500]
    @Test
    public void test2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        Min max = new Min(a);
        //min/max not changed. select max(A) as X from T group by B. X.min is A.min, not A.max
        ColumnStatistic estimated = ExpressionEstimation.estimate(max, stat);
        Assertions.assertEquals(0, estimated.minValue);
        Assertions.assertEquals(1000, estimated.maxValue);
        Assertions.assertEquals(1, estimated.ndv);
    }

    // a + b
    // a belongs to [0, 500]
    // b belongs to [300, 1000]
    @Test
    public void test3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(300)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b.getExprId(), builder1.build());
        Add add = new Add(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(add, stat);
        Assertions.assertEquals(300, estimated.minValue);
        Assertions.assertEquals(1500, estimated.maxValue);
    }

    // a - b
    // a belongs to [0, 500]
    // b belongs to [300, 1000]
    @Test
    public void test4() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(0)
                .setMaxValue(500);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        builder.setMinValue(300);
        builder.setMaxValue(1000);
        slotToColumnStat.put(b.getExprId(), builder.build());
        Subtract subtract = new Subtract(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(subtract, stat);
        Assertions.assertEquals(-1000, estimated.minValue);
        Assertions.assertEquals(200, estimated.maxValue);
    }

    // a * b
    // a belongs to [-200, -100]
    // b belongs to [-300, 1000]
    @Test
    public void test5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(-200)
                .setMaxValue(-100);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        builder.setMinValue(-300);
        builder.setMaxValue(1000);
        slotToColumnStat.put(b.getExprId(), builder.build());
        Multiply multiply = new Multiply(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(multiply, stat);
        Assertions.assertEquals(-200 * 1000, estimated.minValue);
        Assertions.assertEquals(-200 * -300, estimated.maxValue);
    }

    // a * b
    // a belongs to [-200, -100]
    // b belongs to [-1000, -300]
    @Test
    public void test6() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(-200)
                .setMaxValue(-100);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        builder.setMinValue(-1000);
        builder.setMaxValue(-300);
        slotToColumnStat.put(b.getExprId(), builder.build());
        Multiply multiply = new Multiply(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(multiply, stat);
        Assertions.assertEquals(-100 * -300, estimated.minValue);
        Assertions.assertEquals(-200 * -1000, estimated.maxValue);
    }

    // a / b
    // a belongs to [-200, -100]
    // b belongs to [-300, 1000]
    @Test
    public void test7() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(-200)
                .setMaxValue(-100);
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(-300)
                .setMaxValue(1000);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b.getExprId(), builder1.build());
        Divide divide = new Divide(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(divide, stat);
        Assertions.assertTrue(Precision.equals(-0.2, estimated.minValue, 0.001));
        Assertions.assertTrue(Precision.equals(0.666, estimated.maxValue, 0.001));
    }

    // a / b
    // a belongs to [-200, -100]
    // b belongs to [-1000, -100]
    @Test
    public void test8() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Id, ColumnStatistic> slotToColumnStat = new HashMap<>();

        ColumnStatisticBuilder builder = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(500)
                .setMinValue(-200)
                .setMaxValue(-100);
        ColumnStatisticBuilder builder1 = new ColumnStatisticBuilder()
                .setNdv(500)
                .setAvgSizeByte(4)
                .setNumNulls(0)
                .setMinValue(-1000)
                .setMaxValue(-100);
        slotToColumnStat.put(a.getExprId(), builder.build());
        StatsDeriveResult stat = new StatsDeriveResult(1000, 1, 0, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b.getExprId(), builder1.build());
        Divide divide = new Divide(a, b);
        ColumnStatistic estimated = ExpressionEstimation.estimate(divide, stat);
        Assertions.assertTrue(Precision.equals(0.1, estimated.minValue, 0.001));
        Assertions.assertEquals(2, estimated.maxValue);
    }
}
