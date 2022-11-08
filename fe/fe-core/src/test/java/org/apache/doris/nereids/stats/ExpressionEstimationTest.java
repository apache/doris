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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStat;
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        ColumnStat estimated = ExpressionEstimation.estimate(max, stat);
        Assertions.assertEquals(500, estimated.getMinValue());
        Assertions.assertEquals(1, estimated.getNdv());
    }

    // MIN(a)
    // a belongs to [0, 500]
    @Test
    public void test2() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        Min min = new Min(a);
        ColumnStat estimated = ExpressionEstimation.estimate(min, stat);
        Assertions.assertEquals(0, estimated.getMaxValue());
        Assertions.assertEquals(1, estimated.getNdv());
    }

    // a + b
    // a belongs to [0, 500]
    // b belongs to [300, 1000]
    @Test
    public void test3() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, 300, 1000));
        Add add = new Add(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(add, stat);
        Assertions.assertEquals(300, estimated.getMinValue());
        Assertions.assertEquals(1500, estimated.getMaxValue());
    }

    // a - b
    // a belongs to [0, 500]
    // b belongs to [300, 1000]
    @Test
    public void test4() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, 300, 1000));
        Subtract subtract = new Subtract(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(subtract, stat);
        Assertions.assertEquals(-1000, estimated.getMinValue());
        Assertions.assertEquals(200, estimated.getMaxValue());
    }

    // a * b
    // a belongs to [-200, -100]
    // b belongs to [-300, 1000]
    @Test
    public void test5() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, -200, -100));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, -300, 1000));
        Multiply multiply = new Multiply(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(multiply, stat);
        Assertions.assertEquals(-200 * 1000, estimated.getMinValue());
        Assertions.assertEquals(-200 * -300, estimated.getMaxValue());
    }

    // a * b
    // a belongs to [-200, -100]
    // b belongs to [-1000, -300]
    @Test
    public void test6() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, -200, -100));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, -1000, -300));
        Multiply multiply = new Multiply(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(multiply, stat);
        Assertions.assertEquals(-100 * -300, estimated.getMinValue());
        Assertions.assertEquals(-200 * -1000, estimated.getMaxValue());
    }

    // a / b
    // a belongs to [-200, -100]
    // b belongs to [-300, 1000]
    @Test
    public void test7() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, -200, -100));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, -300, 1000));
        Divide divide = new Divide(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(divide, stat);
        Assertions.assertTrue(Precision.equals(-0.2, estimated.getMinValue(), 0.001));
        Assertions.assertTrue(Precision.equals(0.666, estimated.getMaxValue(), 0.001));
    }

    // a / b
    // a belongs to [-200, -100]
    // b belongs to [-1000, -100]
    @Test
    public void test8() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, -200, -100));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, -1000, -100));
        Divide divide = new Divide(a, b);
        ColumnStat estimated = ExpressionEstimation.estimate(divide, stat);
        Assertions.assertTrue(Precision.equals(0.1, estimated.getMinValue(), 0.001));
        Assertions.assertEquals(2, estimated.getMaxValue());
    }
}
