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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.ColumnStat;
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 0, 1000));
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 500, 0, 1000));
        slotToColumnStat.put(c, new ColumnStat(500, 4, 4, 500, 0, 1000));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult expected = filterEstimation.estimate(or);
        Assertions.assertTrue(
                Precision.equals((0.5 * 0.1 + 0.1 - 0.5 * 0.1 * 0.1) * 1000, expected.getRowCount(), 0.01));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 0, 1000));
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 500, 0, 1000));
        slotToColumnStat.put(c, new ColumnStat(500, 4, 4, 500, 0, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 0, 500));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 500, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 500, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 500, 500, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, 501, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 0, 500));
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, 501, 1000));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(500, 4, 4, 0, 501, 1000));
        slotToColumnStat.put(b, new ColumnStat(500, 4, 4, 0, 0, 500));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(10, 4, 4, 0, 1, 10));
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
        Map<Slot, ColumnStat> slotToColumnStat = new HashMap<>();
        slotToColumnStat.put(a, new ColumnStat(10, 4, 4, 0, 1, 10));
        StatsDeriveResult stat = new StatsDeriveResult(1000, slotToColumnStat);
        FilterEstimation filterEstimation = new FilterEstimation(stat);
        StatsDeriveResult estimated = filterEstimation.estimate(not);
        Assertions.assertEquals(1000 * 7.0 / 10.0, estimated.getRowCount());
    }
}
