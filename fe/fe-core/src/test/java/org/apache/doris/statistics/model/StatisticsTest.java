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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StatisticsTest {
    @Test
    public void testAvgSizeAbnormal() {
        SlotReference slot = SlotReference.of("a", IntegerType.INSTANCE);
        ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder();
        colBuilder.setAvgSizeByte(Double.NaN);
        Statistics stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        double tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(Double.POSITIVE_INFINITY);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(Double.NEGATIVE_INFINITY);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);

        colBuilder.setAvgSizeByte(-1.0);
        stats = new Statistics(1, 1, ImmutableMap.of(slot, colBuilder.build()));
        tupleSize = stats.computeTupleSize(ImmutableList.of(slot));
        Assertions.assertEquals(1, tupleSize);
    }

    @Test
    public void testEnforceValid() {
        SlotReference slot = SlotReference.of("a", IntegerType.INSTANCE);
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(1000)
                .setNdv(80).setNumNulls(50).setAvgSizeByte(4).build();
        Statistics stats = statisticsOf(slot, columnStatistic, 100);
        stats.enforceValid();
        ColumnStatistic result = stats.findColumnStatistics(slot);
        Assertions.assertEquals(80, result.ndv, 0.01);
        // numNulls is clamped to `rowCount - ndv`, but not scaled by the row count change
        Assertions.assertEquals(20, result.numNulls, 0.01);
    }

    @Test
    public void testRescaleNumNulls() {
        SlotReference slot = SlotReference.of("a", IntegerType.INSTANCE);
        ColumnStatistic columnStatistic = new ColumnStatisticBuilder(1000)
                .setNdv(10).setNumNulls(200).setAvgSizeByte(4).build();
        Statistics stats = statisticsOf(slot, columnStatistic, 100);
        // 100 / 1000 of the input rows are kept
        stats.rescaleNumNulls(1000);
        Assertions.assertEquals(20, stats.findColumnStatistics(slot).numNulls, 0.01);

        // a non-positive input row count only repairs the statistics and does not produce NaN
        ColumnStatistic zeroInput = new ColumnStatisticBuilder(1000)
                .setNdv(10).setNumNulls(200).setAvgSizeByte(4).build();
        Statistics zeroInputStats = statisticsOf(slot, zeroInput, 100);
        zeroInputStats.rescaleNumNulls(0);
        Assertions.assertEquals(90, zeroInputStats.findColumnStatistics(slot).numNulls, 0.01);
    }

    private static Statistics statisticsOf(SlotReference slot, ColumnStatistic columnStatistic, double rowCount) {
        Map<Expression, ColumnStatistic> columnStats = new HashMap<>();
        columnStats.put(slot, columnStatistic);
        return new Statistics(rowCount, 1, columnStats);
    }
}
