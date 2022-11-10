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

package org.apache.doris.statistics;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnStatsTest {
    private ColumnStat columnStatsUnderTest;

    @Before
    public void setUp() throws Exception {
        columnStatsUnderTest = new ColumnStat();
    }

    @Test
    public void testUpdateStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.MAX_SIZE, "8");
        statsTypeToValue.put(StatsType.MIN_VALUE, "0");
        statsTypeToValue.put(StatsType.MAX_VALUE, "100");

        // Run the test
        columnStatsUnderTest.updateStats(columnType, statsTypeToValue);

        // Verify the results
        double maxSize = columnStatsUnderTest.getMaxSizeByte();
        Assert.assertEquals(8, maxSize, 0.1);

        double minValue = columnStatsUnderTest.getMinValue();
        Assert.assertEquals(0, minValue, 0.1);

        double maxValue = columnStatsUnderTest.getMaxValue();
        Assert.assertEquals(100, maxValue, 0.1);
    }

    @Test
    public void testUpdateStats_ThrowsAnalysisException() {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.AVG_SIZE, "abc");

        // Run the test
        Assert.assertThrows(AnalysisException.class,
                () -> columnStatsUnderTest.updateStats(columnType, statsTypeToValue));
    }

    @Test
    public void testGetShowInfo() throws AnalysisException {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NDV, "1");
        statsTypeToValue.put(StatsType.AVG_SIZE, "8");
        statsTypeToValue.put(StatsType.MAX_SIZE, "8");
        statsTypeToValue.put(StatsType.NUM_NULLS, "2");
        statsTypeToValue.put(StatsType.MIN_VALUE, "0");
        statsTypeToValue.put(StatsType.MAX_VALUE, "1000");

        columnStatsUnderTest.updateStats(columnType, statsTypeToValue);
        String[] expectedInfo = {"1.0", "8.0", "8.0", "2.0", "0.0", "1000.0"};

        // Run the test
        List<String> showInfo = columnStatsUnderTest.getShowInfo();
        String[] result = showInfo.toArray(new String[0]);

        // Verify the results
        Assert.assertArrayEquals(expectedInfo, result);
    }

    @Test
    public void testGetDefaultColumnStats() {
        // Run the test
        ColumnStat defaultColumnStats = ColumnStat.getDefaultColumnStats();

        // Verify the results
        double ndv = defaultColumnStats.getNdv();
        Assert.assertEquals(-1L, ndv, 0.1);

        double avgSize = defaultColumnStats.getAvgSizeByte();
        Assert.assertEquals(-1.0f, avgSize, 0.0001);

        double maxSize = defaultColumnStats.getMaxSizeByte();
        Assert.assertEquals(-1L, maxSize, 0.1);

        double maxValue = defaultColumnStats.getMaxValue();
        Assert.assertEquals(Double.NaN, maxValue, 0.1);

        double minValue = defaultColumnStats.getMinValue();
        Assert.assertEquals(Double.NaN, minValue, 0.1);
    }

    @Test
    public void testAggColumnStats() throws Exception {
        // Setup
        ColumnStat columnStats = ColumnStat.getDefaultColumnStats();
        ColumnStat other = new ColumnStat(1L, 4.0f, 5L, 10L,
                Double.NaN,
                Double.NaN);

        // Run the test
        ColumnStat aggColumnStats = ColumnStat.mergeColumnStats(columnStats, other);

        // Verify the results
        double ndv = aggColumnStats.getNdv();
        // 0(default) + 1
        Assert.assertEquals(1L, ndv, 0.1);

        double avgSize = aggColumnStats.getAvgSizeByte();
        // (0.0f + 4.0f) / 2
        Assert.assertEquals(4.0f, avgSize, 0.0001);

        double maxSize = aggColumnStats.getMaxSizeByte();
        Assert.assertEquals(5L, maxSize, 0.1);

        double numNulls = aggColumnStats.getNumNulls();
        Assert.assertEquals(10L, numNulls, 0.1);

        double minValue = aggColumnStats.getMinValue();
        // null VS sMinValue
        Assert.assertEquals(Double.NaN, minValue, 0.1);

        double maxValue = aggColumnStats.getMaxValue();
        // null VS sMaxValue
        Assert.assertEquals(Double.NaN, maxValue, 0.1);
    }
}
