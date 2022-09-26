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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ColumnStatsTest {
    private ColumnStats columnStatsUnderTest;

    @Before
    public void setUp() throws Exception {
        columnStatsUnderTest = new ColumnStats();
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
        long maxSize = columnStatsUnderTest.getMaxSize();
        Assert.assertEquals(8, maxSize);

        long minValue = columnStatsUnderTest.getMinValue().getLongValue();
        Assert.assertEquals(0, minValue);

        long maxValue = columnStatsUnderTest.getMaxValue().getLongValue();
        Assert.assertEquals(100, maxValue);
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
        String[] expectedInfo = {"1", "8.0", "8", "2", "0", "1000"};

        // Run the test
        List<String> showInfo = columnStatsUnderTest.getShowInfo();
        String[] result = showInfo.toArray(new String[0]);

        // Verify the results
        Assert.assertArrayEquals(expectedInfo, result);
    }

    @Test
    public void testGetDefaultColumnStats() {
        // Run the test
        ColumnStats defaultColumnStats = ColumnStats.getDefaultColumnStats();

        // Verify the results
        long ndv = defaultColumnStats.getNdv();
        Assert.assertEquals(-1L, ndv);

        float avgSize = defaultColumnStats.getAvgSize();
        Assert.assertEquals(-1.0f, avgSize, 0.0001);

        long maxSize = defaultColumnStats.getMaxSize();
        Assert.assertEquals(-1L, maxSize);

        LiteralExpr maxValue = defaultColumnStats.getMaxValue();
        Assert.assertEquals(new NullLiteral(), maxValue);

        LiteralExpr minValue = defaultColumnStats.getMinValue();
        Assert.assertEquals(new NullLiteral(), minValue);
    }

    @Test
    public void testAggColumnStats() throws Exception {
        // Setup
        ColumnStats columnStats = ColumnStats.getDefaultColumnStats();
        Type minValueType = Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.STRING));
        Type maxValueType = Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.STRING));
        ColumnStats other = new ColumnStats(1L, 4.0f, 5L, 10L,
                LiteralExpr.create("sMinValue", minValueType),
                LiteralExpr.create("sMaxValue", maxValueType));

        // Run the test
        ColumnStats aggColumnStats = ColumnStats.aggColumnStats(columnStats, other);

        // Verify the results
        long ndv = aggColumnStats.getNdv();
        // 0(default) + 1
        Assert.assertEquals(1L, ndv);

        float avgSize = aggColumnStats.getAvgSize();
        // (0.0f + 4.0f) / 2
        Assert.assertEquals(4.0f, avgSize, 0.0001);

        long maxSize = aggColumnStats.getMaxSize();
        Assert.assertEquals(5L, maxSize);

        long numNulls = aggColumnStats.getNumNulls();
        Assert.assertEquals(10L, numNulls);

        String minValue = aggColumnStats.getMinValue().getStringValue();
        // null VS sMinValue
        Assert.assertEquals("NULL", minValue);

        String maxValue = aggColumnStats.getMaxValue().getStringValue();
        // null VS sMaxValue
        Assert.assertEquals("sMaxValue", maxValue);
    }
}
