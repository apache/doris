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

public class TableStatsTest {
    private TableStats tableStatsUnderTest;

    @Before
    public void setUp() throws Exception {
        tableStatsUnderTest = new TableStats();
    }

    @Test
    public void testUpdateTableStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        // Run the test
        tableStatsUnderTest.updateTableStats(statsTypeToValue);

        // Verify the results
        double rowCount = tableStatsUnderTest.getRowCount();
        Assert.assertEquals(1000, rowCount, 0.01);

        long dataSize = tableStatsUnderTest.getDataSize();
        Assert.assertEquals(10240, dataSize);
    }

    @Test
    public void testUpdateTableStats_ThrowsAnalysisException() {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.AVG_SIZE, "8");
        statsTypeToValue.put(StatsType.ROW_COUNT, "abc");

        // Run the test
        Assert.assertThrows(AnalysisException.class,
                () -> tableStatsUnderTest.updateTableStats(statsTypeToValue));
    }

    @Test
    public void testUpdatePartitionStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        // Run the test
        tableStatsUnderTest.updatePartitionStats("partitionName", statsTypeToValue);
        PartitionStats partitionStats = tableStatsUnderTest.getNameToPartitionStats().get("partitionName");

        // Verify the results
        long rowCount = partitionStats.getRowCount();
        Assert.assertEquals(1000, rowCount);

        long dataSize = partitionStats.getDataSize();
        Assert.assertEquals(10240, dataSize);
    }

    @Test
    public void testUpdatePartitionStats_ThrowsAnalysisException() {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "abc");

        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> tableStatsUnderTest
                .updatePartitionStats("partitionName", statsTypeToValue));
    }

    @Test
    public void testUpdateColumnStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NDV, "1");
        statsTypeToValue.put(StatsType.AVG_SIZE, "8");
        statsTypeToValue.put(StatsType.MAX_SIZE, "8");
        statsTypeToValue.put(StatsType.NUM_NULLS, "2");
        statsTypeToValue.put(StatsType.MIN_VALUE, "0");
        statsTypeToValue.put(StatsType.MAX_VALUE, "1000");

        // Run the test
        tableStatsUnderTest.updateColumnStats("columnName", columnType, statsTypeToValue);
        ColumnStat columnStats = tableStatsUnderTest.getColumnStats("columnName");

        // Verify the results
        double ndv = columnStats.getNdv();
        Assert.assertEquals(1L, ndv, 0.01);

        double avgSize = columnStats.getAvgSizeByte();
        Assert.assertEquals(8.0f, avgSize, 0.0001);

        double maxSize = columnStats.getMaxSizeByte();
        Assert.assertEquals(8L, maxSize, 0.01);

        double maxValue = columnStats.getMaxValue();
        Assert.assertEquals(1000, maxValue, 0.01);

        double minValue = columnStats.getMinValue();
        Assert.assertEquals(0L, minValue, 0.01);

        double numNulls = columnStats.getNumNulls();
        Assert.assertEquals(2, numNulls, 0.01);
    }

    @Test
    public void testUpdateColumnStats_ThrowsAnalysisException() {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.INVALID_TYPE);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.AVG_SIZE, "abc");
        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> tableStatsUnderTest
                .updateColumnStats("columnName", columnType, statsTypeToValue));
    }

    @Test
    public void testGetShowInfo() throws AnalysisException {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        tableStatsUnderTest.updateTableStats(statsTypeToValue);
        String[] expectedInfo = {"1000.0", "10240"};

        // Run the test
        List<String> showInfo = tableStatsUnderTest.getShowInfo();
        String[] result = showInfo.toArray(new String[0]);

        // Verify the results
        Assert.assertArrayEquals(expectedInfo, result);
    }

    @Test
    public void testGetShowInfoWithPartitionName() throws AnalysisException {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        tableStatsUnderTest.updatePartitionStats("partitionName", statsTypeToValue);
        String[] expectedInfo = {"1000", "10240"};

        // Run the test
        List<String> showInfo = tableStatsUnderTest.getShowInfo("partitionName");
        String[] result = showInfo.toArray(new String[0]);

        // Verify the results
        Assert.assertArrayEquals(expectedInfo, result);
    }
}
