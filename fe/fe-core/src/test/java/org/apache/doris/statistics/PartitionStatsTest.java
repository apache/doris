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

public class PartitionStatsTest {
    private PartitionStats partitionStatsUnderTest;

    @Before
    public void setUp() throws Exception {
        partitionStatsUnderTest = new PartitionStats();
    }

    @Test
    public void testUpdatePartitionStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        // Run the test
        partitionStatsUnderTest.updatePartitionStats(statsTypeToValue);

        // Verify the results
        long rowCount = partitionStatsUnderTest.getRowCount();
        Assert.assertEquals(1000, rowCount);

        long dataSize = partitionStatsUnderTest.getDataSize();
        Assert.assertEquals(10240, dataSize);
    }

    @Test
    public void testUpdatePartitionStats_ThrowsAnalysisException() {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.AVG_SIZE, "8");
        statsTypeToValue.put(StatsType.ROW_COUNT, "abc");

        // Run the test
        Assert.assertThrows(AnalysisException.class,
                () -> partitionStatsUnderTest.updatePartitionStats(statsTypeToValue));
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
        partitionStatsUnderTest.updateColumnStats("columnName", columnType, statsTypeToValue);
        ColumnStat columnStats = partitionStatsUnderTest.getColumnStats("columnName");

        // Verify the results
        double ndv = columnStats.getNdv();
        Assert.assertEquals(1, ndv, 0.1);

        double avgSize = columnStats.getAvgSizeByte();
        Assert.assertEquals(8.0f, avgSize, 0.0001);

        double maxSize = columnStats.getMaxSizeByte();
        Assert.assertEquals(8, maxSize, 0.1);

        double maxValue = columnStats.getMaxValue();
        Assert.assertEquals(1000, maxValue, 0.1);

        double minValue = columnStats.getMinValue();
        Assert.assertEquals(0, minValue, 0.1);

        double numNulls = columnStats.getNumNulls();
        Assert.assertEquals(2, numNulls, 0.1);
    }

    @Test
    public void testUpdateColumnStats_ThrowsAnalysisException() {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.AVG_SIZE, "abc");

        // Run the test
        Assert.assertThrows(
                AnalysisException.class, () -> partitionStatsUnderTest
                        .updateColumnStats("columnName", columnType, statsTypeToValue));
    }

    @Test
    public void testGetShowInfo() throws AnalysisException {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");

        partitionStatsUnderTest.updatePartitionStats(statsTypeToValue);
        String[] expectedInfo = {"1000", "10240"};

        // Run the test
        List<String> showInfo = partitionStatsUnderTest.getShowInfo();
        String[] result = showInfo.toArray(new String[0]);

        // Run the test
        Assert.assertArrayEquals(expectedInfo, result);
    }
}
