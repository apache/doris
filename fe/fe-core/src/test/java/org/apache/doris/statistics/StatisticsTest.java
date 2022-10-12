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
import java.util.Map;

public class StatisticsTest {
    private Statistics statisticsUnderTest;

    @Before
    public void setUp() throws Exception {
        statisticsUnderTest = new Statistics();
    }

    @Test
    public void testUpdateTableStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");

        // Run the test
        statisticsUnderTest.updateTableStats(0L, statsTypeToValue);
        long rowCount = (long) statisticsUnderTest.getTableStats(0L).getRowCount();

        // Verify the results
        Assert.assertEquals(1000L, rowCount);
    }

    @Test
    public void testUpdateTableStats_ThrowsAnalysisException() {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "-100");

        // Run the test
        Assert.assertThrows(AnalysisException.class,
                () -> statisticsUnderTest.updateTableStats(0L, statsTypeToValue));
    }

    @Test
    public void testUpdatePartitionStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");

        // Run the test
        statisticsUnderTest.updatePartitionStats(0L, "partitionName", statsTypeToValue);
        Map<String, PartitionStats> partitionStats = statisticsUnderTest
                .getPartitionStats(0L, "partitionName");
        long rowCount = partitionStats.get("partitionName").getRowCount();

        // Verify the results
        Assert.assertEquals(1000L, rowCount);
    }

    @Test
    public void testUpdatePartitionStats_ThrowsAnalysisException() {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "-100");

        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> statisticsUnderTest
                        .updatePartitionStats(0L, "partitionName", statsTypeToValue));
    }

    @Test
    public void testUpdateTableColumnStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.STRING);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NUM_NULLS, "1000");

        // Run the test
        statisticsUnderTest.updateColumnStats(0L, "columnName", columnType, statsTypeToValue);
        Map<String, ColumnStat> columnStats = statisticsUnderTest.getColumnStats(0L);
        long numNulls = (long) columnStats.get("columnName").getNumNulls();

        // Verify the results
        Assert.assertEquals(1000L, numNulls);
    }

    @Test
    public void testUpdateTableColumnStats_ThrowsAnalysisException() {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.MAX_VALUE, "ABC");

        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> statisticsUnderTest
                .updateColumnStats(0L, "columnName", columnType, statsTypeToValue));
    }

    @Test
    public void testUpdatePartitionColumnStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.STRING);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NUM_NULLS, "1000");

        // Run the test
        statisticsUnderTest.updateColumnStats(0L, "partitionName",
                "columnName", columnType, statsTypeToValue);
        Map<String, ColumnStat> columnStats = statisticsUnderTest
                .getColumnStats(0L, "partitionName");
        long numNulls = (long) columnStats.get("columnName").getNumNulls();

        // Verify the results
        Assert.assertEquals(1000L, numNulls);
    }

    @Test
    public void testUpdatePartitionColumnStats_ThrowsAnalysisException() {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.BIGINT);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "ABC");

        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> statisticsUnderTest.updateColumnStats(
                0L, "partitionName", "columnName", columnType, statsTypeToValue));
    }

    @Test
    public void testGetTableStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statisticsUnderTest.updateTableStats(0L, statsTypeToValue);

        // Run the test
        TableStats result = statisticsUnderTest.getTableStats(0L);

        // Verify the results
        double rowCount = result.getRowCount();
        Assert.assertEquals(1000, rowCount, 0.1);
    }

    @Test
    public void testGetTableStats_ThrowsAnalysisException() {
        // Verify the results
        Assert.assertThrows(AnalysisException.class,
                () -> statisticsUnderTest.getTableStats(0L));
    }

    @Test
    public void testGetPartitionStats() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statisticsUnderTest.updatePartitionStats(0L, "partitionName", statsTypeToValue);

        // Run the test
        Map<String, PartitionStats> result = statisticsUnderTest.getPartitionStats(0L);

        // Verify the results
        PartitionStats partitionStats = result.get("partitionName");
        long rowCount = partitionStats.getRowCount();
        Assert.assertEquals(1000, rowCount);
    }

    @Test
    public void testGetPartitionStats1_ThrowsAnalysisException() {
        // Verify the results
        Assert.assertThrows(AnalysisException.class,
                () -> statisticsUnderTest.getPartitionStats(0L));
    }

    @Test
    public void testGetPartitionStatsWithName() throws Exception {
        // Setup
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statisticsUnderTest.updatePartitionStats(0L, "partitionName", statsTypeToValue);

        // Run the test
        Map<String, PartitionStats> result = statisticsUnderTest
                .getPartitionStats(0L, "partitionName");

        // Verify the results
        PartitionStats partitionStats = result.get("partitionName");
        long rowCount = partitionStats.getRowCount();
        Assert.assertEquals(1000, rowCount);
    }

    @Test
    public void testGetPartitionStatsWithName_ThrowsAnalysisException() {
        // Run the test
        Assert.assertThrows(AnalysisException.class, () -> statisticsUnderTest
                .getPartitionStats(0L, "partitionName"));
    }

    @Test
    public void testGetTableColumnStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.STRING);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NUM_NULLS, "1000");
        statisticsUnderTest.updateColumnStats(0L, "columnName", columnType, statsTypeToValue);

        // Run the test
        Map<String, ColumnStat> result = statisticsUnderTest.getColumnStats(0L);

        // Verify the results
        ColumnStat columnStats = result.get("columnName");
        double numNulls = columnStats.getNumNulls();
        Assert.assertEquals(1000, numNulls, 0.1);
    }

    @Test
    public void testGetTableColumnStats_ThrowsAnalysisException() {
        // Verify the results
        Assert.assertThrows(AnalysisException.class,
                () -> statisticsUnderTest.getColumnStats(0L));
    }

    @Test
    public void testGetPartitionColumnStats() throws Exception {
        // Setup
        Type columnType = Type.fromPrimitiveType(PrimitiveType.STRING);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.NUM_NULLS, "1000");
        statisticsUnderTest.updateColumnStats(0L, "partitionName",
                "columnName", columnType, statsTypeToValue);

        // Run the test
        Map<String, ColumnStat> result = statisticsUnderTest
                .getColumnStats(0L, "partitionName");

        // Verify the results
        ColumnStat columnStats = result.get("columnName");
        double numNulls = columnStats.getNumNulls();
        Assert.assertEquals(1000, numNulls, 0.1);
    }

    @Test
    public void testGetPartitionColumnStats_ThrowsAnalysisException() {
        // Verify the results
        Assert.assertThrows(AnalysisException.class, () -> statisticsUnderTest
                .getColumnStats(0L, "partitionName"));
    }
}
