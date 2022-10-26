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

import org.apache.doris.analysis.DropTableStatsStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;
import org.apache.doris.statistics.StatsCategory.Category;
import org.apache.doris.statistics.StatsGranularity.Granularity;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class StatisticsManagerTest {
    private StatisticsManager statisticsManagerUnderTest;

    @Before
    public void setUp() throws Exception {
        Column col1 = new Column("c1", PrimitiveType.STRING);
        Column col2 = new Column("c2", PrimitiveType.INT);
        OlapTable tbl1 = new OlapTable(0L, "tbl1", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        OlapTable tbl2 = new OlapTable(1L, "tbl2", Arrays.asList(col1, col2), KeysType.DUP_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(0L, "db");
        database.createTable(tbl1);
        database.createTable(tbl2);

        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        statisticsManagerUnderTest = new StatisticsManager();
    }

    @Test
    public void testUpdateStatistics() throws Exception {
        // Setup
        TaskResult taskResult = new TaskResult();
        taskResult.setDbId(0L);
        taskResult.setTableId(0L);
        taskResult.setCategory(Category.TABLE);
        taskResult.setGranularity(Granularity.TABLE);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");
        taskResult.setStatsTypeToValue(statsTypeToValue);

        List<StatisticsTaskResult> statsTaskResults = Collections.singletonList(
                new StatisticsTaskResult(Collections.singletonList(taskResult)));

        // Run the test
        statisticsManagerUnderTest.updateStatistics(statsTaskResults);
        Statistics statistics = statisticsManagerUnderTest.getStatistics();
        TableStats tableStats = statistics.getTableStats(0L);

        // Verify the results
        double rowCount = tableStats.getRowCount();
        Assert.assertEquals(1000L, rowCount, 0.1);

        long dataSize = tableStats.getDataSize();
        Assert.assertEquals(10240L, dataSize);
    }

    @Test
    public void testUpdateStatistics_ThrowsAnalysisException() {
        // Setup
        TaskResult taskResult = new TaskResult();
        taskResult.setDbId(0L);
        taskResult.setTableId(1L);
        taskResult.setPartitionName("partitionName");
        taskResult.setColumnName("columnName");
        taskResult.setCategory(Category.TABLE);
        taskResult.setGranularity(Granularity.TABLE);
        taskResult.setStatsTypeToValue(new HashMap<>());
        List<StatisticsTaskResult> statsTaskResults = Collections.singletonList(
                new StatisticsTaskResult(Collections.singletonList(taskResult)));

        // Run the test
        Assert.assertThrows(AnalysisException.class,
                () -> statisticsManagerUnderTest.updateStatistics(statsTaskResults));
    }

    @Test
    public void testDropStats(@Mocked DropTableStatsStmt stmt) throws AnalysisException {
        TaskResult taskResult = new TaskResult();
        taskResult.setDbId(0L);
        taskResult.setTableId(0L);
        taskResult.setCategory(Category.TABLE);
        taskResult.setGranularity(Granularity.TABLE);
        Map<StatsType, String> statsTypeToValue = new HashMap<>();
        statsTypeToValue.put(StatsType.ROW_COUNT, "1000");
        statsTypeToValue.put(StatsType.DATA_SIZE, "10240");
        taskResult.setStatsTypeToValue(statsTypeToValue);

        List<StatisticsTaskResult> statsTaskResults = Collections.singletonList(
                new StatisticsTaskResult(Collections.singletonList(taskResult)));
        statisticsManagerUnderTest.updateStatistics(statsTaskResults);

        Map<Long, Set<String>> tblIdToPartition = Maps.newHashMap();
        tblIdToPartition.put(0L, null);

        new Expectations() {
            {
                stmt.getTblIdToPartition();
                this.minTimes = 0;
                this.result = tblIdToPartition;
            }
        };

        // Run the test
        statisticsManagerUnderTest.dropStats(stmt);

        // Verify the results
        Statistics statistics = statisticsManagerUnderTest.getStatistics();
        TableStats statsOrDefault = statistics.getTableStatsOrDefault(0L);

        double rowCount = statsOrDefault.getRowCount();
        Assert.assertEquals(-1.0f, rowCount, 0.0001);

        double dataSize = statsOrDefault.getDataSize();
        Assert.assertEquals(-1.0f, dataSize, 0.0001);
    }
}
