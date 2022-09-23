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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.InternalQuery;
import org.apache.doris.statistics.util.InternalQueryResult;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class SQLStatisticsTaskTest {
    private SQLStatisticsTask sqlStatisticsTaskUnderTest;

    @Before
    public void setUp() throws Exception {
        StatsCategory statsCategory = new StatsCategory();
        StatsGranularity statsGranularity = new StatsGranularity();
        List<StatsType> statsTypes = Collections.singletonList(StatsType.ROW_COUNT);
        sqlStatisticsTaskUnderTest = new SQLStatisticsTask(0L,
                Collections.singletonList(new StatisticsDesc(statsCategory, statsGranularity, statsTypes)));

        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        Column column = new Column("columnName", PrimitiveType.STRING);
        OlapTable tableName = new OlapTable(0L, "tableName",
                Collections.singletonList(column), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(0L, "db");
        database.createTable(tableName);

        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        List<String> columns = Collections.singletonList("row_count");
        List<PrimitiveType> types = Arrays.asList(PrimitiveType.STRING,
                PrimitiveType.INT, PrimitiveType.FLOAT,
                PrimitiveType.DOUBLE, PrimitiveType.BIGINT);
        InternalQueryResult queryResult = new InternalQueryResult();
        InternalQueryResult.ResultRow resultRow =
                new InternalQueryResult.ResultRow(columns, types, Collections.singletonList("1000"));
        queryResult.getResultRows().add(resultRow);

        new MockUp<InternalQuery>(InternalQuery.class) {
            @Mock
            public InternalQueryResult query() {
                return queryResult;
            }
        };
    }

    @Test
    public void testConstructQuery() throws Exception {
        // Setup
        String expectedSQL = "SELECT COUNT(1) AS row_count FROM tableName;";

        StatsCategory statsCategory = new StatsCategory();
        statsCategory.setCategory(StatsCategory.Category.TABLE);
        statsCategory.setDbId(0L);
        statsCategory.setTableId(0L);
        statsCategory.setPartitionName("partitionName");
        statsCategory.setColumnName("columnName");
        statsCategory.setStatsValue("statsValue");

        StatsGranularity statsGranularity = new StatsGranularity();
        statsGranularity.setGranularity(StatsGranularity.Granularity.TABLE);
        statsGranularity.setTableId(0L);
        statsGranularity.setPartitionId(0L);
        statsGranularity.setTabletId(0L);

        StatisticsDesc statsDesc = new StatisticsDesc(statsCategory, statsGranularity,
                Collections.singletonList(StatsType.ROW_COUNT));

        // Run the test
        String result = sqlStatisticsTaskUnderTest.constructQuery(statsDesc);

        // Verify the results
        Assert.assertEquals(expectedSQL, result);
    }

    @Test
    public void testConstructQuery_ThrowsDdlException() {
        // Setup
        StatsCategory statsCategory = new StatsCategory();
        statsCategory.setCategory(StatsCategory.Category.TABLE);
        statsCategory.setDbId(0L);
        statsCategory.setTableId(0L);
        statsCategory.setPartitionName("partitionName");
        statsCategory.setColumnName("columnName");
        statsCategory.setStatsValue("statsValue");

        StatsGranularity statsGranularity = new StatsGranularity();
        statsGranularity.setGranularity(StatsGranularity.Granularity.TABLE);
        statsGranularity.setTableId(0L);
        statsGranularity.setPartitionId(0L);
        statsGranularity.setTabletId(0L);

        StatisticsDesc statsDesc = new StatisticsDesc(statsCategory, statsGranularity,
                Collections.singletonList(StatsType.UNKNOWN));

        // Run the test
        Assert.assertThrows(DdlException.class,
                () -> sqlStatisticsTaskUnderTest.constructQuery(statsDesc));
    }

    @Test
    public void testExecuteQuery() throws Exception {
        // Setup
        StatsCategory statsCategory = new StatsCategory();
        statsCategory.setCategory(StatsCategory.Category.TABLE);
        statsCategory.setDbId(0L);
        statsCategory.setTableId(0L);
        statsCategory.setPartitionName("partitionName");
        statsCategory.setColumnName("columnName");
        statsCategory.setStatsValue("statsValue");

        StatsGranularity statsGranularity = new StatsGranularity();
        statsGranularity.setGranularity(StatsGranularity.Granularity.TABLE);
        statsGranularity.setTableId(0L);
        statsGranularity.setPartitionId(0L);
        statsGranularity.setTabletId(0L);

        StatisticsTaskResult.TaskResult expectedResult = new StatisticsTaskResult.TaskResult();
        expectedResult.setDbId(0L);
        expectedResult.setTableId(0L);
        expectedResult.setPartitionName("partitionName");
        expectedResult.setColumnName("columnName");
        expectedResult.setCategory(StatsCategory.Category.TABLE);
        expectedResult.setGranularity(StatsGranularity.Granularity.TABLE);
        HashMap<StatsType, String> hashMap = new HashMap<>();
        hashMap.put(StatsType.ROW_COUNT, "1000");
        expectedResult.setStatsTypeToValue(hashMap);

        StatisticsDesc statsDesc = new StatisticsDesc(statsCategory, statsGranularity,
                Collections.singletonList(StatsType.ROW_COUNT));

        // Run the test
        StatisticsTaskResult.TaskResult result = sqlStatisticsTaskUnderTest.executeQuery(statsDesc);

        // Verify the results
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void testExecuteQuery_ThrowsException() {
        // Setup
        StatsCategory statsCategory = new StatsCategory();
        statsCategory.setCategory(StatsCategory.Category.TABLE);
        statsCategory.setDbId(0L);
        statsCategory.setTableId(0L);
        statsCategory.setPartitionName("partitionName");
        statsCategory.setColumnName("columnName");
        statsCategory.setStatsValue("statsValue");

        StatsGranularity statsGranularity = new StatsGranularity();
        statsGranularity.setGranularity(StatsGranularity.Granularity.TABLE);
        statsGranularity.setTableId(0L);
        statsGranularity.setPartitionId(0L);
        statsGranularity.setTabletId(0L);

        StatisticsDesc statsDesc = new StatisticsDesc(statsCategory, statsGranularity,
                Arrays.asList(StatsType.NDV, StatsType.MAX_VALUE, StatsType.MIN_VALUE));

        // Run the test
        Assert.assertThrows(Exception.class,
                () -> sqlStatisticsTaskUnderTest.executeQuery(statsDesc));
    }
}
