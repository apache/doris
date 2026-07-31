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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsJobAppenderTest {

    @Test
    public void testAppendQueryColumnToHighAndMidJobMap() throws DdlException {
        InternalCatalog testCatalog = new InternalCatalog();
        Database db = new Database(100, "testDb");
        testCatalog.unprotectCreateDb(db);
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = Mockito.spy(new OlapTable(200, "testTable", schema, null, null, null));
        OlapTable table2 = Mockito.spy(new OlapTable(200, "testTable2", schema, null, null, null));
        OlapTable table3 = Mockito.spy(new OlapTable(200, "testTable3", schema, null, null, null));
        for (OlapTable t : new OlapTable[]{table1, table2, table3}) {
            Mockito.doReturn(db).when(t).getDatabase();
            Mockito.doReturn(new Column("mockCol", Type.INT)).when(t).getColumn(Mockito.anyString());
            Mockito.doAnswer(inv -> {
                Set<String> cols = inv.getArgument(0);
                String col = cols.iterator().next();
                return Collections.singleton(Pair.of("mockIndex", col));
            }).when(t).getColumnIndexPairs(Mockito.any());
        }

        try (MockedStatic<StatisticsUtil> statsUtilStatic = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            AtomicInteger idx = new AtomicInteger(0);
            OlapTable[] tables = {table1, table2, table1, table3, table2};
            statsUtilStatic.when(() -> StatisticsUtil.needAnalyzeColumn(Mockito.any(), Mockito.any()))
                    .thenReturn(true);
            statsUtilStatic.when(() -> StatisticsUtil.findTable(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
                    .thenAnswer(inv -> tables[idx.getAndIncrement()]);

            Queue<QueryColumn> testQueue = new ArrayBlockingQueue<>(100);
            Map<TableNameInfo, Set<Pair<String, String>>> testMap = new HashMap<>();
            QueryColumn high1 = new QueryColumn(10, 20, 30, "high1");
            testQueue.add(high1);

            StatisticsJobAppender appender = new StatisticsJobAppender();
            appender.appendColumnsToJobs(testQueue, testMap);
            Assertions.assertEquals(1, testMap.size());
            Assertions.assertEquals(1, testMap.values().size());
            Assertions.assertTrue(testMap.get(new TableNameInfo("internal", "testDb", "testTable")).contains(Pair.of("mockIndex", "high1")));

            QueryColumn high2 = new QueryColumn(10, 20, 30, "high2");
            QueryColumn high3 = new QueryColumn(10, 20, 30, "high3");
            testQueue.add(high2);
            testQueue.add(high3);
            appender.appendColumnsToJobs(testQueue, testMap);
            Assertions.assertEquals(2, testMap.size());

            Set<Pair<String, String>> table1Column = testMap.get(new TableNameInfo("internal", "testDb", "testTable"));
            Assertions.assertEquals(2, table1Column.size());
            Assertions.assertTrue(table1Column.contains(Pair.of("mockIndex", "high1")));
            Assertions.assertTrue(table1Column.contains(Pair.of("mockIndex", "high3")));

            Set<Pair<String, String>> table2Column = testMap.get(new TableNameInfo("internal", "testDb", "testTable2"));
            Assertions.assertEquals(1, table2Column.size());
            Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high2")));

            for (int i = 0; i < StatisticsJobAppender.JOB_MAP_SIZE - 2; i++) {
                testMap.put(new TableNameInfo("a", "b", UUID.randomUUID().toString()), new HashSet<>());
            }
            Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testMap.size());

            QueryColumn high4 = new QueryColumn(10, 20, 30, "high4");
            testQueue.add(high4);
            appender.appendColumnsToJobs(testQueue, testMap);
            Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testMap.size());

            QueryColumn high5 = new QueryColumn(10, 20, 30, "high5");
            testQueue.add(high5);
            appender.appendColumnsToJobs(testQueue, testMap);
            table2Column = testMap.get(new TableNameInfo("internal", "testDb", "testTable2"));
            Assertions.assertEquals(2, table2Column.size());
            Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high2")));
            Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high5")));
        } // close MockedStatic
    }

    @Test
    public void testAppendQueryColumnToLowJobMap() throws DdlException {
        InternalCatalog testCatalog = new InternalCatalog();
        int id = 10;
        for (int i = 0; i < 70; i++) {
            Database db = new Database(id++, "testDb" + i);
            testCatalog.unprotectCreateDb(db);
            Column column1 = new Column("placeholder", PrimitiveType.INT);
            List<Column> schema = new ArrayList<>();
            schema.add(column1);
            OlapTable table1 = Mockito.spy(new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null));
            OlapTable table2 = Mockito.spy(new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null));
            Mockito.doReturn(Lists.newArrayList()).when(table1).getBaseSchema();
            Mockito.doReturn(Collections.singleton(Pair.of("mockIndex", "mockColumn"))).when(table1).getColumnIndexPairs(Mockito.any());
            Mockito.doReturn(Lists.newArrayList()).when(table2).getBaseSchema();
            Mockito.doReturn(Collections.singleton(Pair.of("mockIndex", "mockColumn"))).when(table2).getColumnIndexPairs(Mockito.any());
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<StatisticsUtil> statsUtilStatic = Mockito.mockStatic(StatisticsUtil.class)) {
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(testCatalog);
            statsUtilStatic.when(() -> StatisticsUtil.needAnalyzeColumn(Mockito.any(), Mockito.any()))
                    .thenReturn(true);

            Map<TableNameInfo, Set<Pair<String, String>>> testLowMap = new HashMap<>();
            Map<TableNameInfo, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
            StatisticsJobAppender appender = new StatisticsJobAppender();
            appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(100, testLowMap.size());
            Assertions.assertEquals(0, testVeryLowMap.size());
            testLowMap.clear();
            appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(40, testLowMap.size());
            Assertions.assertEquals(0, testVeryLowMap.size());
            testLowMap.clear();
            // Less than 1 minutes since last iteration.
            appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            Assertions.assertEquals(0, testVeryLowMap.size());

            testLowMap.clear();
            appender.setLastRoundFinishTime(0);
            int processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(100, testLowMap.size());
            Assertions.assertEquals(0, testVeryLowMap.size());
            Assertions.assertEquals(100, processed);
            appender.setLastRoundFinishTime(0);
            processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(100, testLowMap.size());
            Assertions.assertEquals(0, testVeryLowMap.size());
            Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testLowMap.size());
            Assertions.assertEquals(0, processed);
        } // close MockedStatic
    }

    @Test
    public void testAppendQueryColumnToVeryLowJobMap() throws DdlException {
        InternalCatalog testCatalog = new InternalCatalog();
        int id = 10;
        for (int i = 0; i < 70; i++) {
            Database db = new Database(id++, "testDb" + i);
            testCatalog.unprotectCreateDb(db);
            Column column1 = new Column("placeholder", PrimitiveType.INT);
            List<Column> schema = new ArrayList<>();
            schema.add(column1);
            OlapTable table1 = Mockito.spy(new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null));
            OlapTable table2 = Mockito.spy(new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null));
            Mockito.doReturn(Lists.newArrayList()).when(table1).getBaseSchema();
            Mockito.doReturn(Collections.singleton(Pair.of("mockIndex", "mockColumn"))).when(table1).getColumnIndexPairs(Mockito.any());
            Mockito.doReturn(Lists.newArrayList()).when(table2).getBaseSchema();
            Mockito.doReturn(Collections.singleton(Pair.of("mockIndex", "mockColumn"))).when(table2).getColumnIndexPairs(Mockito.any());
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<StatisticsUtil> statsUtilStatic = Mockito.mockStatic(StatisticsUtil.class)) {
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(testCatalog);
            statsUtilStatic.when(() -> StatisticsUtil.needAnalyzeColumn(Mockito.any(), Mockito.any()))
                    .thenReturn(false);
            statsUtilStatic.when(() -> StatisticsUtil.isLongTimeColumn(Mockito.any(), Mockito.any(), Mockito.anyLong()))
                    .thenReturn(true);

            Map<TableNameInfo, Set<Pair<String, String>>> testLowMap = new HashMap<>();
            Map<TableNameInfo, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
            StatisticsJobAppender appender = new StatisticsJobAppender();
            int processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            Assertions.assertEquals(100, testVeryLowMap.size());
            Assertions.assertEquals(100, processed);
            testVeryLowMap.clear();
            processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            Assertions.assertEquals(40, testVeryLowMap.size());
            Assertions.assertEquals(40, processed);

            testLowMap.clear();
            appender.setLastRoundFinishTime(0);
            processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            Assertions.assertEquals(100, testVeryLowMap.size());
            Assertions.assertEquals(100, processed);

            appender.setLastRoundFinishTime(0);
            processed = appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            Assertions.assertEquals(100, testVeryLowMap.size());
            Assertions.assertEquals(0, processed);
        } // close MockedStatic
    }

    @Test
    public void testSkipWideTable() throws DdlException {
        InternalCatalog testCatalog = new InternalCatalog();
        int id = 10;
        Database db = new Database(id++, "testDb");
        testCatalog.unprotectCreateDb(db);
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = Mockito.spy(new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null));
        Mockito.doReturn(Lists.newArrayList(new Column("col1", Type.INT), new Column("col2", Type.INT))).when(table1).getBaseSchema();
        Mockito.doReturn(Collections.singleton(Pair.of("1", "1"))).when(table1).getColumnIndexPairs(Mockito.any());
        db.createTableWithLock(table1, true, false);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<StatisticsUtil> statsUtilStatic = Mockito.mockStatic(StatisticsUtil.class)) {
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(testCatalog);
            AtomicInteger count = new AtomicInteger(0);
            int[] thresholds = {1, 10};
            statsUtilStatic.when(StatisticsUtil::getAutoAnalyzeTableWidthThreshold)
                    .thenAnswer(inv -> thresholds[count.getAndIncrement()]);
            statsUtilStatic.when(() -> StatisticsUtil.needAnalyzeColumn(
                    Mockito.any(), Mockito.any())).thenReturn(true);
            Map<TableNameInfo, Set<Pair<String, String>>> testLowMap = new HashMap<>();
            Map<TableNameInfo, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
            StatisticsJobAppender appender = new StatisticsJobAppender();
            appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(0, testLowMap.size());
            appender.setLastRoundFinishTime(0);
            appender.appendToLowJobs(testLowMap, testVeryLowMap);
            Assertions.assertEquals(1, testLowMap.size());
        } // close MockedStatic
    }

    @Test
    public void testDoAppend() {
        Map<TableNameInfo, Set<Pair<String, String>>> jobMap = Maps.newHashMap();
        TableNameInfo tableNameInfo1 = new TableNameInfo("catalog1", "db1", "table1");
        TableNameInfo tableNameInfo2 = new TableNameInfo("catalog2", "db2", "table2");
        Pair<String, String> pair1 = Pair.of("index1", "col1");

        StatisticsJobAppender appender = new StatisticsJobAppender();
        Assertions.assertTrue(appender.doAppend(jobMap, pair1, tableNameInfo1));
        Assertions.assertEquals(1, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableNameInfo1));
        Assertions.assertEquals(1, jobMap.get(tableNameInfo1).size());
        Assertions.assertTrue(jobMap.get(tableNameInfo1).contains(pair1));

        Pair<String, String> pair2 = Pair.of("index2", "col2");
        Assertions.assertTrue(appender.doAppend(jobMap, pair2, tableNameInfo1));
        Assertions.assertEquals(1, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableNameInfo1));
        Assertions.assertEquals(2, jobMap.get(tableNameInfo1).size());
        Assertions.assertTrue(jobMap.get(tableNameInfo1).contains(pair1));
        Assertions.assertTrue(jobMap.get(tableNameInfo1).contains(pair2));

        Pair<String, String> pair3 = Pair.of("index3", "col3");
        Assertions.assertTrue(appender.doAppend(jobMap, pair3, tableNameInfo2));
        Assertions.assertEquals(2, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableNameInfo2));
        Assertions.assertEquals(1, jobMap.get(tableNameInfo2).size());
        Assertions.assertTrue(jobMap.get(tableNameInfo2).contains(pair3));
    }

    @Test
    public void testSortTables() {
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = new OlapTable(1340000000000L, "testTable", schema, null, null, null);
        OlapTable table2 = new OlapTable(3000000000L, "testTable2", schema, null, null, null);
        OlapTable table3 = new OlapTable(5000000000L, "testTable3", schema, null, null, null);
        OlapTable table4 = new OlapTable(1, "testTable4", schema, null, null, null);
        List<Table> tables = Lists.newArrayList();
        tables.add(table1);
        tables.add(table2);
        tables.add(table3);
        tables.add(table4);
        StatisticsJobAppender appender = new StatisticsJobAppender();
        List<Table> sortedTables = appender.sortTables(tables);
        Assertions.assertEquals(4, sortedTables.size());
        Assertions.assertEquals(1, sortedTables.get(0).getId());
        Assertions.assertEquals(3000000000L, sortedTables.get(1).getId());
        Assertions.assertEquals(5000000000L, sortedTables.get(2).getId());
        Assertions.assertEquals(1340000000000L, sortedTables.get(3).getId());

        sortedTables = appender.sortTables(null);
        Assertions.assertEquals(0, sortedTables.size());
    }
}
