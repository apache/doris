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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

public class StatisticsJobAppenderTest {

    @Test
    public void testAppendQueryColumnToHighAndMidJobMap() throws DdlException {
        InternalCatalog testCatalog = new InternalCatalog();
        Database db = new Database(100, "testDb");
        testCatalog.unprotectCreateDb(db);
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = new OlapTable(200, "testTable", schema, null, null, null);
        OlapTable table2 = new OlapTable(200, "testTable2", schema, null, null, null);
        OlapTable table3 = new OlapTable(200, "testTable3", schema, null, null, null);
        new MockUp<StatisticsUtil>() {
            int i = 0;
            Table[] tables = {table1, table2, table1, table3, table2};

            @Mock
            public boolean needAnalyzeColumn(TableIf table, Pair<String, String> column) {
                return true;
            }

            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return tables[i++];
            }
        };

        new MockUp<Table>() {
            @Mock
            public DatabaseIf getDatabase() {
                return db;
            }

            @Mock
            public Column getColumn(String name) {
                return new Column("mockCol", Type.INT);
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                String column = columns.iterator().next();
                return Collections.singleton(Pair.of("mockIndex", column));
            }
        };

        Queue<QueryColumn> testQueue = new ArrayBlockingQueue<>(100);
        Map<TableName, Set<Pair<String, String>>> testMap = new HashMap<>();
        QueryColumn high1 = new QueryColumn(10, 20, 30, "high1");
        testQueue.add(high1);

        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendColumnsToJobs(testQueue, testMap);
        Assertions.assertEquals(1, testMap.size());
        Assertions.assertEquals(1, testMap.values().size());
        Assertions.assertTrue(testMap.get(new TableName("internal", "testDb", "testTable")).contains(Pair.of("mockIndex", "high1")));

        QueryColumn high2 = new QueryColumn(10, 20, 30, "high2");
        QueryColumn high3 = new QueryColumn(10, 20, 30, "high3");
        testQueue.add(high2);
        testQueue.add(high3);
        appender.appendColumnsToJobs(testQueue, testMap);
        Assertions.assertEquals(2, testMap.size());

        Set<Pair<String, String>> table1Column = testMap.get(new TableName("internal", "testDb", "testTable"));
        Assertions.assertEquals(2, table1Column.size());
        Assertions.assertTrue(table1Column.contains(Pair.of("mockIndex", "high1")));
        Assertions.assertTrue(table1Column.contains(Pair.of("mockIndex", "high3")));

        Set<Pair<String, String>> table2Column = testMap.get(new TableName("internal", "testDb", "testTable2"));
        Assertions.assertEquals(1, table2Column.size());
        Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high2")));

        for (int i = 0; i < StatisticsJobAppender.JOB_MAP_SIZE - 2; i++) {
            testMap.put(new TableName("a", "b", UUID.randomUUID().toString()), new HashSet<>());
        }
        Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testMap.size());

        QueryColumn high4 = new QueryColumn(10, 20, 30, "high4");
        testQueue.add(high4);
        appender.appendColumnsToJobs(testQueue, testMap);
        Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testMap.size());

        QueryColumn high5 = new QueryColumn(10, 20, 30, "high5");
        testQueue.add(high5);
        appender.appendColumnsToJobs(testQueue, testMap);
        table2Column = testMap.get(new TableName("internal", "testDb", "testTable2"));
        Assertions.assertEquals(2, table2Column.size());
        Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high2")));
        Assertions.assertTrue(table2Column.contains(Pair.of("mockIndex", "high5")));
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
            OlapTable table1 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            OlapTable table2 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return testCatalog;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList();
            }

            @Mock
            public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                return Collections.singleton(Pair.of("mockIndex", "mockColumn"));
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public boolean needAnalyzeColumn(TableIf table, Pair<String, String> column) {
                return true;
            }
        };

        Map<TableName, Set<Pair<String, String>>> testLowMap = new HashMap<>();
        Map<TableName, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(100, testLowMap.size());
        testLowMap.clear();
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(40, testLowMap.size());

        for (int i = 0; i < StatisticsJobAppender.JOB_MAP_SIZE; i++) {
            Database db = new Database(id++, "testDb" + i);
            testCatalog.unprotectCreateDb(db);
            Column column1 = new Column("placeholder", PrimitiveType.INT);
            List<Column> schema = new ArrayList<>();
            schema.add(column1);
            OlapTable table1 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            OlapTable table2 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        testLowMap.clear();
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testLowMap.size());
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
            OlapTable table1 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            OlapTable table2 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return testCatalog;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList();
            }

            @Mock
            public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                return Collections.singleton(Pair.of("mockIndex", "mockColumn"));
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public boolean needAnalyzeColumn(TableIf table, Pair<String, String> column) {
                return false;
            }

            @Mock
            public boolean isLongTimeColumn(TableIf table, Pair<String, String> column) {
                return true;
            }
        };

        Map<TableName, Set<Pair<String, String>>> testLowMap = new HashMap<>();
        Map<TableName, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(100, testVeryLowMap.size());
        testVeryLowMap.clear();
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(40, testVeryLowMap.size());

        for (int i = 0; i < StatisticsJobAppender.JOB_MAP_SIZE; i++) {
            Database db = new Database(id++, "testDb" + i);
            testCatalog.unprotectCreateDb(db);
            Column column1 = new Column("placeholder", PrimitiveType.INT);
            List<Column> schema = new ArrayList<>();
            schema.add(column1);
            OlapTable table1 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            OlapTable table2 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
            db.createTableWithLock(table1, true, false);
            db.createTableWithLock(table2, true, false);
        }

        testLowMap.clear();
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(0, testLowMap.size());
        Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testVeryLowMap.size());
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
        OlapTable table1 = new OlapTable(id++, "testTable" + id + "_1", schema, null, null, null);
        db.createTableWithLock(table1, true, false);
        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return testCatalog;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(new Column("col1", Type.INT), new Column("col2", Type.INT));
            }

            @Mock
            public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                return Collections.singleton(Pair.of("1", "1"));
            }
        };

        new MockUp<StatisticsUtil>() {
            int count = 0;
            int[] thresholds = {1, 10};

            @Mock
            public int getAutoAnalyzeTableWidthThreshold() {
                return thresholds[count++];
            }
        };
        Map<TableName, Set<Pair<String, String>>> testLowMap = new HashMap<>();
        Map<TableName, Set<Pair<String, String>>> testVeryLowMap = new HashMap<>();
        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(0, testLowMap.size());
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testLowMap, testVeryLowMap);
        Assertions.assertEquals(1, testLowMap.size());
    }

    @Test
    public void testDoAppend() {
        Map<TableName, Set<Pair<String, String>>> jobMap = Maps.newHashMap();
        Set<Pair<String, String>> columnIndexPairs1 = Sets.newHashSet();
        Set<Pair<String, String>> columnIndexPairs2 = Sets.newHashSet();
        TableName tableName1 = new TableName("catalog1", "db1", "table1");
        TableName tableName2 = new TableName("catalog2", "db2", "table2");
        Pair<String, String> pair1 = Pair.of("index1", "col1");
        columnIndexPairs1.add(pair1);

        StatisticsJobAppender appender = new StatisticsJobAppender();
        Assertions.assertTrue(appender.doAppend(jobMap, columnIndexPairs1, tableName1));
        Assertions.assertEquals(1, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableName1));
        Assertions.assertEquals(1, jobMap.get(tableName1).size());
        Assertions.assertTrue(jobMap.get(tableName1).contains(pair1));

        Pair<String, String> pair2 = Pair.of("index2", "col2");
        columnIndexPairs1.add(pair2);
        Assertions.assertTrue(appender.doAppend(jobMap, columnIndexPairs1, tableName1));
        Assertions.assertEquals(1, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableName1));
        Assertions.assertEquals(2, jobMap.get(tableName1).size());
        Assertions.assertTrue(jobMap.get(tableName1).contains(pair1));
        Assertions.assertTrue(jobMap.get(tableName1).contains(pair2));

        Pair<String, String> pair3 = Pair.of("index3", "col3");
        columnIndexPairs2.add(pair3);
        Assertions.assertTrue(appender.doAppend(jobMap, columnIndexPairs2, tableName2));
        Assertions.assertEquals(2, jobMap.size());
        Assertions.assertTrue(jobMap.containsKey(tableName2));
        Assertions.assertEquals(1, jobMap.get(tableName2).size());
        Assertions.assertTrue(jobMap.get(tableName2).contains(pair3));
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
