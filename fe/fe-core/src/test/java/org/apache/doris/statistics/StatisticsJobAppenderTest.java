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

        Map<TableName, Set<Pair<String, String>>> testMap = new HashMap<>();
        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendToLowJobs(testMap);
        Assertions.assertEquals(100, testMap.size());
        testMap.clear();
        appender.appendToLowJobs(testMap);
        Assertions.assertEquals(40, testMap.size());

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

        testMap.clear();
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        Assertions.assertEquals(StatisticsJobAppender.JOB_MAP_SIZE, testMap.size());
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
        Map<TableName, Set<Pair<String, String>>> testMap = new HashMap<>();
        StatisticsJobAppender appender = new StatisticsJobAppender();
        appender.appendToLowJobs(testMap);
        Assertions.assertEquals(0, testMap.size());
        appender.setLastRoundFinishTime(0);
        appender.appendToLowJobs(testMap);
        Assertions.assertEquals(1, testMap.size());
    }
}
