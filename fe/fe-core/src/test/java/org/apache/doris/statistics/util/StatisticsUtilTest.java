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

package org.apache.doris.statistics.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

class StatisticsUtilTest {
    @Test
    void testConvertToDouble() {
        try {
            //test DATE
            double date1 = StatisticsUtil.convertToDouble(Type.DATE, "1990-01-01");
            double date2 = StatisticsUtil.convertToDouble(Type.DATE, "1990-01-02");
            double date3 = StatisticsUtil.convertToDouble(Type.DATE, "1990-01-03");
            Assertions.assertTrue(date2 > date1);
            Assertions.assertTrue(date3 > date2);
            //test DATEV2
            date1 = StatisticsUtil.convertToDouble(Type.DATEV2, "1990-01-01");
            date2 = StatisticsUtil.convertToDouble(Type.DATEV2, "1990-01-02");
            date3 = StatisticsUtil.convertToDouble(Type.DATEV2, "1990-01-03");
            Assertions.assertTrue(date2 > date1);
            Assertions.assertTrue(date3 > date2);

            //test CHAR
            double str1 = StatisticsUtil.convertToDouble(Type.CHAR, "aaa");
            double str2 = StatisticsUtil.convertToDouble(Type.CHAR, "aab");
            double str3 = StatisticsUtil.convertToDouble(Type.CHAR, "abb");
            Assertions.assertTrue(str1 < str2);
            Assertions.assertTrue(str2 < str3);
            double str4 = StatisticsUtil.convertToDouble(Type.CHAR, "abbccdde");
            double str5 = StatisticsUtil.convertToDouble(Type.CHAR, "abbccddee");
            Assertions.assertTrue(str4 > str3);
            //we only count first 8 char, tailing chars are ignored
            Assertions.assertEquals(str4, str5);
            //test VARCHAR
            str1 = StatisticsUtil.convertToDouble(Type.VARCHAR, "aaa");
            str2 = StatisticsUtil.convertToDouble(Type.VARCHAR, "aab");
            str3 = StatisticsUtil.convertToDouble(Type.VARCHAR, "abb");
            Assertions.assertTrue(str1 < str2);
            Assertions.assertTrue(str2 < str3);
            str4 = StatisticsUtil.convertToDouble(Type.VARCHAR, "abbccdde");
            str5 = StatisticsUtil.convertToDouble(Type.VARCHAR, "abbccddee");
            Assertions.assertTrue(str4 > str3);
            //we only count first 8 char, tailing chars are ignored
            Assertions.assertEquals(str4, str5);

        } catch (AnalysisException e) {
            Assertions.fail();
        }
    }

    @Test
    void testInAnalyzeTime1() {
        new MockUp<StatisticsUtil>() {

            @Mock
            protected SessionVariable findConfigFromGlobalSessionVar(String varName) throws Exception {
                SessionVariable sessionVariable = new SessionVariable();
                sessionVariable.autoAnalyzeStartTime = "00:00:00";
                sessionVariable.autoAnalyzeEndTime = "02:00:00";
                return sessionVariable;
            }
        };
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String now = "01:00:00";
        Assertions.assertTrue(StatisticsUtil.inAnalyzeTime(LocalTime.parse(now, timeFormatter)));
        now = "13:00:00";
        Assertions.assertFalse(StatisticsUtil.inAnalyzeTime(LocalTime.parse(now, timeFormatter)));
    }

    @Test
    void testInAnalyzeTime2() {
        new MockUp<StatisticsUtil>() {

            @Mock
            protected SessionVariable findConfigFromGlobalSessionVar(String varName) throws Exception {
                SessionVariable sessionVariable = new SessionVariable();
                sessionVariable.autoAnalyzeStartTime = "00:00:00";
                sessionVariable.autoAnalyzeEndTime = "23:00:00";
                return sessionVariable;
            }
        };
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String now = "15:00:00";
        Assertions.assertTrue(StatisticsUtil.inAnalyzeTime(LocalTime.parse(now, timeFormatter)));
        now = "23:30:00";
        Assertions.assertFalse(StatisticsUtil.inAnalyzeTime(LocalTime.parse(now, timeFormatter)));
    }


    @Test
    void testEncodeValue() throws Exception {
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(null, 0));

        ResultRow row = new ResultRow(null);
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(row, 0));

        ArrayList<String> values = Lists.newArrayList();
        values.add("a");
        row = new ResultRow(values);
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(row, 1));

        values = Lists.newArrayList();
        values.add(null);
        row = new ResultRow(values);
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(row, 0));

        values.add("a");
        row = new ResultRow(values);
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(row, 0));
        Assertions.assertEquals(Base64.getEncoder()
                .encodeToString("a".getBytes(StandardCharsets.UTF_8)), StatisticsUtil.encodeValue(row, 1));
        Assertions.assertEquals("NULL", StatisticsUtil.encodeValue(row, 2));
    }

    @Test
    void testEscape() {
        // \'"
        String origin = "\\'\"";
        // \\''""
        Assertions.assertEquals("\\\\''\"", StatisticsUtil.escapeSQL(origin));
    }

    @Test
    void testNeedAnalyzeColumn() {
        Column column = new Column("testColumn", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column);
        OlapTable table = new OlapTable(200, "testTable", schema, null, null, null);
        // Test table stats meta is null.
        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return null;
            }
        };
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test user injected flag is set.
        TableStatsMeta tableMeta = new TableStatsMeta();
        tableMeta.userInjected = true;
        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return tableMeta;
            }
        };
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test column meta is null.
        tableMeta.userInjected = false;
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(0, null, null, null, 0, 0, 0, null);
            }
        };

        // Test not supported external table type.
        ExternalTable externalTable = new JdbcExternalTable(1, "jdbctable", "jdbcdb", null);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(externalTable, Pair.of("index", column.getName())));

        // Test hms external table not hive type.
        new MockUp<HMSExternalTable>() {
            @Mock
            public DLAType getDlaType() {
                return DLAType.ICEBERG;
            }
        };
        ExternalTable hmsExternalTable = new HMSExternalTable(1, "hmsTable", "hmsDb", null);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(hmsExternalTable, Pair.of("index", column.getName())));

        // Test partition first load.
        new MockUp<OlapTable>() {
            @Mock
            public boolean isPartitionColumn(String columnName) {
                return true;
            }
        };
        tableMeta.partitionChanged.set(true);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test empty table to non-empty table.
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 100;
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test non-empty table to empty table.
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 0;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(0, null, null, null, 0, 100, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test table still empty.
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(0, null, null, null, 0, 0, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test row count changed more than threshold.
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 1000;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(0, null, null, null, 0, 500, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test update rows changed more than threshold.
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 120;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(0, null, null, null, 0, 100, 80, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        tableMeta.updatedRows.set(200);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test update rows changed less than threshold
        tableMeta.partitionChanged.set(false);
        tableMeta.updatedRows.set(100);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

    }
}
