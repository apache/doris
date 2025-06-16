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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.thrift.TStorageType;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    public void testTableNotAnalyzedForTooLong() throws InterruptedException {
        TableStatsMeta tableMeta = new TableStatsMeta();
        OlapTable olapTable = new OlapTable();
        ExternalTable externalTable = new ExternalTable();

        // Test table or stats is null
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(null, tableMeta));
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, null));

        // Test user injected
        tableMeta.userInjected = true;
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test External table
        tableMeta.userInjected = false;
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(externalTable, tableMeta));

        // Test config is 0
        Config.auto_analyze_interval_seconds = 0;
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test time not long enough
        Config.auto_analyze_interval_seconds = 86400;
        tableMeta.lastAnalyzeTime = System.currentTimeMillis();
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test time long enough and update rows > 0
        Config.auto_analyze_interval_seconds = 1;
        tableMeta.lastAnalyzeTime = System.currentTimeMillis();
        Thread.sleep(2000);
        tableMeta.updatedRows.set(10);
        Assertions.assertTrue(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test row count is not equal with last analyze
        tableMeta.updatedRows.set(0);
        tableMeta.rowCount = 10;
        new MockUp<Table>() {
            @Mock
            public long getRowCount() {
                return 100;
            }
        };
        Assertions.assertTrue(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test visible version changed
        new MockUp<OlapTable>() {
            @Mock
            public long getVisibleVersion() {
                return 100;
            }
        };
        new MockUp<Table>() {
            @Mock
            public long getRowCount() {
                return 10;
            }
        };
        ConcurrentMap<Pair<String, String>, ColStatsMeta> colToColStatsMeta = new ConcurrentHashMap<>();
        ColStatsMeta col1Meta = new ColStatsMeta(0, AnalysisMethod.SAMPLE, AnalysisType.FUNDAMENTALS, JobType.SYSTEM, 0, 100);
        ColStatsMeta col2Meta = new ColStatsMeta(0, AnalysisMethod.SAMPLE, AnalysisType.FUNDAMENTALS, JobType.SYSTEM, 0, 101);
        colToColStatsMeta.put(Pair.of("index1", "col1"), col1Meta);
        colToColStatsMeta.put(Pair.of("index2", "col2"), col2Meta);
        tableMeta.setColToColStatsMeta(colToColStatsMeta);
        Assertions.assertTrue(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));

        // Test visible version unchanged.
        col2Meta = new ColStatsMeta(0, AnalysisMethod.SAMPLE, AnalysisType.FUNDAMENTALS, JobType.SYSTEM, 0, 100);
        colToColStatsMeta.put(Pair.of("index2", "col2"), col2Meta);
        tableMeta.setColToColStatsMeta(colToColStatsMeta);
        Assertions.assertFalse(StatisticsUtil.tableNotAnalyzedForTooLong(olapTable, tableMeta));
    }

    @Test
    void testCanCollectColumn() {
        Column column = new Column("testColumn", Type.INT, true, null, null, "");
        List<Column> schema = new ArrayList<>();
        schema.add(column);
        OlapTable table = new OlapTable(200, "testTable", schema, KeysType.AGG_KEYS, null, null);

        // Test full analyze always return true;
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(column, table, false, 1));

        // Test null table return true;
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(column, null, true, 1));

        // Test external table always return true;
        HMSExternalCatalog externalCatalog = new HMSExternalCatalog();
        HMSExternalDatabase externalDatabase = new HMSExternalDatabase(externalCatalog, 1L, "dbName", "dbName");
        HMSExternalTable hmsTable = new HMSExternalTable(1, "name", "name", externalCatalog, externalDatabase);
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(column, hmsTable, true, 1));

        // Test agg key return true;
        MaterializedIndexMeta meta = new MaterializedIndexMeta(1L, schema, 1, 1, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        new MockUp<OlapTable>() {
            @Mock
            public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
                return meta;
            }
        };
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(column, table, true, 1));

        // Test agg value return false
        column = new Column("testColumn", Type.INT, false, null, null, "");
        Assertions.assertFalse(StatisticsUtil.canCollectColumn(column, table, true, 1));

        // Test unique mor value column return false
        MaterializedIndexMeta meta1 = new MaterializedIndexMeta(1L, schema, 1, 1, (short) 1, TStorageType.COLUMN, KeysType.UNIQUE_KEYS, null);
        new MockUp<OlapTable>() {
            @Mock
            public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
                return meta1;
            }

            @Mock
            public boolean isUniqKeyMergeOnWrite() {
                return false;
            }
        };
        Assertions.assertFalse(StatisticsUtil.canCollectColumn(column, table, true, 1));

        // Test unique mor key column return true
        column = new Column("testColumn", Type.INT, true, null, null, "");
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(column, table, true, 1));

    }
}
