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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergHadoopExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    void testNeedAnalyzeColumn() throws DdlException {
        Column column = new Column("testColumn", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column);
        OlapTable table = new OlapTable(200, "testTable", schema, null, null, null);
        HMSExternalCatalog externalCatalog = new HMSExternalCatalog();
        HMSExternalDatabase externalDatabase = new HMSExternalDatabase(externalCatalog, 1L, "dbName", "dbName");
        // Test olap table auto analyze disabled.
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY, "disable");
        table.setTableProperty(new TableProperty(properties));
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));
        table.setTableProperty(null);

        new MockUp<HMSExternalTable>() {
            @Mock
            protected synchronized void makeSureInitialized() {
            }
        };

        // Test auto analyze catalog disabled.
        HMSExternalTable hmsTable = new HMSExternalTable(1, "name", "name", externalCatalog, externalDatabase);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(hmsTable, Pair.of("index", column.getName())));

        // Test catalog auto analyze enabled.
        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return null;
            }
        };
        externalCatalog.getCatalogProperty().addProperty(ExternalCatalog.ENABLE_AUTO_ANALYZE, "true");
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test external table auto analyze enabled.
        externalCatalog.getCatalogProperty().addProperty(ExternalCatalog.ENABLE_AUTO_ANALYZE, "false");
        HMSExternalTable hmsTable1 = new HMSExternalTable(1, "name", "name", externalCatalog, externalDatabase);
        externalCatalog.setAutoAnalyzePolicy("dbName", "name", "enable");
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(hmsTable1, Pair.of("index", column.getName())));


        // Test table stats meta is null.
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
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 0, 0, 0, null);
            }
        };

        new MockUp<JdbcExternalTable>() {
            @Mock
            protected synchronized void makeSureInitialized() {
            }
        };
        // Test not supported external table type.
        JdbcExternalCatalog jdbcExternalCatalog = new JdbcExternalCatalog(1, "name", "resource", new HashMap<>(), "");
        JdbcExternalDatabase jdbcExternalDatabase = new JdbcExternalDatabase(jdbcExternalCatalog, 1, "jdbcdb", "jdbcdb");
        ExternalTable externalTable = new JdbcExternalTable(1, "jdbctable", "jdbctable", jdbcExternalCatalog, jdbcExternalDatabase);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(externalTable, Pair.of("index", column.getName())));

        // Test hms external table not hive type.
        new MockUp<HMSExternalTable>() {
            @Mock
            public DLAType getDlaType() {
                return DLAType.ICEBERG;
            }
        };
        ExternalTable hmsExternalTable = new HMSExternalTable(1, "hmsTable", "hmsTable", externalCatalog, externalDatabase);
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
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 100, 0, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test table still empty.
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 0, 0, 0, null);
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
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 500, 0, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test row count changed more than threshold.
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 111;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 100, 80, 0, null);
            }
        };
        tableMeta.partitionChanged.set(false);
        tableMeta.updatedRows.set(80);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test update rows changed more than threshold
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 101;
            }
        };
        tableMeta.partitionChanged.set(false);
        tableMeta.updatedRows.set(91);
        Assertions.assertTrue(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));

        // Test row count and update rows changed less than threshold
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 100;
            }
        };
        tableMeta.partitionChanged.set(false);
        tableMeta.updatedRows.set(85);
        Assertions.assertFalse(StatisticsUtil.needAnalyzeColumn(table, Pair.of("index", column.getName())));
    }

    @Test
    void testLongTimeNoAnalyze() {
        Column column = new Column("testColumn", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column);
        OlapTable table = new OlapTable(200, "testTable", schema, null, null, null);

        // Test column is null
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, null, 0));

        // Test table auto analyze is disabled.
        new MockUp<OlapTable>() {
            @Mock
            public boolean autoAnalyzeEnabled() {
                return false;
            }
        };
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));
        new MockUp<OlapTable>() {
            @Mock
            public boolean autoAnalyzeEnabled() {
                return true;
            }
        };

        // Test external table
        new MockUp<ExternalTable>() {
            @Mock
            public boolean autoAnalyzeEnabled() {
                return true;
            }
        };
        IcebergExternalDatabase icebergDatabase = new IcebergExternalDatabase(null, 1L, "", "");
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://tmp");
        IcebergExternalCatalog catalog = new IcebergHadoopExternalCatalog(0, "iceberg_ctl", "", props, "");
        IcebergExternalTable icebergTable = new IcebergExternalTable(0, "", "", catalog, icebergDatabase);
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(icebergTable, Pair.of("index", column.getName()), 0));

        // Test table stats meta is null.
        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return null;
            }
        };
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));

        // Test column stats meta is null
        TableStatsMeta tableMeta = new TableStatsMeta();
        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return tableMeta;
            }
        };
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return null;
            }
        };
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                return new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 100, 0, 0, null);
            }
        };

        // Test table stats is user injected
        tableMeta.userInjected = true;
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));
        tableMeta.userInjected = false;

        // Test Config.auto_analyze_interval_seconds == 0
        Config.auto_analyze_interval_seconds = 0;
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));

        // Test column analyzed within the time interval
        Config.auto_analyze_interval_seconds = 86400;
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 0));

        // Test column hasn't analyzed for longer than time interval, but version and row count doesn't change
        new MockUp<TableStatsMeta>() {
            @Mock
            public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
                ColStatsMeta ret = new ColStatsMeta(System.currentTimeMillis(), null, null, null, 0, 100, 20, 10, null);
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ret;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public long getVisibleVersion() {
                return 10;
            }

            @Mock
            public long fetchRowCount() {
                return 100;
            }
        };
        Config.auto_analyze_interval_seconds = 1;
        Assertions.assertFalse(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 10));

        // Test column hasn't analyzed for longer than time interval, and version change
        new MockUp<OlapTable>() {
            @Mock
            public long getVisibleVersion() {
                return 11;
            }

            @Mock
            public long fetchRowCount() {
                return 100;
            }
        };
        Assertions.assertTrue(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 11));

        // Test column hasn't analyzed for longer than time interval, and row count change
        new MockUp<OlapTable>() {
            @Mock
            public long getVisibleVersion() {
                return 10;
            }

            @Mock
            public long fetchRowCount() {
                return 101;
            }
        };
        Assertions.assertTrue(StatisticsUtil.isLongTimeColumn(table, Pair.of("index", column.getName()), 10));
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

    @Test
    void testGetHotValues() {
        String value1 = "1234 :35 ;222 :34";
        Map<Literal, Float> hotValues = StatisticsUtil.getHotValues(value1, Type.INT, 0.01);
        Assertions.assertEquals(2, hotValues.size());

        int i = 0;
        for (Map.Entry<Literal, Float> entry : hotValues.entrySet()) {
            if (i == 0) {
                Assertions.assertEquals("1234", entry.getKey().getStringValue());
                Assertions.assertEquals("35.0", entry.getValue().toString());
                i++;
            } else {
                Assertions.assertEquals("222", entry.getKey().getStringValue());
                Assertions.assertEquals("34.0", entry.getValue().toString());
            }
        }

        String value2 = "1234 :34";
        hotValues = StatisticsUtil.getHotValues(value2, Type.INT, 0.01);
        Assertions.assertEquals(1, hotValues.size());

        for (Map.Entry<Literal, Float> entry : hotValues.entrySet()) {
            Assertions.assertEquals("1234", entry.getKey().getStringValue());
            Assertions.assertEquals("34.0", entry.getValue().toString());
        }

        String value3 = "aabbcc\\:\\; :34 ; dd :22";
        hotValues = StatisticsUtil.getHotValues(value3, Type.STRING, 0.01);
        Assertions.assertEquals(1, hotValues.size());
        for (Map.Entry<Literal, Float> entry : hotValues.entrySet()) {
            Assertions.assertEquals("aabbcc:;", entry.getKey().getStringValue());
            Assertions.assertEquals("34.0", entry.getValue().toString());
        }
    }

    @Test
    public void testGetOlapTableVersion() throws RpcException {
        Assertions.assertEquals(0, StatisticsUtil.getOlapTableVersion(null));
        OlapTable ot = Mockito.mock(OlapTable.class);
        Mockito.when(ot.getVisibleVersion()).thenReturn(100L);
        Assertions.assertEquals(100, StatisticsUtil.getOlapTableVersion(ot));
        Mockito.when(ot.getVisibleVersion()).thenThrow(new RpcException("", ""));
        Assertions.assertEquals(0, StatisticsUtil.getOlapTableVersion(ot));
    }
}
