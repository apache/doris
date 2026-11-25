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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.analysis.AnalysisInfo.JobType;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.PartitionSampleMeta;
import org.apache.doris.statistics.sampling.TabletSampleMeta;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Maps;
import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OlapAnalysisTaskTest {

    // test manual
    @Test
    public void testSample1() {
        TableIf tableIf = Mockito.mock(TableIf.class);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder()
                .setAnalysisMethod(AnalysisMethod.FULL);
        analysisInfoBuilder.setJobType(JobType.MANUAL);
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.tbl = tableIf;
        TableSample tableSample = olapAnalysisTask.getTableSample();
        Assertions.assertNull(tableSample);

        analysisInfoBuilder.setSampleRows(10);
        analysisInfoBuilder.setJobType(JobType.MANUAL);
        analysisInfoBuilder.setAnalysisMethod(AnalysisMethod.SAMPLE);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        tableSample = olapAnalysisTask.getTableSample();
        Assertions.assertEquals(10, tableSample.getSampleValue());
        Assertions.assertFalse(tableSample.isPercent());
    }

    @Test
    public void testShouldCollectHotValue() {
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.info = new AnalysisInfoBuilder().build();
        Assertions.assertFalse(olapAnalysisTask.shouldCollectHotValue());

        olapAnalysisTask.info = new AnalysisInfoBuilder().setCollectHotValue(true).build();
        Assertions.assertTrue(olapAnalysisTask.shouldCollectHotValue());

        olapAnalysisTask.info = new AnalysisInfoBuilder().setCollectHotValue(false).build();
        Assertions.assertFalse(olapAnalysisTask.shouldCollectHotValue());
    }

    @Test
    public void testCollectHotValueUseShortSerializedName() {
        AnalysisInfo info = new AnalysisInfoBuilder().setCollectHotValue(false).build();
        String json = GsonUtils.GSON.toJson(info);
        Assertions.assertTrue(json.contains("\"chv\":false"));
        Assertions.assertFalse(json.contains("collectHotValue"));

        AnalysisInfo deserialized = GsonUtils.GSON.fromJson(json, AnalysisInfo.class);
        Assertions.assertFalse(deserialized.collectHotValue);
    }

    // test auto small table
    @Test
    public void testSample3() {
        OlapTable tbl = Mockito.mock(OlapTable.class);
        Mockito.when(tbl.getDataSize(ArgumentMatchers.anyBoolean())).thenReturn(StatisticsUtil.getHugeTableLowerBoundSizeInBytes() - 1);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder()
                .setAnalysisMethod(AnalysisMethod.FULL);
        analysisInfoBuilder.setJobType(JobType.SYSTEM);
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.tbl = tbl;
        TableSample tableSample = olapAnalysisTask.getTableSample();
        Assertions.assertNull(tableSample);
    }

    @Test
    public void testMergePartitionSql() {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        params.put("catalogId", "0");
        params.put("dbId", "1");
        params.put("tblId", "2");
        params.put("idxId", "3");
        params.put("colId", "col1");
        params.put("dataSizeFunction", "100");
        params.put("catalogName", "internal");
        params.put("dbName", "db1");
        params.put("colName", "col1");
        params.put("tblName", "tbl1");
        params.put("index", "index1");
        params.put("preAggHint", "");
        params.put("min", "min");
        params.put("max", "max");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(BaseAnalysisTask.MERGE_PARTITION_TEMPLATE);
        Assertions.assertEquals("SELECT CONCAT(2, '-', 3, '-', 'col1') AS `id`, 0 AS `catalog_id`, 1 AS `db_id`, "
                + "2 AS `tbl_id`, 3 AS `idx_id`, 'col1' AS `col_id`, NULL AS `part_id`, SUM(count) AS `row_count`, "
                + "IF(IFNULL(SUM(unseen_ndv), 0) = 0, HLL_CARDINALITY(HLL_UNION(ndv)), "
                + "LEAST(GREATEST(0, IFNULL(SUM(count) - SUM(null_count), 0)), "
                + "HLL_CARDINALITY(HLL_UNION(ndv)) + IFNULL(SUM(unseen_ndv), 0))) AS `ndv`, "
                + "SUM(null_count) AS `null_count`, MIN(min) AS `min`, "
                + "MAX(max) AS `max`, SUM(data_size_in_bytes) AS `data_size`, NOW() AS `update_time`,null as `hot_value` "
                + "FROM internal.__internal_schema.partition_statistics "
                + "WHERE `catalog_id` = 0  AND `db_id` = 1  AND `tbl_id` = 2  AND `idx_id` = 3  AND `col_id` = 'col1'",
                sql);
    }

    @Test
    public void testAddLengthAssertParamForStringColumn() throws Exception {
        // String column with positive config -> emits per-row assert_true guard
        Column strCol = new Column("s", PrimitiveType.VARCHAR);
        long savedLen = org.apache.doris.common.Config.statistics_max_string_column_length;
        org.apache.doris.common.Config.statistics_max_string_column_length = 1024;
        try {
            OlapAnalysisTask task = new OlapAnalysisTask();
            AnalysisInfo info = new AnalysisInfoBuilder()
                    .setJobId(1L)
                    .setTaskId(2L)
                    .setColName("s")

                    .build();
            task.info = info;
            task.col = strCol;
            Map<String, String> params = Maps.newHashMap();
            task.addLengthAssertParam(params);
            String lengthAssert = params.get("lengthAssert");
            Assertions.assertNotNull(lengthAssert);
            Assertions.assertTrue(lengthAssert.contains("assert_true"),
                    "expected assert_true in placeholder, got: " + lengthAssert);
            Assertions.assertTrue(lengthAssert.contains("IS NULL OR LENGTH"),
                    "expected NULL guard, got: " + lengthAssert);
            Assertions.assertTrue(lengthAssert.contains("1024"),
                    "expected max length value, got: " + lengthAssert);
            Assertions.assertTrue(
                    lengthAssert.contains(BaseAnalysisTask.ANALYZE_SKIP_LONG_STRING_COLUMN_MARKER),
                    "expected marker in placeholder, got: " + lengthAssert);
        } finally {
            org.apache.doris.common.Config.statistics_max_string_column_length = savedLen;
        }
    }

    @Test
    public void testAddLengthAssertParamForNonStringColumn() {
        // Non-string columns must emit an empty placeholder so SQL stays unchanged
        Column intCol = new Column("id", PrimitiveType.INT);
        OlapAnalysisTask task = new OlapAnalysisTask();
        AnalysisInfo info = new AnalysisInfoBuilder()
                .setJobId(1L).setTaskId(2L).setColName("id")
                .build();
        task.info = info;
        task.col = intCol;
        Map<String, String> params = Maps.newHashMap();
        task.addLengthAssertParam(params);
        Assertions.assertEquals("", params.get("lengthAssert"));
    }

    @Test
    public void testAddLengthAssertParamConfigDisabled() {
        Column strCol = new Column("s", PrimitiveType.VARCHAR);
        long savedLen = org.apache.doris.common.Config.statistics_max_string_column_length;
        org.apache.doris.common.Config.statistics_max_string_column_length = 0;
        try {
            OlapAnalysisTask task = new OlapAnalysisTask();
            AnalysisInfo info = new AnalysisInfoBuilder()
                    .setJobId(1L).setTaskId(2L).setColName("s")
                    .build();
            task.info = info;
            task.col = strCol;
            Map<String, String> params = Maps.newHashMap();
            task.addLengthAssertParam(params);
            Assertions.assertEquals("", params.get("lengthAssert"));
        } finally {
            org.apache.doris.common.Config.statistics_max_string_column_length = savedLen;
        }
    }

    @Test
    public void testBuildSqlParamsQuotesIdentifiers() {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("cat`alog");
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getFullName()).thenReturn("db`name");
        Mockito.when(table.getId()).thenReturn(3L);
        Mockito.when(table.getName()).thenReturn("table`name");
        Mockito.when(table.getIndexNameById(4L)).thenReturn("index`name");

        OlapAnalysisTask task = new OlapAnalysisTask();
        task.catalog = catalog;
        task.db = database;
        task.tbl = table;
        task.col = new Column("col`'name", PrimitiveType.INT);
        task.info = new AnalysisInfoBuilder()
                .setIndexId(4L)
                .setColName("col`'name")
                .build();

        Map<String, String> params = task.buildSqlParams();
        Assertions.assertEquals("`cat``alog`", params.get("catalogName"));
        Assertions.assertEquals("`db``name`", params.get("dbName"));
        Assertions.assertEquals("`table``name`", params.get("tblName"));
        Assertions.assertEquals("`col``'name`", params.get("colName"));
        Assertions.assertEquals("col`''name", params.get("colId"));
        Assertions.assertEquals("index `index``name`", params.get("index"));
        Assertions.assertEquals("partition `part``name`", task.getPartitionInfo("part`name"));

        String sql = new StringSubstitutor(params).replace(BaseAnalysisTask.FULL_ANALYZE_WITHOUT_HOT_VALUE_TEMPLATE);
        Assertions.assertTrue(sql.contains("FROM (SELECT `col``'name` FROM "
                + "`cat``alog`.`db``name`.`table``name` index `index``name`) __lc_t"), sql);
    }

    @Test
    public void testFullAnalyzeTemplateRendersLengthAssert() {
        // Confirm the rendered FULL_ANALYZE_TEMPLATE wraps the base table in a subquery
        // and carries the assert_true clause for a string column.
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        params.put("catalogId", "0");
        params.put("dbId", "1");
        params.put("tblId", "2");
        params.put("idxId", "3");
        params.put("colId", "s");
        params.put("dataSizeFunction", "100");
        params.put("catalogName", SqlUtils.getIdentSql("internal"));
        params.put("dbName", SqlUtils.getIdentSql("db1"));
        params.put("colName", SqlUtils.getIdentSql("s"));
        params.put("tblName", SqlUtils.getIdentSql("tbl1"));
        params.put("index", "");
        params.put("lengthAssert",
                ", assert_true(`s` IS NULL OR LENGTH(`s`) <= 1024, '"
                        + BaseAnalysisTask.ANALYZE_SKIP_LONG_STRING_COLUMN_MARKER + "') AS `__lc`");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE);
        Assertions.assertTrue(sql.contains("WITH cte1 AS (SELECT `s`, assert_true("), sql);
        Assertions.assertTrue(sql.contains("IS NULL OR LENGTH(`s`) <= 1024"), sql);
        Assertions.assertTrue(sql.contains("FROM cte1), hot_top AS ("), sql);
    }

    @Test
    public void testFullAnalyzeTemplateRendersWithoutLengthAssert() {
        // Non-string columns yield a plain inline view that Nereids can collapse.
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        params.put("catalogId", "0");
        params.put("dbId", "1");
        params.put("tblId", "2");
        params.put("idxId", "3");
        params.put("colId", "id");
        params.put("dataSizeFunction", "100");
        params.put("catalogName", SqlUtils.getIdentSql("internal"));
        params.put("dbName", SqlUtils.getIdentSql("db1"));
        params.put("colName", SqlUtils.getIdentSql("id"));
        params.put("tblName", SqlUtils.getIdentSql("tbl1"));
        params.put("index", "");
        params.put("lengthAssert", "");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE);
        Assertions.assertFalse(sql.contains("assert_true"), sql);
        Assertions.assertTrue(sql.contains("WITH cte1 AS (SELECT `id` FROM `internal`.`db1`.`tbl1`"), sql);
        Assertions.assertTrue(sql.contains("FROM cte1), hot_top AS ("), sql);

    }

    @Test
    public void testFullAnalyzeTemplateSql() {
        Map<String, String> params = new HashMap<>();
        params.put("catalogId", "0");
        params.put("dbId", "1");
        params.put("tblId", "2");
        params.put("idxId", "3");
        params.put("colId", "col1");
        params.put("colName", SqlUtils.getIdentSql("col1"));
        params.put("dataSizeFunction", "SUM(LENGTH(`col1`))");
        params.put("catalogName", SqlUtils.getIdentSql("internal"));
        params.put("dbName", SqlUtils.getIdentSql("db1"));
        params.put("tblName", SqlUtils.getIdentSql("tbl1"));
        params.put("index", "");
        params.put("hotValueCollectCount", "10");
        params.put("subStringColName", "`col1`");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE);
        Assertions.assertTrue(sql.startsWith("WITH cte1 AS ("));
        Assertions.assertTrue(sql.contains("IFNULL(GROUP_CONCAT"));
        Assertions.assertTrue(sql.contains("as `hot_value`"));
        Assertions.assertTrue(sql.contains("LEFT JOIN hot_top ON TRUE"));
        Assertions.assertTrue(sql.contains("LIMIT 10"));
        Assertions.assertTrue(sql.contains("GROUP BY `col1` ORDER BY freq DESC"));
        Assertions.assertFalse(sql.contains("null as `hot_value`"));
    }

    @ParameterizedTest
    @CsvSource({"false,unique,false,false", "false,value,false,false", "true,unique,false,false",
            "true,value,false,false", "true,unique,true,false", "true,value,true,false", "true,unique,true,true",
            "true,unique,false,true", "true,local,true,false", "true,local,false,false", "true,local,true,true",
            "false,local,true,false"})
    void doSampleRendersTheChosenTemplateAndRollup(boolean fixed, String key, boolean census, boolean keyRange) {
        boolean unique = !key.equals("value");
        boolean globallyUnique = key.equals("unique");
        long rows = census ? 10 : 100000;
        long rollup = 20L;
        var column = new Column("c", PrimitiveType.INT);
        column.setIsKey(unique);
        column.setIsAllowNull(false);
        var keys = unique ? KeysType.UNIQUE_KEYS : KeysType.DUP_KEYS;
        var meta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(meta.getColumnByName("c")).thenReturn(column);
        Mockito.when(meta.getSchema()).thenReturn(List.of(column));
        Mockito.when(meta.getKeysType()).thenReturn(keys);
        var table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getKeysType()).thenReturn(keys);
        var metadataLock = new ReentrantReadWriteLock();
        Mockito.doAnswer(call -> {
            metadataLock.readLock().lock();
            return null;
        }).when(table).readLock();
        Mockito.doAnswer(call -> {
            metadataLock.readLock().unlock();
            return null;
        }).when(table).readUnlock();
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenAnswer(call -> {
            Assertions.assertTrue(metadataLock.getReadHoldCount() > 0);
            return !keyRange;
        });
        Mockito.when(table.getIndexMetaByIndexId(rollup)).thenAnswer(call -> {
            Assertions.assertTrue(metadataLock.getReadHoldCount() > 0);
            return meta;
        });
        var partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(table.getPartitionInfo()).thenAnswer(call -> {
            Assertions.assertTrue(metadataLock.getReadHoldCount() > 0);
            return partitionInfo;
        });
        Mockito.when(partitionInfo.getPartitionColumns()).thenReturn(
                List.of(globallyUnique ? column : new Column("p", Type.INT)));
        Mockito.when(table.getRowCountForIndex(rollup, false)).thenReturn(rows);
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getIndexNameById(rollup)).thenReturn("r");
        Mockito.when(table.getName()).thenReturn("t");
        var part = Mockito.mock(Partition.class);
        if (keyRange) {
            var distribution = new HashDistributionInfo(1, List.of(column));
            Mockito.when(table.getDefaultDistributionInfo()).thenReturn(distribution);
            Mockito.when(part.getDistributionInfo()).thenReturn(distribution);
        }
        var index = Mockito.mock(MaterializedIndex.class);
        var tablet = Mockito.mock(LocalTablet.class);
        Mockito.doReturn(List.of(part)).when(table).getPartitions();
        Mockito.when(part.getIndex(rollup)).thenReturn(index);
        Mockito.when(index.getTabletIdsInOrder()).thenReturn(List.of(7L));
        Mockito.when(index.getRowCount()).thenReturn(rows);
        Mockito.when(index.getTablet(7L)).thenReturn(tablet);
        Mockito.when(tablet.getMinReplicaRowCount(Mockito.anyLong())).thenReturn(rows);

        var task = Mockito.spy(new OlapAnalysisTask());
        task.tbl = table;
        task.col = column;
        task.catalog = Mockito.mock(CatalogIf.class);
        task.db = Mockito.mock(DatabaseIf.class);
        Mockito.when(task.catalog.getName()).thenReturn("internal");
        Mockito.when(task.db.getFullName()).thenReturn("db1");
        task.info = new AnalysisInfoBuilder().setIndexId(rollup).setJobType(JobType.MANUAL)
                .setColName("c").setRowCount(rows).build();
        task.tableSample = new TableSample(false, 100L);
        task.job = Mockito.spy(new AnalysisJob(task.info, List.of()));
        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1, List.of(new TabletSampleMeta(7, rows, false))))));
        Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(true);
        Mockito.when(frame.allCandidatesSupport(true)).thenReturn(true);
        Mockito.doReturn(frame).when(task.job).samplingFrame(table, rollup);
        Mockito.doReturn(new ResultRow(List.of("1", "9"))).when(task).getSampleMinMax();
        var query = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(task).runQuery(query.capture());

        try (var util = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(fixed);
            task.doSample();
        }
        String sql = query.getValue().replace("`", "").toLowerCase(Locale.ROOT);
        Assertions.assertFalse(sql.contains("${"), sql);
        Assertions.assertTrue(sql.contains("index r"), sql);
        if (keyRange) {
            Assertions.assertFalse(sql.contains("preaggopen"), sql);
        }
        if (census) {
            Mockito.verify(task, Mockito.never()).getSampleMinMax();
            Assertions.assertFalse(sql.contains("tablesample"), sql);
            Assertions.assertTrue(sql.contains("count(1) as row_count"), sql);
            String ndv = fixed ? "least(count(c), ndv(c))" : "ndv(c)";
            Assertions.assertTrue(sql.contains((globallyUnique ? "count(1)" : ndv) + " as ndv"), sql);
        } else {
            String method = keyRange ? "key_range_block" : "fixed_block";
            Assertions.assertTrue(sql.contains(fixed ? "index r tablesample (" + method : "index r tablet(7)"), sql);
            Assertions.assertTrue(sql.contains(globallyUnique ? "null as hot_value" : "left join hot_top on true"), sql);
        }
    }

    @Test
    void rollupRemovedAfterSamplingAdmissionSkipsBeforeExecutingSql() {
        long rollup = 20L;
        var table = Mockito.mock(OlapTable.class);
        var column = new Column("k", Type.INT, true, null, "0", "");
        var meta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(meta.getColumnByName("k")).thenReturn(column);
        Mockito.when(meta.getSchema()).thenReturn(List.of(column));
        Mockito.when(meta.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.getIndexMetaByIndexId(rollup)).thenReturn(meta);
        Mockito.when(table.getIndexNameById(rollup)).thenReturn("r");
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);
        Mockito.doReturn(Mockito.mock(PartitionInfo.class)).when(table).getPartitionInfo();
        var task = Mockito.spy(new OlapAnalysisTask());
        task.tbl = table;
        task.catalog = Mockito.mock(CatalogIf.class);
        task.db = Mockito.mock(DatabaseIf.class);
        Mockito.when(task.catalog.getName()).thenReturn("internal");
        Mockito.when(task.db.getFullName()).thenReturn("db");
        task.info = new AnalysisInfoBuilder().setIndexId(rollup).setColName("k").setRowCount(1000).build();
        task.tableSample = new TableSample(false, 10L);
        task.job = Mockito.mock(AnalysisJob.class);
        Mockito.when(task.job.reportedIndexRows(table, rollup)).thenReturn(1000L);
        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(true);
        Mockito.when(task.job.samplingFrame(table, rollup)).thenAnswer(call -> {
            Mockito.doReturn(null).when(table).getIndexMetaByIndexId(rollup);
            Mockito.doReturn(null).when(table).getIndexNameById(rollup);
            return frame;
        });
        try (var util = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(true);
            Assertions.assertThrows(AnalyzeSkipException.class, task::doSample);
        }
        Mockito.verify(task, Mockito.never()).runQuery(Mockito.anyString());
    }

    @Test
    public void testDoFullHotValue() throws Exception {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        OlapTable tableIf = Mockito.mock(OlapTable.class);
        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(tableIf.getName()).thenReturn("testTbl");
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("catalogName");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);
        Mockito.when(databaseIf.getFullName()).thenReturn("testDb");

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(
                StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedStatisticsUtil.when(StatisticsUtil::enablePartitionAnalyze).thenReturn(false);
            mockedSessionVariable.when(SessionVariable::getHotValueCollectCount).thenReturn(10);

            OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
            Mockito.doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                Assertions.assertTrue(sql.startsWith("WITH cte1 AS (SELECT `testCol` "
                        + "FROM `catalogName`.`testDb`.`testTbl` "), sql);
                Assertions.assertTrue(sql.contains("IFNULL(GROUP_CONCAT"), sql);
                Assertions.assertTrue(sql.contains("GROUP BY `testCol`"), sql);
                Assertions.assertTrue(sql.contains("LIMIT 10"), sql);
                Assertions.assertTrue(sql.contains("LEFT JOIN hot_top ON TRUE"), sql);
                Assertions.assertFalse(sql.contains("null as `hot_value`"), sql);
                return null;
            }).when(task).runQuery(Mockito.anyString());

            task.col = new Column("testCol", Type.fromPrimitiveType(PrimitiveType.INT),
                true, null, null, null);
            task.tbl = tableIf;
            AnalysisInfoBuilder builder = new AnalysisInfoBuilder();
            builder.setJobType(AnalysisInfo.JobType.MANUAL);
            builder.setColName("testCol");
            builder.setCollectHotValue(true);
            task.info = builder.build();
            task.catalog = catalogIf;
            task.db = databaseIf;
            task.doFull();
        }
    }

    @Test
    public void testDoFullWithoutHotValue() throws Exception {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        OlapTable tableIf = Mockito.mock(OlapTable.class);
        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(tableIf.getName()).thenReturn("testTbl");
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("catalogName");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);
        Mockito.when(databaseIf.getFullName()).thenReturn("testDb");

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(
                StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatisticsUtil.when(StatisticsUtil::enablePartitionAnalyze).thenReturn(false);

            OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
            Mockito.doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                Assertions.assertTrue(sql.startsWith("SELECT CONCAT(30001, '-', -1, '-', 'testCol') AS `id`"), sql);
                Assertions.assertTrue(sql.contains("null as `hot_value`"), sql);
                Assertions.assertTrue(sql.contains(
                        "FROM (SELECT `testCol` FROM `catalogName`.`testDb`.`testTbl` ) __lc_t"), sql);
                Assertions.assertFalse(sql.contains("LEFT JOIN hot_top ON TRUE"), sql);
                Assertions.assertFalse(sql.contains("GROUP BY `hash_value`"), sql);
                return null;
            }).when(task).runQuery(Mockito.anyString());

            task.col = new Column("testCol", Type.fromPrimitiveType(PrimitiveType.INT),
                true, null, null, null);
            task.tbl = tableIf;
            AnalysisInfoBuilder builder = new AnalysisInfoBuilder();
            builder.setJobType(AnalysisInfo.JobType.MANUAL);
            builder.setColName("testCol");
            builder.setCollectHotValue(false);
            task.info = builder.build();
            task.catalog = catalogIf;
            task.db = databaseIf;
            task.doFull();
        }
    }

    @Test
    public void testDoFullHotValueStringColumn() throws Exception {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        OlapTable tableIf = Mockito.mock(OlapTable.class);
        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(tableIf.getName()).thenReturn("testTbl");
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("catalogName");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);
        Mockito.when(databaseIf.getFullName()).thenReturn("testDb");

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(
                StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedStatisticsUtil.when(StatisticsUtil::enablePartitionAnalyze).thenReturn(false);
            mockedSessionVariable.when(SessionVariable::getHotValueCollectCount).thenReturn(10);

            OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
            Mockito.doAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                Assertions.assertTrue(sql.contains(
                        "GROUP BY xxhash_64(SUBSTRING(CAST(`strCol` AS STRING), 1, 1024))"), sql);
                Assertions.assertTrue(sql.contains("MAX(`strCol`) AS column_key"), sql);
                return null;
            }).when(task).runQuery(Mockito.anyString());

            task.col = new Column("strCol", Type.fromPrimitiveType(PrimitiveType.STRING),
                true, null, null, null);
            task.tbl = tableIf;
            AnalysisInfoBuilder builder = new AnalysisInfoBuilder();
            builder.setJobType(AnalysisInfo.JobType.MANUAL);
            builder.setColName("strCol");
            builder.setCollectHotValue(true);
            task.info = builder.build();
            task.catalog = catalogIf;
            task.db = databaseIf;
            task.doFull();
        }
    }
}
