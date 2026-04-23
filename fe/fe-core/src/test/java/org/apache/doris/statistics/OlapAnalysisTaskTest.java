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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void testKeyColumnUseLimitAndNot() {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        OlapTable tableIf = Mockito.mock(OlapTable.class);

        Mockito.when(tableIf.getRowCount()).thenReturn(20000000L);
        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("catalogName");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);

        OlapAnalysisTask olapAnalysisTask = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(new ResultRow(Lists.newArrayList("1", "2"))).when(olapAnalysisTask).collectMinMax();
        Mockito.doNothing().when(olapAnalysisTask).getSampleParams(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
        Mockito.doReturn(true).when(olapAnalysisTask).useLinearAnalyzeTemplate();
        Mockito.doAnswer(inv -> {
            String sql = inv.getArgument(0);
            Assertions.assertEquals("WITH cte1 AS (SELECT `null` FROM `catalogName`.`${dbName}`.`null`  "
                    + "${sampleHints} ${limit} ), cte2 AS (SELECT CONCAT(30001, '-', -1, '-', 'null') AS `id`, "
                    + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                    + "'null' AS `col_id`, NULL AS `part_id`, ${rowCount} AS `row_count`, ${ndvFunction} as `ndv`, "
                    + "ROUND(SUM(CASE WHEN `null` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor}) AS `null_count`, "
                    + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, "
                    + "SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                    + "COUNT(1) * 4 * ${scaleFactor} AS `data_size`, NOW() FROM cte1), "
                    + "cte3 AS (SELECT IFNULL(GROUP_CONCAT(CONCAT(REPLACE(REPLACE(t.`column_key`, "
                    + "\":\", \"\\\\:\"), \";\", \"\\\\;\"), \" :\", ROUND(t.`count` / ${rowCount2}, 2)), "
                    + "\" ;\"), '') as `hot_value` FROM (SELECT ${subStringColName} as `hash_value`, "
                    + "MAX(`null`) as `column_key`, COUNT(1) AS `count` FROM cte1 "
                    + "WHERE `null` IS NOT NULL GROUP BY `hash_value` ORDER BY `count` DESC LIMIT 10) t) "
                    + "SELECT * FROM cte2 CROSS JOIN cte3", sql);
            return null;
        }).when(olapAnalysisTask).runQuery(ArgumentMatchers.anyString());

        olapAnalysisTask.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.INT),
            true, null, null, null);
        olapAnalysisTask.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.catalog = catalogIf;
        olapAnalysisTask.db = databaseIf;
        olapAnalysisTask.tableSample = new TableSample(false, 100L);
        olapAnalysisTask.doSample();

        Mockito.doAnswer(inv -> {
            String sql = inv.getArgument(0);
            Assertions.assertEquals("WITH cte1 AS (SELECT MAX(t0.`col_value`) as `col_value`, COUNT(1) as `count`,"
                    + " SUM(`len`) as `column_length` FROM (SELECT ${subStringColName} AS `hash_value`, "
                    + "`null` AS `col_value`, LENGTH(`null`) as `len` FROM `catalogName`.`${dbName}`.`null`  "
                    + "${sampleHints} ${limit}) as `t0`  GROUP BY `t0`.`hash_value`), "
                    + "cte2 AS ( SELECT CONCAT('30001', '-', '-1', '-', 'null') AS `id`, 10001 AS `catalog_id`, "
                    + "20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, 'null' AS `col_id`, NULL AS `part_id`, "
                    + "${rowCount} AS `row_count`, ${ndvFunction} as `ndv`, IFNULL(SUM(IF(`t1`.`col_value` "
                    + "IS NULL, `t1`.`count`, 0)), 0) * ${scaleFactor} as `null_count`, SUBSTRING(CAST('1' "
                    + "AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                    + "COUNT(1) * 4 * ${scaleFactor} AS `data_size`, NOW() FROM cte1 t1), cte3 AS (SELECT "
                    + "IFNULL(GROUP_CONCAT(CONCAT(REPLACE(REPLACE(t2.`col_value`, \":\", \"\\\\:\"), \";\", \"\\\\;\"), "
                    + "\" :\", ROUND(t2.`count` / ${rowCount2}, 2)), \" ;\"), '') as `hot_value` FROM (SELECT "
                    + "`col_value`, `count` FROM cte1 WHERE `col_value` IS NOT NULL ORDER BY `count` DESC LIMIT 10) "
                    + "t2) SELECT * FROM cte2 CROSS JOIN cte3", sql);
            return null;
        }).when(olapAnalysisTask).runQuery(ArgumentMatchers.anyString());
        Mockito.doReturn(false).when(olapAnalysisTask).useLinearAnalyzeTemplate();
        olapAnalysisTask.doSample();
    }

    @Test
    public void testNeedLimitFalse() throws Exception {
        OlapTable tableIf = Mockito.mock(OlapTable.class);
        ArrayList<Column> columns = Lists.newArrayList();
        columns.add(new Column("test", PrimitiveType.STRING));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, columns));
        Mockito.when(tableIf.isPartitionColumn(ArgumentMatchers.any())).thenReturn(true);

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.STRING),
            true, null, null, null);
        olapAnalysisTask.tbl = tableIf;
        Assertions.assertFalse(olapAnalysisTask.needLimit());

        olapAnalysisTask.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.STRING),
            false, null, null, null);
        Assertions.assertFalse(olapAnalysisTask.needLimit());
    }

    @Test
    public void testNeedLimitTrue() throws Exception {
        OlapTable tableIf = Mockito.mock(OlapTable.class);
        ArrayList<Column> columns = Lists.newArrayList();
        columns.add(new Column("NOFOUND", PrimitiveType.STRING));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, columns));

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.tbl = tableIf;
        olapAnalysisTask.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.STRING),
            false, null, null, null);
        Assertions.assertTrue(olapAnalysisTask.needLimit());

        olapAnalysisTask.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.STRING),
            true, null, null, null);
        olapAnalysisTask.setKeyColumnSampleTooManyRows(true);
        Assertions.assertTrue(olapAnalysisTask.needLimit());
    }

    @Test
    public void testPickSamplePartition() {
        OlapAnalysisTask task = new OlapAnalysisTask();
        AnalysisInfoBuilder builder = new AnalysisInfoBuilder();
        task.info = builder.setIndexId(-1L).build();

        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Mockito.when(olapTable.getRowCount()).thenReturn(1000000000L);
        task.setTable(olapTable);

        Partition p1 = Mockito.mock(Partition.class);
        Partition p2 = Mockito.mock(Partition.class);
        Partition p3 = Mockito.mock(Partition.class);
        Mockito.when(p1.getId()).thenReturn(1L);
        Mockito.when(p2.getId()).thenReturn(2L);
        Mockito.when(p3.getId()).thenReturn(3L);
        Mockito.when(p1.getRowCount()).thenReturn(400000000L);
        Mockito.when(p2.getRowCount()).thenReturn(100000000L);
        Mockito.when(p3.getRowCount()).thenReturn(500000000L);

        MaterializedIndex idx1 = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex idx2 = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex idx3 = Mockito.mock(MaterializedIndex.class);
        Mockito.when(idx1.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(0L, 1L));
        Mockito.when(idx2.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(2L, 3L));
        Mockito.when(idx3.getTabletIdsInOrder()).thenReturn(Lists.newArrayList(2L, 3L));
        Mockito.when(p1.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx1);
        Mockito.when(p2.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx2);
        Mockito.when(p3.getIndex(ArgumentMatchers.anyLong())).thenReturn(idx3);

        List<Partition> partitions = Lists.newArrayList();
        partitions.add(p1);
        partitions.add(p2);
        partitions.add(p3);
        List<Long> ids = Lists.newArrayList();

        long rows = task.pickSamplePartition(partitions, ids, 0);
        Assertions.assertEquals(900000000, rows);
        Assertions.assertEquals(4, ids.size());
        Assertions.assertEquals(0, ids.get(0));
        Assertions.assertEquals(1, ids.get(1));
        Assertions.assertEquals(2, ids.get(2));
        Assertions.assertEquals(3, ids.get(3));
    }

    @Test
    public void testUseLinearAnalyzeTemplate() {
        OlapAnalysisTask task = new OlapAnalysisTask();
        task.setPartitionColumnSampleTooManyRows(true);
        Assertions.assertTrue(task.useLinearAnalyzeTemplate());

        task.setPartitionColumnSampleTooManyRows(false);
        task.setScanFullTable(true);
        Assertions.assertTrue(task.useLinearAnalyzeTemplate());

        task.setScanFullTable(false);
        task.setPartitionColumnSampleTooManyRows(false);
        OlapAnalysisTask spyTask = Mockito.spy(task);
        Mockito.doReturn(true).when(spyTask).isSingleUniqueKey();
        Assertions.assertTrue(spyTask.useLinearAnalyzeTemplate());
    }

    @Test
    public void testGetSampleParams() {
        OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
        Map<String, String> params = Maps.newHashMap();
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(false).when(task).useLinearAnalyzeTemplate();

        OlapTable mockTable = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        task.col = new Column("testColumn", Type.INT, true, null, null, "");
        task.setTable(mockTable);
        task.getSampleParams(params, 10);
        Assertions.assertTrue(task.scanFullTable());
        Assertions.assertEquals("1", params.get("scaleFactor"));
        Assertions.assertEquals("", params.get("sampleHints"));
        Assertions.assertEquals("ROUND(NDV(`${colName}`) * ${scaleFactor})", params.get("ndvFunction"));
        Assertions.assertNull(params.get("preAggHint"));
        Assertions.assertEquals("COUNT(1)", params.get("rowCount"));
        params.clear();

        task.getSampleParams(params, 10000);
        Assertions.assertEquals("10000", params.get("rowCount"));
        params.clear();

        OlapTable mockTable2 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable2.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(false).when(task).useLinearAnalyzeTemplate();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(mockTable2);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("SUM(`t1`.`count`) * COUNT(`t1`.`col_value`) / (SUM(`t1`.`count`) - SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) + SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) * SUM(`t1`.`count`) / 1000)", params.get("ndvFunction"));
        Assertions.assertEquals("SUM(t1.count) * 4", params.get("dataSizeFunction"));
        Assertions.assertEquals("`${colName}`", params.get("subStringColName"));
        Assertions.assertEquals("/*+PREAGGOPEN*/", params.get("preAggHint"));
        params.clear();

        OlapTable mockTable3 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable3.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(mockTable3.isUniqKeyMergeOnWrite()).thenReturn(false);
        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(false).when(task).useLinearAnalyzeTemplate();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(mockTable3);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("/*+PREAGGOPEN*/", params.get("preAggHint"));
        params.clear();

        OlapTable mockTable4 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable4.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(mockTable4.isUniqKeyMergeOnWrite()).thenReturn(true);
        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(false).when(task).useLinearAnalyzeTemplate();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(mockTable4);
        task.getSampleParams(params, 1000);
        Assertions.assertNull(params.get("preAggHint"));
        params.clear();

        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(true).when(task).useLinearAnalyzeTemplate();
        Mockito.doReturn(false).when(task).isSingleUniqueKey();
        OlapTable mockTable5 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable5.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        task.col = new Column("test", PrimitiveType.INT);
        task.setTable(mockTable5);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("ROUND(NDV(`${colName}`) * ${scaleFactor})", params.get("ndvFunction"));
        params.clear();

        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(100L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(false).when(task).needLimit();
        Mockito.doReturn(true).when(task).useLinearAnalyzeTemplate();
        Mockito.doReturn(true).when(task).isSingleUniqueKey();
        OlapTable mockTable6 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable6.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        task.col = new Column("test", PrimitiveType.INT);
        task.setTable(mockTable6);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("1000", params.get("ndvFunction"));
        params.clear();

        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(50L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(true).when(task).needLimit();
        Mockito.doReturn(true).when(task).useLinearAnalyzeTemplate();
        Mockito.doReturn(true).when(task).isSingleUniqueKey();
        OlapTable mockTable7 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable7.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        task.col = new Column("test", PrimitiveType.INT);
        task.setTable(mockTable7);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("20.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("1000", params.get("ndvFunction"));
        Assertions.assertEquals("limit 50", params.get("limit"));
        params.clear();

        task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(50L).when(task).getSampleRows();
        Mockito.doReturn(Pair.of(Lists.newArrayList(1L, 2L), 100L)).when(task).getSampleTablets();
        Mockito.doReturn(true).when(task).needLimit();
        Mockito.doReturn(true).when(task).useLinearAnalyzeTemplate();
        Mockito.doReturn(true).when(task).isSingleUniqueKey();
        OlapTable mockTable8 = Mockito.mock(OlapTable.class);
        Mockito.when(mockTable8.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        task.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.INT),
            true, null, null, null);
        task.setKeyColumnSampleTooManyRows(true);
        task.setTable(mockTable8);
        task.getSampleParams(params, 2000000000);
        Assertions.assertEquals("2.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("2000000000", params.get("ndvFunction"));
        Assertions.assertEquals("limit 1000000000", params.get("limit"));
    }

    @Test
    public void testGetSkipPartitionId() throws AnalysisException {
        OlapTable tableIf = Mockito.mock(OlapTable.class);

        // test null partition list
        OlapAnalysisTask task = new OlapAnalysisTask();
        long skipPartitionId = task.getSkipPartitionId(null);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // test empty partition list
        List<Partition> partitions = Lists.newArrayList();
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // test partition list item less than session variable partition_sample_count
        Partition p1 = new Partition(1, "p1", new MaterializedIndex(), new RandomDistributionInfo());
        partitions.add(p1);
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        partitions.clear();
        int partitionSampleCount = StatisticsUtil.getPartitionSampleCount();
        for (int i = 1; i <= partitionSampleCount; i++) {
            Partition p = new Partition(i, "p" + i, new MaterializedIndex(), new RandomDistributionInfo());
            partitions.add(p);
        }

        // Test List partition return NO_SKIP_TABLET_ID
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.LIST));
        task.tbl = tableIf;
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test Unpartition return NO_SKIP_TABLET_ID
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.UNPARTITIONED));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test more than 1 partition column return NO_SKIP_TABLET_ID
        ArrayList<Column> multiColumns = Lists.newArrayList();
        multiColumns.add(new Column("col1", PrimitiveType.DATEV2));
        multiColumns.add(new Column("col2", PrimitiveType.DATEV2));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, multiColumns));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test not Date type return NO_SKIP_TABLET_ID
        ArrayList<Column> stringColumns = Lists.newArrayList();
        stringColumns.add(new Column("col1", PrimitiveType.STRING));
        Mockito.when(tableIf.getPartitionInfo()).thenReturn(new PartitionInfo(PartitionType.RANGE, stringColumns));
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test return the partition id with the oldest date range.
        ArrayList<Column> columns = Lists.newArrayList();
        Column col1 = new Column("col1", PrimitiveType.DATEV2);
        columns.add(col1);
        PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE, columns);

        List<PartitionValue> lowKey = Lists.newArrayList();
        lowKey.add(new PartitionValue("2025-01-01"));
        List<PartitionValue> highKey = Lists.newArrayList();
        highKey.add(new PartitionValue("2025-01-02"));
        Range<PartitionKey> range1 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                    PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item1 = new RangePartitionItem(range1);

        lowKey.clear();
        lowKey.add(new PartitionValue("2024-11-01"));
        highKey.clear();
        highKey.add(new PartitionValue("2024-11-02"));
        Range<PartitionKey> range2 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item2 = new RangePartitionItem(range2);

        lowKey.clear();
        lowKey.add(new PartitionValue("2025-02-13"));
        highKey.clear();
        highKey.add(new PartitionValue("2025-02-14"));
        Range<PartitionKey> range3 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                PartitionKey.createPartitionKey(highKey, columns));
        RangePartitionItem item3 = new RangePartitionItem(range3);

        partitionInfo.addPartition(1, false, item1, new DataProperty(TStorageMedium.HDD), null, false, false);
        partitionInfo.addPartition(2, false, item2, new DataProperty(TStorageMedium.HDD), null, false, false);
        partitionInfo.addPartition(3, false, item3, new DataProperty(TStorageMedium.HDD), null, false, false);

        Mockito.when(tableIf.getPartitionInfo()).thenReturn(partitionInfo);
        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatisticsUtil.when(StatisticsUtil::getPartitionSampleCount).thenReturn(3);
            partitions.clear();
            for (int i = 1; i <= 3; i++) {
                Partition p = new Partition(i, "p" + i, new MaterializedIndex(), new RandomDistributionInfo());
                partitions.add(p);
            }
            skipPartitionId = task.getSkipPartitionId(partitions);
            Assertions.assertEquals(2, skipPartitionId);

            // Test less than partition
            partitions.add(new Partition(4, "p4", new MaterializedIndex(), new RandomDistributionInfo()));
            partitions.add(new Partition(5, "p5", new MaterializedIndex(), new RandomDistributionInfo()));
            mockedStatisticsUtil.when(StatisticsUtil::getPartitionSampleCount).thenReturn(5);
            highKey.clear();
            highKey.add(new PartitionValue("2024-01-01"));
            Range<PartitionKey> range4 = Range.lessThan(PartitionKey.createPartitionKey(highKey, columns));
            RangePartitionItem item4 = new RangePartitionItem(range4);
            partitionInfo.addPartition(4, false, item4, new DataProperty(TStorageMedium.HDD), null, false, false);
            lowKey.clear();
            lowKey.add(new PartitionValue("2024-03-13"));
            highKey.clear();
            highKey.add(new PartitionValue("2024-03-14"));
            Range<PartitionKey> range5 = Range.closedOpen(PartitionKey.createPartitionKey(lowKey, columns),
                    PartitionKey.createPartitionKey(highKey, columns));
            RangePartitionItem item5 = new RangePartitionItem(range5);
            partitionInfo.addPartition(5, false, item5, new DataProperty(TStorageMedium.HDD), null, false, false);
            skipPartitionId = task.getSkipPartitionId(partitions);
            Assertions.assertEquals(4, skipPartitionId);
        }
    }

    @Test
    public void testGetSampleTablets() {
        OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
        Mockito.doReturn(4000000L).when(task).getSampleRows();

        OlapTable mockTable = Mockito.mock(OlapTable.class);
        task.tbl = mockTable;
        task.col = new Column("col1", PrimitiveType.STRING);
        task.info = new AnalysisInfoBuilder().setIndexId(-1L).build();
        task.tableSample = new TableSample(false, 4000000L, 0L);

        Mockito.when(mockTable.isPartitionColumn(ArgumentMatchers.any())).thenReturn(false);

        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        LocalTablet t = Mockito.mock(LocalTablet.class);

        List<Long> ret = Lists.newArrayList();
        ret.add(10001L);
        ret.add(10002L);
        Mockito.when(index.getTabletIdsInOrder()).thenReturn(ret);
        Mockito.when(index.getRowCount()).thenReturn(1_200_000_000L);
        Mockito.when(index.getTablet(ArgumentMatchers.anyLong())).thenReturn(t);

        final int[] i = {0};
        long[] tabletsRowCount = {1100000000, 100000000};
        Mockito.when(t.getMinReplicaRowCount(ArgumentMatchers.anyLong())).thenAnswer(inv -> tabletsRowCount[i[0]++]);

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getBaseIndex()).thenReturn(index);
        List<Partition> partitions = Lists.newArrayList();
        partitions.add(partition);
        Mockito.when(mockTable.getPartitions()).thenReturn((Collection) partitions);

        // Test set large tablet id back if it doesn't pick enough sample rows.
        Pair<List<Long>, Long> sampleTablets = task.getSampleTablets();
        Assertions.assertEquals(1, sampleTablets.first.size());
        Assertions.assertEquals(10001, sampleTablets.first.get(0));
        Assertions.assertEquals(1100000000L, sampleTablets.second);

        // Test normal pick
        task.tableSample = new TableSample(false, 4000000L, 1L);
        sampleTablets = task.getSampleTablets();
        Assertions.assertEquals(1, sampleTablets.first.size());
        Assertions.assertEquals(10002, sampleTablets.first.get(0));
        Assertions.assertEquals(100000000L, sampleTablets.second);
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
                + "HLL_CARDINALITY(HLL_UNION(ndv)) AS `ndv`, SUM(null_count) AS `null_count`, MIN(min) AS `min`, "
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
        params.put("catalogName", "internal");
        params.put("dbName", "db1");
        params.put("colName", "s");
        params.put("tblName", "tbl1");
        params.put("index", "");
        params.put("lengthAssert",
                ", assert_true(`s` IS NULL OR LENGTH(`s`) <= 1024, '"
                        + BaseAnalysisTask.ANALYZE_SKIP_LONG_STRING_COLUMN_MARKER + "') AS `__lc`");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(BaseAnalysisTask.FULL_ANALYZE_TEMPLATE);
        Assertions.assertTrue(sql.contains("FROM (SELECT `s`, assert_true("), sql);
        Assertions.assertTrue(sql.contains("IS NULL OR LENGTH(`s`) <= 1024"), sql);
        Assertions.assertTrue(sql.endsWith(") __lc_t"), sql);
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
        params.put("catalogName", "internal");
        params.put("dbName", "db1");
        params.put("colName", "id");
        params.put("tblName", "tbl1");
        params.put("index", "");
        params.put("lengthAssert", "");
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(BaseAnalysisTask.FULL_ANALYZE_TEMPLATE);
        Assertions.assertFalse(sql.contains("assert_true"), sql);
        Assertions.assertTrue(sql.contains("FROM (SELECT `id` FROM `internal`.`db1`.`tbl1`"), sql);
        Assertions.assertTrue(sql.endsWith(") __lc_t"), sql);
    }
}
