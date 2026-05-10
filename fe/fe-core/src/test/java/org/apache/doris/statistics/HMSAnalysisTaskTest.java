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

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;

public class HMSAnalysisTaskTest {

    @Test
    public void testNeedLimit() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        ArrayList<Column> columns = Lists.newArrayList();
        columns.add(new Column("int_column", PrimitiveType.INT));
        Mockito.when(tableIf.getFullSchema()).thenReturn(columns);

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.setTable(tableIf);
        task.tableSample = new TableSample(true, 10L);
        Assertions.assertFalse(task.needLimit(100, 5.0));

        task.tableSample = new TableSample(false, 100L);
        Assertions.assertFalse(task.needLimit(100, 5.0));
        Assertions.assertTrue(task.needLimit(2L * 1024 * 1024 * 1024, 5.0));
        task.tableSample = new TableSample(false, 512L * 1024 * 1024);
        Assertions.assertFalse(task.needLimit(2L * 1024 * 1024 * 1024, 5.0));
    }

    @Test
    public void testAutoSampleSmallTable() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        Mockito.when(tableIf.getDataSize(Mockito.anyBoolean()))
                .thenReturn(StatisticsUtil.getHugeTableLowerBoundSizeInBytes() - 1);

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.SYSTEM);
        analysisInfoBuilder.setAnalysisMethod(AnalysisInfo.AnalysisMethod.FULL);
        task.info = analysisInfoBuilder.build();
        TableSample tableSample = task.getTableSample();
        Assertions.assertNull(tableSample);
    }

    @Test
    public void testManualFull() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        Mockito.when(tableIf.getDataSize(Mockito.anyBoolean())).thenReturn(1000L);

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setAnalysisMethod(AnalysisInfo.AnalysisMethod.FULL);
        task.info = analysisInfoBuilder.build();
        TableSample tableSample = task.getTableSample();
        Assertions.assertNull(tableSample);
    }

    @Test
    public void testManualSample() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        Mockito.when(tableIf.getDataSize(Mockito.anyBoolean())).thenReturn(1000L);

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setAnalysisMethod(AnalysisInfo.AnalysisMethod.SAMPLE);
        analysisInfoBuilder.setSampleRows(1000);
        task.info = analysisInfoBuilder.build();
        TableSample tableSample = task.getTableSample();
        Assertions.assertNotNull(tableSample);
        Assertions.assertEquals(1000, tableSample.getSampleValue());
    }

    @Test
    public void testGetSampleInfo() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        Mockito.when(tableIf.getChunkSizes()).thenReturn(Lists.newArrayList());

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.setTable(tableIf);
        task.tableSample = null;
        Pair<Double, Long> info1 = task.getSampleInfo();
        Assertions.assertEquals(1.0, info1.first);
        Assertions.assertEquals(0, info1.second);
        task.tableSample = new TableSample(false, 100L);
        Pair<Double, Long> info2 = task.getSampleInfo();
        Assertions.assertEquals(1.0, info2.first);
        Assertions.assertEquals(0, info2.second);
    }

    @Test
    public void testGetSampleInfoPercent() throws Exception {
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);
        Mockito.when(tableIf.getChunkSizes()).thenReturn(Arrays.asList(1024L, 2048L));

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.setTable(tableIf);
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setAnalysisMethod(AnalysisInfo.AnalysisMethod.SAMPLE);
        analysisInfoBuilder.setSamplePercent(10);
        task.info = analysisInfoBuilder.build();

        task.tableSample = new TableSample(true, 10L);
        Pair<Double, Long> info = task.getSampleInfo();
        Assertions.assertEquals(1.5, info.first);
        Assertions.assertEquals(2048, info.second);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOrdinaryStats() throws Exception {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);

        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(tableIf.getName()).thenReturn("test");
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("hms");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);
        Mockito.when(databaseIf.getFullName()).thenReturn("default");
        Mockito.when(tableIf.getPartitionNames()).thenReturn(ImmutableSet.of("date=20230101/hour=12"));

        HMSAnalysisTask task = Mockito.spy(new HMSAnalysisTask());
        Mockito.doAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            Assertions.assertEquals("SELECT CONCAT(30001, '-', -1, '-', 'hour') AS `id`, "
                    + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, "
                    + "-1 AS `idx_id`, 'hour' AS `col_id`, NULL AS `part_id`, "
                    + "COUNT(1) AS `row_count`, NDV(`hour`) AS `ndv`, "
                    + "COUNT(1) - COUNT(`hour`) AS `null_count`, "
                    + "SUBSTRING(CAST(MIN(`hour`) AS STRING), 1, 1024) AS `min`, "
                    + "SUBSTRING(CAST(MAX(`hour`) AS STRING), 1, 1024) AS `max`, "
                    + "COUNT(1) * 4 AS `data_size`, NOW() AS `update_time`, "
                    + "null as `hot_value` FROM (SELECT `hour` FROM `hms`.`default`.`test` ) __lc_t", sql);
            return null;
        }).when(task).runQuery(Mockito.anyString());

        task.col = new Column("hour", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;
        task.setTable(tableIf);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("hour");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setUsingSqlForExternalTable(true);
        task.info = analysisInfoBuilder.build();

        task.doExecute();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPartitionHMSStats() throws Exception {
        CatalogIf catalogIf = Mockito.mock(CatalogIf.class);
        DatabaseIf databaseIf = Mockito.mock(DatabaseIf.class);
        HMSExternalTable tableIf = Mockito.mock(HMSExternalTable.class);

        Mockito.when(tableIf.getId()).thenReturn(30001L);
        Mockito.when(catalogIf.getId()).thenReturn(10001L);
        Mockito.when(catalogIf.getName()).thenReturn("hms");
        Mockito.when(databaseIf.getId()).thenReturn(20001L);
        Mockito.when(tableIf.getPartitionNames()).thenReturn(ImmutableSet.of("date=20230101/hour=12"));
        Mockito.when(tableIf.getPartitionColumns())
                .thenReturn(ImmutableList.of(new Column("hour", PrimitiveType.INT)));

        HMSAnalysisTask task = Mockito.spy(new HMSAnalysisTask());
        Mockito.doAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            Assertions.assertEquals(" SELECT CONCAT(30001, '-', -1, '-', 'hour') AS `id`, "
                    + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                    + "'hour' AS `col_id`, NULL AS `part_id`, 0 AS `row_count`, 1 AS `ndv`, "
                    + "0 AS `null_count`, SUBSTRING(CAST('12' AS STRING), 1, 1024) AS `min`, "
                    + "SUBSTRING(CAST('12' AS STRING), 1, 1024) AS `max`, 0 AS `data_size`, NOW() ", sql);
            return null;
        }).when(task).runQuery(Mockito.anyString());

        task.col = new Column("hour", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;
        task.setTable(tableIf);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("hour");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setUsingSqlForExternalTable(false);
        task.info = analysisInfoBuilder.build();

        task.doExecute();
    }
}
