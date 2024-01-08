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
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class HMSAnalysisTaskTest {

    @Test
    public void testNeedLimit(@Mocked HMSExternalTable tableIf)
            throws Exception {

        new MockUp<HMSExternalTable>() {
            @Mock
            public List<Column> getFullSchema() {
                ArrayList<Column> objects = Lists.newArrayList();
                objects.add(new Column("int_column", PrimitiveType.INT));
                return objects;
            }
        };
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
    public void testAutoSampleHugeTable(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public long getDataSize(boolean singleReplica) {
                return 6L * 1024 * 1024 * 1024;
            }
        };
        HMSAnalysisTask task = new HMSAnalysisTask();
        task.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.SYSTEM);
        analysisInfoBuilder.setAnalysisMethod(AnalysisInfo.AnalysisMethod.FULL);
        task.info = analysisInfoBuilder.build();
        TableSample tableSample = task.getTableSample();
        Assertions.assertFalse(tableSample.isPercent());
        Assertions.assertEquals(StatisticsUtil.getHugeTableSampleRows(), tableSample.getSampleValue());
    }

    @Test
    public void testAutoSampleSmallTable(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() - 1;
            }
        };
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
    public void testManualFull(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public long getDataSize(boolean singleReplica) {
                return 1000;
            }
        };
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
    public void testManualSample(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public long getDataSize(boolean singleReplica) {
                return 1000;
            }
        };
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
    public void testGetSampleInfo(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public List<Long> getChunkSizes() {
                return Lists.newArrayList();
            }
        };
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
    public void testGetSampleInfoPercent(@Mocked HMSExternalTable tableIf)
            throws Exception {
        new MockUp<HMSExternalTable>() {
            @Mock
            public List<Long> getChunkSizes() {
                return Arrays.asList(1024L, 2048L);
            }
        };
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

    @Test
    public void testOrdinaryStats(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked HMSExternalTable tableIf)
            throws Exception {

        new Expectations() {
            {
                tableIf.getId();
                result = 30001;
                tableIf.getName();
                result = "test";
                catalogIf.getId();
                result = 10001;
                catalogIf.getName();
                result = "hms";
                databaseIf.getId();
                result = 20001;
                databaseIf.getFullName();
                result = "default";
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public Set<String> getPartitionNames() {
                return ImmutableSet.of("date=20230101/hour=12");
            }
        };

        new MockUp<HMSAnalysisTask>() {
            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals("SELECT CONCAT(30001, '-', -1, '-', 'hour') AS `id`,"
                        + "          10001 AS `catalog_id`,"
                        + "          20001 AS `db_id`,"
                        + "          30001 AS `tbl_id`,"
                        + "          -1 AS `idx_id`,"
                        + "          'hour' AS `col_id`,"
                        + "          NULL AS `part_id`,"
                        + "          COUNT(1) AS `row_count`,"
                        + "          NDV(`hour`) AS `ndv`,"
                        + "          COUNT(1) - COUNT(`hour`) AS `null_count`,"
                        + "          SUBSTRING(CAST(MIN(`hour`) AS STRING), 1, 1024) AS `min`,"
                        + "          SUBSTRING(CAST(MAX(`hour`) AS STRING), 1, 1024) AS `max`,"
                        + "          COUNT(1) * 4 AS `data_size`,"
                        + "          NOW() AS `update_time`"
                        + "  FROM `hms`.`default`.`test`", sql);
            }
        };

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.col = new Column("hour", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;
        task.setTable(tableIf);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("hour");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setUsingSqlForPartitionColumn(true);
        task.info = analysisInfoBuilder.build();

        task.getTableColumnStats();
    }


    @Test
    public void testPartitionHMSStats(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked HMSExternalTable tableIf)
            throws Exception {

        new Expectations() {
            {
                tableIf.getId();
                result = 30001;
                catalogIf.getId();
                result = 10001;
                catalogIf.getName();
                result = "hms";
                databaseIf.getId();
                result = 20001;
            }
        };

        new MockUp<HMSExternalTable>() {
            @Mock
            public Set<String> getPartitionNames() {
                return ImmutableSet.of("date=20230101/hour=12");
            }

            @Mock
            public List<Column> getPartitionColumns() {
                return ImmutableList.of(new Column("hour", PrimitiveType.INT));
            }
        };

        new MockUp<HMSAnalysisTask>() {
            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals(" SELECT CONCAT(30001, '-', -1, '-', 'hour') AS `id`, "
                        + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                        + "'hour' AS `col_id`, NULL AS `part_id`, 0 AS `row_count`, 1 AS `ndv`, "
                        + "0 AS `null_count`, SUBSTRING(CAST('12' AS STRING), 1, 1024) AS `min`, "
                        + "SUBSTRING(CAST('12' AS STRING), 1, 1024) AS `max`, 0 AS `data_size`, NOW() ", sql);
            }
        };

        HMSAnalysisTask task = new HMSAnalysisTask();
        task.col = new Column("hour", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;
        task.setTable(tableIf);

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("hour");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setUsingSqlForPartitionColumn(false);
        task.info = analysisInfoBuilder.build();

        task.getTableColumnStats();
    }
}
