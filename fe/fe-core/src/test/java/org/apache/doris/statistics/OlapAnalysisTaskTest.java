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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OlapAnalysisTaskTest {

    // test manual
    @Test
    public void testSample1(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked TableIf tableIf) {

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
    public void testSample3(@Mocked OlapTable tbl) {
        new MockUp<OlapTable>() {

            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() - 1;
            }
        };

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
    public void testKeyColumnUseLimitAndNot(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf) {

        new Expectations() {
            {
                tableIf.getRowCount();
                result = 20000000;
                tableIf.getId();
                result = 30001;
                catalogIf.getId();
                result = 10001;
                catalogIf.getName();
                result = "catalogName";
                databaseIf.getId();
                result = 20001;
            }
        };

        new MockUp<OlapAnalysisTask>() {
            @Mock
            public ResultRow collectMinMax() {
                List<String> values = Lists.newArrayList();
                values.add("1");
                values.add("2");
                return new ResultRow(values);
            }

            @Mock
            protected void getSampleParams(Map<String, String> params, long tableRowCount) {}

            @Mock
            protected boolean useLinearAnalyzeTemplate() {
                return true;
            }

                @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals("SELECT CONCAT(30001, '-', -1, '-', 'null') AS `id`, "
                        + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                        + "'null' AS `col_id`, NULL AS `part_id`, ${rowCount} AS `row_count`, "
                        + "${ndvFunction} as `ndv`, ROUND(SUM(CASE WHEN `null` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor}) AS `null_count`, "
                        + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "COUNT(1) * 4 * ${scaleFactor} AS `data_size`, NOW() FROM "
                        + "( SELECT * FROM `catalogName`.`${dbName}`.`null`  ${sampleHints} ${limit})  as t", sql);
                return;
            }
        };

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
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

        new MockUp<OlapAnalysisTask>() {
            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals("SELECT CONCAT('30001', '-', '-1', '-', 'null') AS `id`, "
                        + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                        + "'null' AS `col_id`, NULL AS `part_id`, ${rowCount} AS `row_count`, ${ndvFunction} as `ndv`, "
                        + "IFNULL(SUM(IF(`t1`.`column_key` IS NULL, `t1`.`count`, 0)), 0) * ${scaleFactor} as `null_count`, "
                        + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "COUNT(1) * 4 * ${scaleFactor} AS `data_size`, NOW() "
                        + "FROM (     SELECT t0.`colValue` as `column_key`, COUNT(1) as `count`, SUM(`len`) as `column_length`     "
                        + "FROM         (SELECT ${subStringColName} AS `colValue`, LENGTH(`null`) as `len`         "
                        + "FROM `catalogName`.`${dbName}`.`null`  ${sampleHints} ${limit}) as `t0`     GROUP BY `t0`.`colValue` ) as `t1` ", sql);
                return;
            }

            @Mock
            protected boolean useLinearAnalyzeTemplate() {
                return false;
            }
        };
        olapAnalysisTask.doSample();
    }

    @Test
    public void testNeedLimitFalse(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf)
            throws Exception {

        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                ArrayList<Column> columns = Lists.newArrayList();
                columns.add(new Column("test", PrimitiveType.STRING));
                return new PartitionInfo(PartitionType.RANGE, columns);
            }

            @Mock
            public boolean isPartitionColumn(String columnName) {
                return true;
            }
        };

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
    public void testNeedLimitTrue(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf)
            throws Exception {

        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                ArrayList<Column> columns = Lists.newArrayList();
                columns.add(new Column("NOFOUND", PrimitiveType.STRING));
                return new PartitionInfo(PartitionType.RANGE, columns);
            }
        };

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
        task.setTable(new OlapTable());
        Partition p1 = new Partition(1, "p1", new MaterializedIndex(), new RandomDistributionInfo());
        Partition p2 = new Partition(2, "p2", new MaterializedIndex(), new RandomDistributionInfo());
        Partition p3 = new Partition(3, "p3", new MaterializedIndex(), new RandomDistributionInfo());
        List<Partition> partitions = Lists.newArrayList();
        partitions.add(p1);
        partitions.add(p2);
        partitions.add(p3);
        List<Long> ids = Lists.newArrayList();

        new MockUp<OlapTable>() {
            @Mock
            public long getRowCount() {
                return 1000000000L;
            }
        };

        long[] partitionRows = new long[3];
        partitionRows[0] = 400000000L;
        partitionRows[1] = 100000000L;
        partitionRows[2] = 500000000L;
        final int[] i = {0};
        new MockUp<Partition>() {
            @Mock
            public long getRowCount() {
                return partitionRows[i[0]++];
            }

            @Mock
            public MaterializedIndex getIndex(long indexId) {
                return new MaterializedIndex();
            }
        };

        final int[] j = {0};
        new MockUp<MaterializedIndex>() {
            @Mock
            public List<Long> getTabletIdsInOrder() {
                List<Long> ret = new ArrayList<>();
                ret.add((long) j[0]++);
                ret.add((long) j[0]++);
                return ret;
            }
        };
        long rows = task.pickSamplePartition(partitions, ids);
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
        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected boolean isSingleUniqueKey() {
                return true;
            }
        };
        Assertions.assertTrue(task.useLinearAnalyzeTemplate());
    }

    @Test
    public void testGetSampleParams() {
        OlapAnalysisTask task = new OlapAnalysisTask();
        Map<String, String> params = Maps.newHashMap();
        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected long getSampleRows() {
                return 100;
            }

            @Mock
            protected Pair<List<Long>, Long> getSampleTablets() {
                List<Long> ids = Lists.newArrayList();
                ids.add(1L);
                ids.add(2L);
                return Pair.of(ids, 100L);
            }

            @Mock
            protected boolean needLimit() {
                return false;
            }

            @Mock
            protected boolean useLinearAnalyzeTemplate() {
                return false;
            }
        };
        task.col = new Column("test", PrimitiveType.INT);
        task.getSampleParams(params, 10);
        Assertions.assertTrue(task.scanFullTable());
        Assertions.assertEquals("1", params.get("scaleFactor"));
        Assertions.assertEquals("", params.get("sampleHints"));
        Assertions.assertEquals("ROUND(NDV(`${colName}`) * ${scaleFactor})", params.get("ndvFunction"));
        params.clear();

        task = new OlapAnalysisTask();
        task.col = new Column("test", PrimitiveType.INT);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("SUM(`t1`.`count`) * COUNT(1) / (SUM(`t1`.`count`) - SUM(IF(`t1`.`count` = 1, 1, 0)) + SUM(IF(`t1`.`count` = 1, 1, 0)) * SUM(`t1`.`count`) / 1000)", params.get("ndvFunction"));
        Assertions.assertEquals("SUM(t1.count) * 4", params.get("dataSizeFunction"));
        Assertions.assertEquals("`${colName}`", params.get("subStringColName"));
        params.clear();

        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected boolean useLinearAnalyzeTemplate() {
                return true;
            }

            @Mock
            protected boolean isSingleUniqueKey() {
                return false;
            }
        };

        task = new OlapAnalysisTask();
        task.col = new Column("test", PrimitiveType.INT);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("ROUND(NDV(`${colName}`) * ${scaleFactor})", params.get("ndvFunction"));
        params.clear();

        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected boolean isSingleUniqueKey() {
                return true;
            }
        };
        task = new OlapAnalysisTask();
        task.col = new Column("test", PrimitiveType.INT);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("1000", params.get("ndvFunction"));
        params.clear();

        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected boolean needLimit() {
                return true;
            }

            @Mock
            protected long getSampleRows() {
                return 50;
            }
        };
        task = new OlapAnalysisTask();
        task.col = new Column("test", PrimitiveType.INT);
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("20.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("1000", params.get("ndvFunction"));
        Assertions.assertEquals("limit 50", params.get("limit"));
        params.clear();

        task = new OlapAnalysisTask();
        task.col = new Column("test", Type.fromPrimitiveType(PrimitiveType.INT),
            true, null, null, null);
        task.setKeyColumnSampleTooManyRows(true);
        task.getSampleParams(params, 2000000000);
        Assertions.assertEquals("2.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("2000000000", params.get("ndvFunction"));
        Assertions.assertEquals("limit 1000000000", params.get("limit"));
    }
}
