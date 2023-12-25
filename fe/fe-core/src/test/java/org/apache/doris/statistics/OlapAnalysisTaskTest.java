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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    // test auto big table
    @Test
    public void testSample2(@Mocked OlapTable tbl) {
        new MockUp<OlapTable>() {

            @Mock
            public long getDataSize(boolean singleReplica) {
                return 1000_0000_0000L;
            }
        };

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder()
                .setAnalysisMethod(AnalysisMethod.FULL);
        analysisInfoBuilder.setJobType(JobType.SYSTEM);
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.tbl = tbl;
        TableSample tableSample = olapAnalysisTask.getTableSample();
        Assertions.assertNotNull(tableSample);
        Assertions.assertEquals(StatisticsUtil.getHugeTableSampleRows(), tableSample.getSampleValue());

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
    public void testManualSampleNonDistributeKey(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf)
            throws Exception {

        new Expectations() {
            {
                tableIf.getRowCount();
                result = 500;
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
            public Pair<List<Long>, Long> calcActualSampleTablets() {
                return Pair.of(Lists.newArrayList(), 100L);
            }

            @Mock
            public ResultRow collectBasicStat(AutoCloseConnectContext context) {
                List<String> values = Lists.newArrayList();
                values.add("1");
                values.add("2");
                return new ResultRow(values);
            }

            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals("SELECT CONCAT('30001', '-', '-1', '-', 'null') "
                        + "AS `id`, 10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, "
                        + "-1 AS `idx_id`, 'null' AS `col_id`, NULL AS `part_id`, 500 AS"
                        + " `row_count`, SUM(`t1`.`count`) * COUNT(1) / (SUM(`t1`.`count`)"
                        + " - SUM(IF(`t1`.`count` = 1, 1, 0)) + SUM(IF(`t1`.`count` = 1, 1, 0))"
                        + " * SUM(`t1`.`count`) / 500) as `ndv`, IFNULL(SUM(IF(`t1`.`column_key`"
                        + " IS NULL, `t1`.`count`, 0)), 0) * 5.0 as `null_count`, "
                        + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`,"
                        + " SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "SUM(LENGTH(`column_key`) * count) * 5.0 AS `data_size`, NOW() "
                        + "FROM (     SELECT t0.`${colName}` as `column_key`, COUNT(1) "
                        + "as `count`     FROM     (SELECT `${colName}` FROM "
                        + "`catalogName`.`${dbName}`.`${tblName}`     "
                        + " limit 100) as `t0`     GROUP BY `t0`.`${colName}` ) as `t1` ", sql);
                return;
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public AutoCloseConnectContext buildConnectContext(boolean scanLimit) {
                return null;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Set<String> getDistributionColumnNames() {
                return Sets.newHashSet();
            }
        };

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.col = new Column("test", PrimitiveType.STRING);
        olapAnalysisTask.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.catalog = catalogIf;
        olapAnalysisTask.db = databaseIf;
        olapAnalysisTask.tableSample = new TableSample(false, 100L);
        olapAnalysisTask.doSample();
    }

    @Test
    public void testManualSampleDistributeKey(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf)
            throws Exception {

        new Expectations() {
            {
                tableIf.getRowCount();
                result = 500;
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
            public Pair<List<Long>, Long> calcActualSampleTablets() {
                return Pair.of(Lists.newArrayList(), 100L);
            }

            @Mock
            public ResultRow collectBasicStat(AutoCloseConnectContext context) {
                List<String> values = Lists.newArrayList();
                values.add("1");
                values.add("2");
                return new ResultRow(values);
            }

            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals(" "
                        + "SELECT CONCAT(30001, '-', -1, '-', 'null') AS `id`, "
                        + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, "
                        + "-1 AS `idx_id`, 'null' AS `col_id`, NULL AS `part_id`, "
                        + "500 AS `row_count`, ROUND(NDV(`${colName}`) * 5.0) as `ndv`, "
                        + "ROUND(SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) * 5.0) "
                        + "AS `null_count`, SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, "
                        + "SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "SUM(LENGTH(`${colName}`)) * 5.0 AS `data_size`, NOW() "
                        + "FROM `catalogName`.`${dbName}`.`${tblName}`  limit 100", sql);
                return;
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public AutoCloseConnectContext buildConnectContext(boolean scanLimit) {
                return null;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Set<String> getDistributionColumnNames() {
                HashSet<String> cols = Sets.newHashSet();
                cols.add("test");
                return cols;
            }

            @Mock
            public boolean isDistributionColumn(String columnName) {
                return true;
            }
        };

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.col = new Column("test", PrimitiveType.STRING);
        olapAnalysisTask.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.catalog = catalogIf;
        olapAnalysisTask.db = databaseIf;
        olapAnalysisTask.tableSample = new TableSample(false, 100L);
        olapAnalysisTask.doSample();
    }

    @Test
    public void testManualSampleTwoDistributeKey(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked OlapTable tableIf)
            throws Exception {

        new Expectations() {
            {
                tableIf.getRowCount();
                result = 500;
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
            public Pair<List<Long>, Long> calcActualSampleTablets() {
                return Pair.of(Lists.newArrayList(), 100L);
            }

            @Mock
            public ResultRow collectBasicStat(AutoCloseConnectContext context) {
                List<String> values = Lists.newArrayList();
                values.add("1");
                values.add("2");
                return new ResultRow(values);
            }

            @Mock
            public void runQuery(String sql) {
                System.out.println(sql);
                Assertions.assertEquals("SELECT CONCAT('30001', '-', '-1', '-', 'null') "
                        + "AS `id`, 10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, "
                        + "-1 AS `idx_id`, 'null' AS `col_id`, NULL AS `part_id`,"
                        + " 500 AS `row_count`, SUM(`t1`.`count`) * COUNT(1) / (SUM(`t1`.`count`) "
                        + "- SUM(IF(`t1`.`count` = 1, 1, 0)) + SUM(IF(`t1`.`count` = 1, 1, 0)) * "
                        + "SUM(`t1`.`count`) / 500) as `ndv`, IFNULL(SUM(IF(`t1`.`column_key` "
                        + "IS NULL, `t1`.`count`, 0)), 0) * 5.0 as `null_count`, "
                        + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, "
                        + "SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "SUM(LENGTH(`column_key`) * count) * 5.0 AS `data_size`, NOW() "
                        + "FROM (     SELECT t0.`${colName}` as `column_key`, COUNT(1) as `count`     FROM     (SELECT `${colName}` FROM `catalogName`.`${dbName}`.`${tblName}`      limit 100) as `t0`     GROUP BY `t0`.`${colName}` ) as `t1` ", sql);
                return;
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public AutoCloseConnectContext buildConnectContext(boolean scanLimit) {
                return null;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Set<String> getDistributionColumnNames() {
                HashSet<String> cols = Sets.newHashSet();
                cols.add("test1");
                cols.add("test2");
                return cols;
            }
        };

        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.col = new Column("test1", PrimitiveType.STRING);
        olapAnalysisTask.tbl = tableIf;
        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.catalog = catalogIf;
        olapAnalysisTask.db = databaseIf;
        olapAnalysisTask.tableSample = new TableSample(false, 100L);
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
    }

}
