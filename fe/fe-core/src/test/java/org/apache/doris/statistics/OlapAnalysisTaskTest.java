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
import org.apache.doris.catalog.Tablet;
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
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
            void getSampleParams(Map<String, String> params, long tableRowCount) {}

            @Mock
            boolean useLinearAnalyzeTemplate() {
                return true;
            }

                @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals("WITH cte1 AS (SELECT `null` FROM `catalogName`.`${dbName}`.`null`  "
                        + "${sampleHints} ${limit} ), cte2 AS (SELECT CONCAT(30001, '-', -1, '-', 'null') AS `id`, "
                        + "10001 AS `catalog_id`, 20001 AS `db_id`, 30001 AS `tbl_id`, -1 AS `idx_id`, "
                        + "'null' AS `col_id`, NULL AS `part_id`, ${rowCount} AS `row_count`, ${ndvFunction} as `ndv`, "
                        + "ROUND(SUM(CASE WHEN `null` IS NULL THEN 1 ELSE 0 END) * ${scaleFactor}) AS `null_count`, "
                        + "SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, "
                        + "SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `max`, "
                        + "COUNT(1) * 4 * ${scaleFactor} AS `data_size`, NOW() FROM cte1), "
                        + "cte3 AS (SELECT GROUP_CONCAT(CONCAT(REPLACE(REPLACE(t.`column_key`, "
                        + "\":\", \"\\\\:\"), \";\", \"\\\\;\"), \" :\", ROUND(t.`count` * 100.0 / ${rowCount2}, 2)), "
                        + "\" ;\") as `hot_value` FROM (SELECT ${subStringColName} as `hash_value`, "
                        + "MAX(`null`) as `column_key`, COUNT(1) AS `count` FROM cte1 "
                        + "WHERE `null` IS NOT NULL GROUP BY `hash_value` ORDER BY `count` DESC LIMIT 3) t) "
                        + "SELECT * FROM cte2 CROSS JOIN cte3", sql);
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
                        + "GROUP_CONCAT(CONCAT(REPLACE(REPLACE(t2.`col_value`, \":\", \"\\\\:\"), \";\", \"\\\\;\"), "
                        + "\" :\", ROUND(t2.`count` * 100.0 / ${rowCount2}, 2)), \" ;\") as `hot_value` FROM (SELECT "
                        + "`col_value`, `count` FROM cte1 WHERE `col_value` IS NOT NULL ORDER BY `count` DESC LIMIT 3) "
                        + "t2) SELECT * FROM cte2 CROSS JOIN cte3", sql);
            }

            @Mock
            boolean useLinearAnalyzeTemplate() {
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
            public boolean isPartitionColumn(Column column) {
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

        new MockUp<OlapTable>() {
            @Mock
            public KeysType getKeysType() {
                return KeysType.DUP_KEYS;
            }
        };
        task.col = new Column("testColumn", Type.INT, true, null, null, "");
        task.setTable(new OlapTable());
        task.getSampleParams(params, 10);
        Assertions.assertTrue(task.scanFullTable());
        Assertions.assertEquals("1", params.get("scaleFactor"));
        Assertions.assertEquals("", params.get("sampleHints"));
        Assertions.assertEquals("ROUND(NDV(`${colName}`) * ${scaleFactor})", params.get("ndvFunction"));
        Assertions.assertNull(params.get("preAggHint"));
        params.clear();

        new MockUp<OlapTable>() {
            @Mock
            public KeysType getKeysType() {
                return KeysType.AGG_KEYS;
            }
        };
        task = new OlapAnalysisTask();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(new OlapTable());
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("10.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("SUM(`t1`.`count`) * COUNT(`t1`.`col_value`) / (SUM(`t1`.`count`) - SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) + SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) * SUM(`t1`.`count`) / 1000)", params.get("ndvFunction"));
        Assertions.assertEquals("SUM(t1.count) * 4", params.get("dataSizeFunction"));
        Assertions.assertEquals("`${colName}`", params.get("subStringColName"));
        Assertions.assertEquals("/*+PREAGGOPEN*/", params.get("preAggHint"));
        params.clear();

        new MockUp<OlapTable>() {
            @Mock
            public KeysType getKeysType() {
                return KeysType.UNIQUE_KEYS;
            }

            @Mock
            public boolean isUniqKeyMergeOnWrite() {
                return false;
            }
        };
        task = new OlapAnalysisTask();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(new OlapTable());
        task.getSampleParams(params, 1000);
        Assertions.assertEquals("/*+PREAGGOPEN*/", params.get("preAggHint"));
        params.clear();

        new MockUp<OlapTable>() {
            @Mock
            public boolean isUniqKeyMergeOnWrite() {
                return true;
            }
        };
        task = new OlapAnalysisTask();
        task.col = new Column("testColumn", Type.INT, false, null, null, "");
        task.setTable(new OlapTable());
        task.getSampleParams(params, 1000);
        Assertions.assertNull(params.get("preAggHint"));
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
        task.setTable(new OlapTable());
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
        task.setTable(new OlapTable());
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
        task.setTable(new OlapTable());
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
        task.setTable(new OlapTable());
        task.getSampleParams(params, 2000000000);
        Assertions.assertEquals("2.0", params.get("scaleFactor"));
        Assertions.assertEquals("TABLET(1, 2)", params.get("sampleHints"));
        Assertions.assertEquals("2000000000", params.get("ndvFunction"));
        Assertions.assertEquals("limit 1000000000", params.get("limit"));
    }

    @Test
    public void testGetSkipPartitionId(@Mocked OlapTable tableIf) throws AnalysisException {
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
        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                return new PartitionInfo(PartitionType.LIST);
            }
        };
        task.tbl = tableIf;
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test Unpartition return NO_SKIP_TABLET_ID
        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                return new PartitionInfo(PartitionType.UNPARTITIONED);
            }
        };
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test more than 1 partition column return NO_SKIP_TABLET_ID
        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                ArrayList<Column> columns = Lists.newArrayList();
                columns.add(new Column("col1", PrimitiveType.DATEV2));
                columns.add(new Column("col2", PrimitiveType.DATEV2));
                return new PartitionInfo(PartitionType.RANGE, columns);
            }
        };
        skipPartitionId = task.getSkipPartitionId(partitions);
        Assertions.assertEquals(OlapAnalysisTask.NO_SKIP_TABLET_ID, skipPartitionId);

        // Test not Date type return NO_SKIP_TABLET_ID
        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                ArrayList<Column> columns = Lists.newArrayList();
                columns.add(new Column("col1", PrimitiveType.STRING));
                return new PartitionInfo(PartitionType.RANGE, columns);
            }
        };
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

        new MockUp<OlapTable>() {
            @Mock
            public PartitionInfo getPartitionInfo() {
                return partitionInfo;
            }
        };
        new MockUp<StatisticsUtil>() {
            @Mock
            public int getPartitionSampleCount() {
                return 3;
            }
        };
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
        new MockUp<StatisticsUtil>() {
            @Mock
            public int getPartitionSampleCount() {
                return 5;
            }
        };
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

    @Test
    public void testGetSampleTablets(@Mocked MaterializedIndex index, @Mocked Tablet t) {
        OlapAnalysisTask task = new OlapAnalysisTask();
        task.tbl = new OlapTable();
        task.col = new Column("col1", PrimitiveType.STRING);
        task.info = new AnalysisInfoBuilder().setIndexId(-1L).build();
        task.tableSample = new TableSample(false, 4000000L, 0L);
        List<Partition> partitions = Lists.newArrayList();
        partitions.add(new Partition(1, "p1", new MaterializedIndex(), new RandomDistributionInfo()));
        final int[] i = {0};
        long[] tabletsRowCount = {1100000000, 100000000};
        List<Long> ret = Lists.newArrayList();
        ret.add(10001L);
        ret.add(10002L);
        new MockUp<OlapAnalysisTask>() {
            @Mock
            protected long getSampleRows() {
                return 4000000;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            boolean isPartitionColumn(Column column) {
                return false;
            }

            @Mock
            public Collection<Partition> getPartitions() {
                return partitions;
            }
        };
        new MockUp<Partition>() {
            @Mock
            public MaterializedIndex getBaseIndex() {
                return index;
            }
        };
        new MockUp<MaterializedIndex>() {
            @Mock
            public List<Long> getTabletIdsInOrder() {
                return ret;
            }

            @Mock
            public long getRowCount() {
                return 1_200_000_000L;
            }

            @Mock
            public Tablet getTablet(long tabletId) {
                return t;
            }
        };
        new MockUp<Tablet>() {
            @Mock
            public long getMinReplicaRowCount(long version) {
                return tabletsRowCount[i[0]++];
            }
        };
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
}
