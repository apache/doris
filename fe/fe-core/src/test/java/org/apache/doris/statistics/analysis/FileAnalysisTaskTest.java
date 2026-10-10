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
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.statistics.repository.ResultRow;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class FileAnalysisTaskTest {
    @Test
    void testFullAndSampleNeverCompareFile() throws Exception {
        CapturingTask task = new CapturingTask();
        task.doFull();
        assertParentStats(task.sql);
        Assertions.assertTrue(task.sql.contains("COUNT(1) AS `row_count`"));
        Assertions.assertFalse(task.sql.contains("TABLET("));
        for (BaseAnalysisTask.AnalyzeSampleAlgorithm algorithm : BaseAnalysisTask.AnalyzeSampleAlgorithm.values()) {
            task.algorithm = algorithm;
            task.tableSample = new TableSample(false, 10L);
            task.doSample();
            assertParentStats(task.sql);
            if (algorithm == BaseAnalysisTask.AnalyzeSampleAlgorithm.FULL) {
                Assertions.assertFalse(task.sql.contains("TABLET("));
                Assertions.assertTrue(task.sql.contains("COUNT(1) AS `row_count`"));
            } else {
                Assertions.assertTrue(task.sql.contains("TABLET(7) limit 10"));
                Assertions.assertTrue(task.sql.contains("* 10.0 AS `data_size`"));
                Assertions.assertTrue(task.sql.contains("100 AS `row_count`"));
            }
        }
    }

    @Test
    void testPartitionCollectionAndMerge() {
        CapturingTask task = new CapturingTask();
        Map<String, String> params = task.buildSqlParams();
        params.put("partName", "'p1'");
        params.put("partId", "7");
        params.put("partitionInfo", "PARTITION p1");
        String partition = new StringSubstitutor(params).replace(BaseAnalysisTask.FILE_PARTITION_ANALYZE_TEMPLATE);
        assertNoComparison(partition);
        Assertions.assertTrue(partition.contains("HLL_EMPTY() AS `ndv`"));
        Assertions.assertTrue(partition.contains("__file_data_size(`f`)"));
        Assertions.assertTrue(partition.contains("COUNT(1) - COUNT(`__file_bytes`)"));
        String merge = new StringSubstitutor(params).replace(BaseAnalysisTask.FILE_MERGE_PARTITION_TEMPLATE);
        assertNoComparison(merge);
        Assertions.assertFalse(merge.contains("HLL_"));
        Assertions.assertTrue(merge.contains("NULL AS `ndv`"));
        Assertions.assertTrue(merge.contains("COALESCE(SUM(null_count), 0)"));
        Assertions.assertTrue(merge.contains("COALESCE(SUM(data_size_in_bytes), 0)"));
    }

    @Test
    void testSizeFunctionAndScalarGetterColumnUnchanged() {
        CapturingTask task = new CapturingTask();
        Assertions.assertEquals("COALESCE(SUM(__file_data_size(${colName})), 0)",
                task.getDataSizeFunction(task.col, false));
        Assertions.assertEquals("COUNT(1) * 8", task.getDataSizeFunction(new Column("size", Type.BIGINT), false));
    }

    private static void assertParentStats(String sql) {
        assertNoComparison(sql);
        Assertions.assertTrue(sql.contains("NULL AS `ndv`"));
        Assertions.assertTrue(sql.contains("NULL AS `min`, NULL AS `max`"));
        Assertions.assertTrue(sql.contains("COALESCE(SUM(`__file_bytes`), 0)"));
        Assertions.assertTrue(sql.contains("__file_data_size(`f`) AS `__file_bytes`"));
        Assertions.assertTrue(sql.contains("COUNT(1) - COUNT(`__file_bytes`)"));
    }

    private static void assertNoComparison(String sql) {
        String upper = sql.toUpperCase(Locale.ROOT);
        for (String forbidden : new String[] {"NDV(", "MIN(", "MAX(", "HLL_HASH(", "GROUP BY", "ORDER BY"}) {
            Assertions.assertFalse(upper.contains(forbidden), sql);
        }
        Assertions.assertFalse(sql.contains("${"), sql);
    }

    private static class CapturingTask extends OlapAnalysisTask {
        private String sql;
        private AnalyzeSampleAlgorithm algorithm;

        CapturingTask() {
            col = new Column("f", FileType.create());
            info = new AnalysisInfoBuilder().setIndexId(-1L).setColName("f").setCollectHotValue(true).build();
            OlapTable table = Mockito.mock(OlapTable.class);
            Mockito.when(table.getRowCount()).thenReturn(100L);
            Mockito.when(table.getKeysType()).thenReturn(KeysType.DUP_KEYS);
            tbl = table;
        }

        @Override
        protected Map<String, String> buildSqlParams() {
            Map<String, String> params = new HashMap<>();
            params.put("catalogId", "0");
            params.put("dbId", "1");
            params.put("tblId", "2");
            params.put("idxId", "-1");
            params.put("colId", "f");
            params.put("colName", "`f`");
            params.put("catalogName", "internal");
            params.put("dbName", "db");
            params.put("tblName", "t");
            params.put("index", "");
            params.put("preAggHint", "");
            return params;
        }

        @Override
        protected SampleCollectInfo getSampleCollectInfo(long count) {
            return new SampleCollectInfo(algorithm, Pair.of(Collections.singletonList(7L), 20L));
        }

        @Override
        protected ResultRow collectMinMax() {
            throw new AssertionError("FILE must never issue the separate MIN/MAX query");
        }

        @Override
        protected void runQuery(String sql) {
            this.sql = sql;
        }
    }
}
