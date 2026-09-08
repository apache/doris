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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BaseAnalysisTaskTest {

    @Test
    public void testGetFunctions() {
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        Column column = new Column("string_column", PrimitiveType.STRING);
        String dataSizeFunction = olapAnalysisTask.getDataSizeFunction(column, true);
        Assertions.assertEquals("SUM(`column_length`)", dataSizeFunction);
        dataSizeFunction = olapAnalysisTask.getDataSizeFunction(column, false);
        Assertions.assertEquals("SUM(LENGTH(`${colName}`))", dataSizeFunction);

        column = new Column("int_column", PrimitiveType.INT);
        dataSizeFunction = olapAnalysisTask.getDataSizeFunction(column, false);
        Assertions.assertEquals("COUNT(1) * 4", dataSizeFunction);
        dataSizeFunction = olapAnalysisTask.getDataSizeFunction(column, true);
        Assertions.assertEquals("SUM(t1.count) * 4", dataSizeFunction);

        String minFunction = olapAnalysisTask.getMinFunction();
        Assertions.assertEquals("CAST(MIN(`${colName}`) as ${type}) ", minFunction);
        olapAnalysisTask.tableSample = new TableSample(true, 20L);
        minFunction = olapAnalysisTask.getMinFunction();
        Assertions.assertEquals("NULL", minFunction);

        olapAnalysisTask.tableSample = null;
        String maxFunction = olapAnalysisTask.getMaxFunction();
        Assertions.assertEquals("CAST(MAX(`${colName}`) as ${type}) ", maxFunction);
        olapAnalysisTask.tableSample = new TableSample(true, 20L);
        maxFunction = olapAnalysisTask.getMaxFunction();
        Assertions.assertEquals("NULL", maxFunction);

        String ndvFunction = olapAnalysisTask.getNdvFunction(String.valueOf(100));
        Assertions.assertEquals("SUM(`t1`.`count`) * COUNT(`t1`.`col_value`) / (SUM(`t1`.`count`) - SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) + SUM(IF(`t1`.`count` = 1 and `t1`.`col_value` is not null, 1, 0)) * SUM(`t1`.`count`) / 100)", ndvFunction);
        System.out.println(ndvFunction);
    }

    @Test
    public void testNdvTooLarge() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add("100"); // count
        values.add("1100"); // ndv
        values.add("300"); // null
        values.add("min");
        values.add("max");
        values.add("400");
        values.add("500");
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());
        Assertions.assertEquals(ColumnStatistic.UNKNOWN, data.toColumnStatistic());
    }

    @Test
    public void testNdv0MinMaxExistsNullNotEqualCount() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add("500"); // count
        values.add("0"); // ndv
        values.add("300"); // null
        values.add("min");
        values.add("max");
        values.add("400");
        values.add("500");
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertFalse(data.isValid());
        Assertions.assertEquals(ColumnStatistic.UNKNOWN, data.toColumnStatistic());
    }

    @Test
    public void testReconcileStaleZeroRowCount() {
        // Case 1: initial rowCount==0, real base-index count>0 -> promote.
        AnalysisInfo info = new AnalysisInfoBuilder().setRowCount(0).build();
        BaseAnalysisTask.rejustStaleZeroRowCount(info, 100L, 720536L, 100L);
        Assertions.assertEquals(720536L, info.rowCount,
                "stale zero rowCount must be promoted from the real base-index scan count");

        // Case 2: initial rowCount already >0 -> preserved (never overwrite a live estimate).
        info = new AnalysisInfoBuilder().setRowCount(500L).build();
        BaseAnalysisTask.rejustStaleZeroRowCount(info, 100L, 720536L, 100L);
        Assertions.assertEquals(500L, info.rowCount,
                "non-zero rowCount must not be overwritten by a per-column scan result");

        // Case 3: initial rowCount==0 but real count is also 0 -> stay 0 (table really empty).
        info = new AnalysisInfoBuilder().setRowCount(0).build();
        BaseAnalysisTask.rejustStaleZeroRowCount(info, 100L, 0L, 100L);
        Assertions.assertEquals(0L, info.rowCount,
                "empty scan result must not spuriously bump rowCount");

        // Case 4: indexId != baseIndexId -> ignored (MV/rollup counts are not table row count).
        info = new AnalysisInfoBuilder().setRowCount(0).build();
        BaseAnalysisTask.rejustStaleZeroRowCount(info, 200L, 720536L, 100L);
        Assertions.assertEquals(0L, info.rowCount,
                "non-base index counts must not overwrite table-level rowCount");
    }
}
