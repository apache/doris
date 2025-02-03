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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
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
        Assertions.assertEquals("SUM(`t1`.`count`) * COUNT(1) / (SUM(`t1`.`count`) - SUM(IF(`t1`.`count` = 1, 1, 0)) "
                + "+ SUM(IF(`t1`.`count` = 1, 1, 0)) * SUM(`t1`.`count`) / 100)", ndvFunction);
        System.out.println(ndvFunction);
    }

    @Test
    public void testInvalidColStats() {
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
        ResultRow row = new ResultRow(values);
        List<ResultRow> result = Lists.newArrayList();
        result.add(row);

        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() {
                return result;
            }
        };
        BaseAnalysisTask task = new OlapAnalysisTask();
        try {
            task.runQuery("test");
        } catch (Exception e) {
            Assertions.assertEquals(e.getMessage(),
                    "ColStatsData is invalid, skip analyzing. "
                        + "('id',10000,20000,30000,0,'col',null,100,1100,300,'min','max',400,'500')");
            return;
        }
        Assertions.fail();
    }

}
