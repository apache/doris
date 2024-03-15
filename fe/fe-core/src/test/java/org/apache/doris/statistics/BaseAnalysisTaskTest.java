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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BaseAnalysisTaskTest {

    @Test
    public void testGetFunctions() {
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        Column column = new Column("string_column", PrimitiveType.STRING);
        String dataSizeFunction = olapAnalysisTask.getDataSizeFunction(column, true);
        Assertions.assertEquals("SUM(LENGTH(`column_key`) * count)", dataSizeFunction);
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

}
