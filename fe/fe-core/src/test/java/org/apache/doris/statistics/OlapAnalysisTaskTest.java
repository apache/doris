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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OlapAnalysisTaskTest {

    @Test
    public void testAutoSample(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked TableIf tableIf) {
        new Expectations() {
            {
                tableIf.getDataSize();
                result = 60_0000_0000L;
            }
        };

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder()
                .setAnalysisMethod(AnalysisMethod.FULL);
        OlapAnalysisTask olapAnalysisTask = new OlapAnalysisTask();
        olapAnalysisTask.info = analysisInfoBuilder.build();
        olapAnalysisTask.tbl = tableIf;
        Config.enable_auto_sample = true;
        String sampleExpr = olapAnalysisTask.getSampleExpression();
        Assertions.assertEquals("TABLESAMPLE(200000 ROWS)", sampleExpr);

        new Expectations() {
            {
                tableIf.getDataSize();
                result = 1_0000_0000L;
            }
        };
        sampleExpr = olapAnalysisTask.getSampleExpression();
        Assertions.assertEquals("", sampleExpr);

        analysisInfoBuilder.setSampleRows(10);
        analysisInfoBuilder.setAnalysisMethod(AnalysisMethod.SAMPLE);
        olapAnalysisTask.info = analysisInfoBuilder.build();
        sampleExpr = olapAnalysisTask.getSampleExpression();
        Assertions.assertEquals("TABLESAMPLE(10 ROWS)", sampleExpr);

    }

}
