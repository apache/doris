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
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Pair;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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

}
