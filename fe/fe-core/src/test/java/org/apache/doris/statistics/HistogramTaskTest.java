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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runners.MethodSorters;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class HistogramTaskTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("histogram_task_test");
        connectContext.setDatabase("histogram_task_test");
        createTable(
                "CREATE TABLE t1 (\n"
                        + "    col1 date not null, \n"
                        + "    col2 int not null, \n"
                        + "    col3 int not null\n"
                        + ")\n"
                        + "PARTITION BY LIST(`col1`)\n"
                        + "(\n"
                        + "    PARTITION `p_201701` VALUES IN (\"2017-10-01\"),\n"
                        + "    PARTITION `default`\n"
                        + ")\n"
                        + "DISTRIBUTED BY HASH(col3)\n"
                        + "BUCKETS 1\n"
                        + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n"
                        + ")");
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void test1TaskCreation() throws Exception {

        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        StmtExecutor executor = getSqlStmtExecutor(
                "ANALYZE TABLE t1(col1) WITH HISTOGRAM");
        Assertions.assertNotNull(executor);

        ConcurrentMap<Long, Map<Long, BaseAnalysisTask>> taskMap =
                Deencapsulation.getField(analysisManager, "analysisJobIdToTaskMap");
        Assertions.assertEquals(1, taskMap.size());

        for (Entry<Long, Map<Long, BaseAnalysisTask>> infoMap : taskMap.entrySet()) {
            Map<Long, BaseAnalysisTask> taskInfo = infoMap.getValue();
            Assertions.assertEquals(1, taskInfo.size());

            for (Entry<Long, BaseAnalysisTask> infoEntry : taskInfo.entrySet()) {
                BaseAnalysisTask task = infoEntry.getValue();
                Assertions.assertEquals("col1", task.info.colName);
            }
        }
    }

    @Test
    public void test2TaskExecution(@Mocked InternalCatalog catalog, @Mocked Database database,
            @Mocked OlapTable olapTable) throws Exception {
        new MockUp<StatisticsUtil>() {

            @Mock
            public DBObjects convertIdToObjects(long catalogId, long dbId, long tblId) {
                return new DBObjects(catalog, database, olapTable);
            }
        };
        new MockUp<OlapTable>() {

            @Mock
            public Column getColumn(String name) {
                return new Column("col1", PrimitiveType.INT);
            }
        };
        AnalysisTaskExecutor analysisTaskExecutor = new AnalysisTaskExecutor(1);
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder()
                .setJobId(0).setTaskId(0).setCatalogId(0)
                .setDBId(0)
                .setTblId(0)
                .setColName("col1").setJobType(JobType.MANUAL)
                .setAnalysisMode(AnalysisMode.FULL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.HISTOGRAM)
                .build();
        HistogramTask task = new HistogramTask(analysisInfo);

        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTaskStatus(AnalysisInfo info, AnalysisState jobState, String message, long time) {}
        };

        Deencapsulation.invoke(analysisTaskExecutor, "submitTask", task);
    }
}
