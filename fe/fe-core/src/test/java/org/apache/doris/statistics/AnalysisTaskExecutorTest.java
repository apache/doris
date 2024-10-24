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
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class AnalysisTaskExecutorTest extends TestWithFeService {


    @Override
    protected void runBeforeAll() throws Exception {
        try {
            InternalSchemaInitializer.createDb();
            createDatabase("analysis_job_test");
            connectContext.setDatabase("analysis_job_test");
            createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"

                    + "DISTRIBUTED BY HASH(col3)\n" + "BUCKETS 1\n"
                    + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n"
                    + ");");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExpiredJobCancellation(@Mocked InternalCatalog catalog, @Mocked Database database,
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
        final AtomicBoolean cancelled = new AtomicBoolean();
        new MockUp<AnalysisTaskWrapper>() {

            @Mock
            public boolean cancel(String msg) {
                cancelled.set(true);
                return true;
            }
        };
        AnalysisInfo analysisJobInfo = new AnalysisInfoBuilder().setJobId(0).setTaskId(0)
                .setCatalogId(0)
                .setDBId(0)
                .setTblId(0)
                .setColName("col1").setJobType(JobType.MANUAL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS)
                .build();
        OlapAnalysisTask analysisJob = new OlapAnalysisTask(analysisJobInfo);

        AnalysisTaskExecutor analysisTaskExecutor = new AnalysisTaskExecutor(1);
        AnalysisTaskWrapper analysisTaskWrapper = new AnalysisTaskWrapper(analysisTaskExecutor, analysisJob);
        Deencapsulation.setField(analysisTaskWrapper, "startTime", 5);
        analysisTaskExecutor.putJob(analysisTaskWrapper);
        analysisTaskExecutor.tryToCancel();
        Assertions.assertTrue(cancelled.get());
        Assertions.assertEquals(0, analysisTaskExecutor.getTaskQueue().size());
    }

    @Test
    public void testTaskExecution(@Mocked InternalCatalog catalog, @Mocked Database database,
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
        new MockUp<StmtExecutor>() {
            @Mock
            public List<ResultRow> executeInternalQuery() {
                return Collections.emptyList();
            }
        };

        new MockUp<OlapAnalysisTask>() {
            @Mock
            public void execSQLs(List<String> partitionAnalysisSQLs, Map<String, String> params) throws Exception {
            }

            @Mock
            protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
                // DO NOTHING
            }
        };

        new MockUp<StatisticsCache>() {

            @Mock
            public void syncLoadColStats(long tableId, long idxId, String colName) {
            }
        };

        AnalysisTaskExecutor analysisTaskExecutor = new AnalysisTaskExecutor(1);
        Set<Pair<String, String>> columns = Sets.newHashSet();
        columns.add(Pair.of("col1", "t1"));
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setJobId(0).setTaskId(0)
                .setCatalogId(0).setDBId(0).setTblId(0)
                .setColName("col1").setJobType(JobType.MANUAL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS)
                .setState(AnalysisState.RUNNING)
                .setJobColumns(columns)
                .build();
        OlapAnalysisTask task = new OlapAnalysisTask(analysisInfo);

        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTaskStatus(AnalysisInfo info, AnalysisState jobState, String message, long time) {}
        };

        Deencapsulation.invoke(analysisTaskExecutor, "submitTask", task);
    }
}
