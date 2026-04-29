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
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
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
    @Timeout(60)
    public void testExpiredJobCancellation() throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            // Prevent the daemon thread "Expired Analysis Task Killer" from starting,
            // so only the test thread calls tryToCancel() — eliminating the race condition.
            mockedEnv.when(Env::isCheckpointThread).thenReturn(true);

            mockedStatisticsUtil.when(() -> StatisticsUtil.convertIdToObjects(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(new DBObjects(catalog, database, olapTable));

            Mockito.when(olapTable.getColumn(Mockito.anyString()))
                    .thenReturn(new Column("col1", PrimitiveType.INT));

            final AtomicBoolean cancelled = new AtomicBoolean();

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
            AnalysisTaskWrapper analysisTaskWrapper = Mockito.spy(
                    new AnalysisTaskWrapper(analysisTaskExecutor, analysisJob));
            Mockito.doAnswer(invocation -> {
                cancelled.set(true);
                return true;
            }).when(analysisTaskWrapper).cancel(Mockito.nullable(String.class));

            Field startTimeField = AnalysisTaskWrapper.class.getDeclaredField("startTime");
            startTimeField.setAccessible(true);
            startTimeField.set(analysisTaskWrapper, 5L);

            analysisTaskExecutor.putJob(analysisTaskWrapper);
            analysisTaskExecutor.tryToCancel();
            Assertions.assertTrue(cancelled.get());
            Assertions.assertEquals(0, analysisTaskExecutor.getTaskQueue().size());
        }
    }

    @Test
    public void testTaskExecution() throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedStatisticsUtil.when(() -> StatisticsUtil.convertIdToObjects(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(new DBObjects(catalog, database, olapTable));

            Mockito.when(olapTable.getColumn(Mockito.anyString()))
                    .thenReturn(new Column("col1", PrimitiveType.INT));

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
            OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask(analysisInfo));
            Mockito.doNothing().when(task).doExecute();

            analysisTaskExecutor.submitTask(task);
        }
    }
}
