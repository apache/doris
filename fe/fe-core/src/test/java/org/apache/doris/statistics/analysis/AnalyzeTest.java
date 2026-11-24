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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeTableCommand;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.analysis.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

public class AnalyzeTest extends TestWithFeService {

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
            createTable("CREATE TABLE t_part (p int not null, v int not null)\n"
                    + "PARTITION BY RANGE(p) (PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20))\n"
                    + "DISTRIBUTED BY HASH(p) BUCKETS 1\n"
                    + "PROPERTIES(\"replication_num\"=\"1\");");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testCreateAnalysisJob() throws Exception {
        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class);
                MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                        (mock, context) -> {
                            Mockito.when(mock.executeInternalQuery()).thenReturn(Collections.emptyList());
                        });
                MockedStatic<ConnectContext> mockedConnectContext = Mockito.mockStatic(ConnectContext.class)) {

            mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);

            mockedStatisticsUtil.when(() -> StatisticsUtil.buildConnectContext(Mockito.anyBoolean()))
                    .thenAnswer(invocation -> new AutoCloseConnectContext(connectContext));
            mockedStatisticsUtil.when(() -> StatisticsUtil.execUpdate(Mockito.anyString()))
                    .thenReturn(null);

            String sql = "ANALYZE TABLE t1";
            Assertions.assertNotNull(getSqlStmtExecutor(sql));
        }
    }

    @Test
    public void testJobExecution() throws Exception {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable olapTable = Mockito.mock(OlapTable.class);

        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getFullName()).thenReturn("analysis_job_test");
        Mockito.when(olapTable.getName()).thenReturn("t1");
        Mockito.when(olapTable.getColumn(Mockito.anyString()))
                .thenReturn(new Column("col1", PrimitiveType.INT));

        try (MockedStatic<StatisticsUtil> mockedStatisticsUtil = Mockito.mockStatic(StatisticsUtil.class);
                MockedConstruction<StmtExecutor> mockedStmtExecutorConstruction =
                        Mockito.mockConstruction(StmtExecutor.class, (mock, context) -> {
                            Mockito.doNothing().when(mock).execute();
                            Mockito.when(mock.executeInternalQuery()).thenReturn(new ArrayList<>());
                        })) {

            mockedStatisticsUtil.when(() -> StatisticsUtil.buildConnectContext(Mockito.anyBoolean()))
                    .thenAnswer(invocation -> new AutoCloseConnectContext(connectContext));
            mockedStatisticsUtil.when(() -> StatisticsUtil.execUpdate(Mockito.anyString()))
                    .thenReturn(null);
            mockedStatisticsUtil.when(() -> StatisticsUtil.convertIdToObjects(
                    Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong()))
                    .thenReturn(new DBObjects(catalog, database, olapTable));

            Set<Pair<String, String>> colList = Sets.newHashSet();
            colList.add(Pair.of("col1", "index1"));
            AnalysisInfo analysisJobInfo = new AnalysisInfoBuilder().setJobId(0).setTaskId(0)
                    .setCatalogId(0)
                    .setDBId(0)
                    .setTblId(0)
                    .setColName("col1").setJobType(JobType.MANUAL)
                    .setAnalysisMethod(AnalysisMethod.FULL)
                    .setAnalysisType(AnalysisType.FUNDAMENTALS)
                    .setJobColumns(colList)
                    .setState(AnalysisState.RUNNING)
                    .setRowCount(10)
                    .build();
            OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask(analysisJobInfo));
            Mockito.doNothing().when(task).runQuery(Mockito.anyString());
            task.doExecute();

            Mockito.verify(task, Mockito.times(1)).runQuery(Mockito.anyString());
        }
    }

    @Test
    public void testPartitionAnalyzeValidationAndExplicitPartitionRouting() throws Exception {
        NereidsParser parser = new NereidsParser();
        AnalyzeTableCommand invalidSubsetSample = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE t_part PARTITION(p1) WITH SAMPLE ROWS 100");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> invalidSubsetSample.validate(connectContext));
        Assertions.assertTrue(ex.getMessage().contains("Partition-scoped ANALYZE with sample is not supported"),
                ex.getMessage());

        AnalyzeTableCommand starSample = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE t_part PARTITION(*) WITH SAMPLE ROWS 100");
        Assertions.assertDoesNotThrow(() -> starSample.validate(connectContext));

        AnalyzeTableCommand explicitSubsetFull = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE t_part PARTITION(p1) WITH SYNC");
        Assertions.assertDoesNotThrow(() -> explicitSubsetFull.validate(connectContext));
        Assertions.assertDoesNotThrow(explicitSubsetFull::checkAndSetSample);
        Assertions.assertEquals(AnalysisMethod.FULL, explicitSubsetFull.getAnalysisMethod());

        // Explicit PARTITION(...) full analyze routes to doPartitionTable() even when enable_partition_analyze=false.
        OlapTable mockedPartTable = Mockito.mock(OlapTable.class);
        Mockito.when(mockedPartTable.isPartitionedTable()).thenReturn(true);
        OlapAnalysisTask partTask = Mockito.spy(new OlapAnalysisTask());
        partTask.tbl = mockedPartTable;
        partTask.col = new Column("p", PrimitiveType.INT);
        partTask.info = new AnalysisInfoBuilder()
                .setPartitionOnly(true)
                .setPartitionNames(Set.of("p1"))
                .setAnalysisMethod(AnalysisMethod.FULL)
                .build();
        Mockito.doNothing().when(partTask).doPartitionTable();
        try (MockedStatic<StatisticsUtil> mockedUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            mockedUtil.when(StatisticsUtil::enablePartitionAnalyze).thenReturn(false);
            partTask.doFull();
        }
        Mockito.verify(partTask, Mockito.times(1)).doPartitionTable();
    }

}
