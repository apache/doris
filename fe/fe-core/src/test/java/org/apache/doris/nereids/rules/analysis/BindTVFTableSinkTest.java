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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFTableSink;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.TVFTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.LocalTableValuedFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BindTVFTableSinkTest {
    private boolean originalEnableLocalTvf;

    @BeforeEach
    void setUp() {
        originalEnableLocalTvf = Config.enable_local_tvf;
        Config.enable_local_tvf = false;
    }

    @AfterEach
    void tearDown() {
        Config.enable_local_tvf = originalEnableLocalTvf;
    }

    @Test
    void disabledLocalSinkRejectsInsertAndExplainBeforeProperties() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String properties : Arrays.asList("'format'='csv'",
                    "'file_path'='/tmp/local_gate_', 'backend_id'='1', 'format'='csv'")) {
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> analyzeInsert(prefix + "INSERT INTO local(" + properties + ") SELECT 1"));
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
    }

    @Test
    void enabledLocalSinkKeepsPropertyValidation() {
        Config.enable_local_tvf = true;
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> analyzeInsert("INSERT INTO local('format'='csv') SELECT 1"));
        Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
    }

    @Test
    void disabledLocalDoesNotBlockRemoteSinks() {
        for (String name : Arrays.asList("s3", "hdfs")) {
            Assertions.assertInstanceOf(LogicalTVFTableSink.class,
                    analyzeInsert("INSERT INTO " + name + "('file_path'='remote/data', 'format'='csv') SELECT 1"));
        }
    }

    @Test
    void disabledLocalWriterRejectsRemoteInsertAndExplain() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String name : Arrays.asList("s3", "hdfs")) {
                for (String writer : Arrays.asList("org.apache.doris.writer.LocalFileJniWriter",
                        "org/apache/doris/writer/LocalFileJniWriter")) {
                    for (String props : Arrays.asList("", "'file_path'='/tmp/local_gate_', 'format'='csv',")) {
                        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> analyzeInsert(prefix + "INSERT INTO " + name + "(" + props
                                        + "'writer_class'='" + writer + "') SELECT 1"));
                        Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
                    }
                }
            }
        }
    }

    @Test
    void directSinkBindingChecksEffectiveLocalWriter() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "s3://bucket/data");
        properties.put("format", "csv");
        properties.put("s3.endpoint", "http://127.0.0.1:9000");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "test-key");
        properties.put("s3.secret_key", "test-secret");
        for (String name : Arrays.asList("s3", "hdfs", "local")) {
            for (String writer : Arrays.asList("org.apache.doris.writer.LocalFileJniWriter",
                    "org/apache/doris/writer/LocalFileJniWriter",
                    "org/apache/doris/writer/LocalFileJniWriter\0ignored")) {
                properties.put("writer_class", writer);
                TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), name, properties,
                        Collections.emptyList());
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
        Config.enable_local_tvf = true;
        properties.put("writer_class", "org.apache.doris.writer.LocalFileJniWriter");
        new TVFTableSink(new PlanNodeId(0), "s3", properties, Collections.emptyList()).bindDataSink();
        Config.enable_local_tvf = false;
        properties.remove("writer_class");
        new TVFTableSink(new PlanNodeId(0), "s3", properties, Collections.emptyList()).bindDataSink();
        properties.put("writer_class", "org.apache.doris.writer.OtherWriter");
        Assertions.assertDoesNotThrow(() -> LocalTableValuedFunction.checkSinkEnabled("s3", properties));
    }

    @Test
    void enabledLocalWriterKeepsRemotePropertyValidation() {
        Config.enable_local_tvf = true;
        for (String name : Arrays.asList("s3", "hdfs")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> analyzeInsert("INSERT INTO " + name
                            + "('writer_class'='org.apache.doris.writer.LocalFileJniWriter') SELECT 1"));
            Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
        }
    }

    @Test
    void loadUserCannotExecuteLocalWriterWhileDisabled() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String sql = "INSERT INTO s3('file_path'='/tmp/local_gate_', 'format'='csv',"
                + "'writer_class'='org.apache.doris.writer.LocalFileJniWriter') SELECT 1";
        MemoTestUtils.createStatementContext(context, sql);
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(sql);
        Env env = Mockito.spy(Env.getCurrentEnv());
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.doReturn(accessManager).when(env).getAccessManager();
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.LOAD)).thenReturn(true);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(context, executor));
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.ADMIN);
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.LOAD);
            Mockito.verifyNoInteractions(executor);
        }
    }

    private Plan analyzeInsert(String sql) {
        ConnectContext context = MemoTestUtils.createConnectContext();
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        if (parsed instanceof ExplainCommand) {
            parsed = ((ExplainCommand) parsed).getLogicalPlan();
        }
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) parsed;
        return PlanChecker.from(context).analyze(command.getExplainPlan(context)).getPlan();
    }

    @Test
    void duplicateOutputNamesKeepDistinctPositions() {
        Config.enable_local_tvf = true;
        ConnectContext context = MemoTestUtils.createConnectContext();
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO local("
                        + "'file_path'='/tmp/tvf_duplicate_columns_',"
                        + "'backend_id'='1',"
                        + "'format'='csv') "
                        + "SELECT 1 AS x, 2 AS x");

        Plan analyzed = PlanChecker.from(context)
                .analyze(command.getExplainPlan(context))
                .getPlan();
        LogicalTVFTableSink<?> sink = Assertions.assertInstanceOf(LogicalTVFTableSink.class, analyzed);
        LogicalProject<?> sinkProject = Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        List<NamedExpression> projects = sinkProject.getProjects();

        Assertions.assertEquals(2, projects.size());
        Assertions.assertNotEquals(projects.get(0).getExprId(), projects.get(1).getExprId(),
                "TVF sink output positions must not be collapsed by duplicate display names");
    }
}
