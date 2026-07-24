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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StmtExecutorTest extends TestWithFeService {
    private static final String CREATE_AI_RESOURCE_SQL = "CREATE EXTERNAL RESOURCE \"ai_resource_log_test\"\n"
            + "PROPERTIES\n"
            + "(\n"
            + "   \"type\" = \"ai\",\n"
            + "   \"ai.provider_type\" = \"openai\",\n"
            + "   \"ai.endpoint\" = \"https://api.test\",\n"
            + "   \"ai.model_name\" = \"gpt-test\",\n"
            + "   \"ai.api_key\" = \"sk-test-secret\"\n"
            + ");";

    @Override
    protected void runBeforeAll() throws Exception {
        Config.allow_replica_on_same_host = true;
        FeConstants.runningUnitTest = true;
        InternalSchemaInitializer.createDb();
        InternalSchemaInitializer.createTbl();
        createDatabase("testDb");
    }

    @Test
    public void testShow() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testShowNull() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    // Arrow Flight SQL keeps a query's coordinator alive across GetFlightInfo -> DoGet (see #62259);
    // it is released later by finalizeArrowFlightQuery(), which closes the coordinator and then
    // unregisters the query. The close and the unregister must be independent: if coord.close()
    // throws, the query registration must still be released (the try/finally), otherwise the query
    // leaks in QeProcessorImpl forever. The thrown error is expected to propagate to the caller
    // (ConnectContext.closeFlightSqlDeferredExecutors), which catches and logs it.
    @Test
    public void testFinalizeArrowFlightQueryUnregistersQueryEvenIfCoordCloseThrows() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        TUniqueId queryId = new TUniqueId(0x6226259L, 0x62259L);
        connectContext.setQueryId(queryId);

        Coordinator coord = Mockito.mock(Coordinator.class);
        Mockito.when(coord.getQueryOptions()).thenReturn(new TQueryOptions());
        Mockito.doThrow(new RuntimeException("coord close failed")).when(coord).close();
        stmtExecutor.setCoord(coord);

        // Simulate the in-flight query whose results DoGet is still pulling.
        QeProcessorImpl.INSTANCE.registerQuery(queryId, new QeProcessorImpl.QueryInfo(coord));
        Assert.assertNotNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId));

        try {
            stmtExecutor.finalizeArrowFlightQuery();
            Assert.fail("expected coord.close() failure to propagate after the query is unregistered");
        } catch (RuntimeException e) {
            Assert.assertEquals("coord close failed", e.getMessage());
        }

        // The coordinator close was attempted (releases SplitSource + query queue slot) ...
        Mockito.verify(coord).close();
        // ... and despite it failing, the query registration was still released (no leak).
        Assert.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(queryId));
    }

    @Test
    public void testKill() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testKillOtherFail() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1000");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testKillNoCtx() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testSet() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testDdlFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "CREATE FILE \\\"ca.pem\\\"\\n\"\n"
                + "                + \"PROPERTIES\\n\"\n"
                + "                + \"(\\n\"\n"
                + "                + \"   \\\"url\\\" = \\\"https://test.bj.bcebos.com/kafka-key/ca.pem\\\",\\n\"\n"
                + "                + \"   \\\"catalog\\\" = \\\"kafka\\\"\\n\"\n"
                + "                + \");");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUse() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use nondb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalog() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.testDb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalogFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.nondb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testBlockSqlAst() throws Exception {
        useDatabase("testDb");
        Config.block_sql_ast_names = "CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        StmtExecutor executor = new StmtExecutor(connectContext, "CREATE FILE \"ca.pem\"\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   \"url\" = \"https://test.bj.bcebos.com/kafka-key/ca.pem\",\n"
                + "   \"catalog\" = \"kafka\"\n"
                + ");");
        try {
            executor.execute();
        } catch (Exception ignore) {
            // do nothing
            ignore.printStackTrace();
            Assert.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        Config.block_sql_ast_names = "AlterStmt, CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "CREATE FILE \"ca.pem\"\n"
                + "PROPERTIES\n"
                + "(\"file_type\" = \"PEM\")");
        try {
            executor.execute();
        } catch (Exception ignore) {
            ignore.printStackTrace();
            Assert.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        Config.block_sql_ast_names = "CreateFunctionStmt, CreateFileCommand";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (\n"
                + "   \"file\"=\"file:///path/to/java-udf-demo-jar-with-dependencies.jar\",\n"
                + "   \"symbol\"=\"org.apache.doris.udf.AddOne\",\n"
                + "   \"always_nullable\"=\"true\",\n"
                + "   \"type\"=\"JAVA_UDF\"\n"
                + ");");
        try {
            executor.execute();
        } catch (Exception ignore) {
            ignore.printStackTrace();
            Assert.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());

        Config.block_sql_ast_names = "";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testSendTextResultRow() throws IOException {
        ConnectContext mockCtx = Mockito.mock(ConnectContext.class);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(mockCtx.getConnectType()).thenReturn(ConnectType.MYSQL);
        Mockito.when(mockCtx.getMysqlChannel()).thenReturn(channel);
        MysqlSerializer mysqlSerializer = MysqlSerializer.newInstance();
        Mockito.when(channel.getSerializer()).thenReturn(mysqlSerializer);
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        Mockito.when(mockCtx.getSessionVariable()).thenReturn(sessionVariable);
        OriginStatement stmt = new OriginStatement("", 1);

        List<List<String>> rows = Lists.newArrayList();
        List<String> row1 = Lists.newArrayList();
        row1.add(null);
        row1.add("row1");
        List<String> row2 = Lists.newArrayList();
        row2.add("1234");
        row2.add("row2");
        rows.add(row1);
        rows.add(row2);
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column());
        columns.add(new Column());
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), rows);
        AtomicInteger i = new AtomicInteger();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                byte[] expected0 = new byte[] {-5, 4, 114, 111, 119, 49};
                byte[] expected1 = new byte[] {4, 49, 50, 51, 52, 4, 114, 111, 119, 50};
                ByteBuffer buffer = invocation.getArgument(0);
                if (i.get() == 0) {
                    Assertions.assertArrayEquals(expected0, buffer.array());
                    i.getAndIncrement();
                } else if (i.get() == 1) {
                    Assertions.assertArrayEquals(expected1, buffer.array());
                    i.getAndIncrement();
                }
                return null;
            }
        }).when(channel).sendOnePacket(Mockito.any(ByteBuffer.class));

        StmtExecutor executor = new StmtExecutor(mockCtx, stmt, false);
        executor.sendTextResultRow(resultSet);
    }

    @Test
    public void testSendBinaryResultRow() throws IOException {
        ConnectContext mockCtx = Mockito.mock(ConnectContext.class);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(mockCtx.getConnectType()).thenReturn(ConnectType.MYSQL);
        Mockito.when(mockCtx.getMysqlChannel()).thenReturn(channel);
        MysqlSerializer mysqlSerializer = MysqlSerializer.newInstance();
        Mockito.when(channel.getSerializer()).thenReturn(mysqlSerializer);
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        Mockito.when(mockCtx.getSessionVariable()).thenReturn(sessionVariable);
        OriginStatement stmt = new OriginStatement("", 1);

        List<List<String>> rows = Lists.newArrayList();
        List<String> row1 = Lists.newArrayList();
        row1.add(null);
        row1.add("2025-01-01 01:02:03");
        List<String> row2 = Lists.newArrayList();
        row2.add("1234");
        row2.add("2025-01-01 01:02:03.123456");
        rows.add(row1);
        rows.add(row2);
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("col1", PrimitiveType.BIGINT));
        columns.add(new Column("col2", PrimitiveType.DATETIMEV2));
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), rows);
        AtomicInteger i = new AtomicInteger();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                byte[] expected0 = new byte[] {0, 4, 7, -23, 7, 1, 1, 1, 2, 3};
                byte[] expected1 = new byte[] {0, 0, -46, 4, 0, 0, 0, 0, 0, 0, 11, -23, 7, 1, 1, 1, 2, 3,
                        64, -30, 1, 0};
                ByteBuffer buffer = invocation.getArgument(0);
                if (i.get() == 0) {
                    Assertions.assertArrayEquals(expected0, buffer.array());
                    i.getAndIncrement();
                } else if (i.get() == 1) {
                    Assertions.assertArrayEquals(expected1, buffer.array());
                    i.getAndIncrement();
                }
                return null;
            }
        }).when(channel).sendOnePacket(Mockito.any(ByteBuffer.class));

        StmtExecutor executor = new StmtExecutor(mockCtx, stmt, false);
        executor.sendBinaryResultRow(resultSet);
    }

    @Test
    public void testSendBinaryBooleanResultRow() throws IOException {
        ConnectContext mockCtx = Mockito.mock(ConnectContext.class);
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(mockCtx.getConnectType()).thenReturn(ConnectType.MYSQL);
        Mockito.when(mockCtx.getMysqlChannel()).thenReturn(channel);
        MysqlSerializer mysqlSerializer = MysqlSerializer.newInstance();
        Mockito.when(channel.getSerializer()).thenReturn(mysqlSerializer);
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        Mockito.when(mockCtx.getSessionVariable()).thenReturn(sessionVariable);
        OriginStatement stmt = new OriginStatement("", 1);

        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("false"));
        rows.add(Lists.newArrayList("1"));
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("col1", PrimitiveType.BOOLEAN));
        ResultSet resultSet = new CommonResultSet(new CommonResultSetMetaData(columns), rows);
        AtomicInteger i = new AtomicInteger();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                byte[] expected0 = new byte[] {0, 0, 0};
                byte[] expected1 = new byte[] {0, 0, 1};
                ByteBuffer buffer = invocation.getArgument(0);
                if (i.get() == 0) {
                    Assertions.assertArrayEquals(expected0, buffer.array());
                    i.getAndIncrement();
                } else if (i.get() == 1) {
                    Assertions.assertArrayEquals(expected1, buffer.array());
                    i.getAndIncrement();
                }
                return null;
            }
        }).when(channel).sendOnePacket(Mockito.any(ByteBuffer.class));

        StmtExecutor executor = new StmtExecutor(mockCtx, stmt, false);
        executor.sendBinaryResultRow(resultSet);
    }

    @Test
    public void testClearDeleteExistingFilesInPlan() throws Exception {
        Planner planner = Mockito.mock(Planner.class);
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        ResultFileSink resultFileSink = Mockito.mock(ResultFileSink.class);
        Mockito.when(fragment.getSink()).thenReturn(resultFileSink);
        Mockito.when(planner.getFragments()).thenReturn(Lists.newArrayList(fragment));

        StmtExecutor executor = new StmtExecutor(connectContext, "");
        Field plannerField = StmtExecutor.class.getDeclaredField("planner");
        plannerField.setAccessible(true);
        plannerField.set(executor, planner);

        Method clearMethod = StmtExecutor.class.getDeclaredMethod("clearDeleteExistingFilesInPlan");
        clearMethod.setAccessible(true);
        clearMethod.invoke(executor);

        Mockito.verify(resultFileSink).setDeleteExistingFiles(false);
    }

    @Test
    public void testParseByNereidsSetsParsedStatementOnStatementContext() throws Exception {
        // This test verifies the fix for a bug in multi-FE environments where
        // parseByNereids() did not propagate the parsed statement to the
        // StatementContext. In the proxy flow (e.g., when a follower FE forwards
        // a query to the master FE), the StmtExecutor is created via the proxy
        // constructor which creates a fresh StatementContext without a
        // parsedStatement. Without the fix, statementContext.getParsedStatement()
        // remains null, causing SessionVariable.canUseNereidsDistributePlanner()
        // to return false, which leads EnvFactory.createCoordinator() to create
        // a legacy Coordinator instead of NereidsCoordinator, resulting in
        // "fragment has no children" error.

        // Simulate the proxy flow: StmtExecutor(ConnectContext, OriginStatement, boolean isProxy)
        StmtExecutor executor = new StmtExecutor(connectContext,
                new OriginStatement("select 1", 0), true);

        // Before parsing, statementContext should exist but parsedStatement should be null
        Assertions.assertNotNull(connectContext.getStatementContext());
        Assertions.assertNull(connectContext.getStatementContext().getParsedStatement(),
                "ParsedStatement should be null before parseByNereids() in proxy flow");

        // Trigger parseByNereids via reflection (it's private)
        Method parseByNereidsMethod = StmtExecutor.class.getDeclaredMethod("parseByNereids");
        parseByNereidsMethod.setAccessible(true);
        parseByNereidsMethod.invoke(executor);

        // After parsing, parsedStatement should be set on the StatementContext
        org.apache.doris.analysis.StatementBase parsedStatement
                = connectContext.getStatementContext().getParsedStatement();
        Assertions.assertNotNull(parsedStatement,
                "ParsedStatement should not be null after parseByNereids() in proxy flow");
        Assertions.assertTrue(
                parsedStatement instanceof org.apache.doris.nereids.glue.LogicalPlanAdapter,
                "ParsedStatement should be a LogicalPlanAdapter after parseByNereids(), but was: "
                        + (parsedStatement == null ? "null" : parsedStatement.getClass().getName()));
    }

    @Test
    public void testShouldDisableCloudVersionCacheOnRetryForE230() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        String originalDeployMode = Config.deploy_mode;
        long originalPartitionTtl = connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs;
        long originalTableTtl = connectContext.getSessionVariable().cloudTableVersionCacheTtlMs;
        try {
            Config.cloud_unique_id = "test-cloud-id";
            StmtExecutor executor = new StmtExecutor(connectContext, "select 1");

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 1000L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 1000L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = some other error"));
            // null error message must not trigger the disable.
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(null));

            // Non-cloud mode must never disable the version cache, even on E-230.
            Config.cloud_unique_id = "";
            Config.deploy_mode = "";
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
            Config.cloud_unique_id = "test-cloud-id";

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 0L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 1000L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 1000L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 0L;
            Assertions.assertTrue(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));

            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = 0L;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = 0L;
            Assertions.assertFalse(executor.shouldDisableCloudVersionCacheOnRetry(
                    "errCode = 2, detailMessage = E-230 versions are already compacted"));
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
            Config.deploy_mode = originalDeployMode;
            connectContext.getSessionVariable().cloudPartitionVersionCacheTtlMs = originalPartitionTtl;
            connectContext.getSessionVariable().cloudTableVersionCacheTtlMs = originalTableTtl;
        }
    }

    @Test
    public void testNeedAuditEncryptionStatementLogsMaskedSql() throws Exception {
        boolean originalPrintRequest = Config.enable_print_request_before_execution;
        Config.enable_print_request_before_execution = true;
        try (TestLogAppender appender = TestLogAppender.attach(StmtExecutor.class)) {
            connectContext.getState().reset();
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, CREATE_AI_RESOURCE_SQL);
            stmtExecutor.execute();

            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, "sk-test-secret"));
            Assertions.assertTrue(appender.contains(org.apache.logging.log4j.Level.INFO, "*XXX"));
        } finally {
            Config.enable_print_request_before_execution = originalPrintRequest;
        }
        connectContext.getState().reset();
        StmtExecutor showExecutor = new StmtExecutor(connectContext, "");
        showExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testAlterResourceSuccessLogDoesNotPrintResourceObject() throws Exception {
        createResource(CREATE_AI_RESOURCE_SQL);
        String alterSql = "ALTER RESOURCE \"ai_resource_log_test\" PROPERTIES ("
                + "\"ai.api_key\" = \"sk-updated-secret\")";
        String fullResourceJson = Env.getCurrentEnv().getResourceMgr().getResource("ai_resource_log_test").toString();

        try (TestLogAppender appender = TestLogAppender.attach(ResourceMgr.class)) {
            connectContext.getState().reset();
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, alterSql);
            stmtExecutor.execute();

            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, "sk-updated-secret"));
            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, "\"properties\""));
            Assertions.assertFalse(appender.contains(org.apache.logging.log4j.Level.INFO, fullResourceJson));
        }
    }

    private void createResource(String sql) throws Exception {
        connectContext.getState().reset();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }
}
