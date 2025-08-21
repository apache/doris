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
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StmtExecutorTest extends TestWithFeService {

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
        Assertions.assertEquals(MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testKill() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testKillOtherFail() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1000");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testKillNoCtx() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "kill 1");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testSet() throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "");
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
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
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUse() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use nondb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalog() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
    }

    @Test
    public void testUseWithCatalogFail() throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext, "use internal.nondb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, connectContext.getState().getStateType());
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
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
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
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
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
            Assertions.assertTrue(ignore.getMessage().contains("SQL is blocked with AST name: CreateFileCommand"));
        }

        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());

        Config.block_sql_ast_names = "";
        StmtExecutor.initBlockSqlAstNames();
        executor = new StmtExecutor(connectContext, "use testDb");
        executor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.OK, connectContext.getState().getStateType());
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
        Mockito.doAnswer(invocation -> {
            byte[] expected0 = new byte[]{-5, 4, 114, 111, 119, 49};
            byte[] expected1 = new byte[]{4, 49, 50, 51, 52, 4, 114, 111, 119, 50};
            ByteBuffer buffer = invocation.getArgument(0);
            if (i.get() == 0) {
                Assertions.assertArrayEquals(expected0, buffer.array());
                i.getAndIncrement();
            } else if (i.get() == 1) {
                Assertions.assertArrayEquals(expected1, buffer.array());
                i.getAndIncrement();
            }
            return null;
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
        Mockito.doAnswer(invocation -> {
            byte[] expected0 = new byte[]{0, 4, 7, -23, 7, 1, 1, 1, 2, 3};
            byte[] expected1 = new byte[]{0, 0, -46, 4, 0, 0, 0, 0, 0, 0, 11, -23, 7, 1, 1, 1, 2, 3, 64, -30, 1, 0};
            ByteBuffer buffer = invocation.getArgument(0);
            if (i.get() == 0) {
                Assertions.assertArrayEquals(expected0, buffer.array());
                i.getAndIncrement();
            } else if (i.get() == 1) {
                Assertions.assertArrayEquals(expected1, buffer.array());
                i.getAndIncrement();
            }
            return null;
        }).when(channel).sendOnePacket(Mockito.any(ByteBuffer.class));

        StmtExecutor executor = new StmtExecutor(mockCtx, stmt, false);
        executor.sendBinaryResultRow(resultSet);
    }
}
