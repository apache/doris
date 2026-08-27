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

package org.apache.doris.httpv2.websql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class WebSqlStatementExecutorTest {
    @Test
    void usesThePersistentConnectionAndTruncatesAtByteLimit() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(connection.getCatalog()).thenReturn("tpcds");
        Mockito.when(statement.execute("SELECT value")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(1);
        Mockito.when(metadata.getColumnName(1)).thenReturn("value");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("VARCHAR");
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getObject(1)).thenReturn("small", "this row exceeds the byte budget");
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlExecutionResult result = new WebSqlStatementExecutor(() -> 20).execute(
                session, "SELECT value", limits());

        Assertions.assertEquals(1, result.getColumns().size());
        Assertions.assertEquals(Collections.singletonList("small"), result.getRows().get(0));
        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("tpcds", result.getDatabase());
        Assertions.assertFalse(session.cancel());
        Mockito.verify(statement).setMaxRows(11);
        Mockito.verify(statement).cancel();
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void countsBinaryAndEscapedValuesAsJsonBytes() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(statement.execute("SELECT payload")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(1);
        Mockito.when(metadata.getColumnName(1)).thenReturn("payload");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("VARBINARY");
        Mockito.when(resultSet.next()).thenReturn(true, true, false);
        Mockito.when(resultSet.getObject(1)).thenReturn(new byte[12], "\\\"\\\"\\\"");
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlExecutionResult result = new WebSqlStatementExecutor(() -> 17).execute(
                session, "SELECT payload", limits());

        Assertions.assertTrue(result.getRows().isEmpty());
        Assertions.assertTrue(result.isTruncated());
        Mockito.verify(statement).cancel();

        WebSqlStatementExecutor executor = new WebSqlStatementExecutor();
        Assertions.assertEquals(18, executor.valueSize(new byte[12]));
        Assertions.assertTrue(executor.valueSize("\"\\\n") > "\"\\\n".length());
    }

    @Test
    void cancelsServerWorkWhenTheRowLimitIsReached() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(statement.execute("SELECT value")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(1);
        Mockito.when(metadata.getColumnName(1)).thenReturn("value");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("INT");
        Mockito.when(resultSet.next()).thenReturn(true, true);
        Mockito.when(resultSet.getObject(1)).thenReturn(1);
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlExecutionResult result = new WebSqlStatementExecutor().execute(
                session, "SELECT value", limits(1));

        Assertions.assertEquals(1, result.getRows().size());
        Assertions.assertTrue(result.isTruncated());
        Mockito.verify(statement).setMaxRows(2);
        Mockito.verify(statement).cancel();
    }

    @Test
    void serializesWideNumericValuesAsExactStrings() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(statement.execute("SELECT wide_values")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(3);
        Mockito.when(metadata.getColumnName(1)).thenReturn("bigint_value");
        Mockito.when(metadata.getColumnName(2)).thenReturn("largeint_value");
        Mockito.when(metadata.getColumnName(3)).thenReturn("decimal_value");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("BIGINT");
        Mockito.when(metadata.getColumnTypeName(2)).thenReturn("LARGEINT");
        Mockito.when(metadata.getColumnTypeName(3)).thenReturn("DECIMAL(38, 9)");
        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getString(1)).thenReturn("9223372036854775807");
        Mockito.when(resultSet.getString(2)).thenReturn("170141183460469231731687303715884105727");
        Mockito.when(resultSet.getString(3)).thenReturn("12345678901234567890123456789.123456789");
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlExecutionResult result = new WebSqlStatementExecutor().execute(
                session, "SELECT wide_values", limits());

        Assertions.assertEquals(Arrays.asList("9223372036854775807",
                "170141183460469231731687303715884105727",
                "12345678901234567890123456789.123456789"), result.getRows().get(0));
        Mockito.verify(resultSet, Mockito.never()).getObject(Mockito.anyInt());
    }

    @Test
    void closesConnectionIfCancellationAtTheResultLimitFails() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(statement.execute("SELECT value")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(1);
        Mockito.when(metadata.getColumnName(1)).thenReturn("value");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("INT");
        Mockito.when(resultSet.next()).thenReturn(true, true);
        Mockito.when(resultSet.getObject(1)).thenReturn(1);
        Mockito.doThrow(new SQLException("cancel failed")).when(statement).cancel();
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> new WebSqlStatementExecutor().execute(session, "SELECT value", limits(1)));

        Assertions.assertEquals(WebSqlError.QUERY_ERROR, exception.getError());
        Mockito.verify(connection).close();
    }

    @Test
    void exposesTheActiveStatementForCancel() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(statement.execute("USE tpcds")).thenAnswer(invocation -> {
            Assertions.assertTrue(activeSession.cancel());
            return false;
        });
        Mockito.when(statement.getUpdateCount()).thenReturn(0);
        activeSession = new WebSqlSession("id", "alice", "http-session", connection, 0);

        new WebSqlStatementExecutor().execute(activeSession, "USE tpcds", limits());

        Assertions.assertFalse(activeSession.cancel());
        Mockito.verify(statement).cancel();
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void validatesWithTheSqlModeReportedByThePersistentConnection() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, true);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        String sql = "SELECT 'a\\'; SELECT 2 --'";
        Mockito.when(statement.execute(sql)).thenReturn(false);
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        Assertions.assertThrows(WebSqlException.class,
                () -> new WebSqlStatementExecutor().execute(session, sql, limits()));
        Mockito.verify(statement, Mockito.never()).execute(Mockito.anyString());
    }

    @Test
    void convertsSqlExceptionToSafeStableDetails() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        mockSqlMode(connection, false);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(statement.execute("SELECT * FROM missing"))
                .thenThrow(new SQLException("table secret_table does not exist", "42S02", 2));
        WebSqlSession session = new WebSqlSession("id", "alice", "http-session", connection, 0);

        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> new WebSqlStatementExecutor().execute(session, "SELECT * FROM missing", limits()));

        Assertions.assertEquals(WebSqlError.QUERY_ERROR, exception.getError());
        Assertions.assertFalse(exception.getMessage().contains("secret_table"));
        @SuppressWarnings("unchecked")
        Map<String, Object> details = (Map<String, Object>) exception.getDetails();
        Assertions.assertEquals("table secret_table does not exist", details.get("message"));
        Assertions.assertEquals("42S02", details.get("sqlState"));
        Assertions.assertEquals(2, details.get("vendorCode"));
    }

    @Test
    void readsTheCurrentByteLimitForEachStatement() {
        AtomicLong configuredLimit = new AtomicLong(32);
        WebSqlStatementExecutor executor = new WebSqlStatementExecutor(configuredLimit::get);

        Assertions.assertEquals(32, executor.currentMaxResultBytes());
        configuredLimit.set(128);
        Assertions.assertEquals(128, executor.currentMaxResultBytes());
    }

    private WebSqlSession activeSession;

    private void mockSqlMode(Connection connection, boolean noBackslashEscapes) throws SQLException {
        org.mariadb.jdbc.Connection mariaDbConnection = Mockito.mock(org.mariadb.jdbc.Connection.class);
        Context context = Mockito.mock(Context.class);
        Mockito.when(connection.unwrap(org.mariadb.jdbc.Connection.class)).thenReturn(mariaDbConnection);
        Mockito.when(mariaDbConnection.getContext()).thenReturn(context);
        Mockito.when(context.getServerStatus()).thenReturn(
                noBackslashEscapes ? ServerStatus.NO_BACKSLASH_ESCAPES : 0);
    }

    private WebSqlLimits limits() {
        return limits(10);
    }

    private WebSqlLimits limits(int maxResultRows) {
        return new WebSqlLimits(true, 1000, 5, 5, maxResultRows, 0, 1, 60);
    }
}
