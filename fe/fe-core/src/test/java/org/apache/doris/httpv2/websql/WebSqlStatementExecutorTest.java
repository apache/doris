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
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

public class WebSqlStatementExecutorTest {
    @Test
    void usesThePersistentConnectionAndTruncatesAtByteLimit() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
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
        WebSqlSession session = new WebSqlSession("id", "alice", connection, 0);

        WebSqlExecutionResult result = new WebSqlStatementExecutor().execute(
                session, "SELECT value", limits(20));

        Assertions.assertEquals(1, result.getColumns().size());
        Assertions.assertEquals(Collections.singletonList("small"), result.getRows().get(0));
        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("tpcds", result.getDatabase());
        Assertions.assertFalse(session.cancel());
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void exposesTheActiveStatementForCancel() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        Mockito.when(statement.execute("USE tpcds")).thenAnswer(invocation -> {
            Assertions.assertTrue(activeSession.cancel());
            return false;
        });
        Mockito.when(statement.getUpdateCount()).thenReturn(0);
        activeSession = new WebSqlSession("id", "alice", connection, 0);

        new WebSqlStatementExecutor().execute(activeSession, "USE tpcds", limits(100));

        Assertions.assertFalse(activeSession.cancel());
        Mockito.verify(statement).cancel();
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void convertsSqlExceptionToSafeStableDetails() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        Mockito.when(statement.execute("SELECT * FROM missing"))
                .thenThrow(new SQLException("table secret_table does not exist", "42S02", 2));
        WebSqlSession session = new WebSqlSession("id", "alice", connection, 0);

        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> new WebSqlStatementExecutor().execute(session, "SELECT * FROM missing", limits(100)));

        Assertions.assertEquals(WebSqlError.QUERY_ERROR, exception.getError());
        Assertions.assertFalse(exception.getMessage().contains("secret_table"));
        @SuppressWarnings("unchecked")
        Map<String, Object> details = (Map<String, Object>) exception.getDetails();
        Assertions.assertEquals("42S02", details.get("sqlState"));
        Assertions.assertEquals(2, details.get("vendorCode"));
    }

    private WebSqlSession activeSession;

    private WebSqlLimits limits(long bytes) {
        return new WebSqlLimits(true, 1000, 5, 5, 10, bytes, 0, 1, 60);
    }
}
