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

package org.apache.doris.httpv2.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class StatementSubmitterTest {
    @Test
    void callerOwnedConnectionRemainsOpenAndStatementIsObservable() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        Mockito.when(connection.createStatement(Mockito.anyInt(), Mockito.anyInt())).thenReturn(statement);
        Mockito.when(statement.execute("USE tpcds")).thenReturn(false);
        Mockito.when(statement.getUpdateCount()).thenReturn(0);
        List<Statement> observed = new ArrayList<>();
        StatementSubmitter.StmtContext context = new StatementSubmitter.StmtContext(
                "USE tpcds", "", "", 10, false, null, "")
                .withQueryTimeoutSeconds(7)
                .withStatementObserver(observed::add);

        ExecutionResultSet result = new StatementSubmitter().execute(connection, context);

        Assertions.assertEquals("exec_status", result.getResult().get("type"));
        Assertions.assertEquals(2, observed.size());
        Assertions.assertSame(statement, observed.get(0));
        Assertions.assertNull(observed.get(1));
        Mockito.verify(statement).setQueryTimeout(7);
        Mockito.verify(statement).close();
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void resultBytesAreBoundedWhileReadingJdbcRows() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement(Mockito.anyInt(), Mockito.anyInt())).thenReturn(statement);
        Mockito.when(statement.execute("SELECT value")).thenReturn(true);
        Mockito.when(statement.getResultSet()).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnCount()).thenReturn(1);
        Mockito.when(metadata.getColumnName(1)).thenReturn("value");
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("VARCHAR");
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getObject(1)).thenReturn("larger-than-budget");
        StatementSubmitter.StmtContext context = new StatementSubmitter.StmtContext(
                "SELECT value", "", "", 10, false, null, "").withMaxResultBytes(4);

        ExecutionResultSet execution = new StatementSubmitter().execute(connection, context);

        Assertions.assertEquals(Boolean.TRUE, execution.getResult().get("truncated"));
        Assertions.assertTrue(((List<?>) execution.getResult().get("data")).isEmpty());
        Mockito.verify(connection, Mockito.never()).close();
    }
}
