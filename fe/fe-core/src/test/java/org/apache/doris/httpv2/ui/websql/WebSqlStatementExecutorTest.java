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

package org.apache.doris.httpv2.ui.websql;

import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WebSqlStatementExecutorTest {
    @Test
    void preservesColumnsAndTruncatesAtByteLimit() throws Exception {
        StatementSubmitter submitter = Mockito.mock(StatementSubmitter.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getCatalog()).thenReturn("tpcds");
        Mockito.when(connection.createStatement()).thenThrow(new SQLException("metadata unavailable"));
        WebSqlSession session = new WebSqlSession("id", "alice", connection, 0);
        Map<String, Object> raw = new HashMap<>();
        Map<String, String> field = new HashMap<>();
        field.put("name", "value");
        field.put("type", "VARCHAR");
        raw.put("meta", Collections.singletonList(field));
        raw.put("data", Arrays.asList(Collections.<Object>singletonList("small"),
                Collections.<Object>singletonList("this row exceeds the byte budget")));
        raw.put("time", 12L);
        raw.put("warnings", Collections.singletonList("warning"));
        Mockito.when(submitter.execute(Mockito.eq(connection), Mockito.any())).thenReturn(new ExecutionResultSet(raw));

        WebSqlExecutionResult result = new WebSqlStatementExecutor(submitter).execute(
                session, "SELECT value", limits(20));

        Assertions.assertEquals(1, result.getColumns().size());
        Assertions.assertEquals(1, result.getRows().size());
        Assertions.assertTrue(result.isTruncated());
        Assertions.assertEquals("tpcds", result.getDatabase());
        Assertions.assertEquals(Collections.singletonList("warning"), result.getWarnings());
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void convertsSqlExceptionToSafeStableDetails() throws Exception {
        StatementSubmitter submitter = Mockito.mock(StatementSubmitter.class);
        Connection connection = Mockito.mock(Connection.class);
        WebSqlSession session = new WebSqlSession("id", "alice", connection, 0);
        SQLException sqlException = new SQLException("table secret_table does not exist", "42S02", 2);
        Mockito.when(submitter.execute(Mockito.eq(connection), Mockito.any())).thenThrow(sqlException);

        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> new WebSqlStatementExecutor(submitter).execute(session, "SELECT * FROM missing", limits(100)));

        Assertions.assertEquals(WebSqlError.QUERY_ERROR, exception.getError());
        Assertions.assertFalse(exception.getMessage().contains("secret_table"));
        @SuppressWarnings("unchecked")
        Map<String, Object> details = (Map<String, Object>) exception.getDetails();
        Assertions.assertEquals("42S02", details.get("sqlState"));
        Assertions.assertEquals(2, details.get("vendorCode"));
    }

    private WebSqlLimits limits(long bytes) {
        return new WebSqlLimits(true, 1000, 5, 5, 10, bytes, 5, 20, 1, 60);
    }
}
