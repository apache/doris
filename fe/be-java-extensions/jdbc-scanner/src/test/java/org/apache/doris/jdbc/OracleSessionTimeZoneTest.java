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


package org.apache.doris.jdbc;

import oracle.jdbc.OracleConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class OracleSessionTimeZoneTest {
    @Test
    void initializesEveryBorrowedConnectionBeforePreparingQuery() throws Exception {
        OracleJdbcExecutor handler = Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        for (int i = 0; i < 2; i++) {
            Connection pooled = Mockito.mock(Connection.class);
            OracleConnection physical = Mockito.mock(OracleConnection.class);
            DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);
            PreparedStatement statement = Mockito.mock(PreparedStatement.class);
            Mockito.when(metadata.getDriverVersion()).thenReturn("23.26.2");
            Mockito.when(pooled.getMetaData()).thenReturn(metadata);
            Mockito.when(pooled.unwrap(Connection.class)).thenReturn(physical);
            Mockito.when(pooled.unwrap(OracleConnection.class)).thenReturn(physical);
            Mockito.when(pooled.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                    .thenReturn(statement);
            handler.initializeStatement(pooled, new JdbcDataSourceConfig().setOp(
                    org.apache.doris.thrift.TJdbcOperation.READ).setBatchSize(100), "SELECT ts FROM t");
            InOrder order = Mockito.inOrder(physical, pooled);
            order.verify(physical).setSessionTimeZone("UTC");
            order.verify(pooled).prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
        }
    }

    @Test
    void failedSessionInitializationAbortsBeforeQuery() throws Exception {
        Connection pooled = Mockito.mock(Connection.class);
        OracleConnection physical = Mockito.mock(OracleConnection.class);
        DatabaseMetaData metadata = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(metadata.getDriverVersion()).thenReturn("23.26.2");
        Mockito.when(pooled.getMetaData()).thenReturn(metadata);
        Mockito.when(pooled.unwrap(Connection.class)).thenReturn(physical);
        Mockito.when(pooled.unwrap(OracleConnection.class)).thenReturn(physical);
        Mockito.when(pooled.prepareStatement(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(Mockito.mock(PreparedStatement.class));
        Mockito.doThrow(new SQLException("session initialization failed")).when(physical).setSessionTimeZone("UTC");
        Assertions.assertThrows(SQLException.class,
                () -> Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS).initializeStatement(pooled,
                        new JdbcDataSourceConfig().setOp(org.apache.doris.thrift.TJdbcOperation.READ), "SELECT ts FROM t"));
        Mockito.verify(pooled, Mockito.never()).prepareStatement(
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
    }
}
