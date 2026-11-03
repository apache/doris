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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;

import com.zaxxer.hikari.HikariDataSource;
import oracle.jdbc.OracleConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class OracleSessionTimeZoneTest {
    @Test
    void binaryFloatingPointBindingsAreEnabledBeforeOpeningConnections() {
        OracleJdbcExecutor handler = Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        try (HikariDataSource ds = new HikariDataSource()) {
            handler.setValidationQuery(ds);
            Assertions.assertEquals("true", ds.getDataSourceProperties().getProperty(
                    OracleConnection.CONNECTION_PROPERTY_SET_FLOAT_AND_DOUBLE_USE_BINARY));
        }
    }

    @Test
    void binaryEncodingPreservesValuesOutsideNumberRange() throws Exception {
        Assertions.assertThrows(SQLException.class, () -> new oracle.sql.NUMBER(1e200));
        for (double value : new double[] {1e200, -1e200, 1e-200, Double.MAX_VALUE, Double.MIN_VALUE}) {
            byte[] encoded = new oracle.sql.BINARY_DOUBLE(value).getBytes();
            Assertions.assertEquals(value, new oracle.sql.BINARY_DOUBLE(encoded).doubleValue());
        }
        for (float value : new float[] {1.25f, -1.25f, Float.MAX_VALUE, Float.MIN_VALUE}) {
            byte[] encoded = new oracle.sql.BINARY_FLOAT(value).getBytes();
            Assertions.assertEquals(value, new oracle.sql.BINARY_FLOAT(encoded).floatValue());
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "oracle.integration.url", matches = ".+")
    void binaryFloatingPointInsertRoundTrip() throws Exception {
        OracleJdbcExecutor handler = Mockito.mock(OracleJdbcExecutor.class, Mockito.CALLS_REAL_METHODS);
        try (HikariDataSource ds = new HikariDataSource()) {
            handler.setValidationQuery(ds);
            ds.setJdbcUrl(System.getProperty("oracle.integration.url"));
            ds.setUsername(System.getProperty("oracle.integration.user"));
            ds.setPassword(System.getProperty("oracle.integration.password"));
            ds.setMaximumPoolSize(1);
            try (Connection connection = ds.getConnection(); Statement ddl = connection.createStatement()) {
                String table = "doris_binary_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
                ddl.execute("CREATE TABLE " + table + " (id INT, f BINARY_FLOAT, d BINARY_DOUBLE)");
                try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?)")) {
                    handler.preparedStatement = insert;
                    VectorColumn floats = Mockito.mock(VectorColumn.class);
                    VectorColumn doubles = Mockito.mock(VectorColumn.class);
                    Mockito.when(floats.getColumnPrimitiveType()).thenReturn(ColumnType.Type.FLOAT);
                    Mockito.when(doubles.getColumnPrimitiveType()).thenReturn(ColumnType.Type.DOUBLE);
                    float[] floatValues = {1.25f, -1.25f, 0.0f};
                    double[] doubleValues = {1e200, -1e200, 1e-200};
                    java.lang.reflect.Method insertColumn = BaseJdbcExecutor.class.getDeclaredMethod(
                            "insertColumn", int.class, int.class, VectorColumn.class);
                    insertColumn.setAccessible(true);
                    Mockito.when(floats.isNullAt(3)).thenReturn(true);
                    Mockito.when(doubles.isNullAt(3)).thenReturn(true);
                    for (int row = 0; row < 4; ++row) {
                        if (row < 3) {
                            Mockito.when(floats.getFloat(row)).thenReturn(floatValues[row]);
                            Mockito.when(doubles.getDouble(row)).thenReturn(doubleValues[row]);
                        }
                        insert.setInt(1, row);
                        insertColumn.invoke(handler, row, 1, floats);
                        insertColumn.invoke(handler, row, 2, doubles);
                        insert.executeUpdate();
                    }
                    try (ResultSet rows = ddl.executeQuery("SELECT f, d FROM " + table + " ORDER BY id")) {
                        for (int row = 0; row < 3; ++row) {
                            Assertions.assertTrue(rows.next());
                            Assertions.assertEquals(floatValues[row], rows.getFloat(1));
                            Assertions.assertEquals(doubleValues[row], rows.getDouble(2));
                        }
                        Assertions.assertTrue(rows.next());
                        Assertions.assertNull(rows.getObject(1));
                        Assertions.assertNull(rows.getObject(2));
                        Assertions.assertFalse(rows.next());
                    }
                } finally {
                    ddl.execute("DROP TABLE " + table);
                }
            }
        }
    }

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
