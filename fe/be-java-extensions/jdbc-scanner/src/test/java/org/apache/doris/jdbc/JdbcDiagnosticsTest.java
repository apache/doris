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

import org.apache.doris.jni.spi.utils.JniUtil;
import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.VectorTable;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class JdbcDiagnosticsTest {
    private static SQLException failure() {
        SQLException error = new SQLException("denied; raw=test-password", "42000", 1142);
        error.setNextException(new SQLException("connection lost", "08006", 0));
        return error;
    }

    @Test
    void scannerOpenReportsRemoteCodesThroughTheActualJniFormatter() throws Exception {
        JdbcJniScanner scanner = new JdbcJniScanner(1, params());
        SQLException cause = failure();
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw cause;
            }
        };
        Method cacheKey = JdbcJniScanner.class.getDeclaredMethod("createCacheKey");
        cacheKey.setAccessible(true);
        String key = (String) cacheKey.invoke(scanner);
        JdbcDataSource.getDataSource().putSource(key, pool);
        try {
            assertDiagnostic(Assertions.assertThrows(IOException.class, scanner::open), cause, "open");
        } finally {
            JdbcDataSource.getDataSource().getSourcesMap().remove(key);
            pool.close();
        }
    }

    @Test
    void scannerGetNextReportsStreamingResultSetFailure() throws Exception {
        JdbcJniScanner scanner = new JdbcJniScanner(1, params());
        SQLException cause = failure();
        ResultSet resultSet = proxy(ResultSet.class, (object, method, args) -> {
            if (method.getName().equals("next")) {
                throw cause;
            }
            throw new UnsupportedOperationException(method.getName());
        });
        field(scanner, "resultSet", resultSet);
        field(scanner, "resultSetOpened", true);
        field(scanner, "block", new ArrayList<>());
        assertDiagnostic(Assertions.assertThrows(IOException.class, scanner::getNext), cause, "getNext");
    }

    @Test
    void writerOpenReportsConnectionFailure() throws Exception {
        JdbcJniWriter writer = new JdbcJniWriter(1, params());
        SQLException cause = failure();
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw cause;
            }
        };
        Method cacheKey = JdbcJniWriter.class.getDeclaredMethod("createCacheKey");
        cacheKey.setAccessible(true);
        String key = (String) cacheKey.invoke(writer);
        JdbcDataSource.getDataSource().putSource(key, pool);
        try {
            assertDiagnostic(Assertions.assertThrows(IOException.class, writer::open), cause, "open");
        } finally {
            JdbcDataSource.getDataSource().getSourcesMap().remove(key);
            pool.close();
        }
    }

    @Test
    void writerReportsBatchExecutionFailure() throws Exception {
        JdbcJniWriter writer = new JdbcJniWriter(1, params());
        SQLException cause = failure();
        PreparedStatement statement = proxy(PreparedStatement.class, (object, method, args) -> {
            if (method.getName().equals("executeBatch")) {
                throw cause;
            }
            throw new UnsupportedOperationException(method.getName());
        });
        field(writer, "preparedStatement", statement);
        OffHeap.setTesting();
        VectorTable table = VectorTable.createWritableTable(new ColumnType[0], new String[0], 1);
        try {
            assertDiagnostic(Assertions.assertThrows(IOException.class, () -> writer.writeInternal(table)),
                    cause, "write");
        } finally {
            table.close();
        }
    }

    @Test
    void writerReportsCommitFailureAndStillAttemptsRollback() throws Exception {
        Map<String, String> params = params();
        params.put("use_transaction", "true");
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        SQLException cause = failure();
        int[] rollbacks = {0};
        Connection connection = proxy(Connection.class, (object, method, args) -> {
            switch (method.getName()) {
                case "isClosed": return false;
                case "commit": throw cause;
                case "rollback":
                    rollbacks[0]++;
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        field(writer, "conn", connection);
        assertDiagnostic(Assertions.assertThrows(IOException.class, writer::close), cause, "close");
        Assertions.assertEquals(1, rollbacks[0]);
    }

    private static void assertDiagnostic(IOException error, SQLException cause, String operation) {
        Assertions.assertSame(cause, error.getCause());
        String terminal = JniUtil.throwableToString(error);
        Assertions.assertTrue(terminal.contains(operation), terminal);
        Assertions.assertTrue(terminal.contains("remote_sqlstate=42000"), terminal);
        Assertions.assertTrue(terminal.contains("remote_vendor_error_code=1142"), terminal);
        Assertions.assertTrue(terminal.contains("remote_sqlstate=08006"), terminal);
        Assertions.assertEquals(terminal.indexOf("remote_sqlstate=42000"),
                terminal.lastIndexOf("remote_sqlstate=42000"));
        Assertions.assertFalse(terminal.contains("test-password"), terminal);
        Assertions.assertFalse(JniUtil.throwableToStackTrace(error).contains("test-password"));
    }

    private static Map<String, String> params() {
        Map<String, String> params = new HashMap<>();
        params.put("jdbc_url", "jdbc:mysql://localhost/diagnostics");
        params.put("jdbc_password", "test-password");
        params.put("jdbc_driver_url", "file:///nonexistent/diagnostics-driver.jar");
        params.put("table_type", "MYSQL");
        return params;
    }

    private static void field(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
