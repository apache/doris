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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JdbcWriterTransactionTest {
    @Test
    void failedSecondBatchMustRollbackInsteadOfCommit() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.failSecondBatch = true;
            f.writer.writeInternal(f.table);
            Assertions.assertEquals("1", f.writer.getStatistics().get("counter:WrittenRows"));
            IOException failure = Assertions.assertThrows(IOException.class, () -> f.writer.writeInternal(f.table));
            Assertions.assertSame(f.batchFailure, failure.getCause());
            f.writer.close(false);
            f.assertFinalization(0, 1);
            Assertions.assertEquals(Arrays.asList("batch1.ok", "batch2.fail", "rollback", "statement.close",
                    "connection.close"), f.events);
        }
    }

    @Test
    void cancellationWithoutJavaWriteErrorRollsBack() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.writer.writeInternal(f.table);
            f.writer.close(false);
            f.assertFinalization(0, 1);
        }
    }

    @Test
    void successfulWritesAndEmptyInputCommitExactlyOnce() throws Exception {
        for (int batches : new int[] {0, 2}) {
            try (Fixture f = new Fixture(true)) {
                for (int i = 0; i < batches; i++) {
                    f.writer.writeInternal(f.table);
                }
                f.writer.close(true);
                f.writer.close(true);
                f.writer.close(false);
                f.assertFinalization(1, 0);
                Assertions.assertEquals(batches, f.executions);
                Assertions.assertEquals(1, f.statementCloses);
                Assertions.assertEquals(1, f.connectionCloses);
            }
        }
    }

    @Test
    void legacyCloseRetainsSuccessfulFinalization() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.writer.writeInternal(f.table);
            f.writer.close();
            f.assertFinalization(1, 0);
        }
    }

    @Test
    void autoCommitModeDoesNotFinalizeAnExplicitTransaction() throws Exception {
        for (boolean succeeded : new boolean[] {true, false}) {
            try (Fixture f = new Fixture(false)) {
                f.failSecondBatch = !succeeded;
                f.writer.writeInternal(f.table);
                if (!succeeded) {
                    Assertions.assertThrows(IOException.class, () -> f.writer.writeInternal(f.table));
                }
                f.writer.close(succeeded);
                f.assertFinalization(0, 0);
            }
        }
    }

    @Test
    void commitFailureRollsBackAndClosesBothResourcesWithoutRetry() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.commitFailure = failure("commit");
            IOException error = Assertions.assertThrows(IOException.class, () -> f.writer.close(true));
            f.assertDiagnostic(error, f.commitFailure);
            f.writer.close(true);
            f.assertFinalization(1, 1);
        }
    }

    @Test
    void rollbackAndBothCloseFailuresPreserveFirstErrorAndEvict() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.rollbackFailure = failure("rollback");
            f.statementFailure = failure("statement");
            f.connectionFailure = failure("connection");
            f.installPool();
            IOException error = Assertions.assertThrows(IOException.class, () -> f.writer.close(false));
            f.assertDiagnostic(error, f.rollbackFailure);
            Assertions.assertEquals(2, error.getSuppressed().length);
            Assertions.assertSame(f.statementFailure, error.getSuppressed()[0].getCause());
            Assertions.assertSame(f.connectionFailure, error.getSuppressed()[1].getCause());
            Assertions.assertEquals(Arrays.asList("rollback", "evict", "statement.close", "connection.close"),
                    f.events);
            f.writer.close(false);
            f.assertFinalization(0, 1);
        }
    }

    @Test
    void statementCloseFailureAfterCommitDoesNotRollback() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.statementFailure = failure("statement");
            IOException error = Assertions.assertThrows(IOException.class, () -> f.writer.close(true));
            f.assertDiagnostic(error, f.statementFailure);
            f.assertFinalization(1, 0);
        }
    }

    @Test
    void runtimeCleanupFailureStillClosesConnection() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.statementFailure = new IllegalStateException("test-password statement failure");
            IOException error = Assertions.assertThrows(IOException.class, () -> f.writer.close(false));
            f.assertDiagnostic(error, f.statementFailure);
            f.assertFinalization(0, 1);
        }
    }

    @Test
    void realOpenTracksTransactionAndClosesSuccessfully() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.open();
            f.writer.close(true);
            f.assertFinalization(1, 0);
            Assertions.assertTrue(f.events.indexOf("begin") < f.events.indexOf("prepare"));
        }
    }

    @Test
    void prepareFailureRollsBackBorrowedConnectionAndPreservesOpenError() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.prepareFailure = failure("prepare");
            f.rollbackFailure = failure("rollback");
            IOException error = Assertions.assertThrows(IOException.class, f::open);
            f.assertDiagnostic(error, f.prepareFailure);
            Assertions.assertTrue(error.getMessage().contains("open failed"));
            Assertions.assertEquals(1, error.getSuppressed().length);
            Assertions.assertSame(f.rollbackFailure, error.getSuppressed()[0].getCause());
            Assertions.assertEquals(Arrays.asList("begin", "prepare", "rollback", "evict", "connection.close"),
                    f.events);
            f.writer.close(false);
            Assertions.assertEquals(0, f.commits);
            Assertions.assertEquals(1, f.connectionCloses);
        }
    }

    @Test
    void failedTransactionSetupDoesNotCommitOrRollback() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.beginFailure = failure("begin");
            IOException error = Assertions.assertThrows(IOException.class, f::open);
            f.assertDiagnostic(error, f.beginFailure);
            Assertions.assertEquals(Arrays.asList("begin", "connection.close"), f.events);
            Assertions.assertEquals(0, f.commits);
            Assertions.assertEquals(0, f.rollbacks);
        }
    }

    private static SQLException failure(String operation) {
        return new SQLException("test-password " + operation + " jdbc:mysql://localhost/transaction_test", "42000", 1142);
    }

    private static class Fixture implements AutoCloseable {
        private final SQLException batchFailure = new SQLException("injected second batch failure", "23000", 1062);
        private final List<String> events = new ArrayList<>();
        private final JdbcJniWriter writer;
        private final VectorTable table;
        private final Connection connection;
        private final PreparedStatement statement;
        private boolean failSecondBatch;
        private Exception commitFailure;
        private Exception rollbackFailure;
        private Exception statementFailure;
        private Exception connectionFailure;
        private Exception prepareFailure;
        private Exception beginFailure;
        private int executions;
        private int commits;
        private int rollbacks;
        private int statementCloses;
        private int connectionCloses;
        private String poolKey;
        private HikariDataSource pool;

        private Fixture(boolean useTransaction) throws Exception {
            Map<String, String> params = new HashMap<>();
            params.put("table_type", "MYSQL");
            params.put("jdbc_url", "jdbc:mysql://localhost/transaction_test");
            params.put("jdbc_password", "test-password");
            params.put("jdbc_driver_url", "file:///nonexistent/transaction-driver.jar");
            params.put("use_transaction", Boolean.toString(useTransaction));
            writer = new JdbcJniWriter(1, params);
            statement = proxy(PreparedStatement.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "setInt":
                    case "addBatch":
                    case "clearBatch":
                        return null;
                    case "executeBatch":
                        executions++;
                        if (failSecondBatch && executions == 2) {
                            events.add("batch2.fail");
                            throw batchFailure;
                        }
                        events.add("batch" + executions + ".ok");
                        return new int[] {1};
                    case "close":
                        events.add("statement.close");
                        statementCloses++;
                        if (statementFailure != null) {
                            throw statementFailure;
                        }
                        return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
            connection = proxy(Connection.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "createStatement":
                        return proxy(Statement.class, (o, m, a) -> {
                            switch (m.getName()) {
                                case "execute": return true;
                                case "close": return null;
                                default: throw new UnsupportedOperationException(m.getName());
                            }
                        });
                    case "setAutoCommit":
                        Assertions.assertEquals(false, args[0]);
                        events.add("begin");
                        if (beginFailure != null) {
                            throw beginFailure;
                        }
                        return null;
                    case "prepareStatement":
                        events.add("prepare");
                        if (prepareFailure != null) {
                            throw prepareFailure;
                        }
                        return statement;
                    case "commit":
                        events.add("commit");
                        commits++;
                        if (commitFailure != null) {
                            throw commitFailure;
                        }
                        return null;
                    case "rollback":
                        events.add("rollback");
                        rollbacks++;
                        if (rollbackFailure != null) {
                            throw rollbackFailure;
                        }
                        return null;
                    case "close":
                        events.add("connection.close");
                        connectionCloses++;
                        if (connectionFailure != null) {
                            throw connectionFailure;
                        }
                        return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            });
            setField(writer, "conn", connection);
            setField(writer, "preparedStatement", statement);
            // These fixtures bypass open; real-open cases below verify this flag's initialization.
            setField(writer, "transactionActive", useTransaction);
            OffHeap.setTesting();
            table = VectorTable.createWritableTable(new ColumnType[] {ColumnType.parseType("id", "int")},
                    new String[] {"id"}, 1);
            table.appendData(0, new Integer[] {1}, false);
        }

        private void installPool() throws Exception {
            pool = new HikariDataSource() {
                @Override
                public Connection getConnection() {
                    return connection;
                }

                @Override
                public void evictConnection(Connection conn) {
                    Assertions.assertSame(connection, conn);
                    events.add("evict");
                }
            };
            setField(writer, "hikariDataSource", pool);
        }

        private void open() throws Exception {
            setField(writer, "conn", null);
            setField(writer, "preparedStatement", null);
            setField(writer, "transactionActive", false);
            installPool();
            Method cacheKey = JdbcJniWriter.class.getDeclaredMethod("createCacheKey");
            cacheKey.setAccessible(true);
            poolKey = (String) cacheKey.invoke(writer);
            JdbcDataSource.getDataSource().putSource(poolKey, pool);
            writer.open();
        }

        private void assertFinalization(int expectedCommits, int expectedRollbacks) {
            Assertions.assertAll(
                    () -> Assertions.assertEquals(expectedCommits, commits),
                    () -> Assertions.assertEquals(expectedRollbacks, rollbacks),
                    () -> Assertions.assertEquals(1, statementCloses),
                    () -> Assertions.assertEquals(1, connectionCloses));
        }

        private void assertDiagnostic(IOException error, Exception cause) {
            Assertions.assertSame(cause, error.getCause());
            String diagnostic = JniUtil.throwableToString(error);
            String stack = JniUtil.throwableToStackTrace(error);
            Assertions.assertFalse(diagnostic.contains("test-password"), diagnostic);
            Assertions.assertFalse(stack.contains("test-password"), stack);
            Assertions.assertFalse(stack.contains("jdbc:mysql://localhost/transaction_test"), stack);
            if (cause instanceof SQLException) {
                Assertions.assertTrue(diagnostic.contains("remote_sqlstate=42000"), diagnostic);
                Assertions.assertTrue(diagnostic.contains("remote_vendor_error_code=1142"), diagnostic);
            }
        }

        @Override
        public void close() {
            table.close();
            if (poolKey != null) {
                JdbcDataSource.getDataSource().getSourcesMap().remove(poolKey, pool);
            }
            if (pool != null) {
                pool.close();
            }
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }
}
