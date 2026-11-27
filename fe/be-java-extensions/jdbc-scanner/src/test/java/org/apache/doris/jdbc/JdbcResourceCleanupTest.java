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

import com.zaxxer.hikari.HikariConfig;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

class JdbcResourceCleanupTest {
    private static final AtomicLong NEXT_ID = new AtomicLong();

    @Test
    void scannerPrepareFailureReleasesBorrowedConnection() throws Exception {
        scannerOpenFailure("prepare", false);
    }

    @Test
    void scannerQueryFailureReleasesBorrowedResources() throws Exception {
        scannerOpenFailure("query", false);
    }

    @Test
    void scannerMetadataFailureReleasesBorrowedResources() throws Exception {
        scannerOpenFailure("metadata", false);
    }

    @Test
    void explicitCallerCleanupAfterPrepareFailureIsAControl() throws Exception {
        scannerOpenFailure("prepare", true);
    }

    @Test
    void explicitCallerCleanupAfterQueryFailureIsAControl() throws Exception {
        scannerOpenFailure("query", true);
    }

    @Test
    void explicitCallerCleanupAfterMetadataFailureIsAControl() throws Exception {
        scannerOpenFailure("metadata", true);
    }

    @Test
    void writerPrepareFailureReleasesConnectionWithoutCommit() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "prepare";
        Map<String, String> params = params();
        params.put("use_transaction", "true");
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        String key = installPool(writer, resources);
        try {
            IOException error = Assertions.assertThrows(IOException.class, writer::open);
            resources.report("writer open prepare");
            Assertions.assertAll(
                    () -> Assertions.assertSame(resources.failure, error.getCause()),
                    () -> Assertions.assertEquals(1, resources.connectionCloses, "borrowed connection"),
                    () -> Assertions.assertEquals(0, resources.commits, "failed initialization must not commit"),
                    () -> Assertions.assertEquals(1, resources.rollbacks));
        } finally {
            removePool(key);
        }
    }

    @Test
    void writerAutoCommitSetupFailureOnlyReleasesConnection() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "setAutoCommit";
        Map<String, String> params = params();
        params.put("use_transaction", "true");
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        String key = installPool(writer, resources);
        try {
            IOException error = Assertions.assertThrows(IOException.class, writer::open);
            Assertions.assertSame(resources.failure, error.getCause());
            writer.close();
            Assertions.assertEquals(0, resources.commits);
            Assertions.assertEquals(0, resources.rollbacks);
            Assertions.assertEquals(1, resources.connectionCloses);
            Assertions.assertEquals(0, resources.statementCloses);
        } finally {
            removePool(key);
        }
    }

    @Test
    void failedWriterOpenRetainsRollbackAndCloseFailuresWithoutCommit() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "prepare";
        resources.failRollback = true;
        SQLException closeFailure = new SQLException("connection close failed", "08006", 7005);
        resources.extraFailures.put("connection.close", closeFailure);
        Map<String, String> params = params();
        params.put("use_transaction", "true");
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        String key = installPool(writer, resources);
        try {
            IOException error = Assertions.assertThrows(IOException.class, writer::open);
            Assertions.assertSame(resources.failure, error.getCause());
            Assertions.assertArrayEquals(new Throwable[] {resources.rollbackFailure, closeFailure},
                    error.getCause().getSuppressed());
            Assertions.assertEquals(0, resources.commits);
            Assertions.assertEquals(1, resources.rollbacks);
            Assertions.assertEquals(1, resources.connectionCloses);
            resources.extraFailures.clear();
            writer.close();
            Assertions.assertEquals(0, resources.commits);
            Assertions.assertEquals(2, resources.connectionCloses);
        } finally {
            removePool(key);
        }
    }

    @Test
    void scannerPermanentConnectionCloseFailureNeverBecomesSuccess() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "connection.close";
        JdbcJniScanner scanner = scannerWithResources(resources);
        Assertions.assertSame(resources.failure,
                Assertions.assertThrows(IOException.class, scanner::close).getCause());
        Assertions.assertSame(resources.failure,
                Assertions.assertThrows(IOException.class, scanner::close).getCause());
        Assertions.assertEquals(2, resources.connectionCloses);
        Assertions.assertEquals(1, resources.statementCloses);
        Assertions.assertEquals(1, resources.resultSetCloses);
    }

    @Test
    void scannerResultSetCloseFailureDoesNotSkipOtherResources() throws Exception {
        scannerCloseFailure("resultSet.close");
    }

    @Test
    void scannerStatementCloseFailureDoesNotSkipConnection() throws Exception {
        scannerCloseFailure("statement.close");
    }

    @Test
    void scannerConnectionCloseFailureIsReported() throws Exception {
        scannerCloseFailure("connection.close");
    }

    @Test
    void writerStatementCloseFailureDoesNotSkipConnection() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "statement.close";
        JdbcJniWriter writer = writerWithResources(resources, false);
        IOException error = Assertions.assertThrows(IOException.class, writer::close);
        resources.report("writer statement close");
        Assertions.assertAll(
                () -> Assertions.assertSame(resources.failure, error.getCause()),
                () -> Assertions.assertEquals(1, resources.statementCloses),
                () -> Assertions.assertEquals(1, resources.connectionCloses));
    }

    @Test
    void writerCommitFailureDoesNotSkipResourceCleanup() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "commit";
        JdbcJniWriter writer = writerWithResources(resources, true);
        IOException error = Assertions.assertThrows(IOException.class, writer::close);
        resources.report("writer commit");
        Assertions.assertAll(
                () -> Assertions.assertSame(resources.failure, error.getCause()),
                () -> Assertions.assertEquals(1, resources.commits),
                () -> Assertions.assertEquals(1, resources.rollbacks),
                () -> Assertions.assertEquals(1, resources.statementCloses),
                () -> Assertions.assertEquals(1, resources.connectionCloses));
    }

    @Test
    void writerRollbackFailureIsAttachedToTheOriginalError() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "commit";
        resources.failRollback = true;
        JdbcJniWriter writer = writerWithResources(resources, true);
        IOException error = Assertions.assertThrows(IOException.class, writer::close);
        resources.report("writer commit and rollback");
        Assertions.assertAll(
                () -> Assertions.assertSame(resources.failure, error.getCause()),
                () -> Assertions.assertTrue(hasSuppressed(error, resources.rollbackFailure),
                        "rollback failure must remain attached to the returned error"),
                () -> Assertions.assertEquals(1, resources.statementCloses),
                () -> Assertions.assertEquals(1, resources.connectionCloses));
    }

    @Test
    void writerFailedConnectionCloseIsNotReportedAsSuccessOnSecondClose() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "connection.close";
        JdbcJniWriter writer = writerWithResources(resources, false);
        IOException first = Assertions.assertThrows(IOException.class, writer::close);
        Assertions.assertSame(resources.failure, first.getCause());
        resources.report("writer connection close before retry");
        Assertions.assertThrows(IOException.class, writer::close,
                "the previous close failed without closing the connection");
    }

    @Test
    void successfulScannerAndWriterCleanupIsAControl() throws Exception {
        Resources scannerResources = new Resources();
        JdbcJniScanner scanner = scannerWithResources(scannerResources);
        scanner.close();
        Assertions.assertAll(
                () -> Assertions.assertEquals(1, scannerResources.resultSetCloses),
                () -> Assertions.assertEquals(1, scannerResources.statementCloses),
                () -> Assertions.assertEquals(1, scannerResources.connectionCloses));
        Resources writerResources = new Resources();
        JdbcJniWriter writer = writerWithResources(writerResources, true);
        writer.close();
        Assertions.assertAll(
                () -> Assertions.assertEquals(1, writerResources.commits),
                () -> Assertions.assertEquals(1, writerResources.statementCloses),
                () -> Assertions.assertEquals(1, writerResources.connectionCloses));
    }

    @Test
    void fetchSizeFailureClosesUntransferredStatementAndPreservesErrors() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "fetchSize";
        RuntimeException closeFailure = new IllegalStateException("statement close failed");
        resources.extraFailures.put("statement.close", closeFailure);
        SQLException error = Assertions.assertThrows(SQLException.class,
                () -> JdbcTypeHandlerFactory.create("POSTGRESQL")
                        .initializeStatement(resources.connection, "select 1", 17));
        Assertions.assertSame(resources.failure, error);
        Assertions.assertArrayEquals(new Throwable[] {closeFailure}, error.getSuppressed());
        Assertions.assertEquals(17, resources.fetchSize);
        Assertions.assertEquals(1, resources.statementCloses);
        Assertions.assertEquals(0, resources.connectionCloses);
        Resources success = new Resources();
        Assertions.assertSame(success.statement, JdbcCleanup.setFetchSizeOrClose(success.statement, 23));
    }

    @Test
    void mysqlStreamingFetchSizeRemainsUnchanged() throws Exception {
        Resources resources = new Resources();
        Assertions.assertSame(resources.statement,
                JdbcTypeHandlerFactory.create("MYSQL")
                        .initializeStatement(resources.connection, "select 1", 17));
        Assertions.assertEquals(Integer.MIN_VALUE, resources.fetchSize);
        Assertions.assertEquals(0, resources.statementCloses);
    }

    @Test
    void mysqlFetchSizeFailureClosesStatementBeforeOwnershipTransfer() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "fetchSize";
        SQLException error = Assertions.assertThrows(SQLException.class,
                () -> JdbcTypeHandlerFactory.create("MYSQL")
                        .initializeStatement(resources.connection, "select 1", 17));
        Assertions.assertSame(resources.failure, error);
        Assertions.assertEquals(Integer.MIN_VALUE, resources.fetchSize);
        Assertions.assertEquals(1, resources.statementCloses);
    }

    @Test
    void openAndCleanupFailuresKeepPrimaryOrderAndRedactedJniDiagnostics() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "metadata";
        SQLException statementFailure = new SQLException("pwd=cleanup-secret", "HY001", 7003);
        SQLException connectionFailure = new SQLException("jdbc:postgresql://hidden/db", "08006", 7004);
        resources.extraFailures.put("statement.close", statementFailure);
        resources.extraFailures.put("connection.close", connectionFailure);
        JdbcJniScanner scanner = new JdbcJniScanner(1, params());
        String key = installPool(scanner, resources);
        try {
            IOException error = Assertions.assertThrows(IOException.class, scanner::open);
            Assertions.assertSame(resources.failure, error.getCause());
            Assertions.assertArrayEquals(new Throwable[] {statementFailure, connectionFailure},
                    error.getCause().getSuppressed());
            Assertions.assertEquals(Arrays.asList("prepare", "fetchSize", "query", "metadata", "resultSet.close",
                    "statement.close", "connection.close"), resources.events);
            String message = JniUtil.throwableToString(error);
            Assertions.assertTrue(message.contains("remote_vendor_error_code=7001"));
            Assertions.assertTrue(message.contains("remote_vendor_error_code=7003"));
            Assertions.assertTrue(message.contains("remote_sqlstate=08006"));
            Assertions.assertFalse(message.contains("cleanup-secret"));
            Assertions.assertFalse(message.contains("hidden/db"));
            resources.failureAt = "none";
            resources.extraFailures.clear();
            scanner.close();
            scanner.close();
            Assertions.assertEquals(2, resources.connectionCloses);
            Assertions.assertEquals(1, resources.resultSetCloses);
        } finally {
            removePool(key);
        }
    }

    @Test
    void streamingAbortFailureDoesNotSkipCleanupOrRepeatAbort() throws Exception {
        for (String tableType : Arrays.asList("MYSQL", "SQLSERVER")) {
            Resources resources = new Resources();
            resources.failureAt = "abort";
            resources.extraFailures.put("connection.close", new SQLException("close failed"));
            Map<String, String> params = params();
            params.put("table_type", tableType);
            JdbcJniScanner scanner = new JdbcJniScanner(1, params);
            field(scanner, "conn", resources.connection);
            field(scanner, "stmt", resources.statement);
            field(scanner, "resultSet", resources.resultSet);
            IOException error = Assertions.assertThrows(IOException.class, scanner::close);
            Assertions.assertSame(resources.failure, error.getCause());
            Assertions.assertEquals(1, resources.aborts);
            Assertions.assertEquals(1, resources.resultSetCloses);
            Assertions.assertEquals(1, resources.statementCloses);
            resources.failureAt = "none";
            resources.extraFailures.clear();
            scanner.close();
            scanner.close();
            Assertions.assertEquals(1, resources.aborts);
            Assertions.assertEquals(2, resources.connectionCloses);
        }
    }

    @Test
    void streamingAbortStateBelongsToEachBorrowedConnection() throws Exception {
        Resources resources = new Resources();
        Map<String, String> params = params();
        params.put("table_type", "MYSQL");
        JdbcJniScanner scanner = new JdbcJniScanner(1, params);
        String key = installPool(scanner, resources);
        try {
            scanner.open();
            scanner.close();
            scanner.open();
            scanner.close();
            Assertions.assertEquals(2, resources.aborts);
            Assertions.assertEquals(2, resources.connectionCloses);
        } finally {
            removePool(key);
        }
    }

    @Test
    void committedWriterDoesNotRollbackOrFinalizeAgainAfterCloseFailure() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "statement.close";
        JdbcJniWriter writer = writerWithResources(resources, true);
        IOException first = Assertions.assertThrows(IOException.class, writer::close);
        Assertions.assertSame(first, Assertions.assertThrows(IOException.class, writer::close));
        Assertions.assertEquals(1, resources.commits);
        Assertions.assertEquals(0, resources.rollbacks);
        Assertions.assertEquals(1, resources.statementCloses);
        Assertions.assertEquals(1, resources.connectionCloses);
    }

    @Test
    void zeroMinimumPoolCleanupRunsAfterBorrowedResourceFailures() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "prepare";
        RuntimeException poolFailure = new IllegalStateException("pool close failed");
        int[] poolCloses = {0};
        Map<String, String> params = params();
        params.put("connection_pool_min_size", "0");
        JdbcJniScanner scanner = new JdbcJniScanner(1, params);
        Method method = scanner.getClass().getDeclaredMethod("createCacheKey");
        method.setAccessible(true);
        String key = (String) method.invoke(scanner);
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() {
                return resources.connection;
            }

            @Override
            public void close() {
                poolCloses[0]++;
                if (poolCloses[0] == 1) {
                    throw poolFailure;
                }
                super.close();
            }
        };
        JdbcDataSource.getDataSource().putSource(key, pool);
        try {
            IOException error = Assertions.assertThrows(IOException.class, scanner::open);
            Assertions.assertSame(resources.failure, error.getCause());
            Assertions.assertArrayEquals(new Throwable[] {poolFailure}, error.getCause().getSuppressed());
            Assertions.assertEquals(1, resources.connectionCloses);
            Assertions.assertSame(pool, JdbcDataSource.getDataSource().getSource(key));
            HikariDataSource replacement = new HikariDataSource();
            JdbcDataSource.getDataSource().putSource(key, replacement);
            scanner.close();
            Assertions.assertEquals(2, poolCloses[0]);
            Assertions.assertSame(replacement, JdbcDataSource.getDataSource().getSource(key));
        } finally {
            removePool(key);
            pool.close();
        }
    }

    @Test
    void writerPoolFailureIsSuppressedWithoutRepeatingCommit() throws Exception {
        Resources resources = new Resources();
        Map<String, String> params = params();
        params.put("connection_pool_min_size", "0");
        params.put("use_transaction", "true");
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        Method method = writer.getClass().getDeclaredMethod("createCacheKey");
        method.setAccessible(true);
        String key = (String) method.invoke(writer);
        RuntimeException poolFailure = new IllegalStateException("pool close failed");
        int[] poolCloses = {0};
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() {
                return resources.connection;
            }

            @Override
            public void close() {
                poolCloses[0]++;
                throw poolFailure;
            }
        };
        JdbcDataSource.getDataSource().putSource(key, pool);
        try {
            writer.open();
            resources.failureAt = "statement.close";
            IOException error = Assertions.assertThrows(IOException.class, writer::close);
            Assertions.assertSame(resources.failure, error.getCause());
            Assertions.assertArrayEquals(new Throwable[] {poolFailure}, error.getCause().getSuppressed());
            Assertions.assertSame(error, Assertions.assertThrows(IOException.class, writer::close));
            Assertions.assertEquals(1, poolCloses[0]);
            Assertions.assertEquals(1, resources.commits);
            Assertions.assertEquals(0, resources.rollbacks);
            Assertions.assertEquals(1, resources.connectionCloses);
        } finally {
            JdbcDataSource.getDataSource().getSourcesMap().remove(key, pool);
        }
    }

    @Test
    void failedOpenReturnsRealHikariPoolSlotWithoutShuttingDownSharedPool() throws Exception {
        Resources resources = new Resources();
        resources.failureAt = "query";
        HikariConfig config = new HikariConfig();
        config.setDataSource(proxy(DataSource.class, (object, method, args) -> {
            switch (method.getName()) {
                case "getConnection": return resources.connection;
                case "getLoginTimeout": return 0;
                case "setLoginTimeout": return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        }));
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(1000);
        try (HikariDataSource pool = new HikariDataSource(config)) {
            JdbcJniScanner scanner = new JdbcJniScanner(1, params());
            Method method = scanner.getClass().getDeclaredMethod("createCacheKey");
            method.setAccessible(true);
            String key = (String) method.invoke(scanner);
            JdbcDataSource.getDataSource().putSource(key, pool);
            try {
                IOException error = Assertions.assertThrows(IOException.class, scanner::open);
                Assertions.assertSame(resources.failure, error.getCause());
                Assertions.assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
                Assertions.assertFalse(pool.isClosed());
                try (Connection borrowed = pool.getConnection()) {
                    Assertions.assertFalse(borrowed.isClosed());
                    Assertions.assertEquals(1, pool.getHikariPoolMXBean().getActiveConnections());
                }
                scanner.close();
                Assertions.assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
            } finally {
                JdbcDataSource.getDataSource().getSourcesMap().remove(key, pool);
            }
        }
    }

    private static void scannerOpenFailure(String failureAt, boolean callerCleanup) throws Exception {
        Resources resources = new Resources();
        resources.failureAt = failureAt;
        JdbcJniScanner scanner = new JdbcJniScanner(1, params());
        String key = installPool(scanner, resources);
        try {
            IOException error = Assertions.assertThrows(IOException.class, scanner::open);
            if (callerCleanup) {
                scanner.close();
            }
            resources.report("scanner open " + failureAt + " callerCleanup=" + callerCleanup);
            Assertions.assertAll(
                    () -> Assertions.assertSame(resources.failure, error.getCause()),
                    () -> Assertions.assertEquals(1, resources.connectionCloses, "borrowed connection"),
                    () -> Assertions.assertEquals("prepare".equals(failureAt) ? 0 : 1,
                            resources.statementCloses, "acquired statement"),
                    () -> Assertions.assertEquals("metadata".equals(failureAt) ? 1 : 0,
                            resources.resultSetCloses, "acquired result set"));
        } finally {
            scanner.close();
            removePool(key);
        }
    }

    private static void scannerCloseFailure(String failureAt) throws Exception {
        Resources resources = new Resources();
        resources.failureAt = failureAt;
        JdbcJniScanner scanner = scannerWithResources(resources);
        IOException error = null;
        try {
            scanner.close();
        } catch (IOException e) {
            error = e;
        }
        IOException observed = error;
        resources.report("scanner " + failureAt + " error=" + (error != null));
        Assertions.assertAll(
                () -> Assertions.assertNotNull(observed, "close failure must be reported"),
                () -> Assertions.assertSame(resources.failure, observed == null ? null : observed.getCause()),
                () -> Assertions.assertEquals(1, resources.resultSetCloses),
                () -> Assertions.assertEquals(1, resources.statementCloses),
                () -> Assertions.assertEquals(1, resources.connectionCloses));
    }

    private static JdbcJniScanner scannerWithResources(Resources resources) throws Exception {
        JdbcJniScanner scanner = new JdbcJniScanner(1, params());
        field(scanner, "conn", resources.connection);
        field(scanner, "stmt", resources.statement);
        field(scanner, "resultSet", resources.resultSet);
        return scanner;
    }

    private static JdbcJniWriter writerWithResources(Resources resources, boolean transaction) throws Exception {
        Map<String, String> params = params();
        params.put("use_transaction", String.valueOf(transaction));
        JdbcJniWriter writer = new JdbcJniWriter(1, params);
        String failureAt = resources.failureAt;
        resources.failureAt = "none";
        String key = installPool(writer, resources);
        try {
            writer.open();
        } finally {
            resources.failureAt = failureAt;
            removePool(key);
        }
        return writer;
    }

    private static Map<String, String> params() {
        Map<String, String> params = new HashMap<>();
        params.put("catalog_id", String.valueOf(NEXT_ID.incrementAndGet()));
        params.put("jdbc_url", "jdbc:postgresql://localhost/cleanup");
        params.put("table_type", "POSTGRESQL");
        params.put("jdbc_driver_url", "file:///nonexistent/cleanup-driver.jar");
        params.put("connection_pool_min_size", "1");
        params.put("query_sql", "select 1");
        params.put("insert_sql", "insert into cleanup_test values (?)");
        return params;
    }

    private static String installPool(Object target, Resources resources) throws Exception {
        Method method = target.getClass().getDeclaredMethod("createCacheKey");
        method.setAccessible(true);
        String key = (String) method.invoke(target);
        JdbcDataSource.getDataSource().putSource(key, new HikariDataSource() {
            @Override
            public Connection getConnection() {
                return resources.connection;
            }
        });
        return key;
    }

    private static void removePool(String key) {
        HikariDataSource pool = JdbcDataSource.getDataSource().getSourcesMap().remove(key);
        pool.close();
    }

    private static void field(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static boolean hasSuppressed(Throwable error, Throwable expected) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed == expected) {
                    return true;
                }
            }
        }
        return false;
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (object, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        switch (method.getName()) {
                            case "toString": return "cleanup fixture " + type.getSimpleName();
                            case "hashCode": return System.identityHashCode(object);
                            case "equals": return object == args[0];
                            default: throw new UnsupportedOperationException(method.getName());
                        }
                    }
                    return handler.invoke(object, method, args);
                }));
    }

    private static class Resources {
        private final SQLException failure = new SQLException("injected cleanup failure", "HY000", 7001);
        private final SQLException rollbackFailure = new SQLException("injected rollback failure", "HY000", 7002);
        private String failureAt = "none";
        private final Map<String, Exception> extraFailures = new HashMap<>();
        private final List<String> events = new ArrayList<>();
        private int aborts;
        private int fetchSize;
        private boolean failRollback;
        private int resultSetCloses;
        private int statementCloses;
        private int connectionCloses;
        private int commits;
        private int rollbacks;
        private final ResultSetMetaData metadata = proxy(ResultSetMetaData.class,
                (object, method, args) -> 0);
        private final ResultSet resultSet = proxy(ResultSet.class, (object, method, args) -> {
            switch (method.getName()) {
                case "getMetaData":
                    fail("metadata");
                    return metadata;
                case "isAfterLast": return false;
                case "isClosed": return false;
                case "close":
                    resultSetCloses++;
                    fail("resultSet.close");
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        private final PreparedStatement statement = proxy(PreparedStatement.class, (object, method, args) -> {
            switch (method.getName()) {
                case "setFetchSize":
                    fetchSize = (int) args[0];
                    fail("fetchSize");
                    return null;
                case "executeQuery":
                    fail("query");
                    return resultSet;
                case "isClosed": return false;
                case "close":
                    statementCloses++;
                    fail("statement.close");
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        private final Connection connection = proxy(Connection.class, (object, method, args) -> {
            switch (method.getName()) {
                case "createStatement":
                    return proxy(Statement.class, (statementObject, operation, values) -> {
                        switch (operation.getName()) {
                            case "execute": return true;
                            case "close": return null;
                            default: throw new UnsupportedOperationException(operation.getName());
                        }
                    });
                case "prepareStatement":
                    fail("prepare");
                    return statement;
                case "abort":
                    aborts++;
                    fail("abort");
                    return null;
                case "getAutoCommit": return true;
                case "isReadOnly": return false;
                case "isValid": return true;
                case "getTransactionIsolation": return Connection.TRANSACTION_READ_COMMITTED;
                case "getNetworkTimeout": return 0;
                case "setNetworkTimeout":
                case "setReadOnly":
                case "clearWarnings":
                    return null;
                case "setAutoCommit":
                    fail("setAutoCommit");
                    return null;
                case "isClosed": return false;
                case "commit":
                    commits++;
                    fail("commit");
                    return null;
                case "rollback":
                    rollbacks++;
                    if (failRollback) {
                        throw rollbackFailure;
                    }
                    return null;
                case "close":
                    connectionCloses++;
                    fail("connection.close");
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });

        private void fail(String location) throws Exception {
            events.add(location);
            if (extraFailures.containsKey(location)) {
                throw extraFailures.get(location);
            }
            if (location.equals(failureAt)) {
                throw failure;
            }
        }

        private void report(String location) {
            System.out.println("CLEANUP_PROBE " + location + " resultSet.close=" + resultSetCloses
                    + " statement.close=" + statementCloses + " connection.close=" + connectionCloses
                    + " commit=" + commits + " rollback=" + rollbacks);
        }
    }
}
