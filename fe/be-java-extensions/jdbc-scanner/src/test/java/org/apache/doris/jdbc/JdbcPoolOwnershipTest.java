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

import org.apache.doris.jni.spi.utils.OffHeap;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

class JdbcPoolOwnershipTest {
    private static final AtomicLong NEXT_ID = new AtomicLong();

    @BeforeEach
    void enableTestAllocator() {
        OffHeap.setTesting();
    }

    @Test
    void concurrentPoolPublicationRetainsOneOwnerAndClosesOnlyUnpublishedCandidate() throws Exception {
        String key = "concurrent-pool-" + NEXT_ID.incrementAndGet();
        HikariDataSource first = new HikariDataSource();
        HikariDataSource second = new HikariDataSource();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch publish = new CountDownLatch(1);
        try {
            Future<HikariDataSource> firstResult = workers.submit(() -> {
                ready.countDown();
                publish.await();
                return JdbcDataSource.getDataSource().putSourceIfAbsent(key, first);
            });
            Future<HikariDataSource> secondResult = workers.submit(() -> {
                ready.countDown();
                publish.await();
                return JdbcDataSource.getDataSource().putSourceIfAbsent(key, second);
            });
            Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS));
            publish.countDown();
            HikariDataSource shared = firstResult.get(5, TimeUnit.SECONDS);
            Assertions.assertSame(shared, secondResult.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(shared, JdbcDataSource.getDataSource().getSource(key));
            Assertions.assertFalse(shared.isClosed());
            Assertions.assertTrue(first.isClosed() ^ second.isClosed(), "only the unpublished pool is closed");
        } finally {
            publish.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            JdbcDataSource.getDataSource().getSourcesMap().remove(key);
            first.close();
            second.close();
        }
    }

    @Test
    void sqlServerEmptyEofDoesNotAbortOrFailClose() throws Exception {
        scanToEof("SQLSERVER", 0);
    }

    @Test
    void sqlServerNonEmptyEofReturnsConnectionNormally() throws Exception {
        scanToEof("SQLSERVER", 1);
    }

    @Test
    void mysqlEmptyEofDoesNotAbortOrFailClose() throws Exception {
        scanToEof("MYSQL", 0);
    }

    @Test
    void sqlServerEarlyAbortEvictsOnlyItsBorrow() throws Exception {
        earlyAbort("SQLSERVER");
    }

    @Test
    void mysqlEarlyAbortEvictsOnlyItsBorrow() throws Exception {
        earlyAbort("MYSQL");
    }

    @Test
    void realCleanupFailureAtEofIsStillReported() throws Exception {
        try (PoolFixture fixture = new PoolFixture(1, 1)) {
            JdbcJniScanner scanner = fixture.scanner("SQLSERVER");
            try {
                scanner.open();
                Assertions.assertEquals(0, scanner.getNextBatchMeta());
                PhysicalConnection physical = fixture.connections.get(0);
                physical.resultCloseFailure = new SQLException("result close failed", "HY000", 42);
                IOException error = Assertions.assertThrows(IOException.class, scanner::close);
                Assertions.assertSame(physical.resultCloseFailure, error.getCause());
                Assertions.assertEquals(0, physical.aborts);
                Assertions.assertEquals(0, fixture.pool.getHikariPoolMXBean().getActiveConnections());
                scanner.close();
            } finally {
                scanner.releaseTable();
            }
        }
    }

    @Test
    void abortFailureStillClosesBorrowedResourcesAndReportsOriginalError() throws Exception {
        try (PoolFixture fixture = new PoolFixture(1, 1)) {
            JdbcJniScanner scanner = fixture.scanner("SQLSERVER");
            scanner.open();
            PhysicalConnection physical = fixture.connections.get(0);
            physical.abortFailure = new SQLException("abort failed", "HY000", 43);
            IOException error = Assertions.assertThrows(IOException.class, scanner::close);
            Assertions.assertSame(physical.abortFailure, error.getCause());
            Assertions.assertEquals(1, physical.aborts);
            Assertions.assertEquals(1, physical.resultCloses);
            Assertions.assertEquals(1, physical.statementCloses);
            Assertions.assertEquals(0, fixture.pool.getHikariPoolMXBean().getActiveConnections());
            scanner.close();
            Assertions.assertEquals(1, physical.aborts);
        }
    }

    @Test
    void timedOutWriterCannotShutdownPoolOrAbortActiveWriter() throws Exception {
        try (PoolFixture fixture = new PoolFixture(1)) {
            JdbcJniWriter active = fixture.writer();
            JdbcJniWriter waiting = fixture.writer();
            active.open();
            IOException error = Assertions.assertThrows(IOException.class, waiting::open);
            Assertions.assertInstanceOf(SQLException.class, error.getCause());
            waiting.close();
            fixture.assertActiveWriterUnaffected(active);
        }
    }

    @Test
    void failedWriterPrepareReleasesOnlyItsConnection() throws Exception {
        try (PoolFixture fixture = new PoolFixture(2)) {
            JdbcJniWriter active = fixture.writer();
            JdbcJniWriter failing = fixture.writer();
            active.open();
            fixture.failPrepare = true;
            IOException error = Assertions.assertThrows(IOException.class, failing::open);
            Assertions.assertSame(fixture.prepareFailure, error.getCause());
            Assertions.assertEquals(1, fixture.connections.get(1).rollbacks);
            Assertions.assertEquals(0, fixture.connections.get(1).commits);
            failing.close();
            fixture.assertActiveWriterUnaffected(active);
        }
    }

    @Test
    void closingWriterCannotShutdownAnotherActiveWriter() throws Exception {
        try (PoolFixture fixture = new PoolFixture(2)) {
            JdbcJniWriter active = fixture.writer();
            JdbcJniWriter closing = fixture.writer();
            active.open();
            closing.open();
            closing.close();
            fixture.assertActiveWriterUnaffected(active);
        }
    }

    @Test
    void timedOutScannerCannotShutdownActiveWriterPool() throws Exception {
        try (PoolFixture fixture = new PoolFixture(1)) {
            JdbcJniWriter active = fixture.writer();
            JdbcJniScanner waiting = fixture.scanner("SQLSERVER");
            active.open();
            Assertions.assertThrows(IOException.class, waiting::open);
            waiting.close();
            fixture.assertActiveWriterUnaffected(active);
        }
    }

    private static void scanToEof(String tableType, int rows) throws Exception {
        try (PoolFixture fixture = new PoolFixture(1, 1)) {
            fixture.rows = rows;
            JdbcJniScanner scanner = fixture.scanner(tableType);
            try {
                scanner.open();
                long address = scanner.getNextBatchMeta();
                Assertions.assertEquals(rows == 0, address == 0);
                Assertions.assertDoesNotThrow(scanner::close);
                Assertions.assertDoesNotThrow(scanner::close);
                Assertions.assertEquals(0, fixture.connections.get(0).aborts);
                Assertions.assertEquals(0, fixture.pool.getHikariPoolMXBean().getActiveConnections());
                Assertions.assertFalse(fixture.pool.isClosed());
                try (Connection nextBorrow = fixture.pool.getConnection()) {
                    Assertions.assertTrue(nextBorrow.isValid(1));
                }
            } finally {
                scanner.releaseTable();
            }
        }
    }

    private static void earlyAbort(String tableType) throws Exception {
        try (PoolFixture fixture = new PoolFixture(2, 1)) {
            JdbcJniScanner scanner = fixture.scanner(tableType);
            scanner.open();
            PhysicalConnection aborted = fixture.connections.get(0);
            try (Connection otherBorrow = fixture.pool.getConnection()) {
                Assertions.assertDoesNotThrow(scanner::close);
                Assertions.assertDoesNotThrow(scanner::close);
                Assertions.assertEquals(1, aborted.aborts);
                Assertions.assertTrue(aborted.closed);
                Assertions.assertEquals(1, fixture.pool.getHikariPoolMXBean().getActiveConnections());
                Assertions.assertTrue(otherBorrow.isValid(1));
                try (Connection replacement = fixture.pool.getConnection()) {
                    Assertions.assertTrue(replacement.isValid(1));
                    Assertions.assertEquals(2, fixture.pool.getHikariPoolMXBean().getActiveConnections());
                }
            }
            Assertions.assertEquals(0, fixture.pool.getHikariPoolMXBean().getActiveConnections());
            Assertions.assertFalse(fixture.pool.isClosed());
        }
    }

    private static class PoolFixture implements AutoCloseable {
        private final CopyOnWriteArrayList<PhysicalConnection> connections = new CopyOnWriteArrayList<>();
        private final SQLException prepareFailure = new SQLException("prepare failed", "HY000", 44);
        private final Map<String, String> params = new HashMap<>();
        private final HikariDataSource pool;
        private String cacheKey;
        private volatile boolean failPrepare;
        private int rows;

        private PoolFixture(int maxSize) {
            this(maxSize, 0);
        }

        private PoolFixture(int maxSize, int minSize) {
            params.put("catalog_id", String.valueOf(-NEXT_ID.incrementAndGet()));
            params.put("jdbc_url", "jdbc:fixture:pool-ownership");
            params.put("jdbc_driver_url", "file:///nonexistent/pool-ownership-driver.jar");
            params.put("connection_pool_min_size", String.valueOf(minSize));
            params.put("connection_pool_max_size", String.valueOf(maxSize));
            params.put("connection_pool_max_wait_time", "1000");
            params.put("query_sql", "select id from ownership_test");
            params.put("insert_sql", "insert into ownership_test values (?)");
            params.put("required_fields", "id");
            params.put("columns_types", "int");
            params.put("use_transaction", "true");
            HikariConfig config = new HikariConfig();
            config.setMinimumIdle(minSize);
            config.setMaximumPoolSize(maxSize);
            config.setConnectionTimeout(1000);
            config.setInitializationFailTimeout(-1);
            config.setDataSource(proxy(DataSource.class, (object, method, args) -> {
                switch (method.getName()) {
                    case "getConnection":
                        PhysicalConnection connection = new PhysicalConnection(this);
                        connections.add(connection);
                        return connection.connection;
                    case "getLoginTimeout": return 0;
                    case "setLoginTimeout": return null;
                    default: throw new UnsupportedOperationException(method.getName());
                }
            }));
            pool = new HikariDataSource(config);
        }

        private JdbcJniScanner scanner(String tableType) throws Exception {
            params.put("table_type", tableType);
            JdbcJniScanner scanner = new JdbcJniScanner(2, params);
            install(scanner);
            return scanner;
        }

        private JdbcJniWriter writer() throws Exception {
            params.put("table_type", "SQLSERVER");
            JdbcJniWriter writer = new JdbcJniWriter(2, params);
            install(writer);
            return writer;
        }

        private void install(Object owner) throws Exception {
            Method keyMethod = owner.getClass().getDeclaredMethod("createCacheKey");
            keyMethod.setAccessible(true);
            cacheKey = (String) keyMethod.invoke(owner);
            JdbcDataSource.getDataSource().putSource(cacheKey, pool);
        }

        private void assertActiveWriterUnaffected(JdbcJniWriter active) throws Exception {
            Assertions.assertFalse(pool.isClosed(), "a borrower must not shut down the shared pool");
            Assertions.assertSame(pool, JdbcDataSource.getDataSource().getSource(cacheKey));
            Assertions.assertEquals(1, pool.getHikariPoolMXBean().getActiveConnections());
            PhysicalConnection physical = connections.get(0);
            Assertions.assertFalse(physical.closed);
            Assertions.assertEquals(0, physical.aborts);
            active.close();
            Assertions.assertEquals(1, physical.commits);
            Assertions.assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
        }

        @Override
        public void close() {
            if (cacheKey != null) {
                JdbcDataSource.getDataSource().getSourcesMap().remove(cacheKey, pool);
            }
            pool.close();
        }
    }

    // Model a driver whose successful abort closes the physical connection, while Hikari's
    // proxy remains open. Resetting that physical connection must then throw, as in SQLServer.
    private static class PhysicalConnection {
        private PoolFixture fixture;
        private volatile boolean closed;
        private boolean autoCommit = true;
        private int cursor = -1;
        private int aborts;
        private int commits;
        private int rollbacks;
        private int resultCloses;
        private int statementCloses;
        private SQLException resultCloseFailure;
        private SQLException abortFailure;
        private final ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (object, method, args) -> {
            switch (method.getName()) {
                case "getColumnCount": return 1;
                case "getColumnLabel": return "id";
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        private final ResultSet resultSet = proxy(ResultSet.class, (object, method, args) -> {
            switch (method.getName()) {
                case "getMetaData": return metadata;
                case "next": return ++cursor < fixture.rows;
                case "getInt":
                case "getObject":
                    return 1;
                case "wasNull": return false;
                case "isAfterLast": return fixture.rows > 0 && cursor >= fixture.rows;
                case "close":
                    resultCloses++;
                    checkOpen();
                    if (resultCloseFailure != null) {
                        throw resultCloseFailure;
                    }
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        private final PreparedStatement statement = proxy(PreparedStatement.class, (object, method, args) -> {
            switch (method.getName()) {
                case "setFetchSize": return null;
                case "executeQuery": return resultSet;
                case "close":
                    statementCloses++;
                    checkOpen();
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });
        private final Connection connection = proxy(Connection.class, (object, method, args) -> {
            switch (method.getName()) {
                case "prepareStatement":
                    checkOpen();
                    if (fixture.failPrepare) {
                        throw fixture.prepareFailure;
                    }
                    return statement;
                case "createStatement":
                    return proxy(Statement.class, (statementObject, operation, values) -> {
                        switch (operation.getName()) {
                            case "execute": return true;
                            case "close": return null;
                            default: throw new UnsupportedOperationException(operation.getName());
                        }
                    });
                case "abort":
                    aborts++;
                    if (abortFailure != null) {
                        throw abortFailure;
                    }
                    closed = true;
                    return null;
                case "isClosed": return closed;
                case "isValid": return !closed;
                case "close":
                    closed = true;
                    return null;
                case "getAutoCommit": return autoCommit;
                case "setAutoCommit":
                    checkOpen();
                    autoCommit = (boolean) args[0];
                    return null;
                case "commit":
                    checkOpen();
                    commits++;
                    return null;
                case "rollback":
                    checkOpen();
                    rollbacks++;
                    return null;
                case "clearWarnings":
                    checkOpen();
                    return null;
                case "isReadOnly": return false;
                case "getTransactionIsolation": return Connection.TRANSACTION_READ_COMMITTED;
                case "getNetworkTimeout": return 0;
                case "setNetworkTimeout":
                case "setReadOnly":
                    return null;
                default: throw new UnsupportedOperationException(method.getName());
            }
        });

        private PhysicalConnection(PoolFixture fixture) {
            this.fixture = fixture;
        }

        private void checkOpen() throws SQLException {
            if (closed) {
                throw new SQLException("physical connection is closed", "08006");
            }
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (object, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        switch (method.getName()) {
                            case "toString": return "pool ownership fixture " + type.getSimpleName();
                            case "hashCode": return System.identityHashCode(object);
                            case "equals": return object == args[0];
                            default: throw new UnsupportedOperationException(method.getName());
                        }
                    }
                    return handler.invoke(object, method, args);
                }));
    }
}
