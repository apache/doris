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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarOutputStream;
import java.util.logging.Logger;

/**
 * A connection borrowed from the HikariCP pool goes back to it only when it is closed. BE does not
 * call close() on a scanner, writer or connection tester whose open() failed, so a failed open has
 * to give the connection back by itself, and close() has to give it back even when closing the
 * statement fails first. Each connection that is not given back holds its pool slot forever, and
 * once all of them are held every query on the catalog times out waiting for a connection.
 *
 * <p>The pools here hold a single connection, so a connection that leaks shows up both as an
 * active connection after the scan and as the next open timing out instead of reaching the
 * database.
 */
public class JdbcConnectionReleaseTest {
    private static final String PREPARE_FAILS = "jdbc:fake:prepare-fails";
    private static final String QUERY_FAILS = "jdbc:fake:query-fails";
    private static final String STATEMENT_CLOSE_FAILS = "jdbc:fake:statement-close-fails";
    private static final String PREPARE_ERROR = "prepare failed on purpose";
    private static final String QUERY_ERROR = "query failed on purpose";

    @TempDir
    Path tempDir;

    @AfterEach
    public void closePools() {
        Iterator<Map.Entry<String, HikariDataSource>> it =
                JdbcDataSource.getDataSource().getSourcesMap().entrySet().iterator();
        while (it.hasNext()) {
            HikariDataSource ds = it.next().getValue();
            if (ds.getJdbcUrl().startsWith("jdbc:fake:")) {
                ds.close();
                it.remove();
            }
        }
    }

    @Test
    public void scannerGivesTheConnectionBackWhenOpenFails() throws IOException {
        for (int i = 0; i < 3; i++) {
            JdbcJniScanner scanner = new JdbcJniScanner(1024, params(PREPARE_FAILS));
            assertOpenFailsWith(PREPARE_ERROR, scanner::open);
        }
        Assertions.assertEquals(0, activeConnections(PREPARE_FAILS));
    }

    @Test
    public void scannerGivesTheConnectionBackWhenClosingTheStatementFails() throws IOException {
        for (int i = 0; i < 3; i++) {
            JdbcJniScanner scanner = new JdbcJniScanner(1024, params(STATEMENT_CLOSE_FAILS));
            scanner.open();
            Assertions.assertEquals(1, activeConnections(STATEMENT_CLOSE_FAILS));
            scanner.close();
            Assertions.assertEquals(0, activeConnections(STATEMENT_CLOSE_FAILS));
        }
    }

    @Test
    public void writerGivesTheConnectionBackWhenOpenFails() throws IOException {
        for (boolean useTransaction : new boolean[] {false, true}) {
            for (int i = 0; i < 3; i++) {
                Map<String, String> params = params(PREPARE_FAILS);
                params.put("insert_sql", "INSERT INTO t VALUES (?)");
                params.put("use_transaction", String.valueOf(useTransaction));
                JdbcJniWriter writer = new JdbcJniWriter(1024, params);
                assertOpenFailsWith(PREPARE_ERROR, writer::open);
            }
            Assertions.assertEquals(0, activeConnections(PREPARE_FAILS));
        }
    }

    @Test
    public void connectionTesterGivesTheConnectionBackWhenTheTestFails() throws IOException {
        for (int i = 0; i < 3; i++) {
            JdbcConnectionTester tester = new JdbcConnectionTester(1024, params(QUERY_FAILS));
            assertOpenFailsWith(QUERY_ERROR, tester::open);
        }
        Assertions.assertEquals(0, activeConnections(QUERY_FAILS));
    }

    private interface Open {
        void open() throws IOException;
    }

    /**
     * The open must fail because of the database, not because the pool had no connection left to
     * hand out, which is how an earlier open that leaked its connection shows up.
     */
    private static void assertOpenFailsWith(String expected, Open open) {
        IOException e = Assertions.assertThrows(IOException.class, open::open);
        Assertions.assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    private static int activeConnections(String jdbcUrl) {
        for (HikariDataSource ds : JdbcDataSource.getDataSource().getSourcesMap().values()) {
            if (ds.getJdbcUrl().equals(jdbcUrl)) {
                return ds.getHikariPoolMXBean().getActiveConnections();
            }
        }
        throw new AssertionError("no connection pool for " + jdbcUrl);
    }

    private Map<String, String> params(String jdbcUrl) throws IOException {
        Path jar = tempDir.resolve("driver.jar");
        if (!Files.exists(jar)) {
            // FakeDriver is resolved through the parent of the driver jar's classloader.
            new JarOutputStream(Files.newOutputStream(jar)).close();
        }
        Map<String, String> params = new HashMap<>();
        params.put("jdbc_url", jdbcUrl);
        params.put("jdbc_driver_class", FakeDriver.class.getName());
        params.put("jdbc_driver_url", jar.toUri().toURL().toString());
        params.put("query_sql", "SELECT 1");
        params.put("connection_pool_min_size", "1");
        params.put("connection_pool_max_size", "1");
        // HikariCP's smallest connection timeout, so that a leaked connection fails fast.
        params.put("connection_pool_max_wait_time", "250");
        return params;
    }

    /** A driver whose connections fail the way their url says. */
    public static class FakeDriver implements Driver {
        @Override
        public Connection connect(String url, Properties info) {
            return acceptsURL(url) ? proxy(Connection.class, connection(url)) : null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url.startsWith("jdbc:fake:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        private static InvocationHandler connection(String url) {
            return (proxy, method, args) -> {
                switch (method.getName()) {
                    case "isValid":
                        return true;
                    case "createStatement":
                        // HikariCP's validation query.
                        return proxy(Statement.class, defaults());
                    case "prepareStatement":
                        if (url.equals(PREPARE_FAILS)) {
                            throw new SQLException(PREPARE_ERROR);
                        }
                        return proxy(PreparedStatement.class, statement(url));
                    default:
                        return defaultValue(proxy, method.getName(), method.getReturnType(), args);
                }
            };
        }

        private static InvocationHandler statement(String url) {
            return (proxy, method, args) -> {
                switch (method.getName()) {
                    case "executeQuery":
                        if (url.equals(QUERY_FAILS)) {
                            throw new SQLException(QUERY_ERROR);
                        }
                        return proxy(ResultSet.class, (rs, m, a) -> m.getName().equals("getMetaData")
                                ? proxy(ResultSetMetaData.class, defaults())
                                : defaultValue(rs, m.getName(), m.getReturnType(), a));
                    case "close":
                        if (url.equals(STATEMENT_CLOSE_FAILS)) {
                            throw new SQLException("statement close failed on purpose");
                        }
                        return null;
                    default:
                        return defaultValue(proxy, method.getName(), method.getReturnType(), args);
                }
            };
        }

        private static InvocationHandler defaults() {
            return (proxy, method, args) -> defaultValue(proxy, method.getName(), method.getReturnType(), args);
        }

        private static Object defaultValue(Object proxy, String name, Class<?> type, Object[] args) {
            switch (name) {
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "toString":
                    return "fake";
                default:
                    break;
            }
            if (type == boolean.class) {
                return false;
            } else if (type == int.class) {
                return 0;
            } else if (type == long.class) {
                return 0L;
            }
            return null;
        }

        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return type.cast(Proxy.newProxyInstance(FakeDriver.class.getClassLoader(),
                    new Class<?>[] {type}, handler));
        }
    }
}
