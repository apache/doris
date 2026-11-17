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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.jdbc.JdbcDbType;
import org.apache.doris.connector.spi.DiagnosticException;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class JdbcDelegatedDiagnosticsTest {
    private static final String PASSWORD = "test-password";
    private static final String URL = "jdbc:oceanbase://localhost/remote?password=url-secret";

    @Test
    void oceanBaseFallbackSharesPasswordWithTheActualDelegateAndSanitizesItsLog() throws Exception {
        SQLException raw = new SQLException("raw=" + PASSWORD + " url=" + URL, "42000", 1142);
        Connection first = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (object, method, args) -> {
                    switch (method.getName()) {
                        case "createStatement": throw raw;
                        case "close": return null;
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });
        int[] calls = {0};
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                if (calls[0]++ == 0) {
                    return first;
                }
                throw raw;
            }
        };
        JdbcOceanBaseConnectorClient client = new JdbcOceanBaseConnectorClient("diagnostics", JdbcDbType.OCEANBASE,
                URL, false, Collections.emptyMap(), Collections.emptyMap(), false, false);
        configure(client, pool);
        List<LogEvent> events = new CopyOnWriteArrayList<>();
        AbstractAppender appender = recordingAppender(events);
        Logger logger = (Logger) LogManager.getLogger(JdbcOceanBaseConnectorClient.class);
        Level previousLevel = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> client.getPrimaryKeys("remote", "diagnostics_table"));
            Assertions.assertSame(raw, error.getCause());
            Assertions.assertTrue(error.getMessage().contains("remote_sqlstate=42000"));
            Assertions.assertFalse(error.getMessage().contains(PASSWORD), error.getMessage());
            Assertions.assertFalse(((DiagnosticException) error).getDiagnosticStackTrace(error).contains(PASSWORD));
            assertSafeLogs(events);
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
            pool.close();
        }
    }

    @Test
    void optionalRowCountFailuresKeepTheirExistingFallbackAndSanitizeLogs() throws Exception {
        HikariDataSource pool = new HikariDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("raw=" + PASSWORD + " url=" + URL, "08001", 0);
            }
        };
        List<JdbcConnectorClient> clients = Arrays.asList(
                new JdbcMySQLConnectorClient("diagnostics", JdbcDbType.MYSQL, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcOracleConnectorClient("diagnostics", JdbcDbType.ORACLE, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcPostgreSQLConnectorClient("diagnostics", JdbcDbType.POSTGRESQL, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false),
                new JdbcSQLServerConnectorClient("diagnostics", JdbcDbType.SQLSERVER, URL, false,
                        Collections.emptyMap(), Collections.emptyMap(), false, false));
        try {
            for (JdbcConnectorClient client : clients) {
                configure(client, pool);
                List<LogEvent> events = new CopyOnWriteArrayList<>();
                AbstractAppender appender = recordingAppender(events);
                Logger logger = (Logger) LogManager.getLogger(client.getClass());
                Level previousLevel = logger.getLevel();
                logger.addAppender(appender);
                logger.setLevel(Level.INFO);
                try {
                    Assertions.assertEquals(-1, client.getRowCount("remote", "diagnostics_table"));
                    assertSafeLogs(events);
                } finally {
                    logger.removeAppender(appender);
                    logger.setLevel(previousLevel);
                    appender.stop();
                }
            }
        } finally {
            pool.close();
        }
    }

    private static void configure(JdbcConnectorClient client, HikariDataSource pool) throws Exception {
        client.dataSource = pool;
        Field password = JdbcConnectorClient.class.getDeclaredField("jdbcPassword");
        password.setAccessible(true);
        password.set(client, PASSWORD);
    }

    private static AbstractAppender recordingAppender(List<LogEvent> events) {
        AbstractAppender appender = new AbstractAppender("jdbc-client-diagnostics-test", null,
                null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        return appender;
    }

    private static void assertSafeLogs(List<LogEvent> events) {
        Assertions.assertFalse(events.isEmpty());
        for (LogEvent event : events) {
            Assertions.assertNull(event.getThrown());
            String text = event.getMessage().getFormattedMessage();
            Assertions.assertTrue(text.contains("remote_sqlstate="), text);
            Assertions.assertFalse(text.contains(PASSWORD), text);
            Assertions.assertFalse(text.contains("url-secret"), text);
        }
    }
}
