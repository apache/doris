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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

class JdbcConnectionLogDiagnosticsTest {
    @Test
    void scannerWriterAndTesterMaskCompleteEscapedSqlServerUrl() throws Exception {
        String password = "two\";words";
        String url = "jdbc:sqlserver://host;password={" + password + "};databaseName=db";
        for (Class<?> type : new Class<?>[] {JdbcJniScanner.class, JdbcJniWriter.class, JdbcConnectionTester.class}) {
            Map<String, String> params = new HashMap<>();
            params.put("table_type", "SQLSERVER");
            params.put("jdbc_url", url);
            params.put("jdbc_password", password);
            params.put("catalog_id", "68811");
            params.put("jdbc_driver_class", StubDriver.class.getName());
            params.put("jdbc_driver_url", StubDriver.class.getProtectionDomain().getCodeSource()
                    .getLocation().toURI().toString());
            Object target = type.getConstructor(int.class, Map.class).newInstance(1, params);
            Method cacheKey = type.getDeclaredMethod("createCacheKey");
            cacheKey.setAccessible(true);
            String key = (String) cacheKey.invoke(target);
            Connection connection = Mockito.mock(Connection.class);
            PreparedStatement statement = Mockito.mock(PreparedStatement.class);
            ResultSet result = Mockito.mock(ResultSet.class);
            Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
            Mockito.when(statement.executeQuery()).thenReturn(result);
            Mockito.when(result.next()).thenReturn(true);
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            if (type == JdbcConnectionTester.class) {
                JdbcDataSource.getDataSource().putSource(key, new HikariDataSource() {
                    @Override
                    public Connection getConnection() {
                        return connection;
                    }
                });
            }
            try (CapturedLog capture = new CapturedLog(type)) {
                Method initialize = type.getDeclaredMethod(type == JdbcConnectionTester.class
                        ? "openInternal" : "initializeClassLoaderAndDataSource");
                initialize.setAccessible(true);
                initialize.invoke(target);
                Assertions.assertNotNull(JdbcDataSource.getDataSource().getSource(key));
                Assertions.assertEquals(1, capture.messages.size(), capture.messages.toString());
                String text = capture.messages.get(0);
                Assertions.assertTrue(text.contains(type.getSimpleName()), text);
                Assertions.assertTrue(text.contains("[redacted JDBC URL]"), text);
                Assertions.assertFalse(text.contains(url), text);
                Assertions.assertFalse(text.contains(password), text);
                Assertions.assertFalse(text.contains("words}"), text);
            } finally {
                try {
                    // Initialization can fail before the pool is added to the cache.
                    HikariDataSource pool = JdbcDataSource.getDataSource().getSourcesMap().remove(key);
                    if (pool != null) {
                        pool.close();
                    }
                } finally {
                    Thread.currentThread().setContextClassLoader(original);
                }
            }
        }
    }

    /** Lets real Hikari configuration load a driver without opening any external connection. */
    public static class StubDriver implements Driver {
        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            throw new SQLException("This test driver must not open a connection");
        }

        @Override
        public boolean acceptsURL(String url) {
            return url.startsWith("jdbc:sqlserver:");
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
        public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getGlobal();
        }
    }

    // Production uses JUL; tests may select the inherited Log4j SLF4J provider instead.
    private static class CapturedLog implements AutoCloseable {
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final java.util.logging.Logger jul;
        private final java.util.logging.Level previousJulLevel;
        private final Logger log4j;
        private final Level previousLevel;
        private final Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                messages.add(record.getMessage());
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        private final AbstractAppender appender = new AbstractAppender("jdbc-success-test", null,
                null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };

        CapturedLog(Class<?> type) {
            jul = java.util.logging.Logger.getLogger(type.getName());
            previousJulLevel = jul.getLevel();
            log4j = (Logger) LogManager.getLogger(type);
            previousLevel = log4j.getLevel();
            jul.setLevel(java.util.logging.Level.ALL);
            jul.addHandler(handler);
            appender.start();
            log4j.addAppender(appender);
            log4j.setLevel(Level.ALL);
        }

        @Override
        public void close() {
            log4j.removeAppender(appender);
            log4j.setLevel(previousLevel);
            appender.stop();
            jul.removeHandler(handler);
            jul.setLevel(previousJulLevel);
            handler.close();
        }
    }
}
