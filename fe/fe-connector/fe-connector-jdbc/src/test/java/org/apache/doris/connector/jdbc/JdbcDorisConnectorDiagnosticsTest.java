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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.jdbc.client.JdbcFieldInfo;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorType;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

class JdbcDorisConnectorDiagnosticsTest {
    @Test
    void clientCreationAndConnectionFailureLogsRedactUrlsAndOriginalCauses() throws Exception {
        String password = "test-password";
        String url = "jdbc:mysql://localhost/remote?password=url-secret";
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", url);
        props.put("password", password);
        props.put("driver_class", "java.lang.Object");
        ConnectorContext context = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "diagnostics";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Collections.emptyMap();
            }
        };
        JdbcDorisConnector connector = new JdbcDorisConnector(props, context);
        SQLException raw = new SQLException("raw=" + password + " url=" + url, "08001", 0);
        JdbcConnectorClient failingClient = new JdbcConnectorClient("diagnostics", JdbcDbType.MYSQL, url, false,
                Collections.emptyMap(), Collections.emptyMap(), false, false) {
            @Override
            public List<String> getDatabaseNameList() {
                throw jdbcException("load databases", raw);
            }

            @Override
            public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo field) {
                throw new UnsupportedOperationException();
            }
        };
        List<LogEvent> events = new CopyOnWriteArrayList<>();
        AbstractAppender appender = new AbstractAppender("jdbc-diagnostics-test", null,
                PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        Logger logger = (Logger) LogManager.getLogger(JdbcDorisConnector.class);
        appender.start();
        Level previousLevel = logger.getLevel();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            // This fails before any remote IO, after logging the client creation event.
            Assertions.assertFalse(connector.testConnection(null).isSuccess());
            Field client = JdbcDorisConnector.class.getDeclaredField("client");
            client.setAccessible(true);
            client.set(connector, failingClient);
            ConnectorTestResult result = connector.testConnection(null);
            Assertions.assertFalse(result.isSuccess());
            Assertions.assertTrue(result.getMessage().contains("remote_sqlstate=08001"));
            Assertions.assertFalse(result.getMessage().contains(password));
            Assertions.assertTrue(events.stream().anyMatch(event -> event.getMessage().getFormattedMessage()
                    .contains("Creating JDBC connector client")));
            Assertions.assertTrue(events.stream().anyMatch(event -> event.getMessage().getFormattedMessage()
                    .contains("remote_sqlstate=08001")));
            for (LogEvent event : events) {
                Assertions.assertNull(event.getThrown(), "A raw cause would bypass diagnostic sanitization");
                String message = event.getMessage().getFormattedMessage();
                Assertions.assertFalse(message.contains(password), message);
                Assertions.assertFalse(message.contains("url-secret"), message);
                Assertions.assertFalse(message.contains(url), message);
            }
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }
}
