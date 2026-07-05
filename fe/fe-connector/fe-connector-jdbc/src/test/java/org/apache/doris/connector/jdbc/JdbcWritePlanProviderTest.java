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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.jdbc.client.JdbcFieldInfo;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TJdbcTableSink;
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Byte-parity tests for {@link JdbcWritePlanProvider} (P6.3-T02 / RFC OQ-1).
 *
 * <p>The jdbc {@code TJdbcTableSink} assembly moved out of fe-core
 * ({@code PluginDrivenTableSink.bindJdbcWriteSink} + the deleted
 * {@code JdbcConnectorMetadata.getWriteConfig} property bag) into this connector
 * provider. These tests lock the produced Thrift field values so the move stays
 * byte-identical to the legacy write path (no BE change).</p>
 */
class JdbcWritePlanProviderTest {

    /**
     * Test double for {@link JdbcConnectorClient}: the protected constructor only sets
     * fields (no data source), so we just feed a db type and the remote columns the
     * write SQL is built from.
     */
    private static final class FakeJdbcClient extends JdbcConnectorClient {
        private final List<JdbcFieldInfo> fields;

        private FakeJdbcClient(JdbcDbType dbType, List<JdbcFieldInfo> fields) {
            super("test_catalog", dbType, "jdbc:mysql://h:3306/test_db",
                    false, null, null, false, false);
            this.fields = fields;
        }

        @Override
        public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
            return fields;
        }

        @Override
        public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
            return null;
        }
    }

    private static JdbcFieldInfo field(String name) {
        return new JdbcFieldInfo(name, Optional.empty(), 0,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static ConnectorWriteHandle writeHandle(ConnectorTableHandle table, List<String> cols) {
        List<ConnectorColumn> columns = new java.util.ArrayList<>();
        for (String c : cols) {
            columns.add(new ConnectorColumn(c, ConnectorType.of("INT"), null, true, null));
        }
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return table;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return columns;
            }

            @Override
            public boolean isOverwrite() {
                return false;
            }

            @Override
            public Map<String, String> getWriteContext() {
                return Collections.emptyMap();
            }
        };
    }

    private static ConnectorSession session(long catalogId, Map<String, String> sessionProps) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en";
            }

            @Override
            public long getCatalogId() {
                return catalogId;
            }

            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return sessionProps;
            }
        };
    }

    @Test
    void planWriteBuildsJdbcTableSinkWithByteParityFields() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://h:3306/test_db");
        props.put("user", "root");
        props.put("password", "secret");
        props.put("driver_class", "com.mysql.cj.jdbc.Driver");
        props.put("driver_url", "mysql-connector-j-8.4.0.jar");
        props.put("checksum", "abc123");
        props.put("connection_pool_min_size", "2");
        props.put("connection_pool_max_size", "20");
        props.put("connection_pool_max_wait_time", "6000");
        props.put("connection_pool_max_life_time", "900000");
        props.put("connection_pool_keep_alive", "true");

        FakeJdbcClient client = new FakeJdbcClient(
                JdbcDbType.MYSQL, Arrays.asList(field("id"), field("name")));
        JdbcWritePlanProvider provider = new JdbcWritePlanProvider(client, props);

        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("enable_odbc_transcation", "true");
        ConnectorWriteHandle handle = writeHandle(
                new JdbcTableHandle("test_db", "t1"), Arrays.asList("id", "name"));

        ConnectorSinkPlan plan = provider.planWrite(session(7L, sessionProps), handle);
        TDataSink dataSink = plan.getDataSink();

        Assertions.assertEquals(TDataSinkType.JDBC_TABLE_SINK, dataSink.getType());
        TJdbcTableSink sink = dataSink.getJdbcTableSink();
        TJdbcTable t = sink.getJdbcTable();

        Assertions.assertEquals("jdbc:mysql://h:3306/test_db", t.getJdbcUrl());
        Assertions.assertEquals("root", t.getJdbcUser());
        Assertions.assertEquals("secret", t.getJdbcPassword());
        Assertions.assertEquals("mysql-connector-j-8.4.0.jar", t.getJdbcDriverUrl());
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", t.getJdbcDriverClass());
        Assertions.assertEquals("abc123", t.getJdbcDriverChecksum());
        Assertions.assertEquals("t1", t.getJdbcTableName());
        Assertions.assertEquals("", t.getJdbcResourceName());
        Assertions.assertEquals(7L, t.getCatalogId());
        Assertions.assertEquals(2, t.getConnectionPoolMinSize());
        Assertions.assertEquals(20, t.getConnectionPoolMaxSize());
        Assertions.assertEquals(6000, t.getConnectionPoolMaxWaitTime());
        Assertions.assertEquals(900000, t.getConnectionPoolMaxLifeTime());
        Assertions.assertTrue(t.isConnectionPoolKeepAlive());

        Assertions.assertEquals(
                "INSERT INTO `test_db`.`t1`(`id`,`name`) VALUES (?, ?)", sink.getInsertSql());
        Assertions.assertTrue(sink.isUseTransaction());
        Assertions.assertEquals(TOdbcTableType.MYSQL, sink.getTableType());
    }

    @Test
    void appendExplainInfoEmitsConnectorWriteDetail() {
        // The unified plan-provider sink is source-agnostic; the jdbc connector restores its
        // INSERT SQL / table type / use-transaction lines in EXPLAIN via this hook.
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://h:3306/test_db");

        FakeJdbcClient client = new FakeJdbcClient(
                JdbcDbType.MYSQL, Arrays.asList(field("id"), field("name")));
        JdbcWritePlanProvider provider = new JdbcWritePlanProvider(client, props);

        Map<String, String> sessionProps = new HashMap<>();
        sessionProps.put("enable_odbc_transcation", "true");
        ConnectorWriteHandle handle = writeHandle(
                new JdbcTableHandle("test_db", "t1"), Arrays.asList("id", "name"));

        StringBuilder sb = new StringBuilder();
        provider.appendExplainInfo(sb, "  ", session(7L, sessionProps), handle);
        String explain = sb.toString();

        Assertions.assertTrue(explain.contains("TABLE TYPE: MYSQL"), explain);
        Assertions.assertTrue(explain.contains(
                "INSERT SQL: INSERT INTO `test_db`.`t1`(`id`,`name`) VALUES (?, ?)"), explain);
        Assertions.assertTrue(explain.contains("USE TRANSACTION: true"), explain);
    }

    @Test
    void planWritePoolAndTxnFieldsFallBackToLegacyDefaultsWhenAbsent() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://h:3306/test_db");

        FakeJdbcClient client = new FakeJdbcClient(
                JdbcDbType.MYSQL, Collections.singletonList(field("c")));
        JdbcWritePlanProvider provider = new JdbcWritePlanProvider(client, props);

        ConnectorWriteHandle handle = writeHandle(
                new JdbcTableHandle("test_db", "t1"), Collections.singletonList("c"));
        ConnectorSinkPlan plan = provider.planWrite(
                session(1L, Collections.emptyMap()), handle);
        TJdbcTableSink sink = plan.getDataSink().getJdbcTableSink();
        TJdbcTable t = sink.getJdbcTable();

        // Pool values come from JdbcConnectorProperties.getInt(..., DEFAULT_*), exactly as the
        // legacy getWriteConfig computed them — NOT the bindJdbcWriteSink hard-coded fallbacks
        // (which were unreachable because the property bag always carried the computed values).
        Assertions.assertEquals(
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE, t.getConnectionPoolMinSize());
        Assertions.assertEquals(
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE, t.getConnectionPoolMaxSize());
        Assertions.assertEquals(
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME, t.getConnectionPoolMaxWaitTime());
        Assertions.assertEquals(
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME, t.getConnectionPoolMaxLifeTime());
        Assertions.assertFalse(t.isConnectionPoolKeepAlive());
        // enable_odbc_transcation absent -> false (note the legacy session-key spelling preserved).
        Assertions.assertFalse(sink.isUseTransaction());
    }
}
