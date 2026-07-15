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

package org.apache.doris.cdcclient.source.reader.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.source.reader.JdbcIncrementalSourceReader;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.JobBaseConfig;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PostgresSourceReaderTest {

    @Test
    void cleanupConnectionUsesDebeziumDirectConnectionFactory() throws Exception {
        JdbcConfiguration config =
                JdbcConfiguration.adapt(
                        Configuration.create()
                                .with(JdbcConfiguration.HOSTNAME, "localhost")
                                .with(JdbcConfiguration.PORT, 5432)
                                .with(JdbcConfiguration.DATABASE, "postgres")
                                .with(JdbcConfiguration.USER, "user")
                                .with(JdbcConfiguration.PASSWORD, "password")
                                .build());

        try (PostgresConnection expected =
                        new PostgresConnection(config, PostgresConnection.CONNECTION_GENERAL);
                PostgresConnection actual =
                        PostgresSourceReader.createCleanupConnection(config)) {
            assertSame(connectionFactory(expected), connectionFactory(actual));
        }
    }

    @Test
    void postgresConfigIncludesSchemaChangesByDefault() throws Exception {
        PostgresSourceConfig config = sourceConfig(Map.of());

        assertTrue(config.isIncludeSchemaChanges());
    }

    @Test
    void postgresConfigCanDisableSchemaChanges() throws Exception {
        PostgresSourceConfig config =
                sourceConfig(Map.of(DataSourceConfigKeys.SCHEMA_CHANGE_ENABLED, "false"));

        assertFalse(config.isIncludeSchemaChanges());
    }

    @Test
    void snapshotSplitContainsOnlyCurrentTableSchema() throws Exception {
        TableId tableId = TableId.parse("public.orders", false);
        TableId otherTableId = TableId.parse("public.other_orders", false);
        TableChanges.TableChange tableChange = tableChange(tableId, "pk");
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(tableId, tableChange);
        tableSchemas.put(otherTableId, tableChange(otherTableId, "other_id"));

        PostgresSourceReader reader =
                new PostgresSourceReader() {
                    @Override
                    protected Class<?> probeSplitKeyClass(
                            TableId ignoredTableId,
                            Column ignoredSplitColumn,
                            JobBaseConfig ignoredJobConfig) {
                        return Integer.class;
                    }
                };
        reader.setTableSchemas(tableSchemas);

        Method method =
                JdbcIncrementalSourceReader.class.getDeclaredMethod(
                        "createSnapshotSplit", Map.class, JobBaseConfig.class);
        method.setAccessible(true);
        SnapshotSplit split =
                (SnapshotSplit)
                        method.invoke(
                                reader,
                                snapshotOffset("public.orders", "pk"),
                                new JobBaseConfig("job-1", "POSTGRES", Map.of(), null));

        assertEquals(tableId, split.getTableId());
        assertEquals(Map.of(tableId, tableChange), split.getTableSchemas());
        assertFalse(split.getTableSchemas().containsKey(otherTableId));
        assertEquals(tableSchemas, reader.getTableSchemas());
    }

    private static PostgresSourceConfig sourceConfig(Map<String, String> overrides)
            throws Exception {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(DataSourceConfigKeys.JDBC_URL, "jdbc:postgresql://localhost:5432/testdb");
        cfg.put(DataSourceConfigKeys.USER, "u");
        cfg.put(DataSourceConfigKeys.PASSWORD, "p");
        cfg.put(DataSourceConfigKeys.DATABASE, "testdb");
        cfg.put(DataSourceConfigKeys.SCHEMA, "public");
        cfg.put(DataSourceConfigKeys.TABLE, "t_test");
        cfg.put(DataSourceConfigKeys.OFFSET, DataSourceConfigKeys.OFFSET_INITIAL);
        cfg.put(DataSourceConfigKeys.SLOT_NAME, "slot_1");
        cfg.put(DataSourceConfigKeys.PUBLICATION_NAME, "pub_1");
        cfg.putAll(overrides);
        Method method =
                PostgresSourceReader.class.getDeclaredMethod(
                        "generatePostgresConfig", Map.class, String.class, int.class);
        method.setAccessible(true);
        return (PostgresSourceConfig) method.invoke(new PostgresSourceReader(), cfg, "job-1", 0);
    }

    private static Object connectionFactory(JdbcConnection connection) throws Exception {
        Field field = JdbcConnection.class.getDeclaredField("factory");
        field.setAccessible(true);
        return field.get(connection);
    }

    private static Map<String, Object> snapshotOffset(String tableId, String splitKey) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("splitId", tableId + ":0");
        offset.put("tableId", tableId);
        offset.put("splitKey", List.of(splitKey));
        offset.put("splitStart", new Object[] {1});
        offset.put("splitEnd", new Object[] {10});
        return offset;
    }

    private static TableChanges.TableChange tableChange(TableId tableId, String splitKey) {
        TableEditor editor = Table.editor().tableId(tableId);
        editor.addColumns(
                Column.editor()
                        .name(splitKey)
                        .type("int4")
                        .jdbcType(Types.INTEGER)
                        .optional(false)
                        .create());
        editor.setPrimaryKeyNames(splitKey);
        return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, editor.create());
    }
}
