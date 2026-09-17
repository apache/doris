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


package org.apache.doris.job.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPassthroughSqlOps;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorQueryResult;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link StreamingSourceClient} against a recording fake of the connector plugin that serves streaming
 * sources: it must reach that plugin through the engine's connector factory with the job's own source
 * properties, hand back the connector's schema as Doris columns, and fail loud when the plugin is absent
 * or cannot run probe queries.
 */
public class StreamingSourceClientTest {

    /** A table handle the fake connector hands out; the client must treat it as opaque. */
    private static final class Handle implements ConnectorTableHandle {
        final String db;
        final String table;

        Handle(String db, String table) {
            this.db = db;
            this.table = table;
        }
    }

    /** Records what the engine asked of it and answers a fixed one-table database. */
    private static final class RecordingMetadata implements ConnectorMetadata, ConnectorPassthroughSqlOps {
        final List<String> calls = new ArrayList<>();
        final List<Object> lastParams = new ArrayList<>();

        @Override
        public List<String> listTableNames(ConnectorSession session, String dbName) {
            calls.add("listTableNames:" + dbName);
            return Collections.singletonList("t");
        }

        @Override
        public Optional<ConnectorTableHandle> getTableHandle(ConnectorSession session, String dbName,
                String tableName) {
            calls.add("getTableHandle:" + dbName + "." + tableName);
            return "t".equals(tableName) ? Optional.of(new Handle(dbName, tableName)) : Optional.empty();
        }

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getTableSchema:" + ((Handle) handle).db + "." + ((Handle) handle).table);
            return new ConnectorTableSchema("t", Arrays.asList(
                    new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null, true),
                    new ConnectorColumn("name", ConnectorType.of("VARCHAR", 20, -1), "", true, null, true)),
                    "FAKE", Collections.emptyMap());
        }

        @Override
        public List<String> getPrimaryKeys(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getPrimaryKeys:" + ((Handle) handle).db + "." + ((Handle) handle).table);
            return Collections.singletonList("id");
        }

        @Override
        public ConnectorQueryResult executeQuery(ConnectorSession session, String sql, List<Object> params) {
            calls.add("executeQuery:" + sql);
            lastParams.clear();
            lastParams.addAll(params);
            return new ConnectorQueryResult(Collections.singletonList("v"),
                    Collections.singletonList(Collections.singletonList(Boolean.TRUE)));
        }
    }

    private static class RecordingConnector implements Connector {
        final ConnectorMetadata metadata;
        int closes;

        RecordingConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public void close() {
            closes++;
        }
    }

    private RecordingMetadata metadata;
    private RecordingConnector connector;
    private Map<String, String> receivedProps;
    private String receivedCatalogName;

    @BeforeEach
    void setUp() {
        metadata = new RecordingMetadata();
        connector = new RecordingConnector(metadata);
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new ConnectorProvider() {
            @Override
            public String getType() {
                return DataSourceType.MYSQL.connectorType();
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                receivedProps = properties;
                receivedCatalogName = context.getCatalogName();
                return connector;
            }
        });
        ConnectorFactory.initPluginManager(manager);
    }

    @AfterEach
    void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    private static Map<String, String> sourceProps() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://h:3306");
        props.put("user", "u");
        props.put("password", "p");
        props.put("database", "db");
        props.put("include_tables", "t");
        return props;
    }

    @Test
    public void opensTheSourcePluginWithTheJobsPropertiesAndClosesIt() throws Exception {
        try (StreamingSourceClient client = StreamingSourceClient.open(DataSourceType.MYSQL, sourceProps())) {
            Assertions.assertEquals(sourceProps(), receivedProps,
                    "the job's source properties are handed to the plugin as they are");
            Assertions.assertEquals("MYSQL", receivedCatalogName,
                    "the source type names the temporary connector, as the old client did");
            Assertions.assertEquals(Collections.singletonList("t"), client.listTables("db"));
            Assertions.assertTrue(client.tableExists("db", "t"));
            Assertions.assertFalse(client.tableExists("db", "nope"));
        }
        Assertions.assertEquals(1, connector.closes, "close() releases the temporary connector");
    }

    @Test
    public void columnsAndPrimaryKeysComeFromTheConnectorSchema() throws Exception {
        try (StreamingSourceClient client = StreamingSourceClient.open(DataSourceType.MYSQL, sourceProps())) {
            List<Column> columns = client.getColumns("db", "t");
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("id", columns.get(0).getName());
            Assertions.assertTrue(columns.get(0).getType().isBigIntType());
            Assertions.assertFalse(columns.get(0).isAllowNull());
            Assertions.assertEquals("name", columns.get(1).getName());
            Assertions.assertEquals(20, columns.get(1).getType().getLength());
            Assertions.assertEquals(Collections.singletonList("id"), client.getPrimaryKeys("db", "t"));

            // The handle is looked up once per table and reused by the schema and key reads.
            Assertions.assertEquals(1, Collections.frequency(metadata.calls, "getTableHandle:db.t"));

            JobException e = Assertions.assertThrows(JobException.class, () -> client.getColumns("db", "nope"));
            Assertions.assertTrue(e.getMessage().contains("db.nope does not exist"), e.getMessage());
        }
    }

    @Test
    public void probeQueriesBindTheirParameters() throws Exception {
        try (StreamingSourceClient client = StreamingSourceClient.open(DataSourceType.MYSQL, sourceProps())) {
            ConnectorQueryResult result = client.executeQuery("SELECT active FROM s WHERE n = ?",
                    Collections.singletonList("slot"));
            Assertions.assertEquals(Boolean.TRUE, result.getRows().get(0).get(0));
            Assertions.assertEquals(Collections.singletonList("slot"), metadata.lastParams);
        }
    }

    @Test
    public void missingPluginFailsLoud() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
        JobException e = Assertions.assertThrows(JobException.class,
                () -> StreamingSourceClient.open(DataSourceType.POSTGRES, sourceProps()));
        Assertions.assertTrue(e.getMessage().contains("requires the 'jdbc' connector plugin"), e.getMessage());
    }

    @Test
    public void sourceWithoutPassthroughSqlCannotBeProbed() throws Exception {
        connector = new RecordingConnector(new ConnectorMetadata() {
        });
        try (StreamingSourceClient client = StreamingSourceClient.open(DataSourceType.MYSQL, sourceProps())) {
            JobException e = Assertions.assertThrows(JobException.class,
                    () -> client.executeQuery("SELECT 1", Collections.emptyList()));
            Assertions.assertTrue(e.getMessage().contains("does not support probe queries"), e.getMessage());
        }
    }
}
