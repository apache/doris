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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The metadata path against a real fluss cluster started in this JVM.
 *
 * <p>{@link FlussConnectorMetadataTest} pins the mapping against canned {@code TableInfo}s, which
 * proves the logic but not the premise: it cannot tell whether a real cluster actually reports column
 * comments where we read them, orders schema columns the way we assume, or answers the calls we make
 * at all. This starts a coordinator, a tablet server and an embedded ZooKeeper, creates tables through
 * the fluss Java client, and reads them back through the connector — every hop real except Doris's own
 * plugin classloading.
 *
 * <p>It is deliberately named {@code ...Test} rather than {@code ...ITCase}: surefire's default
 * includes do not match {@code *ITCase}, so that name would leave the whole class silently unexecuted
 * with a green build.
 */
public class FlussMetadataClusterTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    /**
     * Each test gets its own database. The cluster extension drops every non-built-in database after
     * each test, so a shared fixture would vanish after the first one; a fresh NAME each time (rather
     * than re-creating the same one) also keeps the fluss client's metadata cache from ever holding a
     * dropped table's id for a path that came back.
     */
    private static int databaseCounter;

    private static Connection connection;
    private static Admin admin;
    private static Connector connector;

    private String db;

    @BeforeAll
    public static void connectToCluster() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER.getClientConfig();
        connection = ConnectionFactory.createConnection(clientConf);
        admin = connection.getAdmin();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(FlussConnectorProperties.BOOTSTRAP_SERVERS,
                FLUSS_CLUSTER.getBootstrapServers());
        connector = new FlussConnectorProvider().create(catalogProperties, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "fluss_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        });
    }

    @BeforeEach
    public void createTables() throws Exception {
        db = "doris_metadata_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();

        admin.createTable(TablePath.of(db, "log_table"),
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .withComment("the row id")
                                .column("name", DataTypes.STRING())
                                .column("price", DataTypes.DECIMAL(20, 4))
                                .column("event_time", DataTypes.TIMESTAMP_LTZ(6))
                                .column("started", DataTypes.TIME(3))
                                .column("tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                                .column("nested", DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT(), "inner doc")))
                                .build())
                        .distributedBy(3)
                        .comment("a log table")
                        .build(),
                true).get();

        admin.createTable(TablePath.of(db, "pk_table"),
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("dt", DataTypes.STRING())
                                .column("id", DataTypes.BIGINT())
                                .column("amount", DataTypes.DOUBLE())
                                .primaryKey("dt", "id")
                                .build())
                        .partitionedBy("dt")
                        .distributedBy(2, "id")
                        .build(),
                true).get();
        admin.createPartition(TablePath.of(db, "pk_table"),
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_02")), true).get();
        admin.createPartition(TablePath.of(db, "pk_table"),
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_03")), true).get();
    }

    @AfterAll
    public static void disconnect() throws Exception {
        if (connector != null) {
            connector.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private static ConnectorMetadata metadata() {
        return connector.getMetadata(new FlussTestSession(1L, "cluster-query"));
    }

    @Test
    public void theCatalogSeesTheClusterAndItsNamespaces() {
        ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertTrue(result.isSuccess(), () -> "connectivity test failed: " + result);

        ConnectorMetadata metadata = metadata();
        Assertions.assertTrue(metadata.listDatabaseNames(null).contains(db));
        Assertions.assertTrue(metadata.databaseExists(null, db));
        Assertions.assertFalse(metadata.databaseExists(null, "no_such_db"));
        List<String> tables = metadata.listTableNames(null, db);
        Assertions.assertTrue(tables.containsAll(Arrays.asList("log_table", "pk_table")),
                "expected both tables, got " + tables);
        Assertions.assertEquals(Optional.empty(), metadata.getTableHandle(null, db, "no_such_table"));
    }

    @Test
    public void schemaComesBackFromTheClusterWithTypesAndComments() {
        // The premise the unit tests cannot check: that a real cluster reports these facts where this
        // connector reads them. Column comments are the sharpest case — fluss keeps them on the schema
        // and drops them from the table's row type, so reading the wrong one loses every comment.
        ConnectorMetadata metadata = metadata();
        ConnectorTableHandle handle = metadata.getTableHandle(null, db, "log_table")
                .orElseThrow(AssertionError::new);

        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
        Map<String, ConnectorColumn> columns = byName(schema.getColumns());

        Assertions.assertEquals(ConnectorType.of("BIGINT"), columns.get("id").getType());
        Assertions.assertEquals("the row id", columns.get("id").getComment());
        Assertions.assertEquals(ConnectorType.of("STRING"), columns.get("name").getType());
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 20, 4), columns.get("price").getType());
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), columns.get("event_time").getType());
        Assertions.assertTrue(columns.get("event_time").isWithTimeZone());
        // TIME survives the round trip as an unsupported column rather than making the table unloadable.
        Assertions.assertEquals(ConnectorType.of("UNSUPPORTED"), columns.get("started").getType());
        Assertions.assertEquals(
                ConnectorType.mapOf(ConnectorType.of("STRING"), ConnectorType.of("INT")),
                columns.get("tags").getType());
        ConnectorType nested = columns.get("nested").getType();
        Assertions.assertEquals("STRUCT", nested.getTypeName());
        Assertions.assertEquals(Collections.singletonList("a"), nested.getFieldNames());
        Assertions.assertEquals("inner doc", nested.getChildComment(0));

        Assertions.assertEquals("a log table", metadata.getTableComment(null, db, "log_table"));
        Assertions.assertEquals("FLUSS", schema.getTableFormatType());

        // Column handles are positional, and the position must be the cluster's column order.
        Map<String, ConnectorColumnHandle> handles = metadata.getColumnHandles(null, handle);
        Assertions.assertEquals(new FlussColumnHandle("id", 0), handles.get("id"));
        Assertions.assertEquals(new FlussColumnHandle("nested", 6), handles.get("nested"));
    }

    @Test
    public void theHandleReflectsHowTheClusterCreatedTheTable() {
        ConnectorMetadata metadata = metadata();
        FlussTableHandle logHandle = (FlussTableHandle) metadata.getTableHandle(null, db, "log_table")
                .orElseThrow(AssertionError::new);
        Assertions.assertFalse(logHandle.hasPrimaryKey());
        Assertions.assertFalse(logHandle.isPartitioned());
        Assertions.assertFalse(logHandle.isDataLakeEnabled());
        Assertions.assertEquals(3, logHandle.getBucketCount());
        // A real cluster assigns the ids; all the fixtures can say is that they were carried through.
        Assertions.assertTrue(logHandle.getTableId() > 0, "expected a cluster-assigned table id");

        FlussTableHandle pkHandle = (FlussTableHandle) metadata.getTableHandle(null, db, "pk_table")
                .orElseThrow(AssertionError::new);
        Assertions.assertTrue(pkHandle.hasPrimaryKey());
        Assertions.assertEquals(Arrays.asList("dt", "id"), pkHandle.getPrimaryKeys());
        Assertions.assertEquals(Collections.singletonList("dt"), pkHandle.getPartitionKeys());
        Assertions.assertEquals(Collections.singletonList("id"), pkHandle.getBucketKeys());
        Assertions.assertEquals(2, pkHandle.getBucketCount());
    }

    @Test
    public void partitionsAreListedAsTheClusterCreatedThem() {
        ConnectorMetadata metadata = metadata();
        ConnectorTableHandle handle = metadata.getTableHandle(null, db, "pk_table")
                .orElseThrow(AssertionError::new);

        List<ConnectorPartitionInfo> partitions = metadata.listPartitions(null, handle, Optional.empty());
        List<String> names = new ArrayList<>();
        for (ConnectorPartitionInfo partition : partitions) {
            names.add(partition.getPartitionName());
            Assertions.assertEquals(1, partition.getOrderedPartitionValues().size());
        }
        Collections.sort(names);
        Assertions.assertEquals(Arrays.asList("dt=2026_08_02", "dt=2026_08_03"), names);

        Assertions.assertEquals("dt",
                metadata.getTableSchema(null, handle).getProperties()
                        .get(ConnectorTableSchema.PARTITION_COLUMNS_KEY));

        // The unpartitioned table must come back empty, not fail: fluss rejects the partition call for
        // such a table, so this also proves the connector never makes it.
        ConnectorTableHandle logHandle = metadata.getTableHandle(null, db, "log_table")
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(Collections.emptyList(),
                metadata.listPartitions(null, logHandle, Optional.empty()));
    }

    @Test
    public void statisticsOfAFreshTableAreUnknownRatherThanZero() {
        // Nothing has been written and statistics are off by default, so fluss reports no rows. Reporting
        // that as a row count of zero would tell the optimizer the table is empty.
        ConnectorMetadata metadata = metadata();
        ConnectorTableHandle handle = metadata.getTableHandle(null, db, "log_table")
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(Optional.empty(), metadata.getTableStatistics(null, handle));
    }

    private static Map<String, ConnectorColumn> byName(List<ConnectorColumn> columns) {
        Map<String, ConnectorColumn> byName = new LinkedHashMap<>();
        for (ConnectorColumn column : columns) {
            byName.put(column.getName(), column);
        }
        return byName;
    }
}
