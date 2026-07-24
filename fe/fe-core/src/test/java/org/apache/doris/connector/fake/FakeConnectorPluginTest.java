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

package org.apache.doris.connector.fake;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

/**
 * Exercises the SPI default fall-throughs through {@link FakeConnectorPlugin}.
 *
 * <p>The fake overrides nothing beyond the minimum required to compile — every
 * assertion below targets a default method body added during P0 batches 0+1.
 * If a future change accidentally drops or alters a default, this test fails
 * before the change reaches any real connector.
 */
public class FakeConnectorPluginTest {

    private FakeConnectorPlugin plugin;
    private Connector connector;
    private ConnectorSession session;
    private ConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        plugin = new FakeConnectorPlugin();
        ConnectorContext context = new FakeConnectorPlugin.FakeContext("fake_cat", 1L);
        connector = plugin.create(Collections.emptyMap(), context);
        session = new FakeConnectorPlugin.FakeSession("fake_cat", 1L);
        metadata = connector.getMetadata(session);
    }

    // ──────────────────── ConnectorContext defaults ────────────────────

    @Test
    void contextMetaInvalidatorDefaultsToNoop() {
        ConnectorContext context = new FakeConnectorPlugin.FakeContext("fake_cat", 1L);
        // T04: default getMetaInvalidator() returns NOOP — exercising it must not throw.
        Assertions.assertSame(ConnectorMetaInvalidator.NOOP,
                context.getMetaInvalidator(),
                "default ConnectorContext.getMetaInvalidator() should return NOOP");
        context.getMetaInvalidator().invalidateAll();
        context.getMetaInvalidator().invalidateDatabase("db");
        context.getMetaInvalidator().invalidateTable("db", "t");
        context.getMetaInvalidator().invalidatePartition(
                "db", "t", Collections.singletonList("2024"));
        context.getMetaInvalidator().invalidateStatistics("db", "t");
    }

    // ──────────────────── ConnectorSession defaults ────────────────────

    @Test
    void sessionCurrentTransactionDefaultsToEmpty() {
        // T07: default getCurrentTransaction() returns Optional.empty().
        Assertions.assertEquals(Optional.empty(), session.getCurrentTransaction());
    }

    @Test
    void sessionSessionPropertiesDefaultsToEmpty() {
        Assertions.assertTrue(session.getSessionProperties().isEmpty());
    }

    // ──────────────────── ConnectorMetadata defaults (E5 MVCC) ────────────────────

    @Test
    void mvccSnapshotMethodsDefaultToEmpty() {
        ConnectorTableHandle handle = new ConnectorTableHandle() { };
        // T08: the mvcc defaults return Optional.empty() — connector opts out of MVCC. The old
        // getSnapshotAt/getSnapshotById defaults were retired in B5b-2a and replaced by the unified
        // resolveTimeTravel seam, which also defaults to Optional.empty for non-time-travel connectors.
        Assertions.assertEquals(Optional.empty(),
                metadata.beginQuerySnapshot(session, handle));
        Assertions.assertEquals(Optional.empty(),
                metadata.resolveTimeTravel(session, handle,
                        ConnectorTimeTravelSpec.snapshotId("1")));
    }

    // ──────────────────── ConnectorSchemaOps defaults ────────────────────

    @Test
    void schemaOpsDefaults() {
        Assertions.assertTrue(metadata.listDatabaseNames(session).isEmpty());
        Assertions.assertFalse(metadata.databaseExists(session, "anydb"));
    }

    // ──────────────────── ConnectorTableOps defaults ────────────────────

    @Test
    void tableOpsListDefaults() {
        // SHOW TABLES against an unimplemented connector returns empty rather than throwing.
        Assertions.assertTrue(metadata.listTableNames(session, "any_db").isEmpty());

        Assertions.assertEquals(Optional.empty(),
                metadata.getTableHandle(session, "db", "t"));
        Assertions.assertTrue(metadata.getPrimaryKeys(session, "db", "t").isEmpty());
        Assertions.assertEquals("", metadata.getTableComment(session, "db", "t"));
    }

    @Test
    void partitionListingDefaultsToEmpty() {
        ConnectorTableHandle handle = new ConnectorTableHandle() { };
        // T17-T19: all three listing defaults return empty.
        Assertions.assertTrue(
                metadata.listPartitionNames(session, handle).isEmpty());
        Assertions.assertTrue(
                metadata.listPartitions(session, handle, Optional.empty()).isEmpty());
        Assertions.assertTrue(
                metadata.listPartitionValues(session, handle,
                        Collections.singletonList("dt")).isEmpty());
    }

    @Test
    void createTableRequestDefaultDegradesToLegacy() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db")
                .tableName("t")
                .columns(Collections.emptyList())
                .properties(Collections.emptyMap())
                .build();
        // T14: default createTable(request) falls through to legacy createTable(schema,
        // props), whose own default throws "CREATE TABLE not supported". This proves
        // the fall-through chain is wired correctly, even if the connector ultimately
        // rejects the request.
        DorisConnectorException ex = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> metadata.createTable(session, request));
        Assertions.assertTrue(ex.getMessage().contains("CREATE TABLE not supported"),
                "should propagate legacy createTable's error, got: " + ex.getMessage());
    }

    // ──────────────────── ConnectorWriteOps defaults ────────────────────

    @Test
    void beginTransactionDefaultThrows() {
        // T06: default beginTransaction throws — engine treats statement as auto-commit.
        DorisConnectorException ex = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> metadata.beginTransaction(session));
        Assertions.assertTrue(ex.getMessage().contains("Transactions not supported"),
                "expected transaction-not-supported message, got: " + ex.getMessage());
    }

    // ──────────────────── Connector-level defaults ────────────────────

    @Test
    void connectorTopLevelDefaults() {
        Assertions.assertNull(connector.getScanPlanProvider());
        Assertions.assertTrue(connector.getCapabilities().isEmpty());
        Assertions.assertTrue(connector.getTableProperties().isEmpty());
        Assertions.assertTrue(connector.getSessionProperties().isEmpty());
        Assertions.assertFalse(connector.defaultTestConnection());
        Assertions.assertTrue(connector.testConnection(session).isSuccess());
    }
}
