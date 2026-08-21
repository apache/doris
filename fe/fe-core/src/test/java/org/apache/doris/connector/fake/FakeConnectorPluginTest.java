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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

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
        Assertions.assertEquals("", metadata.getTableComment(session, "db", "t"));
    }

    @Test
    void partitionListingDefaultsToEmpty() {
        ConnectorTableHandle handle = new ConnectorTableHandle() { };
        // T17-T18: both listing defaults return empty.
        Assertions.assertTrue(
                metadata.listPartitionNames(session, handle).isEmpty());
        Assertions.assertTrue(
                metadata.listPartitions(session, handle, Optional.empty()).isEmpty());
    }

    @Test
    void createTableDefaultRejectsInsteadOfDegrading() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db")
                .tableName("t")
                .columns(Collections.emptyList())
                .properties(Collections.emptyMap())
                .build();
        // WHY: a connector that does not implement CREATE TABLE must FAIL, and it must fail on the request
        // overload itself. This default used to build a ConnectorTableSchema and delegate to a narrower
        // (schema, properties) overload, which meant the partition spec, the bucket spec and IF NOT EXISTS were
        // dropped on the way -- a connector implementing only the narrow form reported success on a partitioned
        // CREATE TABLE and produced an unpartitioned table. That degradation path is gone; there is one entry
        // point, and not implementing it is an error rather than a silently narrower table.
        // MUTATION: reinstating the degrading default (build a schema, do nothing) -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> metadata.createTable(session, request));
        Assertions.assertTrue(ex.getMessage().contains("CREATE TABLE not supported"),
                "should reject with the connector-facing message, got: " + ex.getMessage());
    }

    @Test
    void dropDatabaseDefaultRejectsInsteadOfSilentlyDroppingForce() {
        // WHY: the throw now lives on the 4-arg overload. It used to live on a 3-arg form and this overload
        // defaulted to it, discarding `force` -- so DROP DATABASE ... FORCE silently became a non-cascading
        // drop that then failed on a non-empty database, with an error about the database not being empty
        // rather than about FORCE being unsupported. Nothing implemented the 3-arg form.
        // MUTATION: removing the throw from the 4-arg default -> no throw -> red.
        DorisConnectorException ex = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> metadata.dropDatabase(session, "db", false, true));
        Assertions.assertTrue(ex.getMessage().contains("DROP DATABASE not supported"),
                "should reject with the connector-facing message, got: " + ex.getMessage());
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
        Assertions.assertFalse(connector.defaultTestConnection());
        Assertions.assertTrue(connector.testConnection(session).isSuccess());
    }
}
