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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypeRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * T13 DDL tests for {@link PaimonConnectorMetadata#createTable} / {@link #dropTable},
 * pinning (1) the delegation to the {@link PaimonCatalogOps} seam with the correct Identifier,
 * Schema and {@code ignoreIfExists}/{@code ignoreIfNotExists} flags, (2) that raw paimon checked
 * exceptions are wrapped as {@link DorisConnectorException}, and (3) D7=B: every remote DDL call
 * is executed INSIDE {@link org.apache.doris.connector.spi.ConnectorContext#executeAuthenticated}.
 *
 * <p>All tests run offline against the recording seam fake (null real Catalog).
 */
public class PaimonConnectorMetadataDdlTest {

    private static PaimonConnectorMetadata metadata(RecordingPaimonCatalogOps ops,
            RecordingConnectorContext ctx) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), ctx);
    }

    /** Builds a CREATE TABLE request: db1.t1, columns (id INT, name STRING), partitioned by id, ifNotExists. */
    private static ConnectorCreateTableRequest request(boolean ifNotExists) {
        List<ConnectorColumn> columns = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "id col", false, null),
                new ConnectorColumn("name", ConnectorType.of("STRING"), null, true, null));
        ConnectorPartitionSpec partitionSpec = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.IDENTITY,
                Collections.singletonList(
                        new ConnectorPartitionField("id", "identity", Collections.emptyList())),
                Collections.emptyList());
        Map<String, String> props = new HashMap<>();
        props.put("primary-key", "id");
        return ConnectorCreateTableRequest.builder()
                .dbName("db1")
                .tableName("t1")
                .columns(columns)
                .partitionSpec(partitionSpec)
                .properties(props)
                .ifNotExists(ifNotExists)
                .build();
    }

    /** Builds a CREATE TABLE request whose partition spec uses a NON-identity transform (bucket). */
    private static ConnectorCreateTableRequest requestWithNonIdentityPartition() {
        List<ConnectorColumn> columns = Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("INT"), "id col", false, null),
                new ConnectorColumn("name", ConnectorType.of("STRING"), null, true, null));
        ConnectorPartitionSpec partitionSpec = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.TRANSFORM,
                Collections.singletonList(
                        new ConnectorPartitionField("id", "bucket", Collections.singletonList(16))),
                Collections.emptyList());
        return ConnectorCreateTableRequest.builder()
                .dbName("db1")
                .tableName("t1")
                .columns(columns)
                .partitionSpec(partitionSpec)
                .properties(new HashMap<>())
                .ifNotExists(false)
                .build();
    }

    private static PaimonTableHandle handle() {
        return new PaimonTableHandle("db1", "t1",
                Collections.singletonList("id"), Collections.singletonList("id"));
    }

    // ==================== createTable ====================

    @Test
    public void createTableDelegatesToSeamWithBuiltSchemaAndIfNotExists() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).createTable(null, request(true));

        // WHY: createTable is the only path that materializes a Doris CREATE TABLE on the remote
        // Paimon catalog; it must call the seam exactly once with the request's Identifier and the
        // PaimonSchemaBuilder-built Schema, and forward request.isIfNotExists() as paimon's
        // ignoreIfExists so paimon's idempotency semantics (no-op vs throw) match the user's clause.
        // MUTATION: dropping the createTable delegation, or passing a wrong Identifier / false
        // instead of request.isIfNotExists(), flips one of these assertions red.
        Assertions.assertEquals(Collections.singletonList("createTable:db1.t1"), ops.log);
        Assertions.assertEquals("db1", ops.lastCreatedTableId.getDatabaseName());
        Assertions.assertEquals("t1", ops.lastCreatedTableId.getObjectName());
        Assertions.assertTrue(ops.lastCreateTableIgnoreIfExists,
                "request.isIfNotExists()==true must be forwarded as paimon ignoreIfExists");

        // Schema must reflect the request: 2 columns in order, identity partition key id, pk id.
        Schema schema = ops.lastCreatedSchema;
        Assertions.assertEquals(Arrays.asList("id", "name"),
                Arrays.asList(schema.fields().get(0).name(), schema.fields().get(1).name()));
        Assertions.assertEquals(DataTypeRoot.INTEGER, schema.fields().get(0).type().getTypeRoot());
        Assertions.assertEquals(DataTypeRoot.VARCHAR, schema.fields().get(1).type().getTypeRoot());
        Assertions.assertEquals(Collections.singletonList("id"), schema.partitionKeys());
        Assertions.assertEquals(Collections.singletonList("id"), schema.primaryKeys());
    }

    @Test
    public void createTableForwardsIfNotExistsFalse() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).createTable(null, request(false));

        // WHY: when the user omits IF NOT EXISTS, paimon must be told ignoreIfExists=false so a
        // pre-existing table surfaces as TableAlreadyExistException rather than being silently
        // no-op'd. MUTATION: hardcoding true (always-idempotent) makes this red.
        Assertions.assertFalse(ops.lastCreateTableIgnoreIfExists);
    }

    @Test
    public void createTableWrapsTableAlreadyExistAsDorisConnectorException() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableAlreadyExist = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: fe-core only understands DorisConnectorException; a raw paimon
        // TableAlreadyExistException leaking out would bypass the engine's DDL error handling.
        // MUTATION: removing the try/catch wrap lets the raw paimon exception escape (wrapped by
        // executeAuthenticated as a generic Exception) -> assertThrows(DorisConnectorException) red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).createTable(null, request(false)));
        Assertions.assertTrue(ex.getMessage().contains("db1.t1"),
                "wrapped message must name the table");
    }

    @Test
    public void createTableRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        // WHY (D7=B legacy parity): the remote create must run inside executeAuthenticated so the
        // FE-injected auth context (e.g. Kerberos UGI) applies; legacy PaimonMetadataOps wrapped
        // every remote DDL call. When auth fails, the seam call must NOT have run.
        // MUTATION: if createTable called catalogOps.createTable directly instead of inside
        // context.executeAuthenticated, the seam call would run despite the auth failure and the
        // log would contain "createTable:db1.t1" -> the log-empty assertion below would fail.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).createTable(null, request(true)));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the seam createTable call runs");
        Assertions.assertEquals(1, ctx.authCount,
                "createTable must enter executeAuthenticated exactly once");
    }

    @Test
    public void createTableEntersAuthenticatorExactlyOnceOnHappyPath() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).createTable(null, request(true));

        // WHY: confirms the happy path also goes through the authenticator (not just the failure
        // path), and exactly once. MUTATION: an un-wrapped direct seam call leaves authCount==0.
        Assertions.assertEquals(1, ctx.authCount);
        Assertions.assertEquals(Collections.singletonList("createTable:db1.t1"), ops.log);
    }

    @Test
    public void createTableBuildsSchemaOutsideAuthenticatorSoSchemaFailureIsRaw() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: createTable must build the schema OUTSIDE the authenticator try, so a schema
        //   validation failure surfaces its own precise message and never touches the remote
        //   catalog / auth context. PaimonSchemaBuilder rejects a non-identity partition transform
        //   (here: bucket) with a raw DorisConnectorException whose message names the transform.
        // MUTATION: if PaimonSchemaBuilder.build were moved INSIDE context.executeAuthenticated's
        //   try, the DorisConnectorException would be re-wrapped as "Failed to create Paimon table"
        //   (the contains-false assertion fails) and authCount would be 1 (the ==0 assertion fails).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).createTable(null, requestWithNonIdentityPartition()));
        Assertions.assertFalse(ex.getMessage().contains("Failed to create Paimon table"),
                "schema-builder failure must surface its RAW message, not the createTable wrapper");
        Assertions.assertEquals(0, ctx.authCount,
                "schema build failure must abort BEFORE entering the authenticator");
        Assertions.assertTrue(ops.log.isEmpty(),
                "schema build failure must never reach the remote catalog seam");
    }

    // ==================== dropTable ====================

    @Test
    public void dropTableDelegatesToSeamIdempotently() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        metadata(ops, ctx).dropTable(null, handle());

        // WHY: the SPI dropTable is handle-based with no ifExists flag (fe-core pre-resolves the
        // handle), so the remote drop must be issued idempotently (ignoreIfNotExists=true) to mirror
        // MaxCompute and avoid a spurious failure on a concurrently-vanished table.
        // MUTATION: passing false (or a wrong Identifier) flips one of these assertions red.
        Assertions.assertEquals(Collections.singletonList("dropTable:db1.t1"), ops.log);
        Assertions.assertEquals("db1", ops.lastDroppedTableId.getDatabaseName());
        Assertions.assertEquals("t1", ops.lastDroppedTableId.getObjectName());
        Assertions.assertTrue(ops.lastDropTableIgnoreIfNotExists,
                "drop must be idempotent: ignoreIfNotExists=true");
        Assertions.assertEquals(1, ctx.authCount);
    }

    @Test
    public void dropTableWrapsTableNotExistAsDorisConnectorException() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExistOnDrop = true;
        RecordingConnectorContext ctx = new RecordingConnectorContext();

        // WHY: a raw paimon TableNotExistException must be wrapped so fe-core's DDL error handling
        // applies. MUTATION: removing the try/catch wrap lets the raw exception escape -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).dropTable(null, handle()));
        Assertions.assertTrue(ex.getMessage().contains("db1.t1"),
                "wrapped message must name the table");
    }

    @Test
    public void dropTableRunsSeamInsideAuthenticator() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        // WHY (D7=B legacy parity): like createTable, the remote drop must run inside
        // executeAuthenticated. MUTATION: if dropTable called catalogOps.dropTable directly instead
        // of inside context.executeAuthenticated, the log would contain "dropTable:db1.t1" despite
        // the auth failure -> the log-empty assertion below would fail.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata(ops, ctx).dropTable(null, handle()));
        Assertions.assertTrue(ops.log.isEmpty(),
                "auth failure must abort BEFORE the seam dropTable call runs");
        Assertions.assertEquals(1, ctx.authCount);
    }
}
