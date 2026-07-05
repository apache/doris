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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorDatabaseMetadata;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.WriteOperation;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Characterization tests for {@link IcebergConnectorMetadata}, pinning the read-path behavior after
 * the {@link IcebergCatalogOps} seam extraction (P6.1). Mirrors the paimon connector's
 * {@code PaimonConnectorMetadataTest}.
 *
 * <p>The seam fully covers every remote {@code Catalog} call the metadata makes, so each test drives
 * a {@link RecordingIcebergCatalogOps} fake and builds the metadata with a {@code null} real catalog
 * — the tests are entirely offline (no live REST/HMS/Glue/... catalog), which is the whole point of
 * introducing the seam.
 *
 * <p>Behavior is FROZEN this phase: these tests pin the CURRENT production behavior (including the
 * known format-version oddity documented below), NOT the future-fixed parity behavior — the parity
 * fixes land in later tasks (P6-T08/T09).
 */
public class IcebergConnectorMetadataTest {

    private static IcebergConnectorMetadata metadataWith(RecordingIcebergCatalogOps ops) {
        return metadataWith(ops, Collections.emptyMap());
    }

    private static IcebergConnectorMetadata metadataWith(
            RecordingIcebergCatalogOps ops, Map<String, String> props) {
        return new IcebergConnectorMetadata(ops, props, new RecordingConnectorContext());
    }

    private static IcebergConnectorMetadata metadataWith(
            RecordingIcebergCatalogOps ops, RecordingConnectorContext ctx) {
        return new IcebergConnectorMetadata(ops, Collections.emptyMap(), ctx);
    }

    /** A simple 2-column unpartitioned schema (id required, name optional). */
    private static Schema idNameSchema() {
        return new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
    }

    /**
     * A view version whose summary records {@code engine-name} (when non-null) and whose current version
     * carries a SQL representation for {@code reprDialect} (when non-null) — driving the sql/dialect extraction
     * in {@code IcebergConnectorMetadata.getViewDefinition}. Mirrors the helper that used to live in the seam
     * test (the extraction moved up post-H8).
     */
    private static ViewVersion viewVersionWith(String engineName, String reprDialect, String sql) {
        ImmutableViewVersion.Builder builder = ImmutableViewVersion.builder()
                .versionId(1)
                .timestampMillis(0L)
                .schemaId(0)
                .defaultNamespace(Namespace.of("db1"));
        if (engineName != null) {
            builder.putSummary("engine-name", engineName);
        }
        if (reprDialect != null) {
            builder.addRepresentations(ImmutableSQLViewRepresentation.builder()
                    .sql(sql).dialect(reprDialect).build());
        }
        return builder.build();
    }

    // ---------------------------------------------------------------------
    // write capabilities (row-level DML dispatch)
    // ---------------------------------------------------------------------

    @Test
    public void applyRewriteFileScopeThreadsRawPathsOntoHandle() {
        // The distributed rewrite scan-scope pin reaches the connector through the handle: the engine calls
        // applyRewriteFileScope, the iceberg override threads the RAW paths onto an immutable handle copy that
        // the scan provider filters its re-enumerated tasks against. MUTATION: dropping the override (return
        // handle) -> the group scans the whole table -> each group rewrites far beyond its bin-pack set.
        IcebergConnectorMetadata metadata = metadataWith(new RecordingIcebergCatalogOps());
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t1");
        Assertions.assertNull(handle.getRewriteFileScope(), "a fresh handle has no rewrite scope");

        Set<String> paths = new HashSet<>(Arrays.asList(
                "s3://b/db1/t1/a.parquet", "s3://b/db1/t1/b.parquet"));
        ConnectorTableHandle scoped = metadata.applyRewriteFileScope(null, handle, paths);
        Assertions.assertEquals(paths, ((IcebergTableHandle) scoped).getRewriteFileScope(),
                "override must thread the raw paths onto the handle's rewrite scope");

        // null / empty -> handle unchanged (full scan), never an empty scope (which would scan nothing).
        Assertions.assertSame(handle, metadata.applyRewriteFileScope(null, handle, null));
        Assertions.assertSame(handle, metadata.applyRewriteFileScope(null, handle, Collections.emptySet()));
    }

    @Test
    public void getTableCommentReadsCommentProperty() {
        // F9/F12: the SPI default (ConnectorTableOps.getTableComment) returns "", so a flipped iceberg table's
        // COMMENT clause / information_schema.tables.TABLE_COMMENT / SHOW TABLE STATUS Comment column would be
        // blank. This override reads the native iceberg table's "comment" property, mirroring legacy
        // IcebergExternalTable.getComment. MUTATION: dropping the override (SPI default "") -> comment always
        // blank -> red.
        Map<String, String> props = new HashMap<>();
        props.put("comment", "sales fact");
        Assertions.assertEquals("sales fact",
                metadataWithTableProps(props).getTableComment(null, "db1", "t1"));
        // Absent comment -> "" via getOrDefault (byte-identical to legacy properties().getOrDefault).
        Assertions.assertEquals("",
                metadataWithTableProps(new HashMap<>()).getTableComment(null, "db1", "t1"));
    }

    /** A metadata over a single table {@code db1.t1} carrying the given iceberg table properties. */
    private static IcebergConnectorMetadata metadataWithTableProps(Map<String, String> tableProps) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(), "s3://bucket/db1/t1", tableProps);
        return metadataWith(ops);
    }

    private static void assertCopyOnWriteRejected(
            IcebergConnectorMetadata md, WriteOperation op, String operationLabel, String property) {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), op));
        Assertions.assertTrue(e.getMessage().contains(operationLabel), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("copy-on-write"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains(property), e.getMessage());
    }

    @Test
    public void validateRowLevelDmlModeDefaultRejectsCopyOnWrite() {
        // WHY: iceberg's DELETE/UPDATE/MERGE mode defaults to copy-on-write (TableProperties.*_MODE_DEFAULT),
        // which Doris cannot execute (it does merge-on-read position deletes / DVs only). With NO mode
        // property set, every row-level op must be rejected — byte-identical to the legacy fe-resident
        // IcebergDmlCommandUtilsTest.testDefaultModesRejectCopyOnWriteOperations. MUTATION: swapping the
        // getOrDefault fallback to merge-on-read -> default tables wrongly admitted -> red.
        IcebergConnectorMetadata md = metadataWithTableProps(new HashMap<>());
        assertCopyOnWriteRejected(md, WriteOperation.DELETE, "DELETE", TableProperties.DELETE_MODE);
        assertCopyOnWriteRejected(md, WriteOperation.UPDATE, "UPDATE", TableProperties.UPDATE_MODE);
        assertCopyOnWriteRejected(md, WriteOperation.MERGE, "MERGE INTO", TableProperties.MERGE_MODE);
    }

    @Test
    public void validateRowLevelDmlModeExplicitCopyOnWriteRejects() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        props.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        props.put(TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        IcebergConnectorMetadata md = metadataWithTableProps(props);
        assertCopyOnWriteRejected(md, WriteOperation.DELETE, "DELETE", TableProperties.DELETE_MODE);
        assertCopyOnWriteRejected(md, WriteOperation.UPDATE, "UPDATE", TableProperties.UPDATE_MODE);
        assertCopyOnWriteRejected(md, WriteOperation.MERGE, "MERGE INTO", TableProperties.MERGE_MODE);
    }

    @Test
    public void validateRowLevelDmlModeMergeOnReadAllows() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        props.put(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        IcebergConnectorMetadata md = metadataWithTableProps(props);
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.DELETE));
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.UPDATE));
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.MERGE));
    }

    @Test
    public void validateRowLevelDmlModeSelectsPropertyPerOperation() {
        // Load-bearing: each op reads ITS OWN mode property. Only DELETE is set to merge-on-read; UPDATE and
        // MERGE fall back to the copy-on-write default and must still be rejected. MUTATION: routing every op
        // to DELETE_MODE -> UPDATE/MERGE wrongly admitted -> red.
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
        IcebergConnectorMetadata md = metadataWithTableProps(props);
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.DELETE));
        assertCopyOnWriteRejected(md, WriteOperation.UPDATE, "UPDATE", TableProperties.UPDATE_MODE);
        assertCopyOnWriteRejected(md, WriteOperation.MERGE, "MERGE INTO", TableProperties.MERGE_MODE);
    }

    @Test
    public void validateRowLevelDmlModeIsNoOpForNonRowLevelOps() {
        // INSERT / OVERWRITE / REWRITE are not row-level DML: validate is a no-op and never even loads the
        // table to read a mode property — so a copy-on-write table is NOT rejected for a plain append.
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
        IcebergConnectorMetadata md = metadataWithTableProps(props);
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.INSERT));
        Assertions.assertDoesNotThrow(() ->
                md.validateRowLevelDmlMode(null, new IcebergTableHandle("db1", "t1"), WriteOperation.OVERWRITE));
    }

    /** A metadata over a single table {@code db1.t1} with the given schema + partition spec. */
    private static IcebergConnectorMetadata metadataWithSpec(Schema schema, PartitionSpec spec) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable("t1", schema, spec, "s3://bucket/db1/t1", Collections.emptyMap());
        return metadataWith(ops);
    }

    @Test
    public void validateStaticPartitionColumnsAcceptsIdentityColumn() {
        // WHY: static-partition overwrite (INSERT OVERWRITE ... PARTITION(col=val)) is legal only on an IDENTITY
        // partition field. With the iceberg router flipped this validation moved out of the (now dead) fe-resident
        // BindSink.validateStaticPartition into the connector; an identity field must be accepted so valid static
        // overwrites plan. MUTATION: throwing unconditionally -> every static overwrite rejected -> red.
        Schema schema = idNameSchema();
        IcebergConnectorMetadata md = metadataWithSpec(schema,
                PartitionSpec.builderFor(schema).identity("name").bucket("id", 8).build());
        Assertions.assertDoesNotThrow(() -> md.validateStaticPartitionColumns(
                null, new IcebergTableHandle("db1", "t1"), Collections.singletonList("name")));
    }

    @Test
    public void validateStaticPartitionColumnsRejectsUnknownColumn() {
        // WHY: a PARTITION(col=..) naming a column that is not a partition FIELD must fail loud at analysis time
        // (byte-identical to legacy validateStaticPartition), else the unknown column is silently swallowed by the
        // sink's materialize block and surfaces as an unrelated planning error ("Cannot find snapshot"). The lookup
        // is keyed by partition FIELD name, so "id" (the bucket's SOURCE column, not a field) is also "unknown" —
        // mirroring regression test_iceberg_static_partition_overwrite TC29 PARTITION(category) over bucket(category).
        // MUTATION: dropping the containsKey/null check -> unknown column admitted -> red.
        Schema schema = idNameSchema();
        IcebergConnectorMetadata md = metadataWithSpec(schema,
                PartitionSpec.builderFor(schema).identity("name").bucket("id", 8).build());
        for (String bad : new String[] {"invalid_col", "id"}) {
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> md.validateStaticPartitionColumns(
                            null, new IcebergTableHandle("db1", "t1"), Collections.singletonList(bad)));
            Assertions.assertTrue(e.getMessage().contains("Unknown partition column"), e.getMessage());
        }
    }

    @Test
    public void validateStaticPartitionColumnsRejectsNonIdentityField() {
        // WHY: a bucket/truncate/temporal partition FIELD exists in the spec, but static overwrite of a
        // non-identity transform is unsupported and must be rejected with the connector-authored message. The
        // bucket field is named "id_bucket" (iceberg default). MUTATION: dropping the isIdentity check -> a
        // non-identity static overwrite silently admitted -> red.
        Schema schema = idNameSchema();
        IcebergConnectorMetadata md = metadataWithSpec(schema,
                PartitionSpec.builderFor(schema).identity("name").bucket("id", 8).build());
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.validateStaticPartitionColumns(
                        null, new IcebergTableHandle("db1", "t1"), Collections.singletonList("id_bucket")));
        Assertions.assertTrue(
                e.getMessage().contains("Cannot use static partition syntax for non-identity partition field"),
                e.getMessage());
    }

    @Test
    public void validateStaticPartitionColumnsRejectsUnpartitionedTable() {
        // WHY: static partition syntax is meaningless on an unpartitioned table; legacy rejected it up front with
        // a dedicated message. MUTATION: dropping the isPartitioned guard -> an empty partitionFieldMap makes every
        // column read as "Unknown partition column" (wrong message) -> red.
        IcebergConnectorMetadata md = metadataWithSpec(idNameSchema(), PartitionSpec.unpartitioned());
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> md.validateStaticPartitionColumns(
                        null, new IcebergTableHandle("db1", "t1"), Collections.singletonList("name")));
        Assertions.assertTrue(e.getMessage().contains("is not partitioned"), e.getMessage());
    }

    @Test
    public void validateStaticPartitionColumnsEmptyIsNoOp() {
        // An absent PARTITION clause is a plain (non-static) write: validate must early-return without even loading
        // the table, so an unpartitioned table is NOT rejected. MUTATION: removing the empty early return ->
        // unpartitioned plain writes wrongly rejected -> red.
        IcebergConnectorMetadata md = metadataWithSpec(idNameSchema(), PartitionSpec.unpartitioned());
        Assertions.assertDoesNotThrow(() -> md.validateStaticPartitionColumns(
                null, new IcebergTableHandle("db1", "t1"), Collections.emptyList()));
    }

    // ---------------------------------------------------------------------
    // list / exists delegation
    // ---------------------------------------------------------------------

    @Test
    public void listDatabaseNamesDelegatesToOps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.databases = Arrays.asList("db_a", "db_b");

        List<String> result = metadataWith(ops).listDatabaseNames(null);

        // WHY: listDatabaseNames must return exactly what the remote catalog reports, in order; it is
        // the only source of the catalog's database list shown to users. MUTATION: returning
        // emptyList (dropping the delegation) -> red.
        Assertions.assertEquals(Arrays.asList("db_a", "db_b"), result);
        Assertions.assertEquals(Collections.singletonList("listDatabaseNames"), ops.log,
                "listDatabaseNames must make exactly one listDatabaseNames() call on the seam");
    }

    @Test
    public void databaseExistsDelegatesTrue() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.databaseExists = true;

        boolean exists = metadataWith(ops).databaseExists(null, "db1");

        // WHY: databaseExists must surface the seam's existence answer verbatim. MUTATION: hardcoding
        // false (or not delegating) -> red.
        Assertions.assertTrue(exists);
        Assertions.assertEquals(Collections.singletonList("databaseExists:db1"), ops.log);
    }

    @Test
    public void databaseExistsDelegatesFalse() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.databaseExists = false;

        // WHY: the false branch (e.g. a catalog without namespace support, or a genuinely absent db)
        // must also pass through. MUTATION: hardcoding true -> red.
        Assertions.assertFalse(metadataWith(ops).databaseExists(null, "ghost"));
    }

    @Test
    public void listTableNamesDelegatesToOps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");

        List<String> result = metadataWith(ops).listTableNames(null, "db1");

        // WHY: listTableNames must surface exactly the remote table list for the given db. MUTATION:
        // returning emptyList (dropping delegation) -> red.
        Assertions.assertEquals(Arrays.asList("t1", "t2"), result);
        Assertions.assertEquals(Collections.singletonList("listTableNames:db1"), ops.log);
    }

    @Test
    public void listViewNamesDelegatesToOps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.views = Arrays.asList("v1", "v2");

        List<String> result = metadataWith(ops).listViewNames(null, "db1");

        // WHY: post-cutover the catalog re-merges the connector's view names back into SHOW TABLES (iceberg's
        // listTableNames subtracts them) and cascades them on force-drop. listViewNames must surface exactly
        // the remote view list for the given db. MUTATION: returning emptyList (dropping delegation) -> the
        // catalog merge sees no views -> views vanish from SHOW TABLES -> red.
        Assertions.assertEquals(Arrays.asList("v1", "v2"), result);
        Assertions.assertEquals(Collections.singletonList("listViewNames:db1"), ops.log);
    }

    @Test
    public void viewExistsDelegatesToOps() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.viewExists = true;

        boolean present = metadataWith(ops).viewExists(null, "db1", "v1");

        // WHY: PluginDrivenExternalTable.isView() resolves from this; it must report exactly what the seam
        // says for the (db, view) pair. MUTATION: returning false (dropping delegation) -> a flipped iceberg
        // view reports isView()==false -> scanned as a table -> red.
        Assertions.assertTrue(present);
        Assertions.assertEquals(Collections.singletonList("viewExists:db1.v1"), ops.log,
                "viewExists must gate on exactly one viewExists() call carrying the db/view names verbatim");
    }

    @Test
    public void viewExistsFalseWhenSeamReportsFalse() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.viewExists = false;

        // WHY: a plain table must NOT be reported as a view. MUTATION: hard-coding true -> every table looks
        // like a view -> red.
        Assertions.assertFalse(metadataWith(ops).viewExists(null, "db1", "t1"));
    }

    @Test
    public void getViewDefinitionReturnsSqlDialectAndColumns() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema viewSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        ops.view = new FakeIcebergViewCatalog.StubView(
                viewVersionWith("Spark", "spark", "SELECT 1"), viewSchema);

        ConnectorViewDefinition def = metadataWith(ops).getViewDefinition(null, "db1", "v1");

        // WHY (H8): ONE loadView yields the sql/dialect (BindRelation / SHOW CREATE) AND the column schema
        // (DESC / SHOW COLUMNS / information_schema.columns). The dialect is the summary engine-name LOWERCASED;
        // the sql is that dialect's representation; the columns are parseSchema(view.schema()). MUTATION:
        // dropping toLowerCase / wrong representation / not parsing the view schema (empty columns) -> red.
        Assertions.assertEquals("SELECT 1", def.getSql());
        Assertions.assertEquals("spark", def.getDialect());
        List<ConnectorColumn> cols = def.getColumns();
        Assertions.assertEquals(2, cols.size(), "the view columns must come from parseSchema(view.schema())");
        Assertions.assertEquals("id", cols.get(0).getName());
        Assertions.assertEquals("name", cols.get(1).getName());
        Assertions.assertEquals("db1", ops.lastLoadViewDb);
        Assertions.assertEquals("v1", ops.lastLoadViewName);
        Assertions.assertEquals(Collections.singletonList("loadView:db1.v1"), ops.log,
                "getViewDefinition must do exactly one loadView() carrying db/view verbatim");
    }

    @Test
    public void getViewDefinitionParsesColumnsHonoringMappingFlags() {
        // WHY (H8): the view columns are built by the SAME parseSchema the table path uses, so the per-catalog
        // enable.mapping.* flags — which live in THIS layer's properties, not the SDK-only seam — must thread
        // into the view's column types and the WITH_TIMEZONE marker, exactly as for a table. This is the reason
        // the column build lives in the metadata layer (not the seam). MUTATION: building columns in the seam
        // (no flags) / not threading the flags -> STRING/DATETIMEV2 + no marker -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema viewSchema = new Schema(
                Types.NestedField.optional(1, "b", Types.BinaryType.get()),
                Types.NestedField.optional(2, "ts_tz", Types.TimestampType.withZone()));
        ops.view = new FakeIcebergViewCatalog.StubView(
                viewVersionWith("spark", "spark", "SELECT 1"), viewSchema);
        Map<String, String> props = new HashMap<>();
        props.put("enable.mapping.varbinary", "true");
        props.put("enable.mapping.timestamp_tz", "true");

        List<ConnectorColumn> cols =
                metadataWith(ops, props).getViewDefinition(null, "db1", "v1").getColumns();

        Assertions.assertEquals("VARBINARY", cols.get(0).getType().getTypeName(),
                "enable.mapping.varbinary=true must thread into the view's BINARY column");
        Assertions.assertEquals("TIMESTAMPTZ", cols.get(1).getType().getTypeName(),
                "enable.mapping.timestamp_tz=true must thread into the view's TIMESTAMP-with-zone column");
        Assertions.assertTrue(cols.get(1).isWithTimeZone(),
                "a with-zone timestamp view column must carry the WITH_TIMEZONE marker");
    }

    @Test
    public void getViewDefinitionRunsInsideAuthContext() {
        // WHY: the remote load must sit INSIDE executeAuthenticated (same as viewExists/listTableNames). With a
        // context whose executeAuthenticated throws WITHOUT running the task, getViewDefinition must surface a
        // (normalized) failure and NEVER call the seam. MUTATION: hoisting the call outside the auth wrap ->
        // the seam is reached / no failure -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.view = new FakeIcebergViewCatalog.StubView(
                viewVersionWith("spark", "spark", "SELECT 1"), idNameSchema());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;

        Assertions.assertThrows(RuntimeException.class,
                () -> metadataWith(ops, ctx).getViewDefinition(null, "db1", "v1"));
        Assertions.assertFalse(ops.log.contains("loadView:db1.v1"),
                "the seam must not be reached when the auth wrap throws");
    }

    @Test
    public void getViewDefinitionThrowsWhenNoCurrentVersion() {
        // WHY (moved from the seam test post-H8): a view with no current version is unusable; the metadata
        // extraction must fail loud rather than NPE on summary(). MUTATION: dropping the null-version check.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.view = new FakeIcebergViewCatalog.StubView(null, idNameSchema());
        Assertions.assertThrows(RuntimeException.class,
                () -> metadataWith(ops).getViewDefinition(null, "db1", "v1"));
    }

    @Test
    public void getViewDefinitionThrowsWhenEngineNameMissing() {
        // WHY (moved from the seam test post-H8): the dialect IS the summary engine-name; without it the SQL
        // representation cannot be selected. MUTATION: dropping the empty-engine-name check -> wrong behavior.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.view = new FakeIcebergViewCatalog.StubView(
                viewVersionWith(null, "spark", "SELECT 1"), idNameSchema());
        Assertions.assertThrows(RuntimeException.class,
                () -> metadataWith(ops).getViewDefinition(null, "db1", "v1"));
    }

    @Test
    public void getViewDefinitionThrowsWhenNoSqlForDialect() {
        // WHY (moved from the seam test post-H8): engine-name present but no SQL representation for that dialect
        // -> sqlFor returns null -> must fail loud (mirrors legacy "Cannot get view text"). MUTATION: dropping
        // the null-sql check -> NPE.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.view = new FakeIcebergViewCatalog.StubView(
                viewVersionWith("spark", null, null), idNameSchema());
        Assertions.assertThrows(RuntimeException.class,
                () -> metadataWith(ops).getViewDefinition(null, "db1", "v1"));
    }

    // ---------------------------------------------------------------------
    // getTableHandle — present iff tableExists, carries db/table coordinates
    // ---------------------------------------------------------------------

    @Test
    public void getTableHandlePresentWhenTableExists() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tableExists = true;

        Optional<ConnectorTableHandle> handleOpt = metadataWith(ops).getTableHandle(null, "db1", "t1");

        // WHY: a handle is the FE-side coordinate later used to load the schema; it must be present
        // exactly when the seam reports the table exists, and must carry the db/table names verbatim.
        // MUTATION: returning empty on exists==true, or losing the coordinates -> red.
        Assertions.assertTrue(handleOpt.isPresent());
        IcebergTableHandle handle = (IcebergTableHandle) handleOpt.get();
        Assertions.assertEquals("db1", handle.getDbName());
        Assertions.assertEquals("t1", handle.getTableName());
        Assertions.assertEquals(Collections.singletonList("tableExists:db1.t1"), ops.log,
                "getTableHandle must gate on exactly one tableExists() call");
    }

    @Test
    public void getTableHandleEmptyWhenTableMissing() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.tableExists = false;

        Optional<ConnectorTableHandle> handleOpt =
                metadataWith(ops).getTableHandle(null, "db1", "ghost");

        // WHY: a missing table is an absent handle (Optional.empty), not a thrown error and not a
        // present handle that later fails on load. MUTATION: returning a present handle when
        // tableExists==false -> red.
        Assertions.assertFalse(handleOpt.isPresent());
    }

    // ---------------------------------------------------------------------
    // getTableSchema — load via seam, parse columns + table props
    // ---------------------------------------------------------------------

    @Test
    public void getTableSchemaParsesColumnsFromLoadedTable() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableHandle handle = new IcebergTableHandle("db1", "t1");
        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle);

        // WHY: the schema must be derived from the table the seam LOADS for the handle's coordinates;
        // the columns come from the Iceberg Schema in order. MUTATION: not loading via the seam, or
        // dropping/reordering columns -> red. The recorded loadTable proves the read went through the
        // seam with the handle's db/table.
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t1"),
                "getTableSchema must load the table via the seam using the handle coordinates");
        List<ConnectorColumn> cols = schema.getColumns();
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("id", cols.get(0).getName());
        Assertions.assertEquals("INT", cols.get(0).getType().getTypeName());
        Assertions.assertEquals("name", cols.get(1).getName());
        Assertions.assertEquals("STRING", cols.get(1).getType().getTypeName());

        // WHY: legacy IcebergUtils.parseSchema builds EVERY column with isAllowNull=true regardless of
        // the Iceberg field's required/optional flag (rows can still read NULL under schema-evolution
        // default-fill, and nereids must not fold null-rejecting predicates the legacy path permitted).
        // So even a REQUIRED Iceberg field surfaces as nullable. MUTATION: propagating field.isOptional()
        // (required -> NOT NULL) -> red.
        Assertions.assertTrue(cols.get(0).isNullable(),
                "a required Iceberg field must STILL surface as nullable (legacy forces isAllowNull=true)");
        Assertions.assertTrue(cols.get(1).isNullable(),
                "an optional Iceberg field must surface as nullable");

        // WHY: legacy IcebergUtils.parseSchema passes isKey=true for every column, so DESC shows Key=true
        // for all iceberg columns (external-table semantics). MUTATION: the 5-arg ConnectorColumn ctor
        // (default isKey=false) -> red.
        Assertions.assertTrue(cols.get(0).isKey(),
                "every iceberg column must be a key column (legacy parity: isKey=true)");
        Assertions.assertTrue(cols.get(1).isKey(),
                "every iceberg column must be a key column (legacy parity: isKey=true)");

        // WHY (H-10 L3): legacy IcebergUtils.updateIcebergColumnUniqueId set the top-level Column.uniqueId =
        // field.fieldId(); post-flip parseSchema must carry it on ConnectorColumn.withUniqueId so the BE
        // field-id scan path keys its read projection / nested matching off the stable id (rename-safe).
        // idNameSchema assigns field-ids 1 and 2. MUTATION: dropping the withUniqueId(field.fieldId()) call
        // leaves the uniqueId at the default -1 -> red.
        Assertions.assertEquals(1, cols.get(0).getUniqueId(), "id carries iceberg field-id 1");
        Assertions.assertEquals(2, cols.get(1).getUniqueId(), "name carries iceberg field-id 2");

        // WHY: the table-format type tag is the fixed "ICEBERG" discriminator the FE uses to route the
        // schema. MUTATION: emitting a different/empty tag -> red.
        Assertions.assertEquals("ICEBERG", schema.getTableFormatType());
    }

    @Test
    public void getTableSchemaCarriesFieldDocAsComment() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema docSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get(), "the primary id"),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        ops.table = new FakeIcebergTable(
                "t1", docSchema, PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: the Iceberg field doc becomes the Doris column comment; a field with no doc becomes the
        // empty string (NOT null), matching the production `field.doc() != null ? field.doc() : ""`.
        // MUTATION: dropping the doc->comment carry, or passing null for the empty case -> red.
        Assertions.assertEquals("the primary id", schema.getColumns().get(0).getComment());
        Assertions.assertEquals("", schema.getColumns().get(1).getComment(),
                "a doc-less Iceberg field must yield an empty (not null) comment");
    }

    @Test
    public void getTableSchemaCopiesTablePropertiesAndLocation() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "parquet");
        tableProps.put("custom.key", "custom-value");
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", tableProps);

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));
        Map<String, String> props = schema.getProperties();

        // WHY: the Iceberg table properties must be copied verbatim onto the schema (the FE relies on
        // keys like write.format.default), and the table location must be surfaced under the neutral
        // SHOW CREATE render-hint key show.location (rendered as the LOCATION clause, NOT mixed into the
        // user PROPERTIES). MUTATION: dropping the table.properties() copy, or not emitting the location
        // hint -> red.
        Assertions.assertEquals("parquet", props.get("write.format.default"));
        Assertions.assertEquals("custom-value", props.get("custom.key"));
        Assertions.assertEquals("s3://bucket/db1/t1", props.get(ConnectorTableSchema.SHOW_LOCATION_KEY));
        // Byte-faithful PROPERTIES: legacy iceberg SHOW CREATE dumped only the raw table.properties(), never a
        // bare "location" key. MUTATION: reviving the old tableProps.put("location", ...) -> a stray location
        // entry leaks into the rendered PROPERTIES -> red.
        Assertions.assertFalse(props.containsKey("location"),
                "the table location must travel under show.location, not as a bare \"location\" property");
    }

    @Test
    public void getTableSchemaEmitsShowPartitionClauseWithTransforms() {
        // Unpartitioned: no show.partition-clause (and, byte-faithful, no legacy iceberg.partition-spec).
        RecordingIcebergCatalogOps unpartOps = new RecordingIcebergCatalogOps();
        unpartOps.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());
        ConnectorTableSchema unpartSchema =
                metadataWith(unpartOps).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: an unpartitioned table renders no PARTITION BY (production guard `!spec.isUnpartitioned()`).
        // MUTATION: always emitting show.partition-clause -> red.
        Assertions.assertNull(unpartSchema.getProperties().get(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY),
                "an unpartitioned table must not carry a show.partition-clause property");
        // Byte-faithful PROPERTIES: the raw spec.toString() debug string must NOT leak (legacy never showed it).
        Assertions.assertFalse(unpartSchema.getProperties().containsKey("iceberg.partition-spec"),
                "the raw iceberg.partition-spec debug key must not be emitted");

        // Partitioned (identity on name): bare quoted column.
        Schema schema = idNameSchema();
        PartitionSpec identitySpec = PartitionSpec.builderFor(schema).identity("name").build();
        RecordingIcebergCatalogOps identityOps = new RecordingIcebergCatalogOps();
        identityOps.table = new FakeIcebergTable(
                "t2", schema, identitySpec, "s3://bucket/db1/t2", Collections.emptyMap());
        ConnectorTableSchema identitySchema =
                metadataWith(identityOps).getTableSchema(null, new IcebergTableHandle("db1", "t2"));
        // WHY: SHOW CREATE TABLE must render the Doris PARTITION BY LIST(...)() clause the legacy
        // IcebergExternalTable.getPartitionSpecSql produced; an identity transform renders the bare column.
        // MUTATION: dropping the identity branch (or the LIST(...)() wrapper) -> red.
        Assertions.assertEquals("PARTITION BY LIST (`name`) ()",
                identitySchema.getProperties().get(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY),
                "an identity partition must render as the bare quoted column");
        // Also byte-faithful: the partition_columns CSV (functional, consumed by fe-core) is unchanged.
        Assertions.assertEquals("name",
                identitySchema.getProperties().get("partition_columns"));

        // Partitioned with a NON-identity transform (bucket): renders the Doris BUCKET(N, col) term.
        PartitionSpec bucketSpec = PartitionSpec.builderFor(schema).bucket("id", 8).build();
        RecordingIcebergCatalogOps bucketOps = new RecordingIcebergCatalogOps();
        bucketOps.table = new FakeIcebergTable(
                "t3", schema, bucketSpec, "s3://bucket/db1/t3", Collections.emptyMap());
        ConnectorTableSchema bucketSchema =
                metadataWith(bucketOps).getTableSchema(null, new IcebergTableHandle("db1", "t3"));
        // WHY: the transform terms (bucket[N]/truncate[W]/year/month/day/hour) must map to the matching Doris
        // partition function — the connector pre-renders them because the FE plugin path has no live iceberg
        // API. MUTATION: emitting the bare column instead of BUCKET(8, `id`), or a wrong arg order -> red.
        Assertions.assertEquals("PARTITION BY LIST (BUCKET(8, `id`)) ()",
                bucketSchema.getProperties().get(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY),
                "a bucket transform must render as BUCKET(N, `col`)");

        // truncate transform -> TRUNCATE(W, `col`). MUTATION: a wrong width/arg order/function name -> red.
        PartitionSpec truncateSpec = PartitionSpec.builderFor(schema).truncate("name", 4).build();
        RecordingIcebergCatalogOps truncateOps = new RecordingIcebergCatalogOps();
        truncateOps.table = new FakeIcebergTable(
                "t4", schema, truncateSpec, "s3://bucket/db1/t4", Collections.emptyMap());
        ConnectorTableSchema truncateSchema =
                metadataWith(truncateOps).getTableSchema(null, new IcebergTableHandle("db1", "t4"));
        Assertions.assertEquals("PARTITION BY LIST (TRUNCATE(4, `name`)) ()",
                truncateSchema.getProperties().get(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY),
                "a truncate transform must render as TRUNCATE(W, `col`)");

        // a temporal transform (day) on a timestamp column -> DAY(`col`). Covers the temporal branch family
        // (year/month/day/hour share the same rendering shape). MUTATION: mapping day to the wrong function
        // name (e.g. DAYS) -> red.
        Schema tsSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec daySpec = PartitionSpec.builderFor(tsSchema).day("ts").build();
        RecordingIcebergCatalogOps dayOps = new RecordingIcebergCatalogOps();
        dayOps.table = new FakeIcebergTable(
                "t5", tsSchema, daySpec, "s3://bucket/db1/t5", Collections.emptyMap());
        ConnectorTableSchema daySchema =
                metadataWith(dayOps).getTableSchema(null, new IcebergTableHandle("db1", "t5"));
        Assertions.assertEquals("PARTITION BY LIST (DAY(`ts`)) ()",
                daySchema.getProperties().get(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY),
                "a day transform must render as DAY(`col`)");
    }

    @Test
    public void getTableSchemaEmitsShowSortClauseWhenSorted() {
        Schema schema = idNameSchema();

        // Unsorted (FakeIcebergTable.sortOrder() defaults to null -> render path treats as unsorted).
        RecordingIcebergCatalogOps unsortedOps = new RecordingIcebergCatalogOps();
        unsortedOps.table = new FakeIcebergTable(
                "t1", schema, PartitionSpec.unpartitioned(), "s3://bucket/db1/t1", Collections.emptyMap());
        ConnectorTableSchema unsortedSchema =
                metadataWith(unsortedOps).getTableSchema(null, new IcebergTableHandle("db1", "t1"));
        // WHY: an unsorted table renders no ORDER BY. MUTATION: always emitting show.sort-clause -> red.
        Assertions.assertNull(unsortedSchema.getProperties().get(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY),
                "an unsorted table must not carry a show.sort-clause property");

        // Sorted DESC on name: the connector pre-renders the ORDER BY clause (legacy getSortOrderSql +
        // SortFieldInfo.toSql: `col` ASC|DESC NULLS FIRST|LAST). desc() defaults to NULLS LAST in iceberg.
        SortOrder sortOrder = SortOrder.builderFor(schema).desc("name").build();
        FakeIcebergTable sortedTable = new FakeIcebergTable(
                "t2", schema, PartitionSpec.unpartitioned(), "s3://bucket/db1/t2", Collections.emptyMap());
        sortedTable.setSortOrder(sortOrder);
        RecordingIcebergCatalogOps sortedOps = new RecordingIcebergCatalogOps();
        sortedOps.table = sortedTable;
        ConnectorTableSchema sortedSchema =
                metadataWith(sortedOps).getTableSchema(null, new IcebergTableHandle("db1", "t2"));
        // WHY: SHOW CREATE TABLE must render the ORDER BY clause with direction + null order. MUTATION:
        // dropping the clause, or swapping ASC/DESC or NULLS FIRST/LAST -> red.
        Assertions.assertEquals("ORDER BY (`name` DESC NULLS LAST)",
                sortedSchema.getProperties().get(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY),
                "a sorted table must render the ORDER BY clause with direction and null order");

        // Sorted ASC on id: ASC + NULLS FIRST (iceberg asc() default null order). Positively renders the
        // ASC and NULLS FIRST output arms (the DESC case alone cannot catch a hardcode-to-DESC /
        // hardcode-to-NULLS-LAST mutation). MUTATION: hardcoding direction to DESC or null order to LAST -> red.
        SortOrder ascOrder = SortOrder.builderFor(schema).asc("id").build();
        FakeIcebergTable ascTable = new FakeIcebergTable(
                "t3", schema, PartitionSpec.unpartitioned(), "s3://bucket/db1/t3", Collections.emptyMap());
        ascTable.setSortOrder(ascOrder);
        RecordingIcebergCatalogOps ascOps = new RecordingIcebergCatalogOps();
        ascOps.table = ascTable;
        ConnectorTableSchema ascSchema =
                metadataWith(ascOps).getTableSchema(null, new IcebergTableHandle("db1", "t3"));
        Assertions.assertEquals("ORDER BY (`id` ASC NULLS FIRST)",
                ascSchema.getProperties().get(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY),
                "an ascending sort must render ASC NULLS FIRST");
    }

    @Test
    public void getDatabaseSurfacesNamespaceLocation() {
        // WHY: SHOW CREATE DATABASE renders LOCATION from the connector's getDatabase SPI (Trino-aligned
        // properties-map, the "location" key); the connector reads the namespace location through the seam,
        // auth-wrapped like the sibling reads. MUTATION: not surfacing the location, or reading the wrong key
        // -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.namespaceLocation = Optional.of("s3://bucket/db1");
        ConnectorDatabaseMetadata metadata = metadataWith(ops).getDatabase(null, "db1");
        Assertions.assertEquals("s3://bucket/db1",
                metadata.getProperties().get(ConnectorDatabaseMetadata.LOCATION_PROPERTY),
                "getDatabase must surface the namespace location under the location property");
        Assertions.assertTrue(ops.log.contains("loadNamespaceLocation:db1"),
                "getDatabase must read the namespace location through the seam");

        // No namespace location -> no location key (SHOW CREATE DATABASE then renders no LOCATION clause).
        RecordingIcebergCatalogOps emptyOps = new RecordingIcebergCatalogOps();
        ConnectorDatabaseMetadata emptyMetadata = metadataWith(emptyOps).getDatabase(null, "db1");
        Assertions.assertFalse(
                emptyMetadata.getProperties().containsKey(ConnectorDatabaseMetadata.LOCATION_PROPERTY),
                "a location-less namespace must not produce a location property");
    }

    @Test
    public void getTableSchemaEmitsPartitionColumnsForIdentityPartition() {
        // Unpartitioned: no partition_columns key.
        RecordingIcebergCatalogOps unpartOps = new RecordingIcebergCatalogOps();
        unpartOps.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());
        ConnectorTableSchema unpartSchema =
                metadataWith(unpartOps).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: post-cutover, PluginDrivenExternalTable derives its partition columns SOLELY from the
        // generic "partition_columns" CSV property (toSchemaCacheValue), the same key MaxCompute/paimon
        // emit. An unpartitioned table must NOT emit it, or isPartitionedTable() would be wrong post-flip.
        // MUTATION: always emitting partition_columns -> red.
        Assertions.assertNull(unpartSchema.getProperties().get("partition_columns"),
                "an unpartitioned table must not carry a partition_columns property");

        // Partitioned (identity on name): the source column name is surfaced under partition_columns.
        Schema schema = idNameSchema();
        PartitionSpec partSpec = PartitionSpec.builderFor(schema).identity("name").build();
        RecordingIcebergCatalogOps partOps = new RecordingIcebergCatalogOps();
        partOps.table = new FakeIcebergTable(
                "t2", schema, partSpec, "s3://bucket/db1/t2", Collections.emptyMap());
        ConnectorTableSchema partSchema =
                metadataWith(partOps).getTableSchema(null, new IcebergTableHandle("db1", "t2"));

        // WHY: post-flip getPartitionColumns() reads this CSV and must report the SAME partition columns
        // legacy IcebergExternalTable did (IcebergUtils.loadTableSchemaCacheValue: spec source columns).
        // MUTATION: dropping the partition_columns emission -> the key is absent -> red.
        Assertions.assertEquals("name",
                partSchema.getProperties().get("partition_columns"),
                "a partitioned table must surface its partition source columns as a CSV");
    }

    @Test
    public void getTableSchemaEmitsNonIdentityPartitionSourceColumns() {
        // bucket(id) is a NON-identity transform. Legacy IcebergUtils.loadTableSchemaCacheValue walks the
        // CURRENT spec with NO identity filter, collecting the SOURCE column ("id"). The connector must
        // replicate THAT (not the identity-only IcebergPartitionUtils.getIdentityPartitionColumns helper),
        // or post-flip a bucket-partitioned table would report no partition columns (parity break).
        Schema schema = idNameSchema();
        PartitionSpec partSpec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
        RecordingIcebergCatalogOps partOps = new RecordingIcebergCatalogOps();
        partOps.table = new FakeIcebergTable(
                "t3", schema, partSpec, "s3://bucket/db1/t3", Collections.emptyMap());
        ConnectorTableSchema partSchema =
                metadataWith(partOps).getTableSchema(null, new IcebergTableHandle("db1", "t3"));

        // WHY: a bucket/truncate/day transform still has a source column that legacy treats as a partition
        // column. MUTATION: filtering to identity transforms -> "id" absent -> red.
        Assertions.assertEquals("id",
                partSchema.getProperties().get("partition_columns"),
                "a non-identity transform must still surface its source column as a partition column");
    }

    @Test
    public void getTableSchemaDefaultsFormatVersionBelowThreeWhenAbsent() {
        // WHY: getFormatVersion (which drives the v3 row-lineage gate) defaults to 2 when the table carries
        // no `format-version` property, so an absent-format-version table appends NO row-lineage columns. The
        // previously-emitted iceberg.format-version property was removed (it was never read by fe-core and
        // would leak into the rendered SHOW CREATE PROPERTIES), so the default is now pinned via its real
        // consequence: only the data columns. MUTATION: defaulting to >= 3 (or reviving the spec-id quirk that
        // yielded a higher version) -> row-lineage columns appear -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals(2, schema.getColumns().size(),
                "an absent format-version must default below 3 (no row-lineage columns)");
        // And byte-faithful: the internal iceberg.format-version key must not leak into the rendered PROPERTIES
        // (legacy iceberg SHOW CREATE dumped only the raw table.properties()).
        Assertions.assertFalse(schema.getProperties().containsKey("iceberg.format-version"),
                "the internal iceberg.format-version key must not leak into the rendered PROPERTIES");
    }

    @Test
    public void getTableSchemaAppendsV3RowLineageColumnsWhenFormatVersionAtLeast3() {
        // WHY (③-infra part2): legacy IcebergExternalTable.getFullSchema unconditionally calls
        // IcebergUtils.appendRowLineageColumnsForV3, which appends the two hidden row-lineage columns
        // (_row_id / _last_updated_sequence_number, BIGINT, reserved field ids 2147483540 / 2147483539,
        // invisible) for format-version >= 3 tables. Post-cutover the connector owns the table schema, so it
        // must declare them itself through the schema SPI — invisible() + the reserved uniqueId carried
        // across the boundary, re-applied by ConnectorColumnConverter and round-tripped via the schema cache.
        // MUTATION: dropping the append, the >= 3 gate, .invisible(), .withUniqueId(), or swapping the two
        // field ids -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "3");
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", props);

        List<ConnectorColumn> cols =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1")).getColumns();

        // The two data columns (id, name) come first, then the two appended lineage columns IN ORDER
        // (legacy appends _row_id before _last_updated_sequence_number, after the data columns).
        Assertions.assertEquals(4, cols.size(),
                "format-version >= 3 must append the two row-lineage columns after the data columns");
        Assertions.assertEquals("id", cols.get(0).getName());
        Assertions.assertEquals("name", cols.get(1).getName());

        ConnectorColumn rowId = cols.get(2);
        Assertions.assertEquals("_row_id", rowId.getName());
        Assertions.assertEquals("BIGINT", rowId.getType().getTypeName(), "_row_id is BIGINT");
        Assertions.assertFalse(rowId.isVisible(), "_row_id must be hidden");
        Assertions.assertEquals(2147483540, rowId.getUniqueId(), "_row_id reserved field id");
        Assertions.assertTrue(rowId.isNullable(), "_row_id is nullable (legacy isAllowNull=true)");
        Assertions.assertFalse(rowId.isKey(), "_row_id is not a key (legacy isKey=false)");

        ConnectorColumn seq = cols.get(3);
        Assertions.assertEquals("_last_updated_sequence_number", seq.getName());
        Assertions.assertEquals("BIGINT", seq.getType().getTypeName(),
                "_last_updated_sequence_number is BIGINT");
        Assertions.assertFalse(seq.isVisible(), "_last_updated_sequence_number must be hidden");
        Assertions.assertEquals(2147483539, seq.getUniqueId(),
                "_last_updated_sequence_number reserved field id");
        Assertions.assertTrue(seq.isNullable());
        Assertions.assertFalse(seq.isKey());
    }

    @Test
    public void getTableSchemaAppendsV3RowLineageColumnsForFormatVersionAbove3() {
        // WHY (③-infra part2): the gate is ">= 3" (inclusive lower bound, unbounded above) — every v3+ table
        // gets the row-lineage columns, mirroring legacy IcebergUtils.appendRowLineageColumnsForV3's
        // "< ICEBERG_ROW_LINEAGE_MIN_VERSION ? return : append". A format-version=4 table must still append.
        // MUTATION: tightening the gate to "== 3" (or a defensive "> 3" miswrite) would omit v4 -> red here
        // (the v3 case alone cannot catch a "== 3" narrowing).
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "4");
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", props);

        List<ConnectorColumn> cols =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1")).getColumns();

        Assertions.assertEquals(4, cols.size(),
                "format-version > 3 must also append the two row-lineage columns (gate is an inclusive >= 3)");
        Assertions.assertEquals("_row_id", cols.get(2).getName());
        Assertions.assertEquals("_last_updated_sequence_number", cols.get(3).getName());
    }

    @Test
    public void getTableSchemaOmitsV3RowLineageColumnsBelowFormatVersion3() {
        // WHY (③-infra part2): appendRowLineageColumnsForV3 is a no-op for format-version < 3 (the
        // row-lineage columns exist only in v3+). A v2 table must surface ONLY its data columns — no
        // _row_id / _last_updated_sequence_number. This also guards the natural exclusion of system tables,
        // which report format-version 2 (BaseMetadataTable.properties() is empty), matching legacy which
        // only injects lineage for data tables (IcebergSysExternalTable never does). MUTATION: dropping the
        // >= 3 gate (always appending) -> the v2 schema gains lineage columns -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "2");
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", props);

        List<ConnectorColumn> cols =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1")).getColumns();

        Assertions.assertEquals(2, cols.size(),
                "format-version < 3 must NOT append row-lineage columns");
        Assertions.assertTrue(cols.stream().noneMatch(c -> c.getName().equals("_row_id")
                        || c.getName().equals("_last_updated_sequence_number")),
                "no row-lineage columns below format-version 3");
    }

    @Test
    public void getTableSchemaLowercasesColumnNames() {
        // WHY: legacy IcebergUtils.parseSchema builds each column name as
        // field.name().toLowerCase(Locale.ROOT), so a mixed-case Iceberg field surfaces as a lowercase
        // Doris column. The connector must lowercase itself (the SPI bridge only layers user identifier
        // mapping on top). MUTATION: emitting field.name() verbatim -> "ID" -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema upperSchema = new Schema(
                Types.NestedField.required(1, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(2, "Mixed_Name", Types.StringType.get()));
        ops.table = new FakeIcebergTable(
                "t1", upperSchema, PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals("id", schema.getColumns().get(0).getName(),
                "an uppercase Iceberg field name must be lowercased (legacy toLowerCase(ROOT))");
        Assertions.assertEquals("mixed_name", schema.getColumns().get(1).getName(),
                "a mixed-case Iceberg field name must be fully lowercased");
    }

    @Test
    public void getTableSchemaMarksWithTimeZoneFromSourceTypeIndependentOfMappingFlag() {
        // WHY: legacy IcebergUtils.parseSchema sets setWithTZExtraInfo() when the SOURCE field is a
        // TIMESTAMP with shouldAdjustToUTC()==true, REGARDLESS of the enable.mapping.timestamp_tz flag
        // (the marker is keyed on the source type root, not the mapped Doris type). So a with-zone
        // timestamp carries the WITH_TIMEZONE marker even when mapped to plain DATETIMEV2 (flag off),
        // while a without-zone timestamp never does. MUTATION: gating the marker on the mapping flag, or
        // never setting it -> red.
        Schema tsSchema = new Schema(
                Types.NestedField.optional(1, "ts_tz", Types.TimestampType.withZone()),
                Types.NestedField.optional(2, "ts_ntz", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(3, "id", Types.IntegerType.get()));

        // Mapping flag OFF (default): with-zone ts maps to DATETIMEV2 but STILL carries the marker.
        RecordingIcebergCatalogOps offOps = new RecordingIcebergCatalogOps();
        offOps.table = new FakeIcebergTable(
                "t1", tsSchema, PartitionSpec.unpartitioned(), "s3://b/t1", Collections.emptyMap());
        List<ConnectorColumn> offCols =
                metadataWith(offOps).getTableSchema(null, new IcebergTableHandle("db1", "t1")).getColumns();
        Assertions.assertTrue(offCols.get(0).isWithTimeZone(),
                "with-zone timestamp must carry the WITH_TIMEZONE marker even with mapping flag OFF");
        Assertions.assertEquals("DATETIMEV2", offCols.get(0).getType().getTypeName(),
                "with mapping flag off the with-zone timestamp is still mapped to DATETIMEV2");
        Assertions.assertFalse(offCols.get(1).isWithTimeZone(),
                "a without-zone timestamp must NOT carry the WITH_TIMEZONE marker");
        Assertions.assertFalse(offCols.get(2).isWithTimeZone(),
                "a non-timestamp column must NOT carry the WITH_TIMEZONE marker");

        // Mapping flag ON: with-zone ts maps to TIMESTAMPTZ and also carries the marker.
        RecordingIcebergCatalogOps onOps = new RecordingIcebergCatalogOps();
        onOps.table = new FakeIcebergTable(
                "t2", tsSchema, PartitionSpec.unpartitioned(), "s3://b/t2", Collections.emptyMap());
        Map<String, String> onProps = new HashMap<>();
        onProps.put("enable.mapping.timestamp_tz", "true");
        List<ConnectorColumn> onCols = metadataWith(onOps, onProps)
                .getTableSchema(null, new IcebergTableHandle("db1", "t2")).getColumns();
        Assertions.assertTrue(onCols.get(0).isWithTimeZone(),
                "with-zone timestamp must carry the WITH_TIMEZONE marker with mapping flag ON too");
        Assertions.assertEquals("TIMESTAMPTZ", onCols.get(0).getType().getTypeName(),
                "with mapping flag on the with-zone timestamp maps to TIMESTAMPTZ");
    }

    @Test
    public void getTableSchemaOmitsLocationWhenNull() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                null, Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: when the table reports no location, the production code guards with `if (location !=
        // null)`, so the show.location render-hint key must be ABSENT rather than mapped to null. MUTATION:
        // removing the null guard -> a null-valued show.location entry -> red.
        Assertions.assertFalse(schema.getProperties().containsKey(ConnectorTableSchema.SHOW_LOCATION_KEY),
                "a null table location must not produce a show.location property");
    }

    // ---------------------------------------------------------------------
    // type-mapping toggles — enable.mapping.varbinary / enable.mapping.timestamp_tz
    // (dotted keys matching CatalogProperty.ENABLE_MAPPING_* — the spelling real catalog maps carry)
    // ---------------------------------------------------------------------

    @Test
    public void getTableSchemaHonorsVarbinaryAndTimestampTzMappingFlags() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema binTsSchema = new Schema(
                Types.NestedField.optional(1, "b", Types.BinaryType.get()),
                Types.NestedField.optional(2, "ts_tz", Types.TimestampType.withZone()));
        ops.table = new FakeIcebergTable(
                "t1", binTsSchema, PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        // Feed the LITERAL dotted keys that real catalog maps carry (CatalogProperty.ENABLE_MAPPING_*),
        // NOT the connector constants — so this test pins the exact wire spelling and goes red if the
        // connector ever reverts to the underscore key (which would silently read default-false).
        Map<String, String> props = new HashMap<>();
        props.put("enable.mapping.varbinary", "true");
        props.put("enable.mapping.timestamp_tz", "true");

        ConnectorTableSchema schema =
                metadataWith(ops, props).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: with the mapping flags on, an Iceberg BINARY column maps to VARBINARY and a
        // TIMESTAMP-with-zone column maps to TIMESTAMPTZ (via IcebergTypeMapping). The metadata layer
        // must read these flags from the catalog props using the DOTTED key spelling that real catalog
        // maps carry (CatalogProperty.ENABLE_MAPPING_*) and thread them to the type mapper. MUTATION:
        // reading the underscore key, or not reading the flags (always default false) -> STRING /
        // DATETIMEV2 -> red.
        Assertions.assertEquals("VARBINARY", schema.getColumns().get(0).getType().getTypeName(),
                "enable.mapping.varbinary=true must map Iceberg BINARY to VARBINARY");
        Assertions.assertEquals("TIMESTAMPTZ", schema.getColumns().get(1).getType().getTypeName(),
                "enable.mapping.timestamp_tz=true must map Iceberg TIMESTAMP-with-zone to TIMESTAMPTZ");
    }

    @Test
    public void getTableSchemaDefaultsMappingFlagsOff() {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema binTsSchema = new Schema(
                Types.NestedField.optional(1, "b", Types.BinaryType.get()),
                Types.NestedField.optional(2, "ts_tz", Types.TimestampType.withZone()));
        ops.table = new FakeIcebergTable(
                "t1", binTsSchema, PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        // No mapping keys set: the default (mapping off) behavior.
        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: with the toggles absent, BINARY must map to STRING and TIMESTAMP-with-zone to DATETIMEV2
        // (default false). This guards against a fix that accidentally flips the defaults on. MUTATION:
        // defaulting either flag to true -> VARBINARY / TIMESTAMPTZ -> red.
        Assertions.assertEquals("STRING", schema.getColumns().get(0).getType().getTypeName(),
                "absent enable.mapping.varbinary must leave Iceberg BINARY as STRING (default off)");
        Assertions.assertEquals("DATETIMEV2", schema.getColumns().get(1).getType().getTypeName(),
                "absent enable.mapping.timestamp_tz must leave Iceberg TIMESTAMP-with-zone as DATETIMEV2");
    }

    // ---------------------------------------------------------------------
    // auth wrapping — every remote read runs inside ConnectorContext.executeAuthenticated
    // (legacy IcebergMetadataOps + the paimon mirror wrap every list/exists/load call)
    // ---------------------------------------------------------------------

    @Test
    public void everyRemoteReadRunsInsideExecuteAuthenticated() {
        // WHY: legacy IcebergMetadataOps wraps EVERY remote read (list/exists/load) in
        // executionAuthenticator.execute so the FE-injected Kerberos UGI applies; the paimon mirror does
        // the same. Each of the 8 read entry points must wrap exactly one executeAuthenticated call.
        // MUTATION: calling the seam directly (no wrap) -> authCount stays 0 -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.databases = Collections.singletonList("db1");
        ops.tables = Collections.singletonList("t1");
        ops.databaseExists = true;
        ops.tableExists = true;
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(), "s3://b/t1", Collections.emptyMap());
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        IcebergConnectorMetadata md = metadataWith(ops, ctx);

        md.listDatabaseNames(null);
        md.databaseExists(null, "db1");
        md.listTableNames(null, "db1");
        md.listViewNames(null, "db1");
        md.getTableHandle(null, "db1", "t1");
        md.viewExists(null, "db1", "v1");
        md.getTableSchema(null, new IcebergTableHandle("db1", "t1"));
        md.getDatabase(null, "db1");

        Assertions.assertEquals(8, ctx.authCount,
                "each of the 8 remote reads must wrap exactly one executeAuthenticated call");
    }

    @Test
    public void readsSitInsideAuthSoFailedAuthSkipsTheSeamCall() {
        // WHY: with failAuth set, executeAuthenticated throws WITHOUT invoking the task. If a seam call sat
        // OUTSIDE the wrap it would still run; it must NOT — proving the remote call is INSIDE the
        // authenticator. Each read must surface the failure as a RuntimeException (legacy parity).
        // MUTATION: a seam call placed outside the wrap -> ops.log records it -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        IcebergConnectorMetadata md = metadataWith(ops, ctx);

        Assertions.assertThrows(RuntimeException.class, () -> md.listDatabaseNames(null));
        Assertions.assertThrows(RuntimeException.class, () -> md.databaseExists(null, "db1"));
        Assertions.assertThrows(RuntimeException.class, () -> md.listTableNames(null, "db1"));
        Assertions.assertThrows(RuntimeException.class, () -> md.listViewNames(null, "db1"));
        Assertions.assertThrows(RuntimeException.class, () -> md.getTableHandle(null, "db1", "t1"));
        Assertions.assertThrows(RuntimeException.class, () -> md.viewExists(null, "db1", "v1"));
        Assertions.assertThrows(RuntimeException.class,
                () -> md.getTableSchema(null, new IcebergTableHandle("db1", "t1")));
        Assertions.assertThrows(RuntimeException.class, () -> md.getDatabase(null, "db1"));

        Assertions.assertTrue(ops.log.isEmpty(),
                "no seam call may run when executeAuthenticated fails before invoking the task");
    }

    // ---------------------------------------------------------------------
    // getColumnHandles — pruned-column source for the T06 field-id dict
    // ---------------------------------------------------------------------

    @Test
    public void getColumnHandlesKeysByLowercasedNameAndCarriesIcebergFieldId() {
        // The generic PluginDrivenScanNode looks each query slot up here by (lowercased) name to build the
        // pruned column list the T06 field-id dictionary keys its -1 entry off. So the map MUST be keyed by the
        // lowercased name (== the Doris slot name from parseSchema) and the handle MUST carry the iceberg field
        // id (the permanent rename-safe join key). MUTATION: key by the raw iceberg case -> the slot lookup
        // misses -> empty columns -> dict falls back to all-fields. MUTATION: carry the ordinal not the field
        // id -> the dict's field ids are wrong -> BE field-id match fails.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Schema mixed = new Schema(
                Types.NestedField.required(7, "ID", Types.IntegerType.get()),
                Types.NestedField.optional(9, "Name", Types.StringType.get()));
        ops.table = new FakeIcebergTable(
                "t1", mixed, PartitionSpec.unpartitioned(), "s3://bucket/db1/t1", Collections.emptyMap());

        Map<String, ConnectorColumnHandle> handles =
                metadataWith(ops).getColumnHandles(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals(2, handles.size());
        Assertions.assertTrue(handles.containsKey("id"));
        Assertions.assertTrue(handles.containsKey("name"));
        Assertions.assertEquals(7, ((IcebergColumnHandle) handles.get("id")).getFieldId());
        Assertions.assertEquals(9, ((IcebergColumnHandle) handles.get("name")).getFieldId());
        // The remote load must go through the seam (auth-wrapped), mirroring getTableSchema.
        Assertions.assertTrue(ops.log.contains("loadTable:db1.t1"),
                "getColumnHandles must load the table via the seam using the handle coordinates");
    }

    // ---------------------------------------------------------------------
    // P6.3-T03: write transaction wiring (gate-closed / dormant)
    // ---------------------------------------------------------------------

    @Test
    public void beginTransactionReturnsIcebergConnectorTransactionWithEngineId() {
        // beginTransaction opens a connector transaction whose id is the engine-allocated id (so the
        // generic PluginDrivenTransactionManager registers it in both the per-manager map and
        // GlobalExternalTransactionInfoMgr — the BE->FE report path finds the txn by this id). Dormant
        // until the P6.6 cutover; the SDK transaction is opened later by the write plan via beginWrite.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        org.apache.doris.connector.api.handle.ConnectorTransaction txn =
                metadataWith(ops).beginTransaction(new TxnIdSession(31337L));

        Assertions.assertTrue(txn instanceof IcebergConnectorTransaction);
        Assertions.assertEquals(31337L, txn.getTransactionId());
        Assertions.assertEquals("ICEBERG", txn.profileLabel());
        // No remote call at begin time (the table is loaded lazily in beginWrite).
        Assertions.assertTrue(ops.log.isEmpty(), "beginTransaction must not touch the catalog seam");
    }

    /** Minimal {@link org.apache.doris.connector.api.ConnectorSession} that only hands out a txn id. */
    private static final class TxnIdSession implements org.apache.doris.connector.api.ConnectorSession {
        private final long txnId;

        TxnIdSession(long txnId) {
            this.txnId = txnId;
        }

        @Override
        public long allocateTransactionId() {
            return txnId;
        }

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
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 0L;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
