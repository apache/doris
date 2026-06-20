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
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Characterization tests for {@link PaimonConnectorMetadata}, pinning the read-path behavior
 * after the {@link PaimonCatalogOps} seam extraction (B0).
 *
 * <p>The seam fully covers every remote {@code Catalog} call the metadata makes, so each test
 * drives a {@link RecordingPaimonCatalogOps} fake and builds the metadata with a {@code null}
 * real catalog — the tests are entirely offline (no live remote catalog), which is the whole
 * point of introducing the seam.
 */
public class PaimonConnectorMetadataTest {

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        // Read-path tests ignore the context; a default RecordingConnectorContext is a no-op wrapper.
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    @Test
    public void listDatabaseNamesDelegatesToOps() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.databases = Arrays.asList("db_a", "db_b");

        List<String> result = metadataWith(ops).listDatabaseNames(null);

        // WHY: listDatabaseNames must return exactly what the remote catalog reports, in order;
        // it is the only source of the catalog's database list shown to users.
        // MUTATION: returning Collections.emptyList() (dropping the delegation) -> red.
        Assertions.assertEquals(Arrays.asList("db_a", "db_b"), result);
        Assertions.assertEquals(Collections.singletonList("listDatabases"), ops.log,
                "listDatabaseNames must make exactly one listDatabases() call on the seam");
    }

    @Test
    public void databaseExistsTrueWhenGetDatabaseSucceeds() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        boolean exists = metadataWith(ops).databaseExists(null, "db1");

        // WHY: existence is defined as "getDatabase did not throw NotExist". A successful
        // getDatabase must map to true. MUTATION: returning false on success -> red.
        Assertions.assertTrue(exists);
        Assertions.assertEquals(Collections.singletonList("getDatabase:db1"), ops.log);
    }

    @Test
    public void databaseExistsFalseWhenGetDatabaseThrowsNotExist() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseNotExist = true;

        boolean exists = metadataWith(ops).databaseExists(null, "ghost");

        // WHY: the contract is that DatabaseNotExistException means "absent" (false), NOT a
        // thrown error to the caller. MUTATION: removing the catch (letting the exception
        // propagate) or returning true -> red. This is exactly the branch a recording fake can
        // exercise but a live-catalog test cannot reliably force.
        Assertions.assertFalse(exists);
    }

    @Test
    public void listTableNamesDelegatesToOps() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");

        List<String> result = metadataWith(ops).listTableNames(null, "db1");

        // WHY: listTableNames must surface exactly the remote table list for the given db.
        // MUTATION: returning emptyList (dropping delegation) -> red.
        Assertions.assertEquals(Arrays.asList("t1", "t2"), result);
        Assertions.assertEquals(Collections.singletonList("listTables:db1"), ops.log);
    }

    @Test
    public void listTableNamesReturnsEmptyWhenDatabaseMissing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseNotExist = true;

        List<String> result = metadataWith(ops).listTableNames(null, "ghost");

        // WHY: a missing database must degrade to an empty list, not propagate the checked
        // DatabaseNotExistException to the SPI caller. MUTATION: removing that catch -> red.
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void getTableHandleCarriesPartitionAndPrimaryKeysAndSetsTransientTable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "dt", "region"),
                Arrays.asList("dt", "region"),
                Collections.singletonList("id"));
        ops.table = table;

        Optional<ConnectorTableHandle> handleOpt = metadataWith(ops).getTableHandle(null, "db1", "t1");

        Assertions.assertTrue(handleOpt.isPresent());
        PaimonTableHandle handle = (PaimonTableHandle) handleOpt.get();
        // WHY: partition/primary keys are the serializable identity the FE later relies on for
        // partition pruning and bucketing; they MUST be copied from the live table onto the
        // handle. MUTATION: hardcoding emptyList for either -> red.
        Assertions.assertEquals(Arrays.asList("dt", "region"), handle.getPartitionKeys(),
                "partition keys must be carried from the Paimon table onto the handle");
        Assertions.assertEquals(Collections.singletonList("id"), handle.getPrimaryKeys(),
                "primary keys must be carried from the Paimon table onto the handle");
        // WHY: the transient Table is the fast path used by getColumnHandles; failing to set it
        // would force an extra remote reload on every column lookup. MUTATION: dropping
        // handle.setPaimonTable(table) -> getPaimonTable() is null -> red.
        Assertions.assertSame(table, handle.getPaimonTable(),
                "the resolved Paimon table must be stashed on the handle as the transient ref");
    }

    @Test
    public void getTableHandleEmptyWhenTableMissing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExist = true;

        Optional<ConnectorTableHandle> handleOpt = metadataWith(ops).getTableHandle(null, "db1", "ghost");

        // WHY: a missing table is an absent handle (Optional.empty), not a thrown error.
        // MUTATION: removing the TableNotExistException catch -> red.
        Assertions.assertFalse(handleOpt.isPresent());
    }

    @Test
    public void getColumnHandlesReloadFallbackReloadsWhenTransientTableNull() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        // A handle whose transient Table is null (e.g. after serialization across the FE/BE
        // boundary) — the metadata must reload via the seam rather than NPE.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        Assertions.assertNull(handle.getPaimonTable(), "precondition: transient table is null");

        Map<String, ConnectorColumnHandle> handles = metadataWith(ops).getColumnHandles(null, handle);

        // WHY: this is the reload-fallback safety net. With a null transient Table, the only way
        // to get column handles is to re-fetch the table from the catalog seam. MUTATION:
        // removing the `if (table == null) { table = ops.getTable(id); }` block -> NPE on
        // table.rowType() -> red. The recorded getTable call proves the reload happened.
        Assertions.assertEquals(Arrays.asList("id", "name"), new java.util.ArrayList<>(handles.keySet()),
                "column handles must be derived from the reloaded table's row type, in order");
        Assertions.assertTrue(ops.log.contains("getTable:db1.t1"),
                "reload-fallback must re-fetch the table from the seam when the transient ref is null");
    }

    @Test
    public void getColumnHandlesUsesTransientTableWithoutReload() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        Map<String, ConnectorColumnHandle> handles = metadataWith(ops).getColumnHandles(null, handle);

        // WHY: the fast path — when the transient Table is already present, getColumnHandles must
        // use it and NOT make a redundant remote getTable call. MUTATION: always reloading would
        // record a getTable entry -> red. This pins the reload as a fallback, not the default.
        Assertions.assertEquals(Arrays.asList("id", "name"), new java.util.ArrayList<>(handles.keySet()));
        Assertions.assertTrue(ops.log.isEmpty(),
                "with a present transient table, no remote getTable reload must happen");
    }

    @Test
    public void disablesCastPredicatePushdown() {
        PaimonConnectorMetadata metadata =
                new PaimonConnectorMetadata(null, Collections.emptyMap(), new RecordingConnectorContext());

        // WHY: the shared converter unwraps CAST shells, so if this returned true (the SPI
        // default), a predicate like CAST(str_col AS INT)=5 would be pushed to Paimon as
        // str_col="5" and used for file/partition pruning, silently dropping rows like "05"/" 5"
        // at the source (BE re-eval cannot recover source-dropped rows). Returning false keeps
        // CAST conjuncts BE-only, mirroring MaxCompute/Jdbc. MUTATION: removing the override (or
        // flipping it to true) reverts to the default true -> red. The getter touches no instance
        // field, so a null ops / null session keeps this offline.
        Assertions.assertFalse(metadata.supportsCastPredicatePushdown(null),
                "Paimon must disable CAST-predicate pushdown: the converter unwraps CAST shells "
                        + "and pushing the stripped predicate under-matches at the source, "
                        + "silently dropping rows BE re-eval cannot recover");
    }

    // ---------------------------------------------------------------------
    // FIX-READ-NOTNULL — read-path columns forced nullable (legacy parity)
    // ---------------------------------------------------------------------

    @Test
    public void getTableSchemaForcesColumnsNullableForLegacyParity() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // A paimon NOT NULL field (PK-like) mixed with a nullable field; DataTypes.INT() is nullable
        // by default, .notNull() flips it. Paimon forces PK columns NOT NULL, so this is the common case.
        RowType rt = RowType.builder()
                .field("id", DataTypes.INT().notNull())
                .field("val", DataTypes.INT())
                .build();
        FakePaimonTable table = new FakePaimonTable(
                "t1", rt, Collections.emptyList(), Collections.singletonList("id"));
        ops.table = table;

        ConnectorTableHandle handle = metadataWith(ops).getTableHandle(null, "db1", "t1").get();
        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle);

        // WHY: legacy PaimonExternalTable always declared paimon columns nullable (isAllowNull=true)
        // regardless of the field's NOT NULL flag, so nereids cannot fold null-rejecting predicates
        // on a NOT NULL external column that can still read NULL (schema-evolution default-fill). A
        // paimon PK NOT NULL field MUST still surface as nullable to Doris. MUTATION: reverting
        // mapFields to field.type().isNullable() -> the 'id' column becomes isNullable()==false -> red.
        ConnectorColumn id = schema.getColumns().get(0);
        ConnectorColumn val = schema.getColumns().get(1);
        Assertions.assertEquals("id", id.getName());
        Assertions.assertTrue(id.isNullable(),
                "a paimon NOT NULL (PK) column must surface as nullable to Doris (legacy parity)");
        Assertions.assertTrue(val.isNullable());

        // WHY (RC-6 DESC Key parity): legacy PaimonExternalTable/PaimonSysExternalTable built every
        // column with isKey=true (3rd positional Column arg), so DESC shows Key=true for ALL paimon
        // columns (PK and non-PK alike). MUTATION: reverting mapFields to the 5-arg ConnectorColumn ctor
        // (isKey defaults to false) -> both assertions red, and DESC would regress to Key=false.
        Assertions.assertTrue(id.isKey(),
                "every paimon column must report isKey=true for legacy DESC Key parity");
        Assertions.assertTrue(val.isKey(),
                "a non-PK paimon column must also report isKey=true (legacy set isKey=true for all)");
    }

    @Test
    public void getTableSchemaAtSnapshotAlsoForcesNullable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1", rowType("id"), Collections.emptyList(), Collections.singletonList("id"));
        ops.table = table;
        // The historical (at-snapshot) schema's 'id' field is NOT NULL.
        ops.schemaAt = new PaimonCatalogOps.PaimonSchemaSnapshot(
                Collections.singletonList(new DataField(0, "id", DataTypes.INT().notNull())),
                Collections.emptyList(),
                Collections.singletonList("id"));

        ConnectorTableHandle handle = metadataWith(ops).getTableHandle(null, "db1", "t1").get();
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().schemaId(5).build();
        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle, snapshot);

        // WHY: the latest and at-snapshot read paths share mapFields; this pins that the time-travel
        // path also obeys legacy nullable parity and cannot drift from the latest path. MUTATION:
        // reverting mapFields to field.type().isNullable() -> the at-snapshot 'id' becomes
        // non-nullable -> red.
        Assertions.assertTrue(schema.getColumns().get(0).isNullable(),
                "the at-snapshot read path must also force columns nullable (legacy parity)");
    }

    // ---------------------------------------------------------------------
    // no-cache meta-cache: the LATEST schema must be read fresh via schemaManager().latest(),
    // not the CachingCatalog-frozen rowType() (test_paimon_table_meta_cache line 112)
    // ---------------------------------------------------------------------

    @Test
    public void getTableSchemaReadsLatestSchemaNotCachedRowType() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // The handle's transient Table is the CachingCatalog-cached instance whose rowType() is
        // FROZEN at load time (2 columns) — this is what the connector used to read.
        ops.table = new FakePaimonTable(
                "t1", rowType("id", "name"), Collections.emptyList(), Collections.emptyList());
        // After an external ALTER ADD COLUMNS, the schema manager's latest() advances to 3 fields
        // (a NEW schema file, with NO new snapshot). This is the live read the latest path must use.
        ops.latestSchema = Optional.of(new PaimonCatalogOps.PaimonSchemaSnapshot(
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.INT()),
                        new DataField(2, "new_col", DataTypes.INT())),
                Collections.emptyList(),
                Collections.emptyList()));

        ConnectorTableHandle handle = metadataWith(ops).getTableHandle(null, "db1", "t1").get();
        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle);

        // WHY: a paimon CachingCatalog caches the Table object; its rowType() is frozen at load time
        // while schemaManager().latest() reads the schema directory fresh. An external ALTER ADD
        // COLUMNS bumps the schema file (new id) WITHOUT a new snapshot, so the latest snapshot's
        // schemaId stays behind and only schemaManager().latest() sees the 3rd column. The latest
        // path MUST read schemaManager().latest() (legacy PaimonExternalTable parity), not the cached
        // rowType(). This is the no-cache meta-cache regression (test_paimon_table_meta_cache line
        // 112: expected 3 but was 2). MUTATION: reading table.rowType() (the cached 2-col schema) -> red.
        Assertions.assertEquals(3, schema.getColumns().size(),
                "the latest schema path must read schemaManager().latest() (3 cols after external "
                        + "ALTER), not the CachingCatalog-frozen rowType() (2 cols)");
        Assertions.assertEquals("new_col", schema.getColumns().get(2).getName(),
                "the externally-added column must surface via schemaManager().latest()");
    }

    @Test
    public void getTableSchemaKeepsSyntheticRowTypeForSystemTable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        // A system table ($snapshots-like) whose synthetic rowType() is its OWN 2-col schema.
        FakePaimonTable sysTbl = new FakePaimonTable(
                "t1$snapshots", rowType("snapshot_id", "schema_id"),
                Collections.emptyList(), Collections.emptyList());
        // latestSchema would return a DIFFERENT (base-table) 3-col schema; it MUST be ignored for a sys table.
        ops.latestSchema = Optional.of(new PaimonCatalogOps.PaimonSchemaSnapshot(
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.INT()),
                        new DataField(2, "new_col", DataTypes.INT())),
                Collections.emptyList(), Collections.emptyList()));
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable("db1", "t1", "snapshots", false);
        handle.setPaimonTable(sysTbl);

        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle);

        // WHY: system tables ($snapshots/$manifests are NOT DataTable; $ro/$audit_log/$binlog ARE
        // DataTable but their correct DESC is the SYNTHETIC sys rowType(), not the base schema). The
        // latest path MUST keep table.rowType() for ALL sys handles, guarded by isSystemTable().
        // MUTATION: dropping the isSystemTable() guard (always reading latestSchema) -> 3 cols (and
        // a ClassCastException for the non-DataTable sys tables in production) -> red.
        Assertions.assertEquals(2, schema.getColumns().size(),
                "a system table must keep its synthetic rowType(), never schemaManager().latest()");
        Assertions.assertFalse(ops.log.contains("latestSchema"),
                "the latest-schema read must be skipped for a system table");
    }

    @Test
    public void getTableSchemaFallsBackToRowTypeWhenLatestSchemaAbsent() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1", rowType("id", "name"), Collections.emptyList(), Collections.emptyList());
        // latestSchema empty: a non-DataTable (FormatTable) backend or a schema-less table.
        ops.latestSchema = Optional.empty();

        ConnectorTableHandle handle = metadataWith(ops).getTableHandle(null, "db1", "t1").get();
        ConnectorTableSchema schema = metadataWith(ops).getTableSchema(null, handle);

        // WHY: when schemaManager().latest() is unavailable (non-DataTable backend / empty table),
        // the latest path must fall back to table.rowType() rather than crash. MUTATION:
        // unconditionally dereferencing latestSchema().get() -> NoSuchElementException -> red.
        Assertions.assertEquals(2, schema.getColumns().size(),
                "an absent latest schema must fall back to the table's rowType()");
    }

    // ---------------------------------------------------------------------
    // FIX-MAPPING-FLAG-KEYS — type-mapping toggles read the canonical dotted
    // CREATE-CATALOG keys (enable.mapping.varbinary / enable.mapping.timestamp_tz)
    // ---------------------------------------------------------------------

    private static RowType binaryAndLtzRowType() {
        return RowType.builder()
                .field("b", DataTypes.BINARY(16))
                .field("ts_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                .build();
    }

    @Test
    public void getTableSchemaHonorsDottedMappingKeys() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1", binaryAndLtzRowType(), Collections.emptyList(), Collections.emptyList());

        // The user enables both mappings via the canonical DOTTED CREATE-CATALOG keys — the only
        // spelling fe-core ever writes into the catalog property map (CatalogProperty.java:50,52;
        // ExternalCatalog.setDefaultPropsIfMissing). The connector receives that raw map verbatim.
        Map<String, String> props = new java.util.HashMap<>();
        props.put("enable.mapping.varbinary", "true");
        props.put("enable.mapping.timestamp_tz", "true");
        PaimonConnectorMetadata metadata =
                new PaimonConnectorMetadata(ops, props, new RecordingConnectorContext());

        ConnectorTableHandle handle = metadata.getTableHandle(null, "db1", "t1").get();
        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);

        // WHY: when the user enables the mapping at CREATE CATALOG, a Paimon BINARY column must
        // surface as VARBINARY and a TIMESTAMP_WITH_LOCAL_TIME_ZONE column as TIMESTAMPTZ — legacy
        // parity (PaimonExternalTable.java:350 reads the same dotted key and honors it). MUTATION:
        // reverting the connector constants to the underscore spelling (the cutover bug:
        // enable_mapping_binary_as_varbinary / enable_mapping_timestamp_tz) makes getOrDefault miss
        // the dotted keys the map actually carries -> both flags read false -> the column types fall
        // back to STRING / DATETIMEV2 -> red. This closes critic coverage-gap #2.
        Assertions.assertEquals("VARBINARY", schema.getColumns().get(0).getType().getTypeName(),
                "enable.mapping.varbinary=true must map Paimon BINARY to Doris VARBINARY");
        Assertions.assertEquals("TIMESTAMPTZ", schema.getColumns().get(1).getType().getTypeName(),
                "enable.mapping.timestamp_tz=true must map Paimon LTZ to Doris TIMESTAMPTZ");
    }

    @Test
    public void getTableSchemaDefaultsMappingFlagsOff() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1", binaryAndLtzRowType(), Collections.emptyList(), Collections.emptyList());

        // No mapping keys set — the default (legacy-compatible) behavior.
        PaimonConnectorMetadata metadata =
                new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());

        ConnectorTableHandle handle = metadata.getTableHandle(null, "db1", "t1").get();
        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);

        // WHY: with the toggles absent, BINARY must map to STRING and LTZ to DATETIMEV2 (default
        // false), matching legacy. This guards against a fix that accidentally flips the defaults on
        // (e.g. reading the wrong default or inverting the boolean). MUTATION: defaulting either flag
        // to true -> VARBINARY / TIMESTAMPTZ -> red. Green in both the buggy and fixed states (it
        // pins the default, not the key spelling), so it is a regression guard, not the bug-catcher.
        Assertions.assertEquals("STRING", schema.getColumns().get(0).getType().getTypeName(),
                "absent enable.mapping.varbinary must leave Paimon BINARY as STRING (default off)");
        Assertions.assertEquals("DATETIMEV2", schema.getColumns().get(1).getType().getTypeName(),
                "absent enable.mapping.timestamp_tz must leave Paimon LTZ as DATETIMEV2 (default off)");
    }

    @Test
    public void getTableSchemaMarksLtzColumnsWithTimeZoneRegardlessOfMapping() {
        // Legacy parity (test_paimon_catalog_timestamp_tz desc_1/desc_2): PaimonExternalTable.initSchema
        // and PaimonSysExternalTable.buildFullSchema call column.setWithTZExtraInfo() whenever the SOURCE
        // paimon type root is TIMESTAMP_WITH_LOCAL_TIME_ZONE — independent of enable.mapping.timestamp_tz.
        // That marker becomes the DESC "Extra" column = WITH_TIMEZONE (IndexSchemaProcNode reads
        // Column.getExtraInfo()). Because the connector maps an LTZ field to TIMESTAMPTZ when mapping is on
        // but to a plain DATETIMEV2 when off, the marker cannot be recovered from the mapped Doris type
        // alone; mapFields must carry it explicitly via ConnectorColumn.withTimeZone() in BOTH states so
        // fe-core's ConnectorColumnConverter can re-apply setWithTZExtraInfo().
        // MUTATION: dropping the withTimeZone() mark in mapFields -> isWithTimeZone()==false -> the
        // converter never sets the extra info -> DESC Extra goes blank -> red.
        for (Map<String, String> props : Arrays.asList(
                Collections.<String, String>emptyMap(),
                Collections.singletonMap("enable.mapping.timestamp_tz", "true"))) {
            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            ops.table = new FakePaimonTable(
                    "t1", binaryAndLtzRowType(), Collections.emptyList(), Collections.emptyList());
            PaimonConnectorMetadata metadata =
                    new PaimonConnectorMetadata(ops, props, new RecordingConnectorContext());

            ConnectorTableHandle handle = metadata.getTableHandle(null, "db1", "t1").get();
            ConnectorTableSchema schema = metadata.getTableSchema(null, handle);

            Assertions.assertFalse(schema.getColumns().get(0).isWithTimeZone(),
                    "non-LTZ (BINARY) column must not carry the WITH_TIMEZONE marker; mapping props=" + props);
            Assertions.assertTrue(schema.getColumns().get(1).isWithTimeZone(),
                    "Paimon LTZ column must carry the WITH_TIMEZONE marker regardless of mapping; props="
                            + props);
        }
    }
}
