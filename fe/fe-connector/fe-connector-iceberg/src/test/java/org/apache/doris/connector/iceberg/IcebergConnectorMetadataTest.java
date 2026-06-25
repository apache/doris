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
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    // ---------------------------------------------------------------------
    // write capabilities (row-level DML dispatch)
    // ---------------------------------------------------------------------

    @Test
    public void declaresDeleteAndMergeCapabilities() {
        // WHY: iceberg is the first connector to support row-level DELETE/MERGE. The generic
        // RowLevelDmlCommand shell routes to iceberg's plan synthesis by querying these capabilities (at the
        // P6.6 cutover). MUTATION: returning the default false (dropping the override) -> the shell would
        // never dispatch iceberg DELETE/MERGE -> red.
        IcebergConnectorMetadata metadata = metadataWith(new RecordingIcebergCatalogOps());
        Assertions.assertTrue(metadata.supportsDelete(), "iceberg must declare DELETE support");
        Assertions.assertTrue(metadata.supportsMerge(), "iceberg must declare MERGE support");
    }

    @Test
    public void declaresInsertOverwriteCapability() {
        // WHY: iceberg's write path fully implements OVERWRITE (the overwrite flag rides the write handle
        // into IcebergWritePlanProvider, which promotes INSERT->OVERWRITE, and IcebergConnectorTransaction
        // maps it to ReplacePartitions/OverwriteFiles at commit). Post-cutover an iceberg table is a
        // PluginDrivenExternalTable, so InsertOverwriteTableCommand.allowInsertOverwrite admits it ONLY when
        // the connector declares this capability; the SPI default false would fail the command up front.
        // MUTATION: dropping the override (default false) -> allowInsertOverwrite rejects iceberg INSERT
        // OVERWRITE post-flip -> red.
        IcebergConnectorMetadata metadata = metadataWith(new RecordingIcebergCatalogOps());
        Assertions.assertTrue(metadata.supportsInsertOverwrite(),
                "iceberg must declare INSERT OVERWRITE support");
    }

    @Test
    public void declaresWriteBranchCapability() {
        // WHY: iceberg's write path threads a target branch (INSERT INTO t@branch) onto the write handle,
        // and IcebergConnectorTransaction.beginWrite validates+commits to it. Post-cutover an iceberg table
        // is a PluginDrivenExternalTable, so the generic INSERT @branch guard admits it ONLY when the
        // connector declares this capability; the SPI default false would reject the branch up front (and
        // dropping the guard entirely would silently write to the default ref). MUTATION: dropping the
        // override (default false) -> @branch INSERT rejected post-flip -> red.
        IcebergConnectorMetadata metadata = metadataWith(new RecordingIcebergCatalogOps());
        Assertions.assertTrue(metadata.supportsWriteBranch(),
                "iceberg must declare write-branch support");
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
        // keys like write.format.default), and the table location must be surfaced under "location".
        // MUTATION: dropping the table.properties() copy, or not emitting location -> red.
        Assertions.assertEquals("parquet", props.get("write.format.default"));
        Assertions.assertEquals("custom-value", props.get("custom.key"));
        Assertions.assertEquals("s3://bucket/db1/t1", props.get("location"));
    }

    @Test
    public void getTableSchemaEmitsPartitionSpecOnlyWhenPartitioned() {
        // Unpartitioned: no iceberg.partition-spec key.
        RecordingIcebergCatalogOps unpartOps = new RecordingIcebergCatalogOps();
        unpartOps.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());
        ConnectorTableSchema unpartSchema =
                metadataWith(unpartOps).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: an unpartitioned table must NOT advertise a partition spec (the production guard is
        // `!spec.isUnpartitioned()`). MUTATION: always emitting iceberg.partition-spec -> red.
        Assertions.assertNull(unpartSchema.getProperties().get("iceberg.partition-spec"),
                "an unpartitioned table must not carry an iceberg.partition-spec property");

        // Partitioned (identity on name): the spec string is surfaced.
        Schema schema = idNameSchema();
        PartitionSpec partSpec = PartitionSpec.builderFor(schema).identity("name").build();
        RecordingIcebergCatalogOps partOps = new RecordingIcebergCatalogOps();
        partOps.table = new FakeIcebergTable(
                "t2", schema, partSpec, "s3://bucket/db1/t2", Collections.emptyMap());
        ConnectorTableSchema partSchema =
                metadataWith(partOps).getTableSchema(null, new IcebergTableHandle("db1", "t2"));

        // WHY: a partitioned table must expose its spec string (the FE renders partition info from it).
        // MUTATION: dropping the partitioned branch -> the key is absent -> red.
        Assertions.assertEquals(partSpec.toString(),
                partSchema.getProperties().get("iceberg.partition-spec"),
                "a partitioned table must surface its partition spec string");
    }

    @Test
    public void getTableSchemaReadsFormatVersionFromTableProperty() {
        // WHY (T09 parity fix): legacy IcebergUtils.getFormatVersion reads the REAL table format version
        // — from BaseTable.operations().current().formatVersion(), else from the `format-version` table
        // property — NOT from the partition spec id. A FakeIcebergTable is not a BaseTable, so the
        // property branch is exercised: a v1 table must surface format-version=1. MUTATION: the old
        // `spec().specId() >= 0 ? 2 : 1` (always "2"), or ignoring the property -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        Map<String, String> props = new HashMap<>();
        props.put("format-version", "1");
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", props);

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals("1", schema.getProperties().get("iceberg.format-version"),
                "format-version must be read from the table's `format-version` property, not the spec id");
    }

    @Test
    public void getTableSchemaDefaultsFormatVersionToTwoWhenAbsent() {
        // WHY: legacy getFormatVersion defaults to 2 when the table is neither a BaseTable nor carries a
        // `format-version` property. An unpartitioned table with no format-version property must default
        // to "2" (the legacy default), but via the DEFAULT — not via the old spec-id quirk. MUTATION:
        // defaulting to "1", or reviving the spec-id derivation -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals("2", schema.getProperties().get("iceberg.format-version"),
                "an absent format-version property must default to 2 (legacy getFormatVersion default)");
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
        // null)`, so the "location" key must be ABSENT rather than mapped to null. MUTATION: removing
        // the null guard -> a null-valued location entry -> red.
        Assertions.assertFalse(schema.getProperties().containsKey("location"),
                "a null table location must not produce a location property");
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
        // the same. Each of the 5 read entry points must wrap exactly one executeAuthenticated call.
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
        md.getTableHandle(null, "db1", "t1");
        md.getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals(5, ctx.authCount,
                "each of the 5 remote reads must wrap exactly one executeAuthenticated call");
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
        Assertions.assertThrows(RuntimeException.class, () -> md.getTableHandle(null, "db1", "t1"));
        Assertions.assertThrows(RuntimeException.class,
                () -> md.getTableSchema(null, new IcebergTableHandle("db1", "t1")));

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
