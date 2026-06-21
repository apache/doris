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
        return new IcebergConnectorMetadata(ops, Collections.emptyMap());
    }

    private static IcebergConnectorMetadata metadataWith(
            RecordingIcebergCatalogOps ops, Map<String, String> props) {
        return new IcebergConnectorMetadata(ops, props);
    }

    /** A simple 2-column unpartitioned schema (id required, name optional). */
    private static Schema idNameSchema() {
        return new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
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

        // WHY: nullability follows the Iceberg field optional flag directly (required -> NOT NULL,
        // optional -> nullable). This is the CURRENT production behavior (no force-nullable). MUTATION:
        // inverting isOptional, or forcing all nullable -> red.
        Assertions.assertFalse(cols.get(0).isNullable(),
                "a required Iceberg field must surface as NOT NULL (current behavior)");
        Assertions.assertTrue(cols.get(1).isNullable(),
                "an optional Iceberg field must surface as nullable");

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
    public void getTableSchemaStampsFormatVersionTwoForAnyValidSpec() {
        // NOTE (parity oddity, FROZEN this phase): production computes iceberg.format-version as
        // `spec().specId() >= 0 ? 2 : 1`. Every real PartitionSpec — including the UNPARTITIONED spec —
        // has specId 0 (>= 0), so this ALWAYS stamps "2" and can never produce "1". The format version
        // is a table-level v1/v2 concept that should be read from the table metadata (the
        // `format-version` table property), not derived from the partition spec id. This test PINS the
        // current (oddly-always-2) behavior so the later parity fix (P6-T08/T09) is a deliberate,
        // visible change rather than a silent drift. MUTATION: producing "1" for an unpartitioned table
        // under the current logic -> red.
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = new FakeIcebergTable(
                "t1", idNameSchema(), PartitionSpec.unpartitioned(),
                "s3://bucket/db1/t1", Collections.emptyMap());

        ConnectorTableSchema schema =
                metadataWith(ops).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        Assertions.assertEquals("2", schema.getProperties().get("iceberg.format-version"),
                "current behavior stamps format-version=2 for any spec with specId>=0 (incl. unpartitioned)");
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
    // type-mapping toggles — enable_mapping_varbinary / enable_mapping_timestamp_tz
    // (current underscore-spelled keys; the dotted-key parity question is a later task)
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

        Map<String, String> props = new HashMap<>();
        props.put("enable_mapping_varbinary", "true");
        props.put("enable_mapping_timestamp_tz", "true");

        ConnectorTableSchema schema =
                metadataWith(ops, props).getTableSchema(null, new IcebergTableHandle("db1", "t1"));

        // WHY: with the mapping flags on, an Iceberg BINARY column maps to VARBINARY and a
        // TIMESTAMP-with-zone column maps to TIMESTAMPTZV2 (via IcebergTypeMapping). The metadata layer
        // must read these flags from the catalog props with the current underscore key spelling and
        // thread them to the type mapper. MUTATION: not reading the flags (always default false) ->
        // STRING / DATETIMEV2 -> red.
        Assertions.assertEquals("VARBINARY", schema.getColumns().get(0).getType().getTypeName(),
                "enable_mapping_varbinary=true must map Iceberg BINARY to VARBINARY");
        Assertions.assertEquals("TIMESTAMPTZV2", schema.getColumns().get(1).getType().getTypeName(),
                "enable_mapping_timestamp_tz=true must map Iceberg TIMESTAMP-with-zone to TIMESTAMPTZV2");
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
        // defaulting either flag to true -> VARBINARY / TIMESTAMPTZV2 -> red.
        Assertions.assertEquals("STRING", schema.getColumns().get(0).getType().getTypeName(),
                "absent enable_mapping_varbinary must leave Iceberg BINARY as STRING (default off)");
        Assertions.assertEquals("DATETIMEV2", schema.getColumns().get(1).getType().getTypeName(),
                "absent enable_mapping_timestamp_tz must leave Iceberg TIMESTAMP-with-zone as DATETIMEV2");
    }
}
