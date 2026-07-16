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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests {@link HiveConnectorMetadata#getTableSchema} partition-column emission (§4.2 read-side SPI).
 *
 * <p>WHY: the generic fe-core consumer {@code PluginDrivenExternalTable.toSchemaCacheValue} derives a table's
 * partition columns SOLELY from a {@code partition_columns} CSV-of-raw-names table-property (the same key
 * paimon/iceberg/maxcompute emit). If the hive connector appends partition columns to the schema but omits
 * that property, every partitioned hive/hudi table reads as unpartitioned (wrong pruning / row count, MTMV
 * breakage). These assertions pin: the property carries the raw partition-key names in declaration order; a
 * {@code string} PARTITION column is widened to {@code varchar(65533)} for legacy
 * {@code HMSExternalTable.initPartitionColumns} parity while a {@code string} DATA column and a non-string
 * partition column are left untouched; partition columns come after data columns; an unpartitioned table
 * emits no property.</p>
 */
public class HiveConnectorMetadataSchemaTest {

    private static final String PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String ORC_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private static final String TEXT_INPUT_FORMAT =
            "org.apache.hadoop.mapred.TextInputFormat";
    private static final String OPEN_CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
    private static final String LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

    private ConnectorTableSchema schemaOf(HmsTableInfo tableInfo) {
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(tableInfo), Collections.emptyMap(), new FakeConnectorContext());
        HiveTableHandle handle = new HiveTableHandle.Builder(
                tableInfo.getDbName(), tableInfo.getTableName(), HiveTableType.HIVE).build();
        return metadata.getTableSchema(null, handle);
    }

    private static ConnectorColumn col(String name, String typeName) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), null, true, null);
    }

    private static HmsTableInfo.Builder partitionedTable() {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .inputFormat(PARQUET_INPUT_FORMAT)
                .columns(Arrays.asList(col("id", "INT"), col("name", "STRING")))
                .partitionKeys(Arrays.asList(col("year", "INT"), col("region", "STRING")))
                .parameters(Collections.emptyMap());
    }

    private static HmsTableInfo.Builder unpartitionedTable(String inputFormat) {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .inputFormat(inputFormat)
                .columns(Arrays.asList(col("id", "INT"), col("name", "STRING")))
                .partitionKeys(Collections.emptyList())
                .parameters(Collections.emptyMap());
    }

    private static ConnectorColumn arrayCol(String name, String elementTypeName) {
        return new ConnectorColumn(name,
                ConnectorType.arrayOf(ConnectorType.of(elementTypeName)), null, true, null);
    }

    /**
     * A delimited-text table with DECLARED TYPED data columns (int/datetime/boolean + a complex array) and a
     * non-string DATE partition key, parameterized by serde lib. Under OpenCSVSerde the reader serves every data
     * column as plain string; under any other serde the declared types stand.
     */
    private static HmsTableInfo.Builder csvTypedTable(String serdeLib) {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .inputFormat(TEXT_INPUT_FORMAT)
                .serializationLib(serdeLib)
                .columns(Arrays.asList(col("id", "INT"), col("ts", "DATETIMEV2"),
                        col("active", "BOOLEAN"), arrayCol("arr_col", "INT")))
                .partitionKeys(Collections.singletonList(col("dt", "DATEV2")))
                .parameters(Collections.emptyMap());
    }

    private static String perTableCapabilities(ConnectorTableSchema schema) {
        return schema.getProperties().get(ConnectorTableSchema.PER_TABLE_CAPABILITIES_KEY);
    }

    /** Membership test over the CSV marker (order-independent, robust as more per-table capabilities are added). */
    private static boolean hasCapability(ConnectorTableSchema schema, ConnectorCapability capability) {
        String csv = perTableCapabilities(schema);
        if (csv == null) {
            return false;
        }
        for (String name : csv.split(",")) {
            if (name.trim().equals(capability.name())) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testPartitionColumnsPropertyEmittedWithRawNamesInOrder() {
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        Assertions.assertEquals("year,region",
                schema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
    }

    @Test
    public void testStringPartitionColumnWidenedToVarchar65533() {
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        ConnectorColumn region = columnByName(schema, "region");
        Assertions.assertEquals("VARCHAR", region.getType().getTypeName());
        Assertions.assertEquals(65533, region.getType().getPrecision());
    }

    @Test
    public void testNonStringPartitionColumnKeepsDeclaredType() {
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        Assertions.assertEquals("INT", columnByName(schema, "year").getType().getTypeName());
    }

    @Test
    public void testStringDataColumnIsNotWidened() {
        // Only PARTITION string columns are coerced; a plain data column of type string stays STRING.
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        Assertions.assertEquals("STRING", columnByName(schema, "name").getType().getTypeName());
    }

    @Test
    public void testOpenCsvSerdeFlattensEveryDataColumnToString() {
        // WHY: OpenCSVSerde reads the file as PLAIN text — its deserializer reports every top-level column as
        // string, so a declared int/datetime/boolean, and even a complex array/map/struct, is served verbatim as
        // a string and never parsed. Legacy resolved this via the metastore get_schema RPC (all-string); the SPI
        // reads raw sd.getCols() types, so the connector must reproduce the all-string RESULT. MUTATION: emitting
        // the declared typed columns flips TRUE vs 'true', raw datetime vs ISO, empty-string vs NULL — the exact
        // regression in test_open_csv_serde / test_hive_serde_prop.
        ConnectorTableSchema schema = schemaOf(csvTypedTable(OPEN_CSV_SERDE).build());
        for (String dataCol : Arrays.asList("id", "ts", "active", "arr_col")) {
            Assertions.assertEquals("STRING", columnByName(schema, dataCol).getType().getTypeName(),
                    "OpenCSV data column " + dataCol + " must be flattened to STRING");
        }
    }

    @Test
    public void testOpenCsvSerdePartitionKeyKeepsDeclaredType() {
        // Partition keys are appended by hive AFTER the deserializer, so they keep their declared types: a DATE
        // partition key stays a date, NOT string-forced. Guards against flattening the whole schema.
        ConnectorTableSchema schema = schemaOf(csvTypedTable(OPEN_CSV_SERDE).build());
        Assertions.assertEquals("DATEV2", columnByName(schema, "dt").getType().getTypeName());
    }

    @Test
    public void testNonOpenCsvSerdeKeepsDeclaredDataTypes() {
        // The flatten is OpenCSV-gated: an identical table under LazySimpleSerDe (plain text, typed columns) keeps
        // its declared int/datetime types — the LazySimple half of test_hive_serde_prop, which must stay typed.
        // MUTATION: an ungated flatten would break every typed text/parquet/orc table.
        ConnectorTableSchema schema = schemaOf(csvTypedTable(LAZY_SIMPLE_SERDE).build());
        Assertions.assertEquals("INT", columnByName(schema, "id").getType().getTypeName());
        Assertions.assertEquals("DATETIMEV2", columnByName(schema, "ts").getType().getTypeName());
    }

    @Test
    public void testOpenCsvViewColumnsAreNotFlattened() {
        // buildColumns also serves getViewDefinition; a view's logical columns are never an OpenCSV data payload,
        // so the isView guard keeps them at declared types even when the SD carries an OpenCSV serde lib.
        ConnectorTableSchema schema = schemaOf(
                csvTypedTable(OPEN_CSV_SERDE).tableType("VIRTUAL_VIEW").build());
        Assertions.assertEquals("INT", columnByName(schema, "id").getType().getTypeName());
    }

    @Test
    public void testPartitionColumnsComeAfterDataColumns() {
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        List<String> names = schema.getColumns().stream()
                .map(ConnectorColumn::getName).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("id", "name", "year", "region"), names);
    }

    @Test
    public void testUnpartitionedTableEmitsNoPartitionColumnsProperty() {
        HmsTableInfo tableInfo = HmsTableInfo.builder()
                .dbName("db").tableName("t")
                .inputFormat(PARQUET_INPUT_FORMAT)
                .columns(Arrays.asList(col("id", "INT"), col("name", "STRING")))
                .partitionKeys(Collections.emptyList())
                .parameters(Collections.emptyMap())
                .build();
        ConnectorTableSchema schema = schemaOf(tableInfo);
        Assertions.assertFalse(schema.getProperties().containsKey(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
    }

    @Test
    public void testUserPartitionColumnsParameterCannotCollideWithReservedKey() {
        // A user TBLPROPERTY literally named "partition_columns" (bare) can NEVER be mistaken for the reserved
        // partition marker, which is namespaced under __internal. (ConnectorTableSchema.PARTITION_COLUMNS_KEY).
        // On a NON-partitioned hive table: the connector emits NO reserved key -> fe-core sees no partition
        // marker -> the table stays unpartitioned; and the user's bare property flows through unchanged (no
        // silent strip). MUTATION: reverting the reserved key to the bare "partition_columns" -> the user
        // value would be read as the partition CSV -> the assertNull below fails -> red.
        Map<String, String> params = new HashMap<>();
        params.put("partition_columns", "id");   // a real column name, as a plain user property
        HmsTableInfo tableInfo = unpartitionedTable(PARQUET_INPUT_FORMAT).parameters(params).build();
        ConnectorTableSchema schema = schemaOf(tableInfo);
        Assertions.assertNull(schema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY),
                "an unpartitioned hive table must emit no reserved partition marker");
        Assertions.assertEquals("id", schema.getProperties().get("partition_columns"),
                "the user's bare partition_columns property must flow through unchanged (no collision, no strip)");
    }

    @Test
    public void testPartitionedTableReservedKeyCoexistsWithCollidingUserParameter() {
        // A genuinely partitioned table whose parameters ALSO carry a colliding bare "partition_columns"=id:
        // the reserved key (__internal.partition_columns) carries the CONNECTOR's own partition-key CSV
        // (year,region), and the user's bare property coexists independently — the two live in different
        // namespaces and never overwrite each other. MUTATION: bare reserved key -> the user value would
        // overwrite (or be overwritten) -> one of the assertions fails -> red.
        Map<String, String> params = new HashMap<>();
        params.put("partition_columns", "id");
        ConnectorTableSchema schema = schemaOf(partitionedTable().parameters(params).build());
        Assertions.assertEquals("year,region",
                schema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY),
                "the reserved key must carry the connector's partition-key CSV");
        Assertions.assertEquals("id", schema.getProperties().get("partition_columns"),
                "the user's bare property coexists, untouched");
    }

    @Test
    public void testTopNLazyCapabilityMarkerEmittedForParquetAndOrc() {
        // WHY: Top-N lazy materialize is orc/parquet-only in legacy hive (HMSExternalTable.supportedHiveTopNLazyTable).
        // The connector-wide SUPPORTS_TOPN_LAZY_MATERIALIZE cannot express that for a heterogeneous hive catalog, so
        // the connector emits it per-table; fe-core (PluginDrivenExternalTable.supportsTopNLazyMaterialize) enables the
        // optimization only for tables carrying this marker. MUTATION: not emitting it -> orc/parquet hive tables lose
        // Top-N lazy materialization. Membership (not exact CSV) because the same marker also carries auto-analyze.
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(PARQUET_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE));
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(ORC_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE));
    }

    @Test
    public void testTopNLazyCapabilityMarkerAbsentForText() {
        // A text hive table is not Top-N-lazy eligible in legacy; emitting the Top-N marker would over-enable it.
        // (Auto-analyze IS emitted for it — legacy analyzed any hive format — asserted separately below.)
        Assertions.assertFalse(hasCapability(schemaOf(unpartitionedTable(TEXT_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE));
    }

    @Test
    public void testColumnAutoAnalyzeMarkerEmittedForEveryPlainHiveFormat() {
        // WHY: legacy StatisticsUtil.supportAutoAnalyze admitted EVERY plain-hive (dlaType==HIVE) table into
        // background per-column auto-analyze regardless of file format. Emitting it per-table (not connector-wide)
        // is what lets fe-core exclude hudi-on-HMS (which legacy excluded) while admitting plain-hive. Unlike Top-N,
        // it has NO orc/parquet restriction. MUTATION: gating it on input format -> text/csv/json hive tables
        // silently drop out of auto-analyze.
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(PARQUET_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE));
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(ORC_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE));
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(TEXT_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE));
    }

    @Test
    public void testColumnAutoAnalyzeMarkerAbsentForView() {
        // A view has nothing to analyze; excluded like Top-N (isView guard before the format check).
        ConnectorTableSchema schema = schemaOf(
                unpartitionedTable(PARQUET_INPUT_FORMAT).tableType("VIRTUAL_VIEW").build());
        Assertions.assertFalse(hasCapability(schema, ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE));
    }

    @Test
    public void testSampleAnalyzeMarkerEmittedForEveryPlainHiveFormat() {
        // Legacy AnalysisManager.canSample gated on dlaType==HIVE (any file format). Emit SUPPORTS_SAMPLE_ANALYZE
        // per-table for every plain-hive table so fe-core admits ANALYZE ... WITH SAMPLE while excluding
        // iceberg/hudi-on-HMS. Like auto-analyze there is NO orc/parquet restriction. MUTATION: gating on input
        // format -> text/csv/json hive tables silently lose sample.
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(PARQUET_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE));
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(ORC_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE));
        Assertions.assertTrue(hasCapability(schemaOf(unpartitionedTable(TEXT_INPUT_FORMAT).build()),
                ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE));
    }

    @Test
    public void testSampleAnalyzeMarkerAbsentForView() {
        // A view is not sampled (legacy canSample excluded it via dlaType); excluded before the format check.
        ConnectorTableSchema schema = schemaOf(
                unpartitionedTable(PARQUET_INPUT_FORMAT).tableType("VIRTUAL_VIEW").build());
        Assertions.assertFalse(hasCapability(schema, ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE));
    }

    @Test
    public void testDistributionColumnsEmittedRawForBucketedTable() {
        // Bucketing columns are emitted RAW (fe-core lowercases, mirroring legacy getDistributionColumnNames);
        // only a bucketed table carries the marker. MUTATION: not emitting -> a flipped bucketed hive table loses
        // the linear NDV estimator in sampled analyze.
        ConnectorTableSchema schema = schemaOf(
                unpartitionedTable(PARQUET_INPUT_FORMAT).bucketCols(Arrays.asList("Id", "region")).build());
        Assertions.assertEquals("Id,region",
                schema.getProperties().get(ConnectorTableSchema.DISTRIBUTION_COLUMNS_KEY));
    }

    @Test
    public void testDistributionColumnsAbsentForNonBucketedTable() {
        Assertions.assertNull(schemaOf(unpartitionedTable(PARQUET_INPUT_FORMAT).build())
                .getProperties().get(ConnectorTableSchema.DISTRIBUTION_COLUMNS_KEY));
    }

    @Test
    public void testTopNLazyCapabilityMarkerAbsentForView() {
        // A view is excluded even when its SD carries a parquet input format, mirroring legacy
        // supportedHiveTopNLazyTable which returns false for a view BEFORE the format check.
        ConnectorTableSchema schema = schemaOf(
                unpartitionedTable(PARQUET_INPUT_FORMAT).tableType("VIRTUAL_VIEW").build());
        Assertions.assertNull(perTableCapabilities(schema));
    }

    @Test
    public void testTopNLazyCapabilityMarkerAbsentForIcebergOnHms() {
        // An iceberg-on-HMS table (table_type=ICEBERG) is served by the iceberg connector after the cutover; the
        // hive connector must NOT claim Top-N for it even though its data files are parquet (detect() != HIVE).
        ConnectorTableSchema schema = schemaOf(unpartitionedTable(PARQUET_INPUT_FORMAT)
                .parameters(Collections.singletonMap("table_type", "ICEBERG")).build());
        Assertions.assertNull(perTableCapabilities(schema));
    }

    @Test
    public void testBuildTableDescriptorIsHiveTable() {
        // Without the override fe-core falls back to a generic SCHEMA_TABLE descriptor; pin that a hive table
        // produces a HIVE_TABLE descriptor carrying a THiveTable with the db/table names and column count.
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(partitionedTable().build()), Collections.emptyMap(), new FakeConnectorContext());
        TTableDescriptor desc = metadata.buildTableDescriptor(null, 42L, "t", "db", "t", 4, 7L);
        Assertions.assertEquals(TTableType.HIVE_TABLE, desc.getTableType());
        Assertions.assertEquals(4, desc.getNumCols());
        Assertions.assertNotNull(desc.getHiveTable());
        Assertions.assertEquals("db", desc.getHiveTable().getDbName());
        Assertions.assertEquals("t", desc.getHiveTable().getTableName());
    }

    private static ConnectorColumn columnByName(ConnectorTableSchema schema, String name) {
        return schema.getColumns().stream()
                .filter(c -> c.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("column not found: " + name));
    }

    /**
     * Minimal {@link HmsClient} double: {@code getTable} echoes the prebuilt {@link HmsTableInfo};
     * {@code getDefaultColumnValues} returns none. The rest fail loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final HmsTableInfo tableInfo;

        FakeHmsClient(HmsTableInfo tableInfo) {
            this.tableInfo = tableInfo;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return tableInfo;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
