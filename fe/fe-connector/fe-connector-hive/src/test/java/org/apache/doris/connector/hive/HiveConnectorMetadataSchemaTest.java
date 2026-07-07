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

    @Test
    public void testPartitionColumnsPropertyEmittedWithRawNamesInOrder() {
        ConnectorTableSchema schema = schemaOf(partitionedTable().build());
        Assertions.assertEquals("year,region",
                schema.getProperties().get("partition_columns"));
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
        Assertions.assertFalse(schema.getProperties().containsKey("partition_columns"));
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
