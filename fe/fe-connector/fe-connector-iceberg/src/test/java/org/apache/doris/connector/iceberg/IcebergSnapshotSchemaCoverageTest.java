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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IcebergSnapshotSchemaCoverageTest {
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db1", "history");

    private static Table newTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog.createTable(TableIdentifier.of("db1", "history"), new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "original_name", Types.StringType.get())));
    }

    private static long append(Table table, String name) {
        table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://bucket/history/" + name + ".parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        return table.currentSnapshot().snapshotId();
    }

    private static IcebergConnectorMetadata metadata(Table table) {
        RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
        ops.table = table;
        return new IcebergConnectorMetadata(ops, IcebergCatalogProperties.of(Collections.emptyMap()),
                new RecordingConnectorContext());
    }

    private static List<String> names(ConnectorTableSchema schema) {
        return schema.getColumns().stream().map(ConnectorColumn::getName).collect(Collectors.toList());
    }

    @Test
    public void branchUsesLatestSchemaWhileTagAtTheSameSnapshotUsesHistoricalSchema() {
        Table table = newTable();
        long snapshotId = append(table, "initial");
        int originalSchemaId = table.schema().schemaId();
        table.manageSnapshots().createTag("audit_tag", snapshotId)
                .createBranch("audit_branch", snapshotId).commit();
        table.updateSchema().renameColumn("original_name", "renamed").commit();
        IcebergConnectorMetadata metadata = metadata(table);

        ConnectorMvccSnapshot branch = metadata.resolveTimeTravel(null, HANDLE,
                ConnectorTimeTravelSpec.branch("audit_branch")).get();
        ConnectorMvccSnapshot tag = metadata.resolveTimeTravel(null, HANDLE,
                ConnectorTimeTravelSpec.tag("audit_tag")).get();

        // Iceberg branches follow the table schema, whereas tags retain the referenced snapshot's schema.
        Assertions.assertEquals(snapshotId, branch.getSnapshotId());
        Assertions.assertEquals(snapshotId, tag.getSnapshotId());
        Assertions.assertEquals(table.schema().schemaId(), branch.getSchemaId());
        Assertions.assertEquals(originalSchemaId, tag.getSchemaId());
        Assertions.assertEquals(Arrays.asList("id", "renamed", "_file", "_pos"),
                names(metadata.getTableSchema(null, HANDLE, branch)));
        Assertions.assertEquals(Arrays.asList("id", "original_name", "_file", "_pos"),
                names(metadata.getTableSchema(null, HANDLE, tag)));
    }

    @Test
    public void explicitSnapshotRetainsItsSchemaAfterRollbackChangesTheCurrentSnapshot() {
        Table table = newTable();
        long initial = append(table, "initial");
        table.updateSchema().renameColumn("original_name", "renamed").commit();
        long newer = append(table, "newer");
        int newerSchema = table.schema().schemaId();
        table.manageSnapshots().rollbackTo(initial).commit();
        IcebergConnectorMetadata metadata = metadata(table);

        ConnectorMvccSnapshot historical = metadata.resolveTimeTravel(null, HANDLE,
                ConnectorTimeTravelSpec.snapshotId(String.valueOf(newer))).get();

        Assertions.assertEquals(initial, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(newer, historical.getSnapshotId());
        Assertions.assertEquals(newerSchema, historical.getSchemaId());
        Assertions.assertEquals(Arrays.asList("id", "renamed", "_file", "_pos"),
                names(metadata.getTableSchema(null, HANDLE, historical)));
    }

    @Test
    public void historicalSchemaKeepsLineageColumnsAfterFormatUpgradeWithoutAnotherSnapshot() {
        Table table = newTable();
        long initial = append(table, "initial");
        int originalSchema = table.schema().schemaId();
        table.updateSchema().renameColumn("original_name", "renamed").commit();
        table.updateProperties().set("format-version", "3").commit();
        IcebergConnectorMetadata metadata = metadata(table);
        ConnectorMvccSnapshot historical = metadata.resolveTimeTravel(null, HANDLE,
                ConnectorTimeTravelSpec.snapshotId(String.valueOf(initial))).get();

        Assertions.assertEquals(originalSchema, historical.getSchemaId());
        List<ConnectorColumn> columns = metadata.getTableSchema(null, HANDLE, historical).getColumns();
        Assertions.assertEquals(Arrays.asList("id", "original_name", "_file", "_pos",
                "_row_id", "_last_updated_sequence_number"),
                columns.stream().map(ConnectorColumn::getName).collect(Collectors.toList()));
        Assertions.assertFalse(columns.get(4).isVisible());
        Assertions.assertFalse(columns.get(5).isVisible());
        Assertions.assertTrue(columns.get(4).isReservedPassthrough());
        Assertions.assertTrue(columns.get(5).isReservedPassthrough());
    }

    @Test
    public void metadataTablesNeverAcquireDataFileOrLineageColumnsFromAV3Base() {
        Table table = newTable();
        table.updateProperties().set("format-version", "3").commit();
        append(table, "initial");
        IcebergConnectorMetadata metadata = metadata(table);

        for (String systemTable : metadata.listSupportedSysTables(null, HANDLE)) {
            ConnectorTableHandle system = metadata.getSysTableHandle(null, HANDLE, systemTable).get();
            List<String> columnNames = names(metadata.getTableSchema(null, system));
            Assertions.assertFalse(columnNames.contains("_file"), systemTable);
            Assertions.assertFalse(columnNames.contains("_pos"), systemTable);
            Assertions.assertFalse(columnNames.contains("_row_id"), systemTable);
            Assertions.assertFalse(columnNames.contains("_last_updated_sequence_number"), systemTable);
        }
    }
}
