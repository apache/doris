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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo;

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

    private static List<String> names(List<Column> schema) {
        return schema.stream().map(Column::getName).collect(Collectors.toList());
    }

    private static IcebergTableQueryInfo snapshot(Table table, String version) throws Exception {
        return IcebergUtils.getQuerySpecSnapshot(table,
                java.util.Optional.of(TableSnapshot.versionOf(version)), java.util.Optional.empty());
    }

    private static List<Column> schema(Table table, IcebergTableQueryInfo snapshot) {
        return IcebergUtils.appendRowLineageColumnsForV3(IcebergUtils.parseSchemaForDisplay(
                table.schemas().get(snapshot.getSchemaId()), false, false), table);
    }

    @Test
    public void branchUsesLatestSchemaWhileTagAtTheSameSnapshotUsesHistoricalSchema() throws Exception {
        Table table = newTable();
        long snapshotId = append(table, "initial");
        int originalSchemaId = table.schema().schemaId();
        table.manageSnapshots().createTag("audit_tag", snapshotId)
                .createBranch("audit_branch", snapshotId).commit();
        table.updateSchema().renameColumn("original_name", "renamed").commit();

        IcebergTableQueryInfo branch = snapshot(table, "audit_branch");
        IcebergTableQueryInfo tag = snapshot(table, "audit_tag");

        // Iceberg branches follow the table schema, whereas tags retain the referenced snapshot's schema.
        Assertions.assertEquals(snapshotId, branch.getSnapshotId());
        Assertions.assertEquals(snapshotId, tag.getSnapshotId());
        Assertions.assertEquals(table.schema().schemaId(), branch.getSchemaId());
        Assertions.assertEquals(originalSchemaId, tag.getSchemaId());
        Assertions.assertEquals(Arrays.asList("id", "renamed"),
                names(schema(table, branch)));
        Assertions.assertEquals(Arrays.asList("id", "original_name"),
                names(schema(table, tag)));
    }

    @Test
    public void explicitSnapshotRetainsItsSchemaAfterRollbackChangesTheCurrentSnapshot() throws Exception {
        Table table = newTable();
        long initial = append(table, "initial");
        table.updateSchema().renameColumn("original_name", "renamed").commit();
        long newer = append(table, "newer");
        int newerSchema = table.schema().schemaId();
        table.manageSnapshots().rollbackTo(initial).commit();

        IcebergTableQueryInfo historical = snapshot(table, String.valueOf(newer));

        Assertions.assertEquals(initial, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(newer, historical.getSnapshotId());
        Assertions.assertEquals(newerSchema, historical.getSchemaId());
        Assertions.assertEquals(Arrays.asList("id", "renamed"),
                names(schema(table, historical)));
    }

    @Test
    public void historicalSchemaKeepsLineageColumnsAfterFormatUpgradeWithoutAnotherSnapshot() throws Exception {
        Table table = newTable();
        long initial = append(table, "initial");
        int originalSchema = table.schema().schemaId();
        table.updateSchema().renameColumn("original_name", "renamed").commit();
        table.updateProperties().set("format-version", "3").commit();
        IcebergTableQueryInfo historical = snapshot(table, String.valueOf(initial));

        Assertions.assertEquals(originalSchema, historical.getSchemaId());
        List<Column> columns = schema(table, historical);
        Assertions.assertEquals(Arrays.asList("id", "original_name",
                "_row_id", "_last_updated_sequence_number"),
                columns.stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertFalse(columns.get(2).isVisible());
        Assertions.assertFalse(columns.get(3).isVisible());
    }

    @Test
    public void metadataTablesNeverAcquireDataFileOrLineageColumnsFromAV3Base() throws Exception {
        Table table = newTable();
        table.updateProperties().set("format-version", "3").commit();
        append(table, "initial");

        for (org.apache.iceberg.MetadataTableType type : org.apache.iceberg.MetadataTableType.values()) {
            String systemTable = type.name();
            Table system = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(table, type);
            List<String> columnNames = names(IcebergUtils.parseSchema(system.schema(), false, false));
            Assertions.assertFalse(columnNames.contains("_file"), systemTable);
            Assertions.assertFalse(columnNames.contains("_pos"), systemTable);
            Assertions.assertFalse(columnNames.contains("_row_id"), systemTable);
            Assertions.assertFalse(columnNames.contains("_last_updated_sequence_number"), systemTable);
        }
    }
}
