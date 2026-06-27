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
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorSortField;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * End-to-end seam tests for the B1 DDL methods on {@link CatalogBackedIcebergCatalogOps}, exercised against a
 * REAL iceberg {@link InMemoryCatalog} (no Mockito). Proves the thin delegations create/drop real namespaces +
 * tables and that the location helpers read back what the catalog persisted.
 */
public class CatalogBackedIcebergCatalogOpsDdlTest {

    private InMemoryCatalog catalog;
    private CatalogBackedIcebergCatalogOps ops;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ops = new CatalogBackedIcebergCatalogOps(catalog);
    }

    @AfterEach
    public void tearDown() throws Exception {
        catalog.close();
    }

    private static Schema schema() {
        return IcebergSchemaBuilder.buildSchema(Arrays.asList(
                new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", true, null, false),
                new ConnectorColumn("name", ConnectorType.of("VARCHAR", 50, 0), "", true, null, false)));
    }

    @Test
    public void testCreateAndDropDatabase() {
        ops.createDatabase("db1", Collections.emptyMap());
        Assertions.assertTrue(ops.databaseExists("db1"));
        Assertions.assertTrue(ops.listDatabaseNames().contains("db1"));

        ops.dropDatabase("db1");
        Assertions.assertFalse(ops.databaseExists("db1"));
    }

    @Test
    public void testLoadNamespaceLocationReadsBackProperty() {
        ops.createDatabase("db1", Collections.singletonMap("location", "s3://wh/db1"));
        Optional<String> location = ops.loadNamespaceLocation("db1");
        Assertions.assertTrue(location.isPresent());
        Assertions.assertEquals("s3://wh/db1", location.get());
    }

    @Test
    public void testLoadNamespaceLocationAbsentWhenUnset() {
        ops.createDatabase("db1", Collections.emptyMap());
        Assertions.assertFalse(ops.loadNamespaceLocation("db1").isPresent());
    }

    @Test
    public void testCreateAndDropTable() {
        ops.createDatabase("db1", Collections.emptyMap());
        Map<String, String> props = IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null, props);

        Assertions.assertTrue(ops.tableExists("db1", "t1"));
        // The created table carries our columns + the MOR defaults applied by IcebergSchemaBuilder.
        Assertions.assertEquals(Type.TypeID.LONG, ops.loadTable("db1", "t1").schema().findField("id").type().typeId());
        Assertions.assertEquals("merge-on-read", ops.loadTable("db1", "t1").properties().get("write.delete.mode"));
        Assertions.assertTrue(ops.loadTableLocation("db1", "t1").isPresent());

        ops.dropTable("db1", "t1", true);
        Assertions.assertFalse(ops.tableExists("db1", "t1"));
    }

    @Test
    public void testCreateTableWithSortOrder() {
        ops.createDatabase("db1", Collections.emptyMap());
        Schema schema = schema();
        SortOrder sortOrder = IcebergSchemaBuilder.buildSortOrder(
                Collections.singletonList(new ConnectorSortField("id", true, true)), schema);
        ops.createTable("db1", "t1", schema, PartitionSpec.unpartitioned(), sortOrder,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));

        // The write order is persisted (the buildTable().withSortOrder() path).
        Assertions.assertFalse(ops.loadTable("db1", "t1").sortOrder().isUnsorted());
    }

    @Test
    public void testCreateTablePartitioned() {
        ops.createDatabase("db1", Collections.emptyMap());
        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 8).build();
        ops.createTable("db1", "t1", schema, spec, null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        Assertions.assertFalse(ops.loadTable("db1", "t1").spec().isUnpartitioned());
    }

    @Test
    public void testForceDropDatabaseAfterCascade() {
        // Mirror the metadata layer's force path: drop the contained tables, then the namespace.
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        for (String table : ops.listTableNames("db1")) {
            ops.dropTable("db1", table, true);
        }
        ops.dropDatabase("db1");
        Assertions.assertFalse(ops.databaseExists("db1"));
        Assertions.assertFalse(catalog.namespaceExists(Namespace.of("db1")));
    }

    @Test
    public void testDropTablePurgeRemovesIdentifier() {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        ops.dropTable("db1", "t1", true);
        Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("db1", "t1")));
    }

    @Test
    public void testRenameTable() {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        ops.renameTable("db1", "t1", "t2");
        Assertions.assertFalse(ops.tableExists("db1", "t1"));
        Assertions.assertTrue(ops.tableExists("db1", "t2"));
        // The renamed table keeps its schema (proves it's a real rename, not a recreate).
        Assertions.assertEquals(Type.TypeID.LONG,
                ops.loadTable("db1", "t2").schema().findField("id").type().typeId());
    }

    @Test
    public void testRenameMissingTableFailsLoud() {
        ops.createDatabase("db1", Collections.emptyMap());
        Assertions.assertThrows(Exception.class, () -> ops.renameTable("db1", "ghost", "t2"));
    }

    // ---------- Branch / tag (B4): real ManageSnapshots round-trips on an InMemoryCatalog ----------

    /** Creates db1.t1 and seeds {@code snapshots} consecutive snapshots; returns the current snapshot id. */
    private long createTableWithSnapshots(String table, int snapshots) {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", table, schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        Table t = ops.loadTable("db1", table);
        for (int i = 0; i < snapshots; i++) {
            t.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("s3://b/db1/" + table + "-" + i + ".parquet")
                    .withFileSizeInBytes(1024).withRecordCount(1).withFormat(FileFormat.PARQUET).build())
                    .commit();
        }
        return ops.loadTable("db1", table).currentSnapshot().snapshotId();
    }

    private SnapshotRef ref(String table, String name) {
        return ops.loadTable("db1", table).refs().get(name);
    }

    @Test
    public void testCreateBranchPinsExplicitSnapshot() {
        long snap = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, snap, null, null, null));
        SnapshotRef r = ref("t1", "b1");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.isBranch());
        Assertions.assertEquals(snap, r.snapshotId());
    }

    @Test
    public void testCreateBranchNullSnapshotUsesCurrent() {
        long current = createTableWithSnapshots("t1", 2);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, null, null, null, null));
        Assertions.assertEquals(current, ref("t1", "b1").snapshotId());
    }

    @Test
    public void testCreateBranchAppliesRetentionOptions() {
        long snap = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, snap, 86400000L, 5, 172800000L));
        SnapshotRef r = ref("t1", "b1");
        // retain -> maxSnapshotAgeMs, numSnapshots -> minSnapshotsToKeep, retention -> maxRefAgeMs (legacy mapping).
        Assertions.assertEquals(86400000L, r.maxSnapshotAgeMs());
        Assertions.assertEquals(5, r.minSnapshotsToKeep());
        Assertions.assertEquals(172800000L, r.maxRefAgeMs());
    }

    @Test
    public void testReplaceBranchRepointsToNewSnapshot() {
        long snap1 = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, snap1, null, null, null));
        long snap2 = appendOneSnapshot("t1");
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", false, true, false, snap2, null, null, null));
        Assertions.assertEquals(snap2, ref("t1", "b1").snapshotId());
    }

    @Test
    public void testReplaceBranchOnEmptyTableFailsLoud() {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.createOrReplaceBranch("db1", "t1",
                        new BranchChange("b1", false, true, false, null, null, null, null)));
        Assertions.assertTrue(ex.getMessage().contains("has no snapshot"), ex.getMessage());
    }

    @Test
    public void testCreateBranchIfNotExistsKeepsExistingTarget() {
        long snap1 = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, snap1, null, null, null));
        long snap2 = appendOneSnapshot("t1");
        // create IF NOT EXISTS targeting snap2 must NO-OP: the branch keeps pointing at snap1.
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, true, snap2, null, null, null));
        Assertions.assertEquals(snap1, ref("t1", "b1").snapshotId());
    }

    @Test
    public void testCreateBranchEmptyNameFailsLoud() {
        createTableWithSnapshots("t1", 1);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.createOrReplaceBranch("db1", "t1",
                        new BranchChange("  ", true, false, false, null, null, null, null)));
        Assertions.assertTrue(ex.getMessage().contains("Branch name cannot be empty"), ex.getMessage());
    }

    @Test
    public void testCreateTagPinsSnapshotAndRetention() {
        long snap = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceTag("db1", "t1",
                new TagChange("v1", true, false, false, snap, 99000L));
        SnapshotRef r = ref("t1", "v1");
        Assertions.assertNotNull(r);
        Assertions.assertTrue(r.isTag());
        Assertions.assertEquals(snap, r.snapshotId());
        Assertions.assertEquals(99000L, r.maxRefAgeMs());
    }

    @Test
    public void testCreateTagNullSnapshotUsesCurrent() {
        long current = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceTag("db1", "t1",
                new TagChange("v1", true, false, false, null, null));
        Assertions.assertEquals(current, ref("t1", "v1").snapshotId());
    }

    @Test
    public void testCreateTagOnEmptyTableFailsLoud() {
        ops.createDatabase("db1", Collections.emptyMap());
        ops.createTable("db1", "t1", schema(), PartitionSpec.unpartitioned(), null,
                IcebergSchemaBuilder.buildTableProperties(Collections.emptyMap()));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.createOrReplaceTag("db1", "t1",
                        new TagChange("v1", true, false, false, null, null)));
        Assertions.assertTrue(ex.getMessage().contains("has no snapshot"), ex.getMessage());
    }

    @Test
    public void testReplaceTagRepointsToNewSnapshot() {
        long snap1 = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceTag("db1", "t1",
                new TagChange("v1", true, false, false, snap1, null));
        long snap2 = appendOneSnapshot("t1");
        ops.createOrReplaceTag("db1", "t1",
                new TagChange("v1", false, true, false, snap2, null));
        Assertions.assertEquals(snap2, ref("t1", "v1").snapshotId());
    }

    @Test
    public void testCreateTagEmptyNameFailsLoud() {
        long snap = createTableWithSnapshots("t1", 1);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> ops.createOrReplaceTag("db1", "t1",
                        new TagChange(" ", true, false, false, snap, null)));
        Assertions.assertTrue(ex.getMessage().contains("Tag name cannot be empty"), ex.getMessage());
    }

    @Test
    public void testDropBranchRemovesRef() {
        long snap = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceBranch("db1", "t1",
                new BranchChange("b1", true, false, false, snap, null, null, null));
        ops.dropBranch("db1", "t1", new DropRefChange("b1", false));
        Assertions.assertNull(ref("t1", "b1"));
    }

    @Test
    public void testDropBranchIfExistsMissingIsNoOp() {
        createTableWithSnapshots("t1", 1);
        // No exception, and "main" (the default branch) is untouched.
        ops.dropBranch("db1", "t1", new DropRefChange("ghost", true));
        Assertions.assertNotNull(ref("t1", "main"));
    }

    @Test
    public void testDropBranchMissingWithoutIfExistsFailsLoud() {
        createTableWithSnapshots("t1", 1);
        Assertions.assertThrows(Exception.class,
                () -> ops.dropBranch("db1", "t1", new DropRefChange("ghost", false)));
    }

    @Test
    public void testDropTagRemovesRef() {
        long snap = createTableWithSnapshots("t1", 1);
        ops.createOrReplaceTag("db1", "t1",
                new TagChange("v1", true, false, false, snap, null));
        ops.dropTag("db1", "t1", new DropRefChange("v1", false));
        Assertions.assertNull(ref("t1", "v1"));
    }

    @Test
    public void testDropTagIfExistsMissingIsNoOp() {
        createTableWithSnapshots("t1", 1);
        ops.dropTag("db1", "t1", new DropRefChange("ghost", true));
        Assertions.assertNotNull(ref("t1", "main"));
    }

    /** Appends one more snapshot to an existing db1.{table} and returns the new current snapshot id. */
    private long appendOneSnapshot(String table) {
        Table t = ops.loadTable("db1", table);
        t.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/" + table + "-extra-" + t.currentSnapshot().snapshotId() + ".parquet")
                .withFileSizeInBytes(1024).withRecordCount(1).withFormat(FileFormat.PARQUET).build())
                .commit();
        return ops.loadTable("db1", table).currentSnapshot().snapshotId();
    }
}
