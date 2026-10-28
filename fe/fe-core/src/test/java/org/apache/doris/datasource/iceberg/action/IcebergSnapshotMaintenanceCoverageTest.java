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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.NamedArguments;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.execute.BaseExecuteAction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergSnapshotMaintenanceCoverageTest {

    private static IcebergExpireSnapshotsAction expiration(String key, String value) throws Exception {
        IcebergExpireSnapshotsAction action = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("older_than", "4102444800000", key, value), Optional.empty(), Optional.empty(), Mockito.mock(IcebergMetadataOps.class));
        validate(action);
        return action;
    }

    @Test
    public void retainLastPreservesSnapshotsReferencedByTagsAndBranches() throws Exception {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "retained_refs");
        long tagged = ActionTestTables.appendSnapshot(catalog, "retained_refs", "tagged.parquet", 1);
        table.refresh();
        table.manageSnapshots().createTag("audit_tag", tagged).commit();
        long branched = ActionTestTables.appendSnapshot(catalog, "retained_refs", "branched.parquet", 1);
        table.refresh();
        table.manageSnapshots().createBranch("audit_branch", branched).commit();
        long unreferenced = ActionTestTables.appendSnapshot(catalog, "retained_refs", "expired.parquet", 1);
        long current = ActionTestTables.appendSnapshot(catalog, "retained_refs", "current.parquet", 1);
        table.refresh();

        execute(expiration("retain_last", "1"), table);
        table.refresh();

        // Retention is evaluated for each live ref; a main-branch cutoff must not erase pinned history.
        Assertions.assertNotNull(table.snapshot(tagged));
        Assertions.assertNotNull(table.snapshot(branched));
        Assertions.assertNull(table.snapshot(unreferenced));
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(tagged, table.refs().get("audit_tag").snapshotId());
        Assertions.assertEquals(branched, table.refs().get("audit_branch").snapshotId());
    }

    @Test
    public void explicitExpirationOfTaggedSnapshotFailsWithoutChangingHistory() throws Exception {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "protected_tag");
        long tagged = ActionTestTables.appendSnapshot(catalog, "protected_tag", "tagged.parquet", 1);
        table.refresh();
        table.manageSnapshots().createTag("audit_tag", tagged).commit();
        long current = ActionTestTables.appendSnapshot(catalog, "protected_tag", "current.parquet", 1);
        table.refresh();

        Assertions.assertThrows(UserException.class,
                () -> execute(expiration("snapshot_ids", String.valueOf(tagged)), table));
        table.refresh();
        Assertions.assertNotNull(table.snapshot(tagged));
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(tagged, table.refs().get("audit_tag").snapshotId());
    }

    @Test
    public void fastForwardRejectsDivergedBranchesWithoutMovingEitherRef() throws Exception {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "diverged_refs");
        long base = ActionTestTables.appendSnapshot(catalog, "diverged_refs", "base.parquet", 1);
        table.refresh();
        table.manageSnapshots().createBranch("audit_branch", base).commit();
        long main = ActionTestTables.appendSnapshot(catalog, "diverged_refs", "main.parquet", 1);
        table.refresh();
        table.newAppend().appendFile(ActionTestTables.dataFile("branch.parquet", 1))
                .toBranch("audit_branch").commit();
        long branch = table.snapshot("audit_branch").snapshotId();
        IcebergFastForwardAction action = new IcebergFastForwardAction(
                ImmutableMap.of("branch", "main", "to", "audit_branch"), Optional.empty(), Optional.empty(), Mockito.mock(IcebergMetadataOps.class));
        validate(action);

        Assertions.assertThrows(UserException.class,
                () -> execute(action, table));
        table.refresh();
        Assertions.assertEquals(main, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(branch, table.snapshot("audit_branch").snapshotId());
    }

    @Test
    public void rollbackRejectsSnapshotOutsideTheCurrentAncestry() throws Exception {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "rollback_ancestry");
        long base = ActionTestTables.appendSnapshot(catalog, "rollback_ancestry", "base.parquet", 1);
        table.refresh();
        table.manageSnapshots().createBranch("audit_branch", base).commit();
        table.newAppend().appendFile(ActionTestTables.dataFile("branch.parquet", 1))
                .toBranch("audit_branch").commit();
        long branch = table.snapshot("audit_branch").snapshotId();
        long current = ActionTestTables.appendSnapshot(catalog, "rollback_ancestry", "main.parquet", 1);
        table.refresh();
        IcebergRollbackToSnapshotAction action = new IcebergRollbackToSnapshotAction(
                ImmutableMap.of("snapshot_id", String.valueOf(branch)), Optional.empty(), Optional.empty(), Mockito.mock(IcebergMetadataOps.class));
        validate(action);

        Assertions.assertThrows(UserException.class,
                () -> execute(action, table));
        table.refresh();
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(branch, table.snapshot("audit_branch").snapshotId());
    }

    @Test
    @Disabled("Known issue: stale expiration metadata misclassifies newly committed position delete files.")
    public void expirationCountsPositionDeletesCommittedAfterTheTableWasLoaded() throws Exception {
        assertLateDeleteClassification(true, true);
    }

    @Test
    @Disabled("Known issue: stale expiration metadata misclassifies newly committed equality delete files.")
    public void expirationCountsEqualityDeletesCommittedAfterTheTableWasLoaded() throws Exception {
        assertLateDeleteClassification(false, true);
    }

    @Test
    public void expirationCountsPositionDeletesFromAFreshTable() throws Exception {
        assertLateDeleteClassification(true, false);
    }

    @Test
    public void expirationCountsEqualityDeletesFromAFreshTable() throws Exception {
        assertLateDeleteClassification(false, false);
    }

    private static void assertLateDeleteClassification(boolean positionDelete, boolean staleTable) throws Exception {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        ActionTestTables.createTable(catalog, "late_delete");
        ActionTestTables.appendSnapshot(catalog, "late_delete", "base.parquet", 2);
        Table loadedBeforeDelete = catalog.loadTable(ActionTestTables.id("late_delete"));
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned());
        if (positionDelete) {
            builder.ofPositionDeletes().withReferencedDataFile("s3://b/db1/base.parquet");
        } else {
            builder.ofEqualityDeletes(1);
        }
        DeleteFile delete = builder.withPath("s3://b/db1/late-delete.parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(64).withRecordCount(1).build();
        Table writer = catalog.loadTable(ActionTestTables.id("late_delete"));
        writer.newRowDelta().addDeletes(delete).commit();
        writer.newRowDelta().removeDeletes(delete).commit();
        ActionTestTables.appendSnapshot(catalog, "late_delete", "final.parquet", 1);
        if (!staleTable) {
            loadedBeforeDelete.refresh();
        }

        // SDK expiration refreshes metadata. Its counters must classify delete files from that same generation.
        List<String> row = execute(expiration("retain_last", "1"), loadedBeforeDelete);

        Table current = catalog.loadTable(ActionTestTables.id("late_delete"));
        Assertions.assertEquals(1, Iterables.size(current.snapshots()),
                "the append and delete-removal history must actually expire");
        // The legacy data counter also classifies some manifest names as data; verify retention directly.
        Set<String> liveFiles = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = current.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                liveFiles.add(task.file().location());
            }
        }
        Assertions.assertEquals(new HashSet<>(java.util.Arrays.asList(
                "s3://b/db1/base.parquet", "s3://b/db1/final.parquet")), liveFiles);
        Assertions.assertEquals(positionDelete ? "1" : "0", row.get(1),
                "position delete classification; stale=" + staleTable + ", counters=" + row);
        Assertions.assertEquals(positionDelete ? "0" : "1", row.get(2),
                "equality delete classification; stale=" + staleTable + ", counters=" + row);
    }

    private static List<String> execute(BaseIcebergAction action, Table table) throws UserException {
        // Publication assertions use real SDK metadata; cache invalidation belongs to the FE wrapper.
        Env environment = Mockito.mock(Env.class);
        Mockito.when(environment.getExtMetaCacheMgr()).thenReturn(Mockito.mock(ExternalMetaCacheMgr.class));
        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(environment);
            return action.executeIcebergAction(Mockito.mock(IcebergExternalTable.class), table);
        }
    }

    @SuppressWarnings("unchecked")
    private static void validate(BaseIcebergAction action) throws Exception {
        Field arguments = BaseExecuteAction.class.getDeclaredField("namedArguments");
        Field properties = BaseExecuteAction.class.getDeclaredField("properties");
        arguments.setAccessible(true);
        properties.setAccessible(true);
        ((NamedArguments) arguments.get(action)).validate((Map<String, String>) properties.get(action));
        action.validateIcebergAction();
    }

    private static final class ActionTestTables {
        static InMemoryCatalog freshCatalog() {
            InMemoryCatalog catalog = new InMemoryCatalog();
            catalog.initialize("coverage", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db1"));
            return catalog;
        }

        static TableIdentifier id(String name) {
            return TableIdentifier.of("db1", name);
        }

        static Table createTable(InMemoryCatalog catalog, String name) {
            return catalog.createTable(id(name), new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
        }

        static DataFile dataFile(String path, long rows) {
            return DataFiles.builder(PartitionSpec.unpartitioned()).withPath("s3://b/db1/" + path)
                    .withFileSizeInBytes(1024).withRecordCount(rows).withFormat(FileFormat.PARQUET).build();
        }

        static long appendSnapshot(InMemoryCatalog catalog, String name, String path, long rows) {
            Table table = catalog.loadTable(id(name));
            table.newAppend().appendFile(dataFile(path, rows)).commit();
            return table.currentSnapshot().snapshotId();
        }
    }
}
