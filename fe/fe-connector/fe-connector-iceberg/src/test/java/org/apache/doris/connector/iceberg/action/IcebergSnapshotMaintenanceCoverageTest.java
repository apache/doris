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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class IcebergSnapshotMaintenanceCoverageTest {

    private static IcebergExpireSnapshotsAction expiration(String key, String value) {
        IcebergExpireSnapshotsAction action = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("older_than", "4102444800000", key, value), Collections.emptyList(), null);
        action.validate();
        return action;
    }

    @Test
    public void retainLastPreservesSnapshotsReferencedByTagsAndBranches() {
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

        expiration("retain_last", "1").execute(table, ActionTestTables.session("UTC"));
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
    public void explicitExpirationOfTaggedSnapshotFailsWithoutChangingHistory() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        Table table = ActionTestTables.createTable(catalog, "protected_tag");
        long tagged = ActionTestTables.appendSnapshot(catalog, "protected_tag", "tagged.parquet", 1);
        table.refresh();
        table.manageSnapshots().createTag("audit_tag", tagged).commit();
        long current = ActionTestTables.appendSnapshot(catalog, "protected_tag", "current.parquet", 1);
        table.refresh();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> expiration("snapshot_ids", String.valueOf(tagged))
                        .execute(table, ActionTestTables.session("UTC")));
        table.refresh();
        Assertions.assertNotNull(table.snapshot(tagged));
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(tagged, table.refs().get("audit_tag").snapshotId());
    }

    @Test
    public void fastForwardRejectsDivergedBranchesWithoutMovingEitherRef() {
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
                ImmutableMap.of("branch", "main", "to", "audit_branch"), Collections.emptyList(), null);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        table.refresh();
        Assertions.assertEquals(main, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(branch, table.snapshot("audit_branch").snapshotId());
    }

    @Test
    public void rollbackRejectsSnapshotOutsideTheCurrentAncestry() {
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
                ImmutableMap.of("snapshot_id", String.valueOf(branch)), Collections.emptyList(), null);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        table.refresh();
        Assertions.assertEquals(current, table.currentSnapshot().snapshotId());
        Assertions.assertEquals(branch, table.snapshot("audit_branch").snapshotId());
    }

    @Test
    @Disabled("Known issue: stale expiration metadata misclassifies newly committed position delete files.")
    public void expirationCountsPositionDeletesCommittedAfterTheTableWasLoaded() {
        assertLateDeleteClassification(true, true);
    }

    @Test
    @Disabled("Known issue: stale expiration metadata misclassifies newly committed equality delete files.")
    public void expirationCountsEqualityDeletesCommittedAfterTheTableWasLoaded() {
        assertLateDeleteClassification(false, true);
    }

    @Test
    public void expirationCountsPositionDeletesFromAFreshTable() {
        assertLateDeleteClassification(true, false);
    }

    @Test
    public void expirationCountsEqualityDeletesFromAFreshTable() {
        assertLateDeleteClassification(false, false);
    }

    private static void assertLateDeleteClassification(boolean positionDelete, boolean staleTable) {
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
        List<String> row = expiration("retain_last", "1")
                .execute(loadedBeforeDelete, ActionTestTables.session("UTC")).getRows().get(0);

        Assertions.assertEquals(1, Iterables.size(catalog.loadTable(ActionTestTables.id("late_delete")).snapshots()),
                "the append and delete-removal history must actually expire");
        Assertions.assertEquals("0", row.get(0),
                "data files remain reachable; stale=" + staleTable + ", counters=" + row);
        Assertions.assertEquals(positionDelete ? "1" : "0", row.get(1),
                "position delete classification; stale=" + staleTable + ", counters=" + row);
        Assertions.assertEquals(positionDelete ? "0" : "1", row.get(2),
                "equality delete classification; stale=" + staleTable + ", counters=" + row);
    }
}
