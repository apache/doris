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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

/** Snapshot statistics must not open manifests or construct a split plan. */
public class PaimonCatalogRowCountTest {
    @TempDir
    Path warehouse;

    private final PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(null);

    @Test
    public void latestAppendAndPrimaryKeyCountsNeedNoManifests() throws Exception {
        for (boolean primaryKey : new boolean[] {false, true}) {
            FileStoreTable table = newTable("latest_" + primaryKey, primaryKey);
            snapshot(table, 1, 10L);
            snapshot(table, 2, 25L);
            Assertions.assertEquals(25L, ops.rowCount(table));
        }
    }

    @Test
    public void historicalSnapshotAndTimestampUseSelectedCount() throws Exception {
        FileStoreTable table = newTable("history", true);
        snapshot(table, 1, 10L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.snapshot-id", "1"))));
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.timestamp-millis", "1500"))));
    }

    @Test
    public void tagSurvivesExpiredSnapshot() throws Exception {
        FileStoreTable table = newTable("tag", false);
        snapshot(table, 1, 10L);
        table.createTag("retained", 1L);
        snapshot(table, 2, 25L);
        table.fileIO().delete(table.snapshotManager().snapshotPath(1), false);
        Assertions.assertEquals(10L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.tag-name", "retained"))));
    }

    @Test
    public void branchUsesItsOwnSnapshot() throws Exception {
        FileStoreTable table = newTable("branch", false);
        snapshot(table, 1, 10L);
        table.createBranch("dev");
        FileStoreTable branch = table.switchToBranch("dev");
        snapshot(branch, 1, 7L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(7L, ops.rowCount(branch));
        Assertions.assertEquals(25L, ops.rowCount(table));
    }

    @Test
    public void emptyAndLegacySnapshotsDoNotFallBackToPlanning() throws Exception {
        FileStoreTable table = newTable("empty", false);
        Assertions.assertEquals(-1L, ops.rowCount(table));
        snapshot(table, 1, null);
        Assertions.assertEquals(-1L, ops.rowCount(table));
        snapshot(table, 2, 0L);
        Assertions.assertEquals(0L, ops.rowCount(table));
    }

    @Test
    public void partialScansDoNotUseWholeSnapshotCount() throws Exception {
        FileStoreTable table = newTable("partial", false);
        snapshot(table, 1, 10L);
        snapshot(table, 2, 25L);
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("incremental-between", "1,2"))));
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.file-creation-time-millis", "1500"))));
        Assertions.assertEquals(-1L, ops.rowCount(table.copy(
                Collections.singletonMap("scan.mode", "compacted-full"))));
    }

    @Test
    public void systemAndFallbackTablesDoNotUseBaseCount() throws Exception {
        FileStoreTable table = newTable("system", false);
        snapshot(table, 1, 10L);
        Assertions.assertEquals(-1L, ops.rowCount(new SnapshotsTable(table)));
        Assertions.assertEquals(-1L, ops.rowCount(new FakePaimonTable("format",
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                Collections.emptyList(), Collections.emptyList())));
        FileStoreTable fallback = newTable("fallback", false);
        snapshot(fallback, 1, 20L);
        Assertions.assertEquals(-1L, ops.rowCount(new FallbackReadFileStoreTable(table, fallback)));
    }

    private FileStoreTable newTable(String name, boolean primaryKey) throws Exception {
        LocalFileIO fileIO = new ManifestGuardFileIO();
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(warehouse.resolve(name).toUri());
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT());
        if (primaryKey) {
            schema.primaryKey("id").option("bucket", "1");
        }
        new SchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void snapshot(FileStoreTable table, long id, Long count) throws IOException {
        // Deliberately no manifest files: a return to split planning must fail this test.
        Snapshot snapshot = new Snapshot(id, 0L, "unused-base", null, "unused-delta", null,
                null, null, null, "test", id, Snapshot.CommitKind.APPEND, id * 1000,
                Collections.emptyMap(), count, count, null, null, null, null, null);
        table.fileIO().mkdirs(table.snapshotManager().snapshotPath(id).getParent());
        table.fileIO().overwriteFileUtf8(table.snapshotManager().snapshotPath(id), snapshot.toJson());
        table.snapshotManager().commitLatestHint(id);
    }

    private static class ManifestGuardFileIO extends LocalFileIO {
        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path) throws IOException {
            Assertions.assertFalse(path.toString().contains("/manifest/"),
                    "Row count estimation must not read manifests: " + path);
            return super.newInputStream(path);
        }
    }
}
