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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pins {@code expire_snapshots}: the validation pass and the six-counter result.
 *
 * <p><b>WHY this matters:</b> this is the destructive GC procedure with the widest argument surface and a
 * fixed six-column ({@code BIGINT}) result built from {@code deleteWith}-callback counters. The validation
 * messages (at-least-one-required, the older_than/snapshot_ids format errors) are byte-checked. The happy
 * path proves {@code retain_last} actually expires snapshots through the SDK and that the result row matches
 * the schema width (the single-row contract over six counters).</p>
 */
public class IcebergExpireSnapshotsActionTest {

    private static IcebergExpireSnapshotsAction action(Map<String, String> props) {
        return new IcebergExpireSnapshotsAction(props, Collections.emptyList(), null);
    }

    @Test
    public void requiresAtLeastOneOfOlderThanRetainLastSnapshotIds() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(Collections.emptyMap()).validate());
        Assertions.assertEquals("At least one of 'older_than', 'retain_last', or "
                + "'snapshot_ids' must be specified", e.getMessage());
    }

    @Test
    public void rejectsBadOlderThanFormat() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("older_than", "yesterday")).validate());
        Assertions.assertEquals("Invalid older_than format. Expected ISO datetime "
                + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: yesterday", e.getMessage());
    }

    @Test
    public void rejectsBadSnapshotIdsFormat() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("snapshot_ids", "1,abc,3")).validate());
        Assertions.assertEquals("Invalid snapshot_id format: abc", e.getMessage());
    }

    @Test
    public void expiresOldSnapshotsKeepingRetainLastAndReturnsSixCounters() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Assertions.assertEquals(3, Iterables.size(catalog.loadTable(id).snapshots()));

        IcebergExpireSnapshotsAction action = action(ImmutableMap.of("retain_last", "1"));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals(6, row.size(), "expire_snapshots returns the six-counter Spark schema");
        for (String count : row) {
            Assertions.assertDoesNotThrow(() -> Long.parseLong(count), "every counter is a number: " + count);
        }
        // Pin the deleteWith path-classification deterministically. Expiring 2 of 3 append-only snapshots
        // (retain_last=1) reclaims the 2 expired snapshots' manifest-LIST files (snap-*.avro, index 4 == 2)
        // but NOT the data manifests (still referenced by the retained snapshot, index 3 == 0) nor the data
        // files themselves (index 0 == 0); there are no delete/statistics files. A mutation swapping the
        // manifest vs manifest-list branch in the deleteWith callback would flip indices 3/4 and go RED.
        Assertions.assertEquals(ImmutableList.of("0", "0", "0", "0", "2", "0"), row,
                "deleteWith classification: 2 manifest-lists reclaimed, everything else 0");
        Assertions.assertEquals(1, Iterables.size(catalog.loadTable(id).snapshots()),
                "retain_last=1 must leave exactly one snapshot");
    }

    @Test
    public void countsExpiredManifestsAsManifestFiles() {
        // Iceberg names a manifest <commit UUID>-m<N>.avro (SnapshotProducer.newManifestOutputFile), so a "-m-"
        // file name check never matched one and counted every expired manifest as a deleted data file. Removing
        // f1 and then appending f2 leaves the first snapshot's manifest unreferenced by the last snapshot, so
        // retain_last=1 deletes that manifest together with f1 and the two older manifest lists.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        catalog.loadTable(id).newDelete().deleteFile("s3://b/db1/f1.parquet").commit();
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);

        Table table = catalog.loadTable(id);
        Set<String> retained = new HashSet<>();
        for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
            retained.add(manifest.path());
        }
        Set<String> expired = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!retained.contains(manifest.path())) {
                    expired.add(manifest.path());
                }
            }
        }
        // Sanity: the fixture must expire a manifest, and no manifest name carries "-m-".
        Assertions.assertFalse(expired.isEmpty(), "the last snapshot must not reference every manifest");
        Assertions.assertTrue(expired.stream().noneMatch(path -> path.contains("-m-")), expired.toString());

        IcebergExpireSnapshotsAction action = action(ImmutableMap.of("retain_last", "1"));
        action.validate();
        List<String> row = action.execute(table, ActionTestTables.session("UTC")).getRows().get(0);
        Assertions.assertEquals(ImmutableList.of("1", "0", "0", String.valueOf(expired.size()), "2", "0"), row,
                "f1 is the only deleted data file; expired manifests " + expired + " count as manifest files");
    }

    @Test
    public void countsMetadataOfSnapshotsCommittedAfterTheTableWasLoaded() {
        // The expiration refreshes the table when it commits (RemoveSnapshots.internalApply), so it also expires
        // snapshots that another writer committed after the action loaded the table and built its file kind map.
        // Their manifests and manifest lists are not in the map and must still count as such, not as deleted
        // data files.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f0.parquet", 1L);
        Table loaded = catalog.loadTable(id);
        // Another writer appends f1, deletes it and appends f2, which leaves f1 and the manifests that listed it
        // unreferenced by the last snapshot.
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        catalog.loadTable(id).newDelete().deleteFile("s3://b/db1/f1.parquet").commit();
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);

        Table current = catalog.loadTable(id);
        Set<String> retained = new HashSet<>();
        for (ManifestFile manifest : current.currentSnapshot().allManifests(current.io())) {
            retained.add(manifest.path());
        }
        Set<String> expired = new HashSet<>();
        for (Snapshot snapshot : current.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(current.io())) {
                if (!retained.contains(manifest.path())) {
                    expired.add(manifest.path());
                }
            }
        }
        // Sanity: manifests expire, and the map built from the loaded table knows none of them.
        Map<String, IcebergExpireSnapshotsAction.FileKind> kindByPath =
                action(ImmutableMap.of("retain_last", "1")).buildFileKindMap(loaded);
        Assertions.assertFalse(expired.isEmpty(), "the last snapshot must not reference every manifest");
        Assertions.assertTrue(expired.stream().noneMatch(kindByPath::containsKey), expired.toString());

        // An older_than in 2100 expires every snapshot but the retained one, however fast the test runs.
        IcebergExpireSnapshotsAction action =
                action(ImmutableMap.of("older_than", "4102444800000", "retain_last", "1"));
        action.validate();
        List<String> row = action.execute(loaded, ActionTestTables.session("UTC")).getRows().get(0);
        Assertions.assertEquals(ImmutableList.of("1", "0", "0", String.valueOf(expired.size()), "3", "0"), row,
                "f1 is the only deleted data file; expired manifests " + expired
                        + " and the manifest lists of the three expired snapshots count as such");
        Assertions.assertEquals(1, Iterables.size(catalog.loadTable(id).snapshots()),
                "retain_last=1 must leave exactly one snapshot");
    }

    @Test
    public void classifiesUnmappedMetadataFilesByIcebergFileNames() {
        String dir = "s3://b/db1/t/metadata/";
        Assertions.assertEquals(IcebergExpireSnapshotsAction.FileKind.MANIFEST_LIST,
                IcebergExpireSnapshotsAction.metadataFileKindByName(
                        dir + "snap-1234-1-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.avro"));
        Assertions.assertEquals(IcebergExpireSnapshotsAction.FileKind.MANIFEST,
                IcebergExpireSnapshotsAction.metadataFileKindByName(
                        dir + "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-m12.avro"));
        Assertions.assertEquals(IcebergExpireSnapshotsAction.FileKind.MANIFEST,
                IcebergExpireSnapshotsAction.metadataFileKindByName(
                        dir + "optimized-m-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.avro"));
        // Data files, including Avro ones, and statistics files are left to the callback's other branches.
        Assertions.assertNull(IcebergExpireSnapshotsAction.metadataFileKindByName(
                "s3://b/db1/t/data/00000-0-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-00001.avro"));
        Assertions.assertNull(IcebergExpireSnapshotsAction.metadataFileKindByName(
                "s3://b/db1/t/data/00000-0-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-00001.parquet"));
        Assertions.assertNull(IcebergExpireSnapshotsAction.metadataFileKindByName(
                dir + "1234-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9.stats"));
        // Only the file name counts, not a directory that looks like a manifest list.
        Assertions.assertNull(IcebergExpireSnapshotsAction.metadataFileKindByName(
                "s3://b/snap-dir/00000-0-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-00001.avro"));
        // Every data file is classified by name, so Avro data files whose names only resemble metadata names are
        // not metadata: the whole name has to match, UUID included.
        for (String dataFile : ImmutableList.of("sales-m1.avro", "sales-m-1.avro", "optimized-m-1.avro",
                "snap-1.avro", "snap-1234-1-sales.avro")) {
            Assertions.assertNull(
                    IcebergExpireSnapshotsAction.metadataFileKindByName("s3://b/db1/t/data/" + dataFile), dataFile);
        }
    }

    @Test
    public void countsAnAvroDataFileNamedLikeAManifestAsADataFile() {
        // The file kind map holds no data files, so every expired data file is classified by its name. An Avro
        // data file whose name ends in -m<N>.avro is still a data file.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        DataFile avroDataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/t/data/sales-m1.avro")
                .withFileSizeInBytes(1024)
                .withRecordCount(1L)
                .withFormat(FileFormat.AVRO)
                .build();
        catalog.loadTable(id).newAppend().appendFile(avroDataFile).commit();
        catalog.loadTable(id).newDelete().deleteFile(avroDataFile.location()).commit();
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);

        Table table = catalog.loadTable(id);
        Set<String> retained = new HashSet<>();
        for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
            retained.add(manifest.path());
        }
        Set<String> expired = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!retained.contains(manifest.path())) {
                    expired.add(manifest.path());
                }
            }
        }
        Assertions.assertFalse(expired.isEmpty(), "the last snapshot must not reference every manifest");

        IcebergExpireSnapshotsAction action =
                action(ImmutableMap.of("older_than", "4102444800000", "retain_last", "1"));
        action.validate();
        List<String> row = action.execute(table, ActionTestTables.session("UTC")).getRows().get(0);
        Assertions.assertEquals(ImmutableList.of("1", "0", "0", String.valueOf(expired.size()), "2", "0"), row,
                "sales-m1.avro is the only deleted data file; expired manifests " + expired);
    }

    @Test
    public void resultSchemaIsSixBigintCounters() {
        List<ConnectorColumn> schema = action(ImmutableMap.of("retain_last", "1")).getResultSchema();
        String[] names = {"deleted_data_files_count", "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count", "deleted_manifest_files_count",
                "deleted_manifest_lists_count", "deleted_statistics_files_count"};
        Assertions.assertEquals(6, schema.size());
        for (int i = 0; i < 6; i++) {
            Assertions.assertEquals(names[i], schema.get(i).getName(), "column " + i + " name");
            Assertions.assertEquals("BIGINT", schema.get(i).getType().getTypeName(), "column " + i + " type");
        }
    }

    @Test
    public void buildFileKindMapDedupsManifestsSharedAcrossSnapshots() {
        // WHY: expire_snapshots builds the delete-file -> content classification map by scanning every snapshot's
        // delete manifests. Iceberg carries immutable delete manifests forward across snapshots, so the naive
        // per-snapshot scan re-reads the same manifest once per referencing snapshot. Deduping by manifest path
        // must (a) read each DISTINCT delete manifest exactly once — strictly fewer than the per-snapshot total —
        // and (b) leave the resulting map byte-identical (every delete path still classified correctly). A
        // regression that reset the visited set per snapshot (no dedup) or skipped a distinct manifest (dropping
        // its delete files) is killed here.
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        // A position-delete snapshot creates delete manifest DM1...
        ActionTestTables.addPositionDeleteSnapshot(catalog, "t", "pos-del.parquet", "f1.parquet");
        // ...an equality-delete snapshot creates DM2 and carries DM1 forward...
        ActionTestTables.addEqualityDeleteSnapshot(catalog, "t", "eq-del.parquet");
        // ...and two more appends carry both delete manifests forward unchanged (the redundant re-read source).
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 1L);

        Table table = catalog.loadTable(id);
        Set<String> distinctDeleteManifests = new HashSet<>();
        int perSnapshotReads = 0;
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                distinctDeleteManifests.add(manifest.path());
                perSnapshotReads++;
            }
        }
        // Sanity: the fixture must actually reuse a delete manifest across snapshots, else the test proves nothing.
        Assertions.assertTrue(perSnapshotReads > distinctDeleteManifests.size(),
                "fixture must share a delete manifest across snapshots: perSnapshot=" + perSnapshotReads
                        + " distinct=" + distinctDeleteManifests.size());

        IcebergExpireSnapshotsAction action = action(ImmutableMap.of("retain_last", "1"));
        Map<String, IcebergExpireSnapshotsAction.FileKind> kindByPath = action.buildFileKindMap(table);

        Assertions.assertEquals(distinctDeleteManifests.size(), action.lastDeleteManifestReadCount,
                "each distinct delete manifest must be read exactly once (dedup), not once per referencing snapshot");
        Assertions.assertEquals(IcebergExpireSnapshotsAction.FileKind.POSITION_DELETES,
                kindByPath.get("s3://b/db1/pos-del.parquet"),
                "the position-delete file must stay classified as POSITION_DELETES");
        Assertions.assertEquals(IcebergExpireSnapshotsAction.FileKind.EQUALITY_DELETES,
                kindByPath.get("s3://b/db1/eq-del.parquet"),
                "the equality-delete file must stay classified as EQUALITY_DELETES");
    }

    @Test
    public void rejectsNegativeOlderThanTimestamp() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("older_than", "-1")).validate());
        Assertions.assertEquals("older_than timestamp must be non-negative", e.getMessage());
    }

    @Test
    public void rejectsPartitionSpecification() {
        IcebergExpireSnapshotsAction a = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("retain_last", "1"), Collections.singletonList("p1"), null);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'expire_snapshots' does not support partition specification",
                e.getMessage());
    }

    @Test
    public void rejectsWhereCondition() {
        ConnectorPredicate where =
                new ConnectorPredicate(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE));
        IcebergExpireSnapshotsAction a = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("retain_last", "1"), Collections.emptyList(), where);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'expire_snapshots' does not support WHERE condition", e.getMessage());
    }
}
