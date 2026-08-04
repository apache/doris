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

import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Guards the {@code OPTIONS} arm of {@link PaimonConnectorMetadata#resolveTimeTravel} when the resolved
 * option map pins a TAG.
 *
 * <p>A paimon tag owns a RETAINED copy of its snapshot, which outlives the ordinary
 * {@code snapshot/snapshot-<id>} file. So a tag pin must take BOTH its snapshot id and its schema id off
 * that retained copy — which is what the {@code TAG} arm has always done. The {@code OPTIONS} arm instead
 * used the tag only to look up the ID and then re-derived the schema id through
 * {@code snapshotManager().snapshot(id)}, i.e. through the expirable file. On a table whose snapshot had
 * expired but whose tag survived, that threw
 * {@code "Snapshot file ... does not exist. It might have been expired by other jobs"} during ANALYSIS —
 * External Regression 1007447, {@code paimon_time_travel.qt_expired_tag_options_count}, while the
 * {@code FOR VERSION AS OF '<tag>'} form on the very same table passed.
 *
 * <p>These drive a real local-filesystem paimon table (the option resolution needs a genuine
 * {@code FileStoreTable}) but keep {@link RecordingPaimonCatalogOps} for the id lookups, so the test can
 * assert WHICH seam the schema id came from rather than merely that a value came back.
 */
public class PaimonConnectorMetadataOptionsTagPinTest {

    @Test
    public void optionsTagPinTakesSchemaIdFromTheTagNotTheSnapshotFile(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);
            table.createTag("retained_tag", 1L);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = handleOver(ops, table);
            ops.tagSnapshot = new PaimonCatalogOps.TagSnapshot(1L, 7L);
            // What reading the (expirable) snapshot file would have produced. On the failing fixture this
            // seam does not merely answer differently -- it throws, because the file is gone.
            ops.snapshotSchemaId = OptionalLong.of(99L);

            ConnectorMvccSnapshot snap = metadataWith(ops).resolveTimeTravel(null, handle,
                    ConnectorTimeTravelSpec.options(
                            ImmutableMap.of(CoreOptionsKeys.SCAN_TAG_NAME, "retained_tag"))).get();

            // MUTATION: deriving schemaId via snapshotSchemaId again -> 99 != 7, and the seam assertion
            // below goes red as well.
            Assertions.assertEquals(1L, snap.getSnapshotId(), "the tag's snapshot id must be pinned");
            Assertions.assertEquals(7L, snap.getSchemaId(),
                    "an @options tag pin must stamp the TAG's own schemaId");
            Assertions.assertEquals("retained_tag",
                    snap.getProperties().get(CoreOptionsKeys.SCAN_TAG_NAME),
                    "the canonical tag selector must survive onto the pin");
            Assertions.assertTrue(readSnapshotFileSeamCalls(ops).isEmpty(),
                    "an @options tag pin must not read the expirable snapshot file for its schemaId, got: "
                            + readSnapshotFileSeamCalls(ops));
        }
    }

    @Test
    public void optionsTagValuedVersionPinAlsoAvoidsTheSnapshotFile(@TempDir Path warehouse)
            throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);
            table.createTag("canonical_tag", 1L);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = handleOver(ops, table);
            ops.tagSnapshot = new PaimonCatalogOps.TagSnapshot(1L, 7L);
            ops.snapshotSchemaId = OptionalLong.of(99L);

            // scan.version accepts a snapshot id OR a tag name; paimon canonicalizes a tag-valued one into
            // scan.tag-name while copying the selected table, so it lands on the same pin path. This is the
            // suite's third expired-tag query (qt_expired_version_options_count), which never got to run
            // because the previous statement aborted the suite.
            ConnectorMvccSnapshot snap = metadataWith(ops).resolveTimeTravel(null, handle,
                    ConnectorTimeTravelSpec.options(
                            ImmutableMap.of(CoreOptionsKeys.SCAN_VERSION, "canonical_tag"))).get();

            Assertions.assertEquals(7L, snap.getSchemaId(),
                    "a tag-valued scan.version must stamp the TAG's own schemaId");
            Assertions.assertTrue(readSnapshotFileSeamCalls(ops).isEmpty(),
                    "a tag-valued scan.version must not read the expirable snapshot file for its schemaId");
        }
    }

    @Test
    public void optionsSnapshotIdPinStillReadsTheSnapshotFile(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            Table table = tableWithSnapshots(catalog, 2);

            RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
            PaimonTableHandle handle = handleOver(ops, table);
            ops.snapshotSchemaId = OptionalLong.of(3L);

            // Counterpart guard: only a TAG pin may skip the snapshot file. An explicit snapshot-id pin has
            // no retained copy to read from, so it must keep resolving its schema through the snapshot.
            // MUTATION: routing every @options pin through the tag seam -> schemaId -1 != 3 red.
            ConnectorMvccSnapshot snap = metadataWith(ops).resolveTimeTravel(null, handle,
                    ConnectorTimeTravelSpec.options(
                            ImmutableMap.of(CoreOptionsKeys.SCAN_SNAPSHOT_ID, "1"))).get();

            Assertions.assertEquals(1L, snap.getSnapshotId());
            Assertions.assertEquals(3L, snap.getSchemaId());
            Assertions.assertFalse(readSnapshotFileSeamCalls(ops).isEmpty(),
                    "a snapshot-id @options pin must still resolve its schema through the snapshot");
        }
    }

    /** Option keys spelled out so the test asserts the WIRE names, not whatever CoreOptions returns. */
    private static final class CoreOptionsKeys {
        private static final String SCAN_TAG_NAME = "scan.tag-name";
        private static final String SCAN_VERSION = "scan.version";
        private static final String SCAN_SNAPSHOT_ID = "scan.snapshot-id";

        private CoreOptionsKeys() {
        }
    }

    private static List<String> readSnapshotFileSeamCalls(RecordingPaimonCatalogOps ops) {
        return ops.log.stream()
                .filter(call -> call.startsWith("snapshotSchemaId"))
                .collect(java.util.stream.Collectors.toList());
    }

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    /** A normal (non-sys) handle whose transient paimon Table is the REAL local table. */
    private static PaimonTableHandle handleOver(RecordingPaimonCatalogOps ops, Table table) {
        PaimonTableHandle handle = new PaimonTableHandle(
                "db", "t", Collections.emptyList(), Collections.emptyList());
        ops.table = table;
        handle.setPaimonTable(table);
        return handle;
    }

    private static Catalog localCatalog(Path warehouse) {
        return new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()));
    }

    /** A real local table carrying {@code snapshots} committed snapshots (one row each). */
    private static Table tableWithSnapshots(Catalog catalog, int snapshots) throws Exception {
        catalog.createDatabase("db", false);
        Identifier id = Identifier.create("db", "t");
        catalog.createTable(id, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("val", DataTypes.BIGINT())
                .primaryKey("id")
                .option("bucket", "1")
                .build(), false);
        Table table = catalog.getTable(id);
        for (int i = 1; i <= snapshots; i++) {
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite()) {
                write.write(GenericRow.of(i, (long) i * 100));
                List<CommitMessage> messages = write.prepareCommit();
                try (BatchTableCommit commit = wb.newCommit()) {
                    commit.commit(messages);
                }
            }
        }
        return catalog.getTable(id);
    }
}
