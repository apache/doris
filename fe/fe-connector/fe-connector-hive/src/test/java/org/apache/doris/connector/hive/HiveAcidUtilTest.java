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

import org.apache.doris.connector.hms.HmsAcidConstants;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.local.LocalFileSystem;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the pure ACID directory descent {@link HiveAcidUtil#getAcidState} against a real Doris
 * {@link FileSystem} (the test-only {@link LocalFileSystem}) over a local temp tree — the Doris equivalent of the
 * old Hadoop {@code LocalFileSystem}, now that {@link HiveAcidUtil} lists via the engine-injected Doris filesystem.
 *
 * <p>WHY: for a transactional Hive table the reader must reconstruct the correct snapshot from the
 * base/delta/delete-delta directory layout — pick the best valid base, layer on the deltas whose
 * write-id range is still valid, drop obsolete/out-of-snapshot directories, and hand the BE the
 * delete-delta file names so it can subtract deleted rows. A wrong base or a dropped delta silently
 * returns stale or over-/under-deleted data. These tests pin that selection algorithm.</p>
 *
 * <p>The filesystem is a {@link LiteralListingLocalFileSystem}: it forbids the glob-aware
 * {@link FileSystem#listFiles}, so every test here also pins that {@link HiveAcidUtil} lists via the literal
 * {@link FileSystem#list} (matching the old {@code FileSystem.listStatus}).</p>
 */
public class HiveAcidUtilTest {

    @TempDir
    java.nio.file.Path tempDir;

    private FileSystem localFs() {
        return new LiteralListingLocalFileSystem();
    }

    /** Creates {@code <partition>/<dir>/<file>} with 1 byte of content. */
    private void createBucketFile(String dir, String fileName) throws IOException {
        java.nio.file.Path d = tempDir.resolve(dir);
        Files.createDirectories(d);
        Files.write(d.resolve(fileName), new byte[] {1});
    }

    /** Snapshot with all visibility txns valid and write-ids valid up to {@code highWatermark}. */
    private Map<String, String> snapshot(long highWatermark) {
        ValidTxnList validTxnList =
                new ValidReadTxnList(new long[0], new BitSet(), Long.MAX_VALUE, Long.MAX_VALUE);
        ValidWriteIdList validWriteIdList =
                new ValidReaderWriteIdList("db.tbl", new long[0], new BitSet(), highWatermark);
        Map<String, String> ids = new HashMap<>();
        ids.put(HmsAcidConstants.VALID_TXNS_KEY, validTxnList.writeToString());
        ids.put(HmsAcidConstants.VALID_WRITEIDS_KEY, validWriteIdList.writeToString());
        return ids;
    }

    @Test
    public void testBestBaseWithDeltaAndDeleteDelta() throws IOException {
        // Best base = base_5 (base_2 superseded); delta_6 layered on; delete_delta_6 survives via the
        // split-update pairing with delta_6; delta_9 is out of the write-id=6 snapshot; the staging dir
        // and the _flush_length side file are ignored.
        createBucketFile("base_0000005", "bucket_00000");
        createBucketFile("base_0000002", "bucket_00000");
        createBucketFile("delta_0000006_0000006", "bucket_00000");
        createBucketFile("delta_0000006_0000006", "bucket_00000_flush_length");
        createBucketFile("delete_delta_0000006_0000006", "bucket_00000");
        createBucketFile("delta_0000009_0000009", "bucket_00000");
        createBucketFile(".hive-staging_hive_2026-01-01", "junk");

        HiveAcidUtil.AcidState state = HiveAcidUtil.getAcidState(
                localFs(), tempDir.toString(), snapshot(6L), true);

        List<FileEntry> dataFiles = state.getDataFiles();
        Assertions.assertEquals(2, dataFiles.size(),
                "surviving data files must be exactly base_5 + delta_6 bucket files");
        boolean hasBase5 = false;
        boolean hasDelta6 = false;
        for (FileEntry f : dataFiles) {
            String p = f.location().uri();
            Assertions.assertFalse(p.contains("base_0000002"), "superseded base must be dropped: " + p);
            Assertions.assertFalse(p.contains("delta_0000009"), "out-of-snapshot delta dropped: " + p);
            Assertions.assertFalse(p.endsWith("_flush_length"), "side file excluded: " + p);
            hasBase5 |= p.contains("/base_0000005/bucket_00000");
            hasDelta6 |= p.contains("/delta_0000006_0000006/bucket_00000");
        }
        Assertions.assertTrue(hasBase5, "best base_5 bucket file must survive");
        Assertions.assertTrue(hasDelta6, "delta_6 bucket file must survive");

        List<HiveAcidUtil.DeleteDelta> deletes = state.getDeleteDeltas();
        Assertions.assertEquals(1, deletes.size(), "the paired delete_delta_6 must survive");
        HiveAcidUtil.DeleteDelta d = deletes.get(0);
        Assertions.assertTrue(d.getDirectoryLocation().endsWith("/delete_delta_0000006_0000006"),
                "delete-delta dir: " + d.getDirectoryLocation());
        Assertions.assertEquals(List.of("bucket_00000"), d.getFileNames(),
                "delete-delta file names must be captured (BE under-deletes without them)");
    }

    @Test
    public void testInsertOnlyRejectsDeleteDelta() throws IOException {
        // An insert-only ACID table must never carry delete deltas; a stray one is a corruption signal.
        createBucketFile("base_0000005", "000000_0");
        createBucketFile("delta_0000006_0000006", "000000_0");
        createBucketFile("delete_delta_0000006_0000006", "000000_0");

        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> HiveAcidUtil.getAcidState(localFs(), tempDir.toString(), snapshot(6L), false));
        Assertions.assertTrue(ex.getMessage().contains("delete_delta"),
                "insert-only table with a delete delta must fail loud: " + ex.getMessage());
    }

    @Test
    public void testInsertOnlyAcceptsAllFileNames() throws IOException {
        // Insert-only uses the accept-all filter: files that are not bucket_* still count as data.
        createBucketFile("base_0000005", "000000_0");
        createBucketFile("delta_0000006_0000006", "000001_0");

        HiveAcidUtil.AcidState state = HiveAcidUtil.getAcidState(
                localFs(), tempDir.toString(), snapshot(6L), false);
        Assertions.assertEquals(2, state.getDataFiles().size(),
                "insert-only accepts non-bucket_ data file names");
        Assertions.assertTrue(state.getDeleteDeltas().isEmpty());
    }

    @Test
    public void testMissingValidWriteIdsThrows() throws IOException {
        createBucketFile("base_0000005", "bucket_00000");
        Map<String, String> ids = snapshot(6L);
        ids.remove(HmsAcidConstants.VALID_WRITEIDS_KEY);

        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> HiveAcidUtil.getAcidState(localFs(), tempDir.toString(), ids, true));
        Assertions.assertTrue(ex.getMessage().contains("ValidWriteIdList"), ex.getMessage());
    }

    @Test
    public void testOriginalFilesWithoutBaseThrows() throws IOException {
        // A bare file directly under the partition (no base_) means an unconverted non-ACID table.
        Files.write(tempDir.resolve("000000_0"), new byte[] {1});

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> HiveAcidUtil.getAcidState(localFs(), tempDir.toString(), snapshot(6L), true));
    }

    @Test
    public void testBaseWithUncommittedVisibilityTxnIsSkipped() throws IOException {
        // base_5 was produced by visibility txn 100, which is NOT committed in this snapshot -> it must
        // be skipped, falling back to the older committed base_3. This pins the visibility-txn filter.
        createBucketFile("base_0000005_v0000100", "bucket_00000");
        createBucketFile("base_0000003", "bucket_00000");

        // Txn high-watermark 50: visibility txn 100 is not yet valid; base_3's visibility (0) is valid.
        ValidTxnList txns = new ValidReadTxnList(new long[0], new BitSet(), 50L, Long.MAX_VALUE);
        ValidWriteIdList writeIds = new ValidReaderWriteIdList("db.tbl", new long[0], new BitSet(), 5L);
        Map<String, String> ids = new HashMap<>();
        ids.put(HmsAcidConstants.VALID_TXNS_KEY, txns.writeToString());
        ids.put(HmsAcidConstants.VALID_WRITEIDS_KEY, writeIds.writeToString());

        HiveAcidUtil.AcidState state = HiveAcidUtil.getAcidState(
                localFs(), tempDir.toString(), ids, true);
        Assertions.assertEquals(1, state.getDataFiles().size());
        Assertions.assertTrue(state.getDataFiles().get(0).location().uri().contains("/base_0000003/"),
                "a base whose visibility txn is uncommitted must be skipped for the committed base");
    }

    @Test
    public void testHighestBaseInvalidFallsBackToLowerValidBaseAndDropsObsoleteDelta() throws IOException {
        // base_8 is beyond the write-id watermark (invalid) and must be rejected despite its higher
        // write id, falling back to base_4; delta_2 predates base_4 so it is obsolete and dropped. This
        // pins isValidBase discrimination + the selection loop's "delta below the base" rejection.
        createBucketFile("base_0000008", "bucket_00000");
        createBucketFile("base_0000004", "bucket_00000");
        createBucketFile("delta_0000002_0000002", "bucket_00000");

        HiveAcidUtil.AcidState state = HiveAcidUtil.getAcidState(
                localFs(), tempDir.toString(), snapshot(5L), true);

        Assertions.assertEquals(1, state.getDataFiles().size(),
                "only the best VALID base (base_4) survives; the higher but invalid base_8 is rejected");
        String p = state.getDataFiles().get(0).location().uri();
        Assertions.assertTrue(p.contains("/base_0000004/"), p);
        Assertions.assertFalse(p.contains("base_0000008"), "base above the write-id watermark is invalid");
    }

    @Test
    public void testNoValidBaseThrowsNotEnoughHistory() throws IOException {
        // Only base_8 exists but it is beyond the write-id watermark -> no usable base and no original
        // files -> the reader cannot reconstruct the snapshot and must fail loud.
        createBucketFile("base_0000008", "bucket_00000");

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> HiveAcidUtil.getAcidState(localFs(), tempDir.toString(), snapshot(5L), true));
        Assertions.assertTrue(ex.getMessage().contains("Not enough history"), ex.getMessage());
    }

    @Test
    public void testMissingValidTxnListThrows() throws IOException {
        createBucketFile("base_0000005", "bucket_00000");
        Map<String, String> ids = snapshot(6L);
        ids.remove(HmsAcidConstants.VALID_TXNS_KEY);

        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                () -> HiveAcidUtil.getAcidState(localFs(), tempDir.toString(), ids, true));
        Assertions.assertTrue(ex.getMessage().contains("ValidTxnList"), ex.getMessage());
    }

    /**
     * A Doris {@link LocalFileSystem} that FORBIDS the glob-aware {@link FileSystem#listFiles} /
     * {@link FileSystem#listFilesRecursive}. Every test lists through this, so any regression in
     * {@link HiveAcidUtil} from the literal {@link FileSystem#list} to {@code listFiles()} fails loud here.
     *
     * <p>The production per-scheme filesystems ({@code DFSFileSystem}, {@code S3CompatibleFileSystem}) override
     * {@code listFiles} to treat a location containing {@code [}/{@code *}/{@code ?} as a glob pattern; a real hive
     * delta/partition location can contain those, and the old {@code FileSystem.listStatus} never glob-expanded.
     * Plain {@link LocalFileSystem#listFiles} is the (literal) interface default, so it alone cannot catch a
     * {@code list()->listFiles()} regression — hence this guard. Mirrors {@code FakeFileSystem.listFiles} throwing
     * in the non-ACID listing tests.
     */
    private static final class LiteralListingLocalFileSystem extends LocalFileSystem {
        LiteralListingLocalFileSystem() {
            super(Collections.emptyMap());
        }

        @Override
        public List<FileEntry> listFiles(Location dir) {
            throw new AssertionError(
                    "HiveAcidUtil must list via the literal list(), never the glob-aware listFiles()");
        }

        @Override
        public List<FileEntry> listFilesRecursive(Location dir) {
            throw new AssertionError(
                    "HiveAcidUtil must list via the literal list(), never the glob-aware listFilesRecursive()");
        }
    }
}
