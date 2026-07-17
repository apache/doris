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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for {@link DFSFileSystem} constructor and lifecycle.
 * No real HDFS cluster required — tests focus on construction, close behavior,
 * and post-close error detection.
 */
class DFSFileSystemTest {

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    @Test
    void constructor_succeedsWithEmptyProperties() {
        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(new HashMap<>()),
                "DFSFileSystem should accept empty properties (simple auth, no Kerberos)");
    }

    @Test
    void constructor_succeedsWithHadoopUsername() {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.username", "testuser");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    @Test
    void constructor_succeedsWithHdfsProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns1");
        props.put("hadoop.username", "doris");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    // ------------------------------------------------------------------
    // close() and post-close behavior
    // ------------------------------------------------------------------

    @Test
    void close_isIdempotent() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();
        Assertions.assertDoesNotThrow(fs::close, "Second close should not throw");
    }

    @Test
    void exists_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("hdfs://namenode/test")));
        Assertions.assertTrue(ex.getMessage().contains("closed"),
                "Error message should indicate the filesystem is closed");
    }

    @Test
    void mkdirs_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.mkdirs(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void delete_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("hdfs://namenode/file"), false));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void rename_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("hdfs://nn/src"), Location.of("hdfs://nn/dst")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void list_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.list(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    // ------------------------------------------------------------------
    // globListWithLimit — maxFile pagination cursor semantics
    // ------------------------------------------------------------------

    /**
     * Injects a pre-created Hadoop {@link org.apache.hadoop.fs.FileSystem} into the
     * {@link DFSFileSystem#fsByAuthority fsByAuthority} cache for {@code authority},
     * so {@code getHadoopFs(Path)} returns it without touching the real HDFS.
     */
    @SuppressWarnings("unchecked")
    private static void injectHadoopFs(DFSFileSystem fs, String authority,
            org.apache.hadoop.fs.FileSystem hadoopFs) throws Exception {
        Field f = DFSFileSystem.class.getDeclaredField("fsByAuthority");
        f.setAccessible(true);
        ConcurrentHashMap<String, org.apache.hadoop.fs.FileSystem> map =
                (ConcurrentHashMap<String, org.apache.hadoop.fs.FileSystem>) f.get(fs);
        map.put(authority, hadoopFs);
    }

    private static FileStatus fileStatus(String uri, long len) {
        return new FileStatus(len, false, 1, 1024L, 0L, new Path(uri));
    }

    @Test
    void globListWithLimit_maxFileIsNextCursorWhenPageLimitHit() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[10];
        for (int i = 0; i < 10; i++) {
            statuses[i] = fileStatus("hdfs://nn/glob/f" + i + ".csv", 100L);
        }
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 3L);

        Assertions.assertEquals(3, listing.getFiles().size());
        Assertions.assertEquals("hdfs://nn/glob/f0.csv", listing.getFiles().get(0).location().uri());
        Assertions.assertEquals("hdfs://nn/glob/f2.csv", listing.getFiles().get(2).location().uri());
        // Page limit hit, next matching key past the page is f3.csv — that is the cursor.
        Assertions.assertEquals("hdfs://nn/glob/f3.csv", listing.getMaxFile());
        fs.close();
    }

    @Test
    void globListWithLimit_maxFileIsLastKeyWhenListingExhausted() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[] {
                fileStatus("hdfs://nn/glob/a.csv", 10L),
                fileStatus("hdfs://nn/glob/b.csv", 10L),
                fileStatus("hdfs://nn/glob/c.csv", 10L),
        };
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 10L);

        Assertions.assertEquals(3, listing.getFiles().size());
        FileEntry last = listing.getFiles().get(listing.getFiles().size() - 1);
        Assertions.assertEquals("hdfs://nn/glob/c.csv", last.location().uri());
        // Listing exhausted before any page limit — maxFile equals last entry on the page.
        Assertions.assertEquals(last.location().uri(), listing.getMaxFile());
        fs.close();
    }

    @Test
    void globListWithLimit_maxFileIsEmptyWhenNoMatches() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class)))
                .thenReturn(new FileStatus[0]);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 10L);

        Assertions.assertTrue(listing.getFiles().isEmpty());
        Assertions.assertEquals("", listing.getMaxFile());
        fs.close();
    }

    // ------------------------------------------------------------------
    // delete() — boolean return-value handling
    // ------------------------------------------------------------------

    @Test
    void delete_throwsWhenHadoopReturnsFalseAndPathStillExists() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.delete(ArgumentMatchers.any(Path.class), ArgumentMatchers.anyBoolean()))
                .thenReturn(false);
        Mockito.when(hadoopFs.exists(ArgumentMatchers.any(Path.class))).thenReturn(true);
        injectHadoopFs(fs, "nn", hadoopFs);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("hdfs://nn/path/file"), false));
        Assertions.assertTrue(ex.getMessage().contains("delete returned false"),
                "message should mention the false return: " + ex.getMessage());
        fs.close();
    }

    @Test
    void delete_returnsSilentlyWhenHadoopReturnsFalseAndPathAbsent() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.delete(ArgumentMatchers.any(Path.class), ArgumentMatchers.anyBoolean()))
                .thenReturn(false);
        Mockito.when(hadoopFs.exists(ArgumentMatchers.any(Path.class))).thenReturn(false);
        injectHadoopFs(fs, "nn", hadoopFs);

        Assertions.assertDoesNotThrow(() -> fs.delete(Location.of("hdfs://nn/path/gone"), true));
        fs.close();
    }

    // ------------------------------------------------------------------
    // mkdirs() — boolean return-value handling
    // ------------------------------------------------------------------

    @Test
    void mkdirs_throwsWhenPathExistsAsFile() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.mkdirs(ArgumentMatchers.any(Path.class))).thenReturn(false);
        FileStatus fileStat = Mockito.mock(FileStatus.class);
        Mockito.when(fileStat.isDirectory()).thenReturn(false);
        Mockito.when(hadoopFs.getFileStatus(ArgumentMatchers.any(Path.class))).thenReturn(fileStat);
        injectHadoopFs(fs, "nn", hadoopFs);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.mkdirs(Location.of("hdfs://nn/path/blocked")));
        Assertions.assertTrue(ex.getMessage().contains("not a directory"),
                "message should indicate file-blocks-dir case: " + ex.getMessage());
        fs.close();
    }

    @Test
    void mkdirs_idempotentWhenPathAlreadyDirectory() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.mkdirs(ArgumentMatchers.any(Path.class))).thenReturn(false);
        FileStatus dirStat = Mockito.mock(FileStatus.class);
        Mockito.when(dirStat.isDirectory()).thenReturn(true);
        Mockito.when(hadoopFs.getFileStatus(ArgumentMatchers.any(Path.class))).thenReturn(dirStat);
        injectHadoopFs(fs, "nn", hadoopFs);

        Assertions.assertDoesNotThrow(() -> fs.mkdirs(Location.of("hdfs://nn/path/dir")));
        fs.close();
    }

    // ------------------------------------------------------------------
    // rename() — error context (Finding #8)
    // ------------------------------------------------------------------

    @Test
    void rename_errorMessageIncludesSrcDstExistence() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.rename(ArgumentMatchers.any(Path.class),
                ArgumentMatchers.any(Path.class))).thenReturn(false);
        Path srcPath = new Path("hdfs://nn/a/src");
        Path dstPath = new Path("hdfs://nn/a/dst");
        Mockito.when(hadoopFs.exists(srcPath)).thenReturn(true);
        Mockito.when(hadoopFs.exists(dstPath)).thenReturn(true);
        injectHadoopFs(fs, "nn", hadoopFs);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("hdfs://nn/a/src"), Location.of("hdfs://nn/a/dst")));
        Assertions.assertTrue(ex.getMessage().contains("srcExists=true"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("dstExists=true"), ex.getMessage());
        fs.close();
    }

    // ------------------------------------------------------------------
    // renameDirectory() — Finding #7
    // ------------------------------------------------------------------

    @Test
    void renameDirectory_failsFastOnCrossAuthority() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.exists(ArgumentMatchers.any(Path.class))).thenReturn(true);
        injectHadoopFs(fs, "nn1", hadoopFs);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.renameDirectory(
                        Location.of("hdfs://nn1/a/src"),
                        Location.of("hdfs://nn2/a/dst"),
                        () -> Assertions.fail("callback should not run")));
        Assertions.assertTrue(ex.getMessage().contains("across authorities"), ex.getMessage());
        // No mkdirs / rename should have been attempted on the cached FS for nn1.
        Mockito.verify(hadoopFs, Mockito.never())
                .mkdirs(ArgumentMatchers.any(Path.class));
        Mockito.verify(hadoopFs, Mockito.never())
                .rename(ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Path.class));
        fs.close();
    }

    @Test
    void renameDirectory_callsCallbackWhenSrcAbsent() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.exists(ArgumentMatchers.any(Path.class))).thenReturn(false);
        injectHadoopFs(fs, "nn", hadoopFs);

        boolean[] called = {false};
        fs.renameDirectory(
                Location.of("hdfs://nn/a/missing"),
                Location.of("hdfs://nn/b/dst"),
                () -> called[0] = true);
        Assertions.assertTrue(called[0], "callback must run when src is absent");
        Mockito.verify(hadoopFs, Mockito.never())
                .rename(ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Path.class));
        fs.close();
    }

    @Test
    void renameDirectory_unconditionalParentMkdirs() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.exists(new Path("hdfs://nn/a/src"))).thenReturn(true);
        Mockito.when(hadoopFs.mkdirs(ArgumentMatchers.any(Path.class))).thenReturn(true);
        Mockito.when(hadoopFs.rename(ArgumentMatchers.any(Path.class),
                ArgumentMatchers.any(Path.class))).thenReturn(true);
        injectHadoopFs(fs, "nn", hadoopFs);

        fs.renameDirectory(
                Location.of("hdfs://nn/a/src"),
                Location.of("hdfs://nn/parent/dst"),
                () -> Assertions.fail("src exists; callback must not run"));

        // No exists() pre-check on dstParent — only exists(src) at the very top.
        Mockito.verify(hadoopFs, Mockito.never()).exists(new Path("hdfs://nn/parent"));
        // mkdirs called on dst's parent unconditionally.
        Mockito.verify(hadoopFs).mkdirs(new Path("hdfs://nn/parent"));
        Mockito.verify(hadoopFs).rename(new Path("hdfs://nn/a/src"), new Path("hdfs://nn/parent/dst"));
        fs.close();
    }

    // ------------------------------------------------------------------
    // listFiles(glob) — Finding #10
    // ------------------------------------------------------------------

    @Test
    void listFiles_globFiltersDirectoryMatches() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        // glob "/data/_*" matches one directory and one file; the directory must be filtered.
        FileStatus dirMatch = new FileStatus(0L, true, 1, 1024L, 0L, new Path("hdfs://nn/data/_dir"));
        FileStatus fileMatch = fileStatus("hdfs://nn/data/_top.csv", 50L);
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class)))
                .thenReturn(new FileStatus[] {dirMatch, fileMatch});
        injectHadoopFs(fs, "nn", hadoopFs);

        java.util.List<FileEntry> result = fs.listFiles(Location.of("hdfs://nn/data/_*"));

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("hdfs://nn/data/_top.csv", result.get(0).location().uri());
        // listStatus must NOT be invoked: directory matches are filtered, not expanded.
        Mockito.verify(hadoopFs, Mockito.never()).listStatus(ArgumentMatchers.any(Path.class));
        fs.close();
    }

    @Test
    void listFiles_globReturnsEmptyWhenAllMatchesAreDirectories() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus dirA = new FileStatus(0L, true, 1, 1024L, 0L, new Path("hdfs://nn/data/_a"));
        FileStatus dirB = new FileStatus(0L, true, 1, 1024L, 0L, new Path("hdfs://nn/data/_b"));
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class)))
                .thenReturn(new FileStatus[] {dirA, dirB});
        injectHadoopFs(fs, "nn", hadoopFs);

        java.util.List<FileEntry> result = fs.listFiles(Location.of("hdfs://nn/data/_*"));

        Assertions.assertTrue(result.isEmpty());
        fs.close();
    }

    // ------------------------------------------------------------------
    // globListWithLimit — Findings #11, #12
    // ------------------------------------------------------------------

    @Test
    void globListWithLimit_nonGlobPathListsDirectly() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[] {
                fileStatus("hdfs://nn/dir/a.csv", 10L),
                fileStatus("hdfs://nn/dir/b.csv", 10L),
        };
        Mockito.when(hadoopFs.listStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/dir"), null, 0L, 10L);

        Assertions.assertEquals(2, listing.getFiles().size());
        // Should have used listStatus, NOT globStatus, for a non-glob path.
        Mockito.verify(hadoopFs).listStatus(ArgumentMatchers.any(Path.class));
        Mockito.verify(hadoopFs, Mockito.never()).globStatus(ArgumentMatchers.any(Path.class));
        fs.close();
    }

    @Test
    void globListWithLimit_startAfterAppliedBeforeLimit() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[] {
                fileStatus("hdfs://nn/glob/a.csv", 10L), // skipped by startAfter
                fileStatus("hdfs://nn/glob/b.csv", 10L), // skipped by startAfter (== startAfter)
                fileStatus("hdfs://nn/glob/c.csv", 10L), // page entry 1
                fileStatus("hdfs://nn/glob/d.csv", 10L), // page entry 2
                fileStatus("hdfs://nn/glob/e.csv", 10L), // next-page cursor
        };
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        // maxFiles=2 — must NOT count the skipped a/b toward the page budget.
        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), "hdfs://nn/glob/b.csv", 0L, 2L);

        Assertions.assertEquals(2, listing.getFiles().size());
        Assertions.assertEquals("hdfs://nn/glob/c.csv", listing.getFiles().get(0).location().uri());
        Assertions.assertEquals("hdfs://nn/glob/d.csv", listing.getFiles().get(1).location().uri());
        Assertions.assertEquals("hdfs://nn/glob/e.csv", listing.getMaxFile());
        fs.close();
    }

    // ------------------------------------------------------------------
    // newInputFile(Location, long) — Finding #14
    // ------------------------------------------------------------------

    @Test
    void newInputFile_withLengthHintSkipsGetFileStatus() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        injectHadoopFs(fs, "nn", hadoopFs);

        org.apache.doris.filesystem.DorisInputFile in = fs.newInputFile(
                Location.of("hdfs://nn/data/file.parquet"), 4242L);
        Assertions.assertEquals(4242L, in.length());
        Mockito.verify(hadoopFs, Mockito.never())
                .getFileStatus(ArgumentMatchers.any(Path.class));
        fs.close();
    }
}
