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

package org.apache.doris.fs;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MemoryFileSystemTest {

    private MemoryFileSystem fs;

    @Before
    public void setUp() {
        fs = new MemoryFileSystem();
    }

    // ─────────────────────────── exists ───────────────────────────

    @Test
    public void testExistsReturnsFalseForMissingFile() throws IOException {
        Assert.assertFalse(fs.exists(Location.of("memory://bucket/missing.txt")));
    }

    @Test
    public void testExistsReturnsTrueAfterPut() throws IOException {
        Location loc = Location.of("memory://bucket/file.txt");
        fs.put(loc, "hello".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(fs.exists(loc));
    }

    @Test
    public void testExistsReturnsTrueForDirectoryViaChild() throws IOException {
        fs.put(Location.of("memory://bucket/dir/child.txt"), new byte[0]);
        Assert.assertTrue(fs.exists(Location.of("memory://bucket/dir")));
    }

    // ─────────────────────────── newOutputFile / newInputFile ───────────────────────────

    @Test
    public void testCreateAndRead() throws IOException {
        Location loc = Location.of("memory://bucket/data.bin");
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

        try (OutputStream out = fs.newOutputFile(loc).createOrOverwrite()) {
            out.write(data);
        }

        byte[] stored = fs.get(loc);
        Assert.assertArrayEquals(data, stored);
    }

    @Test
    public void testCreateFailsIfFileExists() throws IOException {
        Location loc = Location.of("memory://bucket/existing.txt");
        fs.put(loc, new byte[0]);

        try (OutputStream out = fs.newOutputFile(loc).create()) {
            out.write(1);
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
            return;
        }
        Assert.fail("Expected IOException for overwriting existing file via create()");
    }

    @Test
    public void testCreateOrOverwriteReplacesExistingFile() throws IOException {
        Location loc = Location.of("memory://bucket/replace.txt");
        fs.put(loc, "old".getBytes(StandardCharsets.UTF_8));

        try (OutputStream out = fs.newOutputFile(loc).createOrOverwrite()) {
            out.write("new".getBytes(StandardCharsets.UTF_8));
        }

        Assert.assertArrayEquals("new".getBytes(StandardCharsets.UTF_8), fs.get(loc));
    }

    @Test
    public void testInputFileLength() throws IOException {
        Location loc = Location.of("memory://bucket/sized.bin");
        byte[] data = new byte[42];
        fs.put(loc, data);

        Assert.assertEquals(42L, fs.newInputFile(loc).length());
    }

    @Test
    public void testInputFileLengthHintSkipsLookup() throws IOException {
        Location loc = Location.of("memory://bucket/hinted.bin");
        fs.put(loc, new byte[10]);

        Assert.assertEquals(99L, fs.newInputFile(loc, 99L).length());
    }

    @Test
    public void testInputFileExistsAndNotExists() throws IOException {
        Location present = Location.of("memory://bucket/present.txt");
        Location absent = Location.of("memory://bucket/absent.txt");
        fs.put(present, new byte[0]);

        Assert.assertTrue(fs.newInputFile(present).exists());
        Assert.assertFalse(fs.newInputFile(absent).exists());
    }

    @Test
    public void testInputFileLocationRoundtrip() throws IOException {
        Location loc = Location.of("memory://bucket/path/file.txt");
        Assert.assertEquals(loc, fs.newInputFile(loc).location());
    }

    @Test
    public void testOutputFileLocationRoundtrip() throws IOException {
        Location loc = Location.of("memory://bucket/path/out.txt");
        Assert.assertEquals(loc, fs.newOutputFile(loc).location());
    }

    // ─────────────────────────── deleteFile ───────────────────────────

    @Test
    public void testDeleteFile() throws IOException {
        Location loc = Location.of("memory://bucket/del.txt");
        fs.put(loc, new byte[1]);
        fs.delete(loc, false);
        Assert.assertFalse(fs.exists(loc));
    }

    @Test
    public void testDeleteFileMissingThrows() {
        try {
            fs.delete(Location.of("memory://bucket/ghost.txt"), false);
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("not found"));
        }
    }

    // ─────────────────────────── renameFile ───────────────────────────

    @Test
    public void testRenameFile() throws IOException {
        Location src = Location.of("memory://bucket/a.txt");
        Location dst = Location.of("memory://bucket/b.txt");
        byte[] data = "rename".getBytes(StandardCharsets.UTF_8);
        fs.put(src, data);

        fs.rename(src, dst);

        Assert.assertFalse(fs.exists(src));
        Assert.assertArrayEquals(data, fs.get(dst));
    }

    @Test
    public void testRenameMissingSourceThrows() {
        try {
            fs.rename(
                    Location.of("memory://bucket/missing.txt"),
                    Location.of("memory://bucket/target.txt"));
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("not found"));
        }
    }

    // ─────────────────────────── deleteDirectory ───────────────────────────

    @Test
    public void testDeleteDirectory() throws IOException {
        fs.put(Location.of("memory://bucket/dirA/f1.txt"), new byte[0]);
        fs.put(Location.of("memory://bucket/dirA/f2.txt"), new byte[0]);
        fs.put(Location.of("memory://bucket/dirB/f3.txt"), new byte[0]);

        fs.delete(Location.of("memory://bucket/dirA"), true);

        Assert.assertFalse(fs.exists(Location.of("memory://bucket/dirA/f1.txt")));
        Assert.assertFalse(fs.exists(Location.of("memory://bucket/dirA/f2.txt")));
        Assert.assertTrue(fs.exists(Location.of("memory://bucket/dirB/f3.txt")));
    }

    // ─────────────────────────── createDirectory ───────────────────────────

    @Test
    public void testCreateDirectory() throws IOException {
        Location dir = Location.of("memory://bucket/newdir");
        fs.mkdirs(dir);
        Assert.assertTrue(fs.exists(dir));
    }

    // ─────────────────────────── renameDirectory ───────────────────────────

    @Test
    public void testRenameDirectory() throws IOException {
        fs.put(Location.of("memory://bucket/src/a.txt"), "a".getBytes(StandardCharsets.UTF_8));
        fs.put(Location.of("memory://bucket/src/b.txt"), "b".getBytes(StandardCharsets.UTF_8));

        fs.rename(
                Location.of("memory://bucket/src"),
                Location.of("memory://bucket/dst"));

        Assert.assertFalse(fs.exists(Location.of("memory://bucket/src/a.txt")));
        Assert.assertArrayEquals("a".getBytes(StandardCharsets.UTF_8),
                fs.get(Location.of("memory://bucket/dst/a.txt")));
        Assert.assertArrayEquals("b".getBytes(StandardCharsets.UTF_8),
                fs.get(Location.of("memory://bucket/dst/b.txt")));
    }

    @Test
    public void testRenameMissingDirectoryThrows() {
        try {
            fs.rename(
                    Location.of("memory://bucket/nosuchdir"),
                    Location.of("memory://bucket/target"));
            Assert.fail("Expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("not found"));
        }
    }

    // ─────────────────────────── listFiles ───────────────────────────

    @Test
    public void testListFilesNonRecursive() throws IOException {
        fs.put(Location.of("memory://bucket/root/a.txt"), new byte[1]);
        fs.put(Location.of("memory://bucket/root/b.txt"), new byte[2]);
        fs.put(Location.of("memory://bucket/root/sub/c.txt"), new byte[3]);

        List<FileEntry> entries = drain(fs.list(Location.of("memory://bucket/root")));

        // Should see a.txt and b.txt but not sub/c.txt (as a file)
        List<String> names = new ArrayList<>();
        for (FileEntry e : entries) {
            if (e.isFile()) {
                names.add(e.name());
            }
        }
        Assert.assertTrue(names.contains("a.txt"));
        Assert.assertTrue(names.contains("b.txt"));
        for (FileEntry e : entries) {
            if (e.isFile()) {
                Assert.assertFalse("sub/c.txt must not appear at top level",
                        e.name().equals("c.txt"));
            }
        }
    }

    @Test
    public void testListFilesRecursive() throws IOException {
        fs.put(Location.of("memory://bucket/tree/a.txt"), new byte[1]);
        fs.put(Location.of("memory://bucket/tree/sub/b.txt"), new byte[2]);
        fs.put(Location.of("memory://bucket/tree/sub/deep/c.txt"), new byte[3]);

        List<FileEntry> entries = fs.listFilesRecursive(Location.of("memory://bucket/tree"));

        List<String> fileNames = new ArrayList<>();
        for (FileEntry e : entries) {
            if (e.isFile()) {
                fileNames.add(e.name());
            }
        }
        Assert.assertTrue(fileNames.contains("a.txt"));
        Assert.assertTrue(fileNames.contains("b.txt"));
        Assert.assertTrue(fileNames.contains("c.txt"));
    }

    @Test
    public void testListFilesEmptyDirectory() throws IOException {
        fs.mkdirs(Location.of("memory://bucket/empty"));
        List<FileEntry> entries = drain(fs.list(Location.of("memory://bucket/empty")));
        Assert.assertTrue(entries.isEmpty());
    }

    @Test
    public void testListFilesCloseable() throws IOException {
        fs.put(Location.of("memory://bucket/closeable/x.txt"), new byte[0]);
        try (FileIterator it = fs.list(Location.of("memory://bucket/closeable"))) {
            Assert.assertTrue(it.hasNext());
        }
        // no exception on close
    }

    // ─────────────────────────── listDirectories ───────────────────────────

    @Test
    public void testListDirectories() throws IOException {
        fs.put(Location.of("memory://bucket/parent/childA/f1.txt"), new byte[0]);
        fs.put(Location.of("memory://bucket/parent/childB/f2.txt"), new byte[0]);

        Set<String> dirs = fs.listDirectories(Location.of("memory://bucket/parent"));

        Assert.assertEquals(2, dirs.size());
        boolean hasA = false;
        boolean hasB = false;
        for (String d : dirs) {
            if (d.contains("childA")) {
                hasA = true;
            }
            if (d.contains("childB")) {
                hasB = true;
            }
        }
        Assert.assertTrue(hasA);
        Assert.assertTrue(hasB);
    }

    @Test
    public void testListDirectoriesEmptyParent() throws IOException {
        fs.mkdirs(Location.of("memory://bucket/leafdir"));
        Set<String> dirs = fs.listDirectories(Location.of("memory://bucket/leafdir"));
        Assert.assertTrue(dirs.isEmpty());
    }

    // ─────────────────────────── deleteFiles (default batch) ───────────────────────────

    @Test
    public void testDeleteFilesMultiple() throws IOException {
        Location a = Location.of("memory://bucket/batch/a.txt");
        Location b = Location.of("memory://bucket/batch/b.txt");
        fs.put(a, new byte[0]);
        fs.put(b, new byte[0]);

        List<Location> toDelete = new ArrayList<>();
        toDelete.add(a);
        toDelete.add(b);
        fs.deleteFiles(toDelete);

        Assert.assertFalse(fs.exists(a));
        Assert.assertFalse(fs.exists(b));
    }

    @Test
    public void testCloseIsIdempotent() throws IOException {
        fs.close();
        fs.close(); // must not throw
    }

    // ─────────────────────────── helpers ───────────────────────────

    private static List<FileEntry> drain(FileIterator it) throws IOException {
        List<FileEntry> result = new ArrayList<>();
        try (FileIterator iter = it) {
            while (iter.hasNext()) {
                result.add(iter.next());
            }
        }
        return result;
    }
}
