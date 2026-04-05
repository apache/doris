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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Tests for the default methods defined in the {@link FileSystem} interface.
 * Uses a minimal stub implementation to verify behavior.
 */
class FileSystemDefaultMethodsTest {

    /**
     * Minimal FileSystem stub that supports list() via a configurable set of entries.
     * All other methods throw UnsupportedOperationException.
     */
    private static class StubFileSystem implements FileSystem {

        private final List<FileEntry> entries;
        private boolean existsResult = true;
        private int deleteCallCount = 0;
        private final List<Location> deletedLocations = new ArrayList<>();
        private boolean renameCalled = false;

        StubFileSystem(List<FileEntry> entries) {
            this.entries = entries;
        }

        void setExistsResult(boolean result) {
            this.existsResult = result;
        }

        @Override
        public boolean exists(Location location) {
            return existsResult;
        }

        @Override
        public void mkdirs(Location location) {
        }

        @Override
        public void delete(Location location, boolean recursive) {
            deleteCallCount++;
            deletedLocations.add(location);
        }

        @Override
        public void rename(Location src, Location dst) {
            renameCalled = true;
        }

        @Override
        public FileIterator list(Location location) {
            Iterator<FileEntry> it = entries.iterator();
            return new FileIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public FileEntry next() {
                    return it.next();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public DorisInputFile newInputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DorisOutputFile newOutputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static FileEntry fileEntry(String uri, long size) {
        return new FileEntry(Location.of(uri), size, false, 0, null);
    }

    private static FileEntry dirEntry(String uri) {
        return new FileEntry(Location.of(uri), 0, true, 0, null);
    }

    // --- listFiles ---

    @Test
    void listFilesFiltersOutDirectories() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of(
                fileEntry("s3://b/dir/a.csv", 100),
                dirEntry("s3://b/dir/subdir"),
                fileEntry("s3://b/dir/b.csv", 200)
        ));

        List<FileEntry> files = fs.listFiles(Location.of("s3://b/dir"));
        Assertions.assertEquals(2, files.size());
        Assertions.assertEquals("a.csv", files.get(0).name());
        Assertions.assertEquals("b.csv", files.get(1).name());
    }

    @Test
    void listFilesReturnsEmptyForDirectoryOnly() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of(
                dirEntry("s3://b/dir/sub1"),
                dirEntry("s3://b/dir/sub2")
        ));

        List<FileEntry> files = fs.listFiles(Location.of("s3://b/dir"));
        Assertions.assertTrue(files.isEmpty());
    }

    // --- listDirectories ---

    @Test
    void listDirectoriesCollectsOnlyDirectories() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of(
                fileEntry("s3://b/dir/a.csv", 100),
                dirEntry("s3://b/dir/subdir1"),
                dirEntry("s3://b/dir/subdir2"),
                fileEntry("s3://b/dir/b.csv", 200)
        ));

        Set<String> dirs = fs.listDirectories(Location.of("s3://b/dir"));
        Assertions.assertEquals(2, dirs.size());
        Assertions.assertTrue(dirs.contains("s3://b/dir/subdir1"));
        Assertions.assertTrue(dirs.contains("s3://b/dir/subdir2"));
    }

    // --- deleteFiles ---

    @Test
    void deleteFilesCallsDeleteForEachLocation() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of());
        Collection<Location> toDelete = List.of(
                Location.of("s3://b/a"), Location.of("s3://b/b"), Location.of("s3://b/c"));

        fs.deleteFiles(toDelete);
        Assertions.assertEquals(3, fs.deleteCallCount);
        Assertions.assertEquals(Location.of("s3://b/a"), fs.deletedLocations.get(0));
        Assertions.assertEquals(Location.of("s3://b/b"), fs.deletedLocations.get(1));
        Assertions.assertEquals(Location.of("s3://b/c"), fs.deletedLocations.get(2));
    }

    // --- renameDirectory ---

    @Test
    void renameDirectoryWhenSrcExistsCallsRename() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of());
        fs.setExistsResult(true);

        Runnable callback = () -> {
            throw new AssertionError("should not be called");
        };
        fs.renameDirectory(Location.of("s3://b/src"), Location.of("s3://b/dst"), callback);
        Assertions.assertTrue(fs.renameCalled);
    }

    @Test
    void renameDirectoryWhenSrcNotExistsCallsCallback() throws IOException {
        StubFileSystem fs = new StubFileSystem(List.of());
        fs.setExistsResult(false);

        boolean[] callbackCalled = {false};
        fs.renameDirectory(Location.of("s3://b/src"), Location.of("s3://b/dst"),
                () -> callbackCalled[0] = true);
        Assertions.assertTrue(callbackCalled[0]);
    }

    // --- newInputFile with length hint ---

    @Test
    void newInputFileWithLengthDelegatesToSingleArgVersion() {
        // The default implementation of newInputFile(location, length) delegates to
        // newInputFile(location). Since our stub throws UnsupportedOperationException,
        // we verify the delegation by expecting the same exception.
        StubFileSystem fs = new StubFileSystem(List.of());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> fs.newInputFile(Location.of("s3://b/f"), 100));
    }

    // --- globListWithLimit ---

    @Test
    void globListWithLimitThrowsUnsupportedByDefault() {
        StubFileSystem fs = new StubFileSystem(List.of());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> fs.globListWithLimit(Location.of("s3://b/*"), null, 0, 0));
    }
}
