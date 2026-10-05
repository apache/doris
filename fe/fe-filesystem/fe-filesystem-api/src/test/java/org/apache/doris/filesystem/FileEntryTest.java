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

import java.util.List;

class FileEntryTest {

    @Test
    void nameForRegularFile() {
        FileEntry entry = new FileEntry(Location.of("s3://bucket/dir/file.csv"), 100, false, 0, null);
        Assertions.assertEquals("file.csv", entry.name());
    }

    @Test
    void nameForDirectory() {
        FileEntry entry = new FileEntry(Location.of("s3://bucket/dir/subdir"), 0, true, 0, null);
        Assertions.assertEquals("subdir", entry.name());
    }

    @Test
    void nameForDirectoryWithTrailingSlash() {
        FileEntry entry = new FileEntry(Location.of("s3://bucket/dir/subdir/"), 0, true, 0, null);
        Assertions.assertEquals("subdir", entry.name());
    }

    @Test
    void nameForRootFile() {
        FileEntry entry = new FileEntry(Location.of("s3://bucket/file.txt"), 50, false, 0, null);
        Assertions.assertEquals("file.txt", entry.name());
    }

    @Test
    void isFileReturnsTrueForFile() {
        FileEntry entry = new FileEntry(Location.of("s3://b/f"), 1, false, 0, null);
        Assertions.assertTrue(entry.isFile());
        Assertions.assertFalse(entry.isDirectory());
    }

    @Test
    void isDirectoryReturnsTrueForDirectory() {
        FileEntry entry = new FileEntry(Location.of("s3://b/d"), 0, true, 0, null);
        Assertions.assertTrue(entry.isDirectory());
        Assertions.assertFalse(entry.isFile());
    }

    @Test
    void blocksReturnsEmptyListWhenNull() {
        FileEntry entry = new FileEntry(Location.of("s3://b/f"), 0, false, 0, null);
        Assertions.assertTrue(entry.blocks().isEmpty());
    }

    @Test
    void blocksReturnsImmutableCopy() {
        BlockInfo block = new BlockInfo(0, 100, new String[]{"host1"});
        List<BlockInfo> original = new java.util.ArrayList<>();
        original.add(block);
        FileEntry entry = new FileEntry(Location.of("s3://b/f"), 100, false, 0, original);

        Assertions.assertEquals(1, entry.blocks().size());
        // Modifying the original list should NOT affect the entry
        original.add(new BlockInfo(100, 200, new String[]{"host2"}));
        Assertions.assertEquals(1, entry.blocks().size());
    }

    @Test
    void blocksListIsUnmodifiable() {
        BlockInfo block = new BlockInfo(0, 100, new String[]{"host1"});
        FileEntry entry = new FileEntry(Location.of("s3://b/f"), 100, false, 0, List.of(block));

        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> entry.blocks().add(new BlockInfo(0, 0, null)));
    }

    @Test
    void locationAndLengthAndModTime() {
        Location loc = Location.of("hdfs://nn/path/to/file");
        FileEntry entry = new FileEntry(loc, 12345L, false, 9999L, null);
        Assertions.assertEquals(loc, entry.location());
        Assertions.assertEquals(12345L, entry.length());
        Assertions.assertEquals(9999L, entry.modificationTime());
    }
}
