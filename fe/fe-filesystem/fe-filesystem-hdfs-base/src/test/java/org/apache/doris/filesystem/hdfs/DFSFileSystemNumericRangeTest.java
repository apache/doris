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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class DFSFileSystemNumericRangeTest {
    @TempDir
    Path directory;

    @Test
    void brokerLoadListingExpandsNumericRangesAndFiltersDirectories() throws Exception {
        for (int i = 1; i <= 3; i++) {
            Files.writeString(directory.resolve("data_" + i + ".txt"), "row");
        }
        Files.createDirectory(directory.resolve("data_4.txt"));
        try (DFSFileSystem fs = new DFSFileSystem(Map.of())) {
            List<FileEntry> files = Assertions.assertDoesNotThrow(
                    () -> fs.listFiles(location("data_{1..4}.txt")));
            Assertions.assertEquals(List.of("data_1.txt", "data_2.txt", "data_3.txt"), names(files));
            Assertions.assertEquals(names(files), names(fs.listFiles(location("data_{3..1}.txt"))));
            Assertions.assertEquals(names(files), names(fs.listFiles(location("data_{1..2,2,3}.txt"))));
            Assertions.assertEquals(List.of("data_1.txt", "data_3.txt"),
                    names(fs.listFiles(location("data_{1,3}.txt"))));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> fs.listFiles(location("data_{1..10001}.txt")));
        }
    }

    @Test
    void paginatedListingUsesTheSameExpandedPattern() throws Exception {
        for (int i = 1; i <= 3; i++) {
            Files.writeString(directory.resolve("data_" + i + ".txt"), "row");
        }
        try (DFSFileSystem fs = new DFSFileSystem(Map.of())) {
            GlobListing first = Assertions.assertDoesNotThrow(
                    () -> fs.globListWithLimit(location("data_{1..3}.txt"), null, 0, 1));
            Assertions.assertEquals(List.of("data_1.txt"), names(first.getFiles()));
            GlobListing remaining = fs.globListWithLimit(location("data_{1..3}.txt"),
                    first.getFiles().get(0).location().uri(), 0, 10);
            Assertions.assertEquals(List.of("data_2.txt", "data_3.txt"), names(remaining.getFiles()));
        }
    }

    private Location location(String name) {
        return Location.of(directory.toUri() + name);
    }

    private List<String> names(List<FileEntry> files) {
        return files.stream().map(f -> f.location().uri().substring(f.location().uri().lastIndexOf('/') + 1))
                .sorted().collect(Collectors.toList());
    }
}
