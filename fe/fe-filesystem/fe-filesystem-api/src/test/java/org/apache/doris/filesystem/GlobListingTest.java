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

import java.util.ArrayList;
import java.util.List;

class GlobListingTest {

    @Test
    void gettersReturnCorrectValues() {
        FileEntry entry = new FileEntry(Location.of("s3://bucket/key"), 100, false, 0, null);
        GlobListing listing = new GlobListing(List.of(entry), "bucket", "prefix/", "prefix/z.csv");

        Assertions.assertEquals(1, listing.getFiles().size());
        Assertions.assertEquals("bucket", listing.getBucket());
        Assertions.assertEquals("prefix/", listing.getPrefix());
        Assertions.assertEquals("prefix/z.csv", listing.getMaxFile());
    }

    @Test
    void filesListIsImmutable() {
        FileEntry entry = new FileEntry(Location.of("s3://b/k"), 10, false, 0, null);
        GlobListing listing = new GlobListing(List.of(entry), "b", "", "");

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> listing.getFiles().add(new FileEntry(Location.of("s3://b/k2"), 0, false, 0, null)));
    }

    @Test
    void modifyingSourceListDoesNotAffectGlobListing() {
        FileEntry entry = new FileEntry(Location.of("s3://b/k"), 10, false, 0, null);
        List<FileEntry> source = new ArrayList<>();
        source.add(entry);
        GlobListing listing = new GlobListing(source, "b", "", "");

        // Modify the source list after construction
        source.add(new FileEntry(Location.of("s3://b/k2"), 20, false, 0, null));
        Assertions.assertEquals(1, listing.getFiles().size());
    }

    @Test
    void emptyFilesListIsHandled() {
        GlobListing listing = new GlobListing(List.of(), "b", "p", "");
        Assertions.assertTrue(listing.getFiles().isEmpty());
    }
}
