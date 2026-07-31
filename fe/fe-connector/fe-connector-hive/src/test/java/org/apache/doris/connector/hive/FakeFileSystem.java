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

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A recording {@link FileSystem} test double for the hive connector's file-listing tests (the module has no
 * Mockito). Configurable to return a canned {@link FileEntry} listing from {@link #list}, or to fail at the
 * resolution boundary ({@link #forLocation}) or the listing boundary ({@link #list}) — exercising the two
 * failure semantics {@code HiveFileListingCache.listFromFileSystem} keeps distinct.
 *
 * <p>{@link #listFiles} deliberately throws {@link AssertionError}: the production lister MUST list via the
 * literal {@link #list} (matching the old {@code FileSystem.listStatus}), never the glob-aware {@code listFiles}
 * override that a real per-scheme filesystem provides — so any test that lists through this double also pins that
 * contract.
 */
final class FakeFileSystem implements FileSystem {

    private List<FileEntry> entries = Collections.emptyList();
    // Tree mode: a per-location listing, keyed by Location.uri(). Populated => list(loc) returns tree.get(uri)
    // (empty if absent); empty (the default) => list() falls back to the flat `entries`, so the existing flat-fake
    // tests are untouched. Needed to model recursive descent, where each sub-directory must list its OWN entries.
    private Map<String, List<FileEntry>> tree = Collections.emptyMap();
    private IOException forLocationError;
    private IOException listError;
    // Per-location list() failures (uri -> error): models "top-level dir lists fine, one sub-directory fails",
    // which the single global listError cannot (it fails every list()).
    private final Map<String, IOException> listErrorByLocation = new HashMap<>();

    FakeFileSystem withEntries(FileEntry... e) {
        this.entries = Arrays.asList(e);
        return this;
    }

    /** Tree mode: {@code list(loc)} returns the entries mapped to {@code loc.uri()} (empty if unmapped). */
    FakeFileSystem withTree(Map<String, List<FileEntry>> t) {
        this.tree = t;
        return this;
    }

    /** Makes {@link #list} throw only for {@code location} (a single failing directory among healthy ones). */
    FakeFileSystem failListAt(String location, IOException e) {
        this.listErrorByLocation.put(location, e);
        return this;
    }

    /** Makes {@link #forLocation} throw — the SYSTEMIC (scheme/storage resolution) boundary. */
    FakeFileSystem failForLocation(IOException e) {
        this.forLocationError = e;
        return this;
    }

    /** Makes {@link #list} throw — the LOCAL per-directory boundary (or, with UnsupportedFileSystemException,
     * the lazily-surfaced systemic scheme-not-registered case). */
    FakeFileSystem failList(IOException e) {
        this.listError = e;
        return this;
    }

    static FileEntry file(String uri, long length, long modificationTime) {
        return new FileEntry(Location.of(uri), length, false, modificationTime, null);
    }

    static FileEntry dir(String uri) {
        return new FileEntry(Location.of(uri), 0L, true, 0L, null);
    }

    @Override
    public FileSystem forLocation(Location location) throws IOException {
        if (forLocationError != null) {
            throw forLocationError;
        }
        return this;
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        IOException perLocation = listErrorByLocation.get(location.uri());
        if (perLocation != null) {
            throw perLocation;
        }
        if (listError != null) {
            throw listError;
        }
        if (!tree.isEmpty()) {
            return new ListFileIterator(tree.getOrDefault(location.uri(), Collections.emptyList()).iterator());
        }
        return new ListFileIterator(entries.iterator());
    }

    @Override
    public List<FileEntry> listFiles(Location dir) {
        throw new AssertionError(
                "listFromFileSystem must list via the literal list(), never the glob-aware listFiles()");
    }

    // ---- unused abstract methods (no listing test drives them) ----

    @Override
    public boolean exists(Location location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mkdirs(Location location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Location location, boolean recursive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(Location src, Location dst) {
        throw new UnsupportedOperationException();
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

    private static final class ListFileIterator implements FileIterator {
        private final Iterator<FileEntry> it;

        ListFileIterator(Iterator<FileEntry> it) {
            this.it = it;
        }

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
    }
}
