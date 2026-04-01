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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Core filesystem abstraction.
 * All methods throw IOException (not UserException or checked vendor exceptions).
 */
public interface FileSystem extends AutoCloseable {

    boolean exists(Location location) throws IOException;

    void mkdirs(Location location) throws IOException;

    void delete(Location location, boolean recursive) throws IOException;

    /**
     * Deletes multiple files. The default implementation calls
     * {@link #delete(Location, boolean)} with {@code recursive=false} for each location.
     * Implementations may override for batch efficiency.
     */
    default void deleteFiles(Collection<Location> locations) throws IOException {
        for (Location location : locations) {
            delete(location, false);
        }
    }

    void rename(Location src, Location dst) throws IOException;

    FileIterator list(Location location) throws IOException;

    /**
     * Returns all non-directory entries directly under {@code dir} (non-recursive).
     * The default implementation iterates {@link #list(Location)} and filters out directories.
     */
    default List<FileEntry> listFiles(Location dir) throws IOException {
        List<FileEntry> result = new ArrayList<>();
        try (FileIterator it = list(dir)) {
            while (it.hasNext()) {
                FileEntry e = it.next();
                if (!e.isDirectory()) {
                    result.add(e);
                }
            }
        }
        return result;
    }

    /**
     * Returns all non-directory entries under {@code dir}, recursively traversing
     * sub-directories reported by {@link #list(Location)}.
     *
     * <p>For object-storage backends (e.g. S3) that return a flat list of objects from
     * {@link #list}, this method is equivalent to {@link #listFiles(Location)}.
     */
    default List<FileEntry> listFilesRecursive(Location dir) throws IOException {
        List<FileEntry> result = new ArrayList<>();
        try (FileIterator it = list(dir)) {
            while (it.hasNext()) {
                FileEntry e = it.next();
                if (e.isDirectory()) {
                    result.addAll(listFilesRecursive(e.location()));
                } else {
                    result.add(e);
                }
            }
        }
        return result;
    }

    /**
     * Returns the URIs of direct child <em>directories</em> under {@code dir}.
     *
     * <p>The default implementation iterates {@link #list(Location)} and collects entries
     * where {@link FileEntry#isDirectory()} is {@code true}.  Object-storage backends that
     * do not model directories (e.g. S3) will return an empty set.
     */
    default Set<String> listDirectories(Location dir) throws IOException {
        Set<String> result = new HashSet<>();
        try (FileIterator it = list(dir)) {
            while (it.hasNext()) {
                FileEntry e = it.next();
                if (e.isDirectory()) {
                    result.add(e.location().uri());
                }
            }
        }
        return result;
    }

    /**
     * Renames (moves) the directory at {@code src} to {@code dst}.
     *
     * <p>If {@code src} does not exist, {@code whenSrcNotExists} is invoked and the method
     * returns without error—this matches the semantics of
     * {@code LegacyFileSystemApi.renameDir(src, dst, Runnable)}.
     *
     * <p>The default implementation delegates to {@link #exists(Location)} and
     * {@link #rename(Location, Location)}.  Implementations backed by file-systems that offer
     * an atomic directory-rename operation should override this method.
     */
    default void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        if (!exists(src)) {
            whenSrcNotExists.run();
            return;
        }
        rename(src, dst);
    }

    DorisInputFile newInputFile(Location location) throws IOException;

    /**
     * Opens an input file at {@code location} with a pre-known length hint.
     * Implementations may use the hint to skip a remote size query; they must
     * NOT trust the hint for bounds-checking if it could be inaccurate.
     *
     * <p>Default implementation ignores the length hint.
     *
     * @param location the file location
     * @param length   the expected file length in bytes; ignored if &le; 0
     */
    default DorisInputFile newInputFile(Location location, long length) throws IOException {
        return newInputFile(location);
    }

    DorisOutputFile newOutputFile(Location location) throws IOException;

    /**
     * Lists objects under {@code path} whose names match the given glob pattern, returning
     * at most {@code maxFiles} entries whose total size does not exceed {@code maxBytes}.
     *
     * <p>The listing is resumed from {@code startAfter} (exclusive, lexicographic order) when
     * non-null and non-empty.  Results are returned in ascending lexicographic key order.
     *
     * <p>The returned {@link GlobListing#getMaxFile()} contains the last key <em>seen</em>
     * in the full listing (which may be beyond the page limit), allowing callers to detect
     * whether additional objects exist without issuing another request.
     *
     * @param path       the base object-storage URI (may include a glob pattern, e.g.
     *                   {@code s3://bucket/prefix/*.csv})
     * @param startAfter exclusive lower-bound key; pass {@code null} or empty to start from the
     *                   beginning
     * @param maxBytes   maximum cumulative object size (bytes) for the returned page; 0 means
     *                   unlimited
     * @param maxFiles   maximum number of objects in the returned page; 0 means unlimited
     * @return {@link GlobListing} with matched entries plus listing metadata
     * @throws UnsupportedOperationException if the implementation does not support glob listing
     */
    default GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        throw new UnsupportedOperationException(
                "globListWithLimit is not supported by " + getClass().getSimpleName());
    }

    @Override
    void close() throws IOException;
}
