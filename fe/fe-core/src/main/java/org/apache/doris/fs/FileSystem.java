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

import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Core filesystem abstraction for Apache Doris.
 * <p>
 * All methods use {@link Location} for path arguments and throw {@link IOException}
 * on failure rather than returning a {@link org.apache.doris.backup.Status} object.
 * <p>
 * Implementations must be thread-safe. Instances may be shared and cached.
 *
 * @see LegacyFileSystemAdapter for adapting existing Status-based implementations
 * @see MemoryFileSystem for in-memory testing
 * @see LegacyFileSystemApi for the legacy Status-based interface (deprecated)
 */
public interface FileSystem extends Closeable {

    // ─────────────────────────── I/O FILE ACCESS ───────────────────────────

    /**
     * Creates a new {@link DorisInputFile} for reading from the given location.
     * Does not check existence; the file is opened lazily when
     * {@link DorisInputFile#newInput()} or {@link DorisInputFile#newStream()} is called.
     */
    DorisInputFile newInputFile(Location location);

    /**
     * Creates a new {@link DorisInputFile} with a known file length.
     * The {@code length} hint may be used to skip a remote {@code stat()} call.
     */
    DorisInputFile newInputFile(Location location, long length);

    /**
     * Creates a new {@link DorisOutputFile} for writing to the given location.
     */
    DorisOutputFile newOutputFile(Location location);

    // ─────────────────────────── METADATA OPERATIONS ───────────────────────────

    /**
     * Returns {@code true} if the location exists (file or directory).
     *
     * @throws IOException if the existence check fails for a reason other than "not found"
     */
    boolean exists(Location location) throws IOException;

    // ─────────────────────────── MUTATION OPERATIONS ───────────────────────────

    /**
     * Deletes the file at the given location.
     *
     * @throws IOException if deletion fails or the location is a directory
     */
    void deleteFile(Location location) throws IOException;

    /**
     * Deletes multiple files. Default implementation calls {@link #deleteFile} in a loop;
     * implementations may override for batch efficiency.
     */
    default void deleteFiles(Collection<Location> locations) throws IOException {
        for (Location location : locations) {
            deleteFile(location);
        }
    }

    /**
     * Atomically renames (moves) a file from {@code source} to {@code target}.
     *
     * @throws IOException if the rename fails
     */
    void renameFile(Location source, Location target) throws IOException;

    /**
     * Recursively deletes a directory and all its contents.
     *
     * @throws IOException if deletion fails
     */
    void deleteDirectory(Location location) throws IOException;

    /**
     * Creates a directory (and any missing parent directories).
     *
     * @throws IOException if creation fails
     */
    void createDirectory(Location location) throws IOException;

    /**
     * Renames (moves) a directory from {@code source} to {@code target}.
     *
     * @throws IOException if the rename fails
     */
    void renameDirectory(Location source, Location target) throws IOException;

    // ─────────────────────────── LISTING OPERATIONS ───────────────────────────

    /**
     * Returns a lazy iterator over files under {@code location}.
     * <p>
     * If {@code recursive} is {@code false}, only direct children are listed.
     * If {@code recursive} is {@code true}, the entire subtree is traversed.
     * The iterator must be closed after use (use try-with-resources).
     *
     * @throws IOException if the listing request fails
     */
    FileIterator listFiles(Location location, boolean recursive) throws IOException;

    /**
     * Returns the set of direct child directory locations under {@code location}.
     *
     * @throws IOException if the listing request fails
     */
    Set<Location> listDirectories(Location location) throws IOException;

    /**
     * Returns a lazy iterator of files matching the glob {@code pattern}.
     * Default implementation throws {@link UnsupportedOperationException}.
     * Override for native glob support (e.g., HDFS glob).
     */
    default FileIterator globFiles(Location pattern) throws IOException {
        throw new UnsupportedOperationException(
                "globFiles not supported by " + getClass().getSimpleName());
    }

    /**
     * Releases resources held by this filesystem (connections, thread pools, etc.).
     * Idempotent — safe to call multiple times.
     */
    @Override
    void close() throws IOException;
}
