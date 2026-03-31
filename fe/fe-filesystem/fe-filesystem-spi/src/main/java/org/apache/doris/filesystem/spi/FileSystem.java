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

package org.apache.doris.filesystem.spi;

import java.io.IOException;

/**
 * Core filesystem abstraction.
 * All methods throw IOException (not UserException or checked vendor exceptions).
 */
public interface FileSystem extends AutoCloseable {

    boolean exists(Location location) throws IOException;

    void mkdirs(Location location) throws IOException;

    void delete(Location location, boolean recursive) throws IOException;

    void rename(Location src, Location dst) throws IOException;

    FileIterator list(Location location) throws IOException;

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
