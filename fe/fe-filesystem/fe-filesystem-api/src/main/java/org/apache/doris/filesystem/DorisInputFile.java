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

/**
 * Represents a readable file in the filesystem.
 *
 * <p>The minimal contract ({@link #location()}, {@link #length()}, {@link #newStream()}) is
 * required by all implementations. Optional capabilities ({@link #exists()},
 * {@link #lastModifiedTime()}) provide defaults that throw {@link UnsupportedOperationException};
 * implementations should override them where the underlying storage supports the operation.
 */
public interface DorisInputFile {

    /** Returns the location of this file. */
    Location location();

    /**
     * Returns the length of the file in bytes.
     *
     * @throws IOException if the length cannot be determined
     */
    long length() throws IOException;

    /**
     * Returns {@code true} if the file exists on the remote filesystem.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}.
     * Override in implementations that can perform a lightweight existence check
     * (e.g., a HEAD request on object storage, or {@code stat()} on HDFS).
     *
     * @throws IOException if the check fails for a reason other than "not found"
     */
    default boolean exists() throws IOException {
        throw new UnsupportedOperationException(
                "exists() is not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns the last-modified time of this file in milliseconds since the Unix epoch.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}.
     *
     * @throws IOException if the metadata cannot be retrieved
     */
    default long lastModifiedTime() throws IOException {
        throw new UnsupportedOperationException(
                "lastModifiedTime() is not supported by " + getClass().getSimpleName());
    }

    /**
     * Opens a new {@link DorisInputStream} positioned at offset 0.
     * Each call returns an independent stream; callers must close it after use.
     *
     * @throws IOException if the stream cannot be opened
     */
    DorisInputStream newStream() throws IOException;
}
