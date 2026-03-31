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

    @Override
    void close() throws IOException;
}
