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

import java.util.List;

/**
 * Immutable representation of a file or directory entry returned by FileSystem.list().
 */
public final class FileEntry {

    private final Location location;
    private final long length;
    private final boolean isDirectory;
    private final long modificationTime;
    private final List<BlockInfo> blocks;

    public FileEntry(Location location, long length, boolean isDirectory,
            long modificationTime, List<BlockInfo> blocks) {
        this.location = location;
        this.length = length;
        this.isDirectory = isDirectory;
        this.modificationTime = modificationTime;
        this.blocks = blocks == null ? List.of() : List.copyOf(blocks);
    }

    public Location location() {
        return location;
    }

    public long length() {
        return length;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    /** Last-modified time in milliseconds since epoch. 0 if not available. */
    public long modificationTime() {
        return modificationTime;
    }

    public List<BlockInfo> blocks() {
        return blocks;
    }
}
