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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable record describing a file or directory in a storage system.
 * <p>
 * Replaces {@link org.apache.doris.fs.remote.RemoteFile} with zero Hadoop dependency.
 * Block location information uses {@link BlockInfo} instead of {@code BlockLocation[]}.
 */
public final class FileEntry {

    private final Location location;
    private final boolean directory;
    private final long length;
    private final long blockSize;
    private final long modificationTime;
    private final List<BlockInfo> blocks;

    private FileEntry(Builder builder) {
        this.location = Objects.requireNonNull(builder.location, "location");
        this.directory = builder.directory;
        this.length = builder.length;
        this.blockSize = builder.blockSize;
        this.modificationTime = builder.modificationTime;
        this.blocks = builder.blocks == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(builder.blocks);
    }

    /** Full location (URI) of this file or directory. */
    public Location location() {
        return location;
    }

    /** Returns the last component of the path (file or directory name). */
    public String name() {
        return location.name();
    }

    public boolean isDirectory() {
        return directory;
    }

    public boolean isFile() {
        return !directory;
    }

    /** File size in bytes. -1 if unknown. */
    public long length() {
        return length;
    }

    /** Underlying block size of the storage (e.g., HDFS 128 MB). 0 for object storage. */
    public long blockSize() {
        return blockSize;
    }

    /** Last-modified time in milliseconds since epoch. */
    public long modificationTime() {
        return modificationTime;
    }

    /** Block location hints for split planning. Empty list for object storage. */
    public List<BlockInfo> blocks() {
        return blocks;
    }

    @Override
    public String toString() {
        return "FileEntry{location=" + location
                + ", directory=" + directory
                + ", length=" + length
                + ", modificationTime=" + modificationTime + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FileEntry)) {
            return false;
        }
        FileEntry that = (FileEntry) o;
        return directory == that.directory
                && length == that.length
                && modificationTime == that.modificationTime
                && location.equals(that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, directory, length, modificationTime);
    }

    public static Builder builder(Location location) {
        return new Builder(location);
    }

    /** Convenience factory for simple file entries (no block info). */
    public static FileEntry file(Location location, long length, long modificationTime) {
        return builder(location).length(length).modificationTime(modificationTime).build();
    }

    /** Convenience factory for directory entries. */
    public static FileEntry directory(Location location, long modificationTime) {
        return builder(location).directory(true).modificationTime(modificationTime).build();
    }

    public static final class Builder {
        private final Location location;
        private boolean directory = false;
        private long length = -1L;
        private long blockSize = 0L;
        private long modificationTime = 0L;
        private List<BlockInfo> blocks;

        private Builder(Location location) {
            this.location = Objects.requireNonNull(location);
        }

        public Builder directory(boolean isDirectory) {
            this.directory = isDirectory;
            return this;
        }

        public Builder length(long fileLength) {
            this.length = fileLength;
            return this;
        }

        public Builder blockSize(long bSize) {
            this.blockSize = bSize;
            return this;
        }

        public Builder modificationTime(long modTime) {
            this.modificationTime = modTime;
            return this;
        }

        public Builder blocks(List<BlockInfo> blockList) {
            this.blocks = blockList;
            return this;
        }

        public Builder addBlock(BlockInfo block) {
            if (this.blocks == null) {
                this.blocks = new java.util.ArrayList<>();
            }
            this.blocks.add(block);
            return this;
        }

        public FileEntry build() {
            return new FileEntry(this);
        }
    }

    /**
     * Lightweight replacement for {@code org.apache.hadoop.fs.BlockLocation}.
     * Contains only the data needed by Doris split planning.
     */
    public static final class BlockInfo {
        private final long offset;
        private final long length;
        private final List<String> hosts;

        public BlockInfo(long offset, long length, List<String> hosts) {
            this.offset = offset;
            this.length = length;
            this.hosts = hosts == null ? Collections.emptyList() : Collections.unmodifiableList(hosts);
        }

        public long offset() {
            return offset;
        }

        public long length() {
            return length;
        }

        /** Data-local host names for this block (for scheduling affinity). */
        public List<String> hosts() {
            return hosts;
        }

        @Override
        public String toString() {
            return "BlockInfo{offset=" + offset + ", length=" + length + ", hosts=" + hosts + "}";
        }
    }
}
