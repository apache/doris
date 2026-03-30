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

package org.apache.doris.fs.remote;

import org.apache.doris.fs.FileEntry;
import org.apache.doris.fs.Location;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

/**
 * Represents a file or directory in remote storage.
 *
 * @deprecated use {@link FileEntry} instead, which has no Hadoop dependency
 */
@Deprecated
// represent a file or a dir in remote storage
public class RemoteFile {
    // Only file name, not full path
    private final String name;
    private final boolean isFile;
    private final boolean isDirectory;
    private final long size;
    // Block size of underlying file system. e.g. HDFS and S3.
    // A large file will split into multiple blocks. The blocks are transparent to the user.
    // Default block size for HDFS 2.x is 128M.
    private final long blockSize;
    private long modificationTime;
    private Path path;
    BlockLocation[] blockLocations;

    public RemoteFile(String name, boolean isFile, long size, long blockSize) {
        this(name, null, isFile, !isFile, size, blockSize, 0, null);
    }

    public RemoteFile(String name, boolean isFile, long size, long blockSize, long modificationTime) {
        this(name, null, isFile, !isFile, size, blockSize, modificationTime, null);
    }

    public RemoteFile(Path path, boolean isDirectory, long size, long blockSize, long modificationTime,
            BlockLocation[] blockLocations) {
        this(path.getName(), path, !isDirectory, isDirectory, size, blockSize, modificationTime, blockLocations);
    }

    public RemoteFile(String name, Path path, boolean isFile, boolean isDirectory,
            long size, long blockSize, long modificationTime, BlockLocation[] blockLocations) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        this.name = name;
        this.isFile = isFile;
        this.isDirectory = isDirectory;
        this.size = size;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.path = path;
        this.blockLocations = blockLocations;
    }

    public String getName() {
        return name;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public boolean isFile() {
        return isFile;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public long getSize() {
        return size;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public BlockLocation[] getBlockLocations() {
        return blockLocations;
    }

    /**
     * Converts this RemoteFile to a {@link FileEntry}.
     *
     * @return a new FileEntry representing the same file metadata
     */
    public FileEntry toFileEntry() {
        FileEntry.Builder builder = FileEntry.builder(
                        path != null ? Location.of(path.toString()) : Location.of(name))
                .directory(isDirectory)
                .length(size)
                .blockSize(blockSize)
                .modificationTime(modificationTime);
        if (blockLocations != null) {
            for (BlockLocation bl : blockLocations) {
                try {
                    builder.addBlock(new FileEntry.BlockInfo(
                            bl.getOffset(), bl.getLength(), java.util.Arrays.asList(bl.getHosts())));
                } catch (java.io.IOException e) {
                    builder.addBlock(new FileEntry.BlockInfo(
                            bl.getOffset(), bl.getLength(), java.util.Collections.emptyList()));
                }
            }
        }
        return builder.build();
    }

    /**
     * Creates a RemoteFile from a {@link FileEntry}.
     *
     * @param entry the FileEntry to convert
     * @return a new RemoteFile representing the same file metadata
     */
    public static RemoteFile fromFileEntry(FileEntry entry) {
        Path hadoopPath = new Path(entry.location().toString());
        BlockLocation[] bls = new BlockLocation[entry.blocks().size()];
        for (int i = 0; i < bls.length; i++) {
            FileEntry.BlockInfo bi = entry.blocks().get(i);
            bls[i] = new BlockLocation(null, bi.hosts().toArray(new String[0]), bi.offset(), bi.length());
        }
        return new RemoteFile(
                hadoopPath,
                entry.isDirectory(),
                entry.length(),
                entry.blockSize(),
                entry.modificationTime(),
                bls.length == 0 ? null : bls);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RemoteFile [");
        sb.append("name: ").append(name);
        sb.append(", isFile: ").append(isFile);
        sb.append(", isDirectory: ").append(isDirectory);
        sb.append(", size: ").append(size);
        sb.append(", blockSize: ").append(blockSize);
        sb.append(", modificationTime: ").append(modificationTime);
        sb.append(", path: ").append(path);
        sb.append("]");

        return sb.toString();
    }
}
