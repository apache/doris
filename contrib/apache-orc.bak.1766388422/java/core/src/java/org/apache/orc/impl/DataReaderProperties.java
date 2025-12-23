/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;

import java.util.function.Supplier;

public final class DataReaderProperties {

  private final Supplier<FileSystem> fileSystemSupplier;
  private final Path path;
  private final FSDataInputStream file;
  private final InStream.StreamOptions compression;
  private final boolean zeroCopy;
  private final int maxDiskRangeChunkLimit;
  private final int minSeekSize;
  private final double minSeekSizeTolerance;

  private DataReaderProperties(Builder builder) {
    this.fileSystemSupplier = builder.fileSystemSupplier;
    this.path = builder.path;
    this.file = builder.file;
    this.compression = builder.compression;
    this.zeroCopy = builder.zeroCopy;
    this.maxDiskRangeChunkLimit = builder.maxDiskRangeChunkLimit;
    this.minSeekSize = builder.minSeekSize;
    this.minSeekSizeTolerance = builder.minSeekSizeTolerance;
  }

  public Supplier<FileSystem> getFileSystemSupplier() {
    return fileSystemSupplier;
  }

  public Path getPath() {
    return path;
  }

  public FSDataInputStream getFile() {
    return file;
  }

  public InStream.StreamOptions getCompression() {
    return compression;
  }

  public boolean getZeroCopy() {
    return zeroCopy;
  }

  public int getMaxDiskRangeChunkLimit() {
    return maxDiskRangeChunkLimit;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getMinSeekSize() {
    return minSeekSize;
  }

  public double getMinSeekSizeTolerance() {
    return minSeekSizeTolerance;
  }

  public static class Builder {

    private Supplier<FileSystem> fileSystemSupplier;
    private Path path;
    private FSDataInputStream file;
    private InStream.StreamOptions compression;
    private boolean zeroCopy;
    private int maxDiskRangeChunkLimit =
        (int) OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getDefaultValue();
    private int minSeekSize = (int) OrcConf.ORC_MIN_DISK_SEEK_SIZE.getDefaultValue();
    private double minSeekSizeTolerance = (double) OrcConf.ORC_MIN_DISK_SEEK_SIZE_TOLERANCE
        .getDefaultValue();

    private Builder() {

    }

    public Builder withFileSystemSupplier(Supplier<FileSystem> supplier) {
      this.fileSystemSupplier = supplier;
      return this;
    }

    public Builder withFileSystem(FileSystem filesystem) {
      this.fileSystemSupplier = () -> filesystem;
      return this;
    }

    public Builder withPath(Path path) {
      this.path = path;
      return this;
    }

    public Builder withFile(FSDataInputStream file) {
      this.file = file;
      return this;
    }

    public Builder withCompression(InStream.StreamOptions value) {
      this.compression = value;
      return this;
    }

    public Builder withZeroCopy(boolean zeroCopy) {
      this.zeroCopy = zeroCopy;
      return this;
    }

    public Builder withMaxDiskRangeChunkLimit(int value) {
      maxDiskRangeChunkLimit = value;
      return this;
    }

    public Builder withMinSeekSize(int value) {
      minSeekSize = value;
      return this;
    }

    public Builder withMinSeekSizeTolerance(double value) {
      minSeekSizeTolerance = value;
      return this;
    }

    public DataReaderProperties build() {
      if (fileSystemSupplier == null || path == null) {
        throw new NullPointerException("Filesystem = " + fileSystemSupplier +
                                           ", path = " + path);
      }

      return new DataReaderProperties(this);
    }

  }
}
