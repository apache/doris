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

package org.apache.orc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.OrcConf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * This class provides an adaptor so that tools that want to read an ORC
 * file from an FSDataInputStream can do so. Create an instance with the
 * stream, path, and fileSize and pass it in to the reader as the FileSystem.
 */
public class StreamWrapperFileSystem extends FileSystem {
  private final FSDataInputStream stream;
  private final FileStatus status;

  /**
   * Create a FileSystem that only has information about the given stream.
   * @param stream the data of the stream
   * @param status the file status of the stream
   * @param conf the configuration to use
   */
  public StreamWrapperFileSystem(FSDataInputStream stream,
                                 FileStatus status,
                                 Configuration conf) {
    this.stream = stream;
    this.status = status;
    setConf(conf);
  }

  /**
   * Create a FileSystem that only has information about the given stream.
   * @param stream the data of the stream
   * @param path the file name of the stream
   * @param fileSize the length of the stream in bytes
   * @param conf the configuration to use
   */
  public StreamWrapperFileSystem(FSDataInputStream stream,
                                 Path path,
                                 long fileSize,
                                 Configuration conf) {
    this(stream,
        new FileStatus(fileSize, false, 1, OrcConf.BLOCK_SIZE.getInt(conf), 0, path),
        conf);
  }

  @Override
  public URI getUri() {
    return URI.create("stream://" + status.getPath());
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    if (status.getPath().equals(path)) {
      return stream;
    } else {
      throw new FileNotFoundException(path.toString());
    }
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                   boolean b, int i, short i1, long l,
                                   Progressable progressable) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public FSDataOutputStream append(Path path, int i,
                                   Progressable progressable) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public boolean rename(Path path, Path path1) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public boolean delete(Path path, boolean b) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public Path getWorkingDirectory() {
    return status.getPath().getParent();
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) {
    throw new UnsupportedOperationException("Write operations on " +
        getClass().getName());
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    return new FileStatus[]{getFileStatus(path)};
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (status.getPath().equals(path)) {
      return status;
    } else {
      throw new FileNotFoundException(path.toString());
    }
  }
}
