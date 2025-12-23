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

package org.apache.orc.bench.core;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class NullFileSystem extends FileSystem {
  @Override
  public URI getUri() {
    try {
      return new URI("null:///");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad URL", e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int i) {
    return new FSDataInputStream(new InputStream() {
      @Override
      public int read() {
        return -1;
      }
    });
  }

  static class NullOutput extends OutputStream {

    @Override
    public void write(int b) {
      // pass
    }

    public void write(byte[] buffer, int offset, int length) {
      // pass
    }
  }
  private static final OutputStream NULL_OUTPUT = new NullOutput();

  @Override
  public FSDataOutputStream create(Path path,
                                   FsPermission fsPermission,
                                   boolean b,
                                   int i,
                                   short i1,
                                   long l,
                                   Progressable progressable) throws IOException {
    return new FSDataOutputStream(NULL_OUTPUT, null);
  }

  @Override
  public FSDataOutputStream append(Path path,
                                   int i,
                                   Progressable progressable) throws IOException {
    return new FSDataOutputStream(NULL_OUTPUT, null);
  }

  @Override
  public boolean rename(Path path, Path path1) {
    return false;
  }

  @Override
  public boolean delete(Path path, boolean b) {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path path)  {
    return null;
  }

  @Override
  public void setWorkingDirectory(Path path) {
    // pass
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path path) {
    return null;
  }
}
