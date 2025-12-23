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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class TrackingLocalFileSystem extends RawLocalFileSystem {
  static final URI NAME = URI.create("track:///");

  class TrackingFileInputStream extends RawLocalFileSystem.LocalFSFileInputStream {

    TrackingFileInputStream(Path f) throws IOException {
      super(f);
    }

    public int read() throws IOException {
      statistics.incrementReadOps(1);
      return super.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
      statistics.incrementReadOps(1);
      return super.read(b, off, len);
    }

    public int read(long position, byte[] b, int off, int len) throws IOException {
      statistics.incrementReadOps(1);
      return super.read(position, b, off, len);
    }
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    return new FSDataInputStream(new BufferedFSInputStream(
        new TrackingFileInputStream(f), bufferSize));
  }

  @Override
  public URI getUri() {
    return NAME;
  }

  public FileSystem.Statistics getLocalStatistics() {
    return statistics;
  }
}
