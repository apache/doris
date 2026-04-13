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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.HadoopAuthenticator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * HDFS-backed {@link DorisInputFile} that opens a seekable {@link DorisInputStream}
 * via Hadoop's {@link FSDataInputStream}.
 */
class HdfsInputFile implements DorisInputFile {

    private final Path path;
    private final HadoopAuthenticator authenticator;
    private final DFSFileSystem dfs;
    private final Location location;

    HdfsInputFile(Path path, HadoopAuthenticator authenticator, DFSFileSystem dfs) {
        this.path = path;
        this.authenticator = authenticator;
        this.dfs = dfs;
        this.location = Location.of(path.toString());
    }

    @Override
    public Location location() {
        return location;
    }

    @Override
    public long length() throws IOException {
        return authenticator.doAs(() -> dfs.requireFs(path).getFileStatus(path).getLen());
    }

    @Override
    public boolean exists() throws IOException {
        return authenticator.doAs(() -> dfs.requireFs(path).exists(path));
    }

    @Override
    public long lastModifiedTime() throws IOException {
        return authenticator.doAs(() -> dfs.requireFs(path).getFileStatus(path).getModificationTime());
    }

    @Override
    public DorisInputStream newStream() throws IOException {
        FSDataInputStream fds = authenticator.doAs(() -> dfs.requireFs(path).open(path));
        return new HdfsSeekableInputStream(path.toString(), fds);
    }
}
