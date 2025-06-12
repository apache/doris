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

package org.apache.doris.fs.io.hdfs;

import org.apache.doris.backup.Status;
import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.DorisPath;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import static java.util.Objects.requireNonNull;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsInputFile implements DorisInputFile {
    private final DorisPath path;
    private final Path hadoopPath;
    private final DFSFileSystem dfs;

    // The following fields are lazily initialized
    private long length;
    private FileStatus status;

    public HdfsInputFile(DorisPath path, long length, DFSFileSystem dfs) {
        this.path = requireNonNull(path, "path is null");
        this.dfs = requireNonNull(dfs, "hdfs file system is null");
        this.hadoopPath = path.toHadoopPath();
        this.length = length;
    }

    @Override
    public DorisInput newInput() throws IOException {
        return new HdfsInput(dfs.openFile(hadoopPath), this);
    }

    @Override
    public DorisInputStream newStream() throws IOException {
        return new HdfsInputStream(path, dfs.openFile(hadoopPath));
    }

    @Override
    public long length() throws IOException {
        if (length == -1) {
            length = getFileStatus().getLen();
        }
        return length;
    }

    @Override
    public long lastModifiedTime() throws IOException {
        return getFileStatus().getModificationTime();
    }

    @Override
    public boolean exists() throws IOException {
        Status status = dfs.exists(path.toString());
        return status.ok();
    }

    @Override
    public DorisPath path() {
        return path;
    }

    @Override
    public String toString() {
        return path().toString();
    }

    private FileStatus getFileStatus() throws IOException {
        if (status == null) {
            status = dfs.getFileStatus(hadoopPath);
        }
        return status;
    }
}
