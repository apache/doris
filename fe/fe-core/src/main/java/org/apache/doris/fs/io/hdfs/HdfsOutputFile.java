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

import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.DorisPath;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import static java.util.Objects.requireNonNull;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class HdfsOutputFile implements DorisOutputFile {
    private final DorisPath path;
    private final Path hadoopPath;
    private final DFSFileSystem dfs;

    public HdfsOutputFile(DorisPath path, DFSFileSystem dfs) {
        this.path = requireNonNull(path, "path is null");
        this.hadoopPath = path.toHadoopPath();
        this.dfs = requireNonNull(dfs, "dfs is null");
    }

    @Override
    public OutputStream create() throws IOException {
        return dfs.createFile(hadoopPath, false);
    }

    @Override
    public OutputStream createOrOverwrite() throws IOException {
        return dfs.createFile(hadoopPath, true);
    }

    @Override
    public DorisPath path() {
        return path;
    }

    @Override
    public String toString() {
        return path().toString();
    }
}
