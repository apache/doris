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
import org.apache.doris.fs.io.ParsedPath;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * HdfsOutputFile provides an implementation of DorisOutputFile for writing data to HDFS.
 * It wraps a ParsedPath and DFSFileSystem to create or overwrite files in HDFS.
 */
public class HdfsOutputFile implements DorisOutputFile {
    // The ParsedPath representing the file location in HDFS.
    private final ParsedPath path;
    // The Hadoop Path object corresponding to the file.
    private final Path hadoopPath;
    // The DFSFileSystem used to interact with HDFS.
    private final DFSFileSystem dfs;

    /**
     * Constructs a HdfsOutputFile with the given ParsedPath and DFSFileSystem.
     *
     * @param path the ParsedPath representing the file location
     * @param dfs the DFSFileSystem used to interact with HDFS
     */
    public HdfsOutputFile(ParsedPath path, DFSFileSystem dfs) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.hadoopPath = path.toHadoopPath();
        this.dfs = Objects.requireNonNull(dfs, "dfs is null");
    }

    /**
     * Creates a new file in HDFS. Fails if the file already exists.
     *
     * @return OutputStream for writing to the new file
     * @throws IOException if an I/O error occurs
     */
    @Override
    public OutputStream create() throws IOException {
        return dfs.createFile(hadoopPath, false);
    }

    /**
     * Creates a new file or overwrites the file if it already exists in HDFS.
     *
     * @return OutputStream for writing to the file
     * @throws IOException if an I/O error occurs
     */
    @Override
    public OutputStream createOrOverwrite() throws IOException {
        return dfs.createFile(hadoopPath, true);
    }

    /**
     * Returns the ParsedPath associated with this output file.
     *
     * @return the ParsedPath
     */
    @Override
    public ParsedPath path() {
        return path;
    }

    /**
     * Returns the string representation of the file path.
     *
     * @return the file path as a string
     */
    @Override
    public String toString() {
        return path().toString();
    }
}
