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
import org.apache.doris.fs.Location;
import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;

/**
 * HdfsInputFile provides an implementation of DorisInputFile for reading data from HDFS.
 * It wraps a Location and DFSFileSystem to open files and retrieve file metadata from HDFS.
 */
public class HdfsInputFile implements DorisInputFile {
    // The Location representing the file location in HDFS.
    private final Location location;
    // The Hadoop Path object corresponding to the file.
    private final Path hadoopPath;
    // The DFSFileSystem used to interact with HDFS.
    private final DFSFileSystem dfs;

    // The length of the file, lazily initialized.
    private long length;
    // The FileStatus object for the file, lazily initialized.
    private FileStatus status;

    /**
     * Constructs a HdfsInputFile with the given Location, file length, and DFSFileSystem.
     *
     * @param location the Location representing the file location
     * @param length the length of the file, or -1 if unknown
     * @param dfs the DFSFileSystem used to interact with HDFS
     */
    public HdfsInputFile(Location location, long length, DFSFileSystem dfs) {
        this.location = Objects.requireNonNull(location, "location is null");
        this.dfs = Objects.requireNonNull(dfs, "hdfs file system is null");
        this.hadoopPath = new Path(location.toString());
        this.length = length;
    }

    /**
     * Returns a new DorisInput for reading from this file.
     *
     * @return a new DorisInput instance
     * @throws IOException if an I/O error occurs
     */
    @Override
    public DorisInput newInput() throws IOException {
        return new HdfsInput(dfs.openFile(hadoopPath), this);
    }

    /**
     * Returns a new DorisInputStream for streaming reads from this file.
     *
     * @return a new DorisInputStream instance
     * @throws IOException if an I/O error occurs
     */
    @Override
    public DorisInputStream newStream() throws IOException {
        return new HdfsInputStream(location.toString(), dfs.openFile(hadoopPath));
    }

    /**
     * Returns the length of the file, querying HDFS if necessary.
     *
     * @return the file length
     * @throws IOException if an I/O error occurs
     */
    @Override
    public long length() throws IOException {
        if (length == -1) {
            length = getFileStatus().getLen();
        }
        return length;
    }

    /**
     * Returns the last modified time of the file.
     *
     * @return the last modified time in milliseconds
     * @throws IOException if an I/O error occurs
     */
    @Override
    public long lastModifiedTime() throws IOException {
        return getFileStatus().getModificationTime();
    }

    /**
     * Checks if the file exists in HDFS.
     *
     * @return true if the file exists, false otherwise
     * @throws IOException if an I/O error occurs
     */
    @Override
    public boolean exists() throws IOException {
        Status existsStatus = dfs.exists(location.toString());
        return existsStatus.ok();
    }

    /**
     * Returns the Location associated with this input file.
     *
     * @return the Location
     */
    @Override
    public Location location() {
        return location;
    }

    /**
     * Returns the string representation of the file path.
     *
     * @return the file path as a string
     */
    @Override
    public String toString() {
        return location.toString();
    }

    /**
     * Lazily retrieves the FileStatus from HDFS for this file.
     *
     * @return the FileStatus object
     * @throws IOException if an I/O error occurs
     */
    private FileStatus getFileStatus() throws IOException {
        if (status == null) {
            status = dfs.getFileStatus(hadoopPath);
        }
        return status;
    }
}
