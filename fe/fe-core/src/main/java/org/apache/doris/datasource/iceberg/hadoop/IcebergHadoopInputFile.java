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

package org.apache.doris.datasource.iceberg.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IcebergHadoopInputFile implements InputFile {

    private String location;
    private final FileSystem fs;
    private final Path path;
    private final Configuration conf;
    private FileStatus stat = null;
    private Long length;

    public IcebergHadoopInputFile(FileSystem fs, String location, Configuration conf) {
        this(fs, location, null, conf);
    }

    public IcebergHadoopInputFile(FileSystem fs, Path path, Configuration conf) {
        this.fs = fs;
        this.path = path;
        this.conf = conf;
    }

    public IcebergHadoopInputFile(FileSystem fs, String location, Long length, Configuration conf) {
        this.fs = fs;
        this.location = location;
        this.path = new Path(location);
        this.conf = conf;
        this.length = length;
    }

    @Override
    public long getLength() {
        if (this.length == null) {
            this.length = this.lazyStat().getLen();
        }
        return this.length;
    }

    public SeekableInputStream newStream() {
        try {
            return new IcebergSeekableInputStream(openFile());
        } catch (FileNotFoundException e) {
            throw new NotFoundException(e, "Failed to open input stream for file: %s", this.path);
        } catch (IOException ex) {
            throw new RuntimeIOException(ex, "Failed to open input stream for file: %s", this.path);
        }
    }

    private FSDataInputStream openFile() throws IOException {
        return fs.open(path);
    }

    @Override
    public String location() {
        return location;
    }

    public boolean exists() {
        try {
            return this.lazyStat() != null;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private FileStatus lazyStat() {
        if (this.stat == null) {
            try {
                this.stat = this.fs.getFileStatus(this.path);
            } catch (FileNotFoundException e) {
                throw new NotFoundException(e, "File does not exist: %s", this.path);
            } catch (IOException e) {
                throw new RuntimeIOException(e, "Failed to get status for file: %s", this.path);
            }
        }
        return this.stat;
    }
}
