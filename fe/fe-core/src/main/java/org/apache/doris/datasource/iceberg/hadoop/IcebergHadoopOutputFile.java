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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;

public class IcebergHadoopOutputFile implements OutputFile  {

    private final FileSystem fs;
    private final Path path;
    private final Configuration conf;

    public IcebergHadoopOutputFile(FileSystem fs, Path path, Configuration hadoopConf) {
        this.fs = fs;
        this.path = path;
        this.conf = hadoopConf;
    }

    @Override
    public PositionOutputStream create() {
        try {
            return new IcebergPositionOutputStream(this.fs.create(this.path, false));
        } catch (FileAlreadyExistsException e) {
            throw new AlreadyExistsException(e, "Path already exists: %s", this.path);
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to create file: %s", this.path);
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        try {
            return new IcebergPositionOutputStream(this.fs.create(this.path, true));
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to create file: %s", this.path);
        }
    }

    @Override
    public String location() {
        return this.path.toString();
    }

    @Override
    public InputFile toInputFile() {
        return new IcebergHadoopInputFile(this.fs, this.path, this.conf);
    }
}
