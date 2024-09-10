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

import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;

public class IcebergHadoopFileIO extends HadoopFileIO {

    private final DFSFileSystem fs;
    private final Configuration hadoopConf;

    public IcebergHadoopFileIO(Configuration hadoopConf, DFSFileSystem fs) {
        this.hadoopConf = hadoopConf;
        this.fs = fs;
    }

    @Override
    public InputFile newInputFile(String path) {
        return new IcebergHadoopInputFile(getFs(), path, this.hadoopConf);
    }

    private FileSystem getFs() {
        try {
            return this.fs.rawFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new IcebergHadoopInputFile(getFs(), path, length, this.hadoopConf);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new IcebergHadoopOutputFile(getFs(), new Path(path), this.hadoopConf);
    }

    @Override
    public void deleteFile(String path) {
        Path toDelete = new Path(path);
        try {
            fs.delete(toDelete, false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete file: " + path, e);
        }
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
        // TODO
    }
}
