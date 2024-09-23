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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;

public class IcebergHadoopFileIO extends HadoopFileIO {

    private FileSystem fs;
    private Configuration hadoopConf;

    public IcebergHadoopFileIO(Configuration hadoopConf, FileSystem fs) {
        this.hadoopConf = hadoopConf;
        this.fs = fs;
    }

    @Override
    public InputFile newInputFile(String path) {
        return new IcebergHadoopInputFile(this.fs, path, this.hadoopConf);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new IcebergHadoopInputFile(this.fs, path, length, this.hadoopConf);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new IcebergHadoopOutputFile(this.fs, new Path(path), this.hadoopConf);
    }

    @Override
    public void deleteFile(String path) {
        Path toDelete = new Path(path);
        try {
            fs.delete(toDelete, false);
        } catch (IOException var5) {
            IOException e = var5;
            throw new RuntimeIOException(e, "Failed to delete file: %s", path);
        }
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
        // TODO
    }
}
