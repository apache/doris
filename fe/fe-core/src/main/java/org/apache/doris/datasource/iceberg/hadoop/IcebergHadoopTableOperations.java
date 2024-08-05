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
import org.apache.iceberg.LockManager;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;

public class IcebergHadoopTableOperations extends HadoopTableOperations {
    private final DFSFileSystem fileSystem;

    public IcebergHadoopTableOperations(Path location, FileIO fileIO, Configuration conf,
                                        LockManager lockManager, DFSFileSystem fileSystem) {
        super(location, fileIO, conf, lockManager);
        this.fileSystem = fileSystem;
    }

    @Override
    protected FileSystem getFileSystem(Path path, Configuration hadoopConf) {
        try {
            return fileSystem.rawFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
