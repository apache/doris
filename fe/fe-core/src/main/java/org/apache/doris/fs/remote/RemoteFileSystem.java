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

package org.apache.doris.fs.remote;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.PersistentFileSystem;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public abstract class RemoteFileSystem extends PersistentFileSystem {
    protected org.apache.hadoop.fs.FileSystem dfsFileSystem = null;

    public RemoteFileSystem(String name, StorageBackend.StorageType type) {
        super(name, type);
    }

    protected org.apache.hadoop.fs.FileSystem getFileSystem(String remotePath) throws UserException {
        throw new UserException("Not support to getFileSystem.");
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(String remotePath) throws UserException {
        org.apache.hadoop.fs.FileSystem fileSystem = getFileSystem(remotePath);
        try {
            return fileSystem.listLocatedStatus(new Path(remotePath));
        } catch (IOException e) {
            throw new UserException("Failed to list located status for path: " + remotePath, e);
        }
    }
}
