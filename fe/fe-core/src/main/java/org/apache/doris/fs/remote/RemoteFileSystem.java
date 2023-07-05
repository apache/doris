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
import org.apache.doris.fs.RemoteFiles;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class RemoteFileSystem extends PersistentFileSystem {
    // this field will be visited by multi-threads, better use volatile qualifier
    protected volatile org.apache.hadoop.fs.FileSystem dfsFileSystem = null;

    public RemoteFileSystem(String name, StorageBackend.StorageType type) {
        super(name, type);
    }

    protected org.apache.hadoop.fs.FileSystem nativeFileSystem(String remotePath) throws UserException {
        throw new UserException("Not support to getFileSystem.");
    }

    @Override
    public RemoteFiles listLocatedFiles(String remotePath, boolean onlyFiles, boolean recursive) throws UserException {
        org.apache.hadoop.fs.FileSystem fileSystem = nativeFileSystem(remotePath);
        try {
            Path locatedPath = new Path(remotePath);
            RemoteIterator<LocatedFileStatus> locatedFiles = onlyFiles ? fileSystem.listFiles(locatedPath, recursive)
                        : fileSystem.listLocatedStatus(locatedPath);
            return getFileLocations(locatedFiles);
        } catch (IOException e) {
            throw new UserException("Failed to list located status for path: " + remotePath, e);
        }
    }

    private RemoteFiles getFileLocations(RemoteIterator<LocatedFileStatus> locatedFiles) throws IOException {
        List<RemoteFile> locations = new ArrayList<>();
        while (locatedFiles.hasNext()) {
            LocatedFileStatus fileStatus = locatedFiles.next();
            RemoteFile location = new RemoteFile(fileStatus.getPath(), fileStatus.isDirectory(), fileStatus.getLen(),
                    fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getBlockLocations());
            locations.add(location);
        }
        return new RemoteFiles(locations);
    }
}
