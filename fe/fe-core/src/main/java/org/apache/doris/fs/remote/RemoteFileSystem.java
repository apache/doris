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
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.PersistentFileSystem;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        try {
            org.apache.hadoop.fs.FileSystem fileSystem = nativeFileSystem(remotePath);
            Path locatedPath = new Path(remotePath);
            RemoteIterator<LocatedFileStatus> locatedFiles = fileSystem.listFiles(locatedPath, recursive);
            while (locatedFiles.hasNext()) {
                LocatedFileStatus fileStatus = locatedFiles.next();
                RemoteFile location = new RemoteFile(
                        fileStatus.getPath(), fileStatus.isDirectory(), fileStatus.getLen(),
                        fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getBlockLocations());
                result.add(location);
            }
        } catch (FileNotFoundException e) {
            return new Status(Status.ErrCode.NOT_FOUND, e.getMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        try {
            FileSystem fileSystem = nativeFileSystem(remotePath);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(remotePath));
            result.addAll(
                    Arrays.stream(fileStatuses)
                            .filter(FileStatus::isDirectory)
                            .map(file -> file.getPath().toString() + "/")
                            .collect(ImmutableSet.toImmutableSet()));
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }

    @Override
    public Status renameDir(String origFilePath,
                            String destFilePath,
                            Runnable runWhenPathNotExist) {
        Status status = exists(destFilePath);
        if (status.ok()) {
            return new Status(Status.ErrCode.COMMON_ERROR, "Destination directory already exists: " + destFilePath);
        }

        String targetParent = new Path(destFilePath).getParent().toString();
        status = exists(targetParent);
        if (Status.ErrCode.NOT_FOUND.equals(status.getErrCode())) {
            status = makeDir(targetParent);
        }
        if (!status.ok()) {
            return new Status(Status.ErrCode.COMMON_ERROR, status.getErrMsg());
        }

        runWhenPathNotExist.run();

        return rename(origFilePath, destFilePath);
    }
}
