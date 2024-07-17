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

package org.apache.doris.fs;

import org.apache.doris.backup.Status;
import org.apache.doris.fs.remote.RemoteFile;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalDfsFileSystem implements FileSystem {

    public LocalFileSystem fs = LocalFileSystem.getLocal(new Configuration());

    public LocalDfsFileSystem() throws IOException {
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public Status directoryExists(String dir) {
        return exists(dir);
    }

    @Override
    public Status exists(String remotePath) {
        boolean exists = false;
        try {
            exists = fs.exists(new Path(remotePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (exists) {
            return Status.OK;
        } else {
            return new Status(Status.ErrCode.NOT_FOUND, "");
        }
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        return null;
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        return null;
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        return null;
    }

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        try {
            fs.rename(new Path(origFilePath), new Path(destFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Status.OK;
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath, Runnable runWhenPathNotExist) {
        Status status = exists(destFilePath);
        if (status.ok()) {
            throw new RuntimeException("Destination directory already exists: " + destFilePath);
        }
        String targetParent = new Path(destFilePath).getParent().toString();
        status = exists(targetParent);
        if (Status.ErrCode.NOT_FOUND.equals(status.getErrCode())) {
            status = makeDir(targetParent);
        }
        if (!status.ok()) {
            throw new RuntimeException(status.getErrMsg());
        }

        runWhenPathNotExist.run();

        return rename(origFilePath, destFilePath);
    }

    @Override
    public Status delete(String remotePath) {
        try {
            fs.delete(new Path(remotePath), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        try {
            fs.mkdirs(new Path(remotePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Status.OK;
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            FileStatus[] locatedFileStatusRemoteIterator = fs.globStatus(new Path(remotePath));
            if (locatedFileStatusRemoteIterator == null) {
                return Status.OK;
            }
            for (FileStatus fileStatus : locatedFileStatusRemoteIterator) {
                RemoteFile remoteFile = new RemoteFile(
                        fileNameOnly ? fileStatus.getPath().getName() : fileStatus.getPath().toString(),
                        !fileStatus.isDirectory(), fileStatus.isDirectory() ? -1 : fileStatus.getLen(),
                        fileStatus.getBlockSize(), fileStatus.getModificationTime());
                result.add(remoteFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Status.OK;
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        try {
            Path locatedPath = new Path(remotePath);
            RemoteIterator<LocatedFileStatus> locatedFiles = fs.listFiles(locatedPath, recursive);
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
            FileStatus[] fileStatuses = fs.listStatus(new Path(remotePath));
            result.addAll(
                    Arrays.stream(fileStatuses)
                        .filter(FileStatus::isDirectory)
                        .map(file -> file.getPath().toString() + "/")
                        .collect(ImmutableSet.toImmutableSet()));
        } catch (IOException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }

    public void createFile(String path) throws IOException {
        Path path1 = new Path(path);
        if (!exists(path1.getParent().toString()).ok()) {
            makeDir(path1.getParent().toString());
        }
        FSDataOutputStream build = fs.createFile(path1).build();
        build.close();
    }
}
