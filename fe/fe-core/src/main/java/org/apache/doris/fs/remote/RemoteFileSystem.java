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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RemoteFileSystem extends PersistentFileSystem implements Closeable {

    protected AtomicBoolean closed = new AtomicBoolean(false);

    public RemoteFileSystem(String name, StorageBackend.StorageType type) {
        super(name, type);
    }

    protected FileStatus[] getFileStatuses(String remotePath, FileSystem fileSystem) throws IOException {
        return fileSystem.listStatus(new Path(remotePath));
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

    public boolean connectivityTest(List<String> filePaths) throws UserException {
        return true;
    }
}
