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

import org.apache.doris.backup.RemoteFile;
import org.apache.doris.backup.Status;

import java.util.List;

public class LocalFileSystem implements FileSystem {
    @Override
    public Status exists(String remotePath) {
        return null;
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
        return null;
    }

    @Override
    public Status delete(String remotePath) {
        return null;
    }

    @Override
    public Status makeDir(String remotePath) {
        return null;
    }

    @Override
    public Status list(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        return null;
    }
}
