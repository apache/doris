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
import org.apache.doris.common.UserException;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.util.List;

public interface FileSystem {
    Status exists(String remotePath);

    Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize);

    Status upload(String localPath, String remotePath);

    Status directUpload(String content, String remoteFile);

    Status rename(String origFilePath, String destFilePath);

    Status delete(String remotePath);

    default RemoteIterator<LocatedFileStatus> listLocatedStatus(String remotePath) throws UserException {
        throw new UserException("Not support to listLocatedStatus.");
    }

    // List files in remotePath
    // The remote file name will only contains file name only(Not full path)
    Status list(String remotePath, List<RemoteFile> result);

    Status list(String remotePath, List<RemoteFile> result, boolean fileNameOnly);

    Status makeDir(String remotePath);
}
