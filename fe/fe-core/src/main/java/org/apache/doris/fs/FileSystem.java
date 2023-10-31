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
import org.apache.doris.common.UserException;
import org.apache.doris.fs.remote.RemoteFile;

import java.util.List;
import java.util.Map;

/**
 * File system interface.
 * All file operations should use DFSFileSystem.
 * @see org.apache.doris.fs.remote.dfs.DFSFileSystem
 * If the file system use the object storage's SDK, use ObjStorage
 * @see org.apache.doris.fs.remote.ObjFileSystem
 * Read and Write operation put in FileOperations
 * @see org.apache.doris.fs.operations.FileOperations
 */
public interface FileSystem {
    Map<String, String> getProperties();

    Status exists(String remotePath);

    Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize);

    Status upload(String localPath, String remotePath);

    Status directUpload(String content, String remoteFile);

    Status rename(String origFilePath, String destFilePath);

    Status delete(String remotePath);

    Status makeDir(String remotePath);

    // Get files and directories located status, not only files
    RemoteFiles listLocatedFiles(String remotePath, boolean onlyFiles, boolean recursive) throws UserException;

    // List files in remotePath
    // The remote file name will only contain file name only(Not full path)
    default Status list(String remotePath, List<RemoteFile> result) {
        return list(remotePath, result, true);
    }

    Status list(String remotePath, List<RemoteFile> result, boolean fileNameOnly);
}
