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

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    default Status directoryExists(String dir) {
        return exists(dir);
    }

    Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize);

    Status upload(String localPath, String remotePath);

    Status directUpload(String content, String remoteFile);

    Status rename(String origFilePath, String destFilePath);

    default Status renameDir(String origFilePath, String destFilePath) {
        return renameDir(origFilePath, destFilePath, () -> {});
    }

    default Status renameDir(String origFilePath,
                             String destFilePath,
                             Runnable runWhenPathNotExist) {
        throw new UnsupportedOperationException("Unsupported operation rename dir on current file system.");
    }

    Status delete(String remotePath);

    default Status deleteDirectory(String dir) {
        return delete(dir);
    }

    Status makeDir(String remotePath);

    /*
     * List files in remotePath
     * @param remotePath remote path
     * @param recursive whether to list files recursively
     * <pre>
     * If the path is a directory,
     *   if recursive is false, returns files in the directory;
     *   if recursive is true, return files in the subtree rooted at the path.
     * If the path is a file, return the file's status and block locations.
     * </pre>
     * */
    Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result);

    /**
     * List files in remotePath by wildcard <br/>
     * The {@link RemoteFile}'name will only contain file name (Not full path)
     * @param remotePath remote path
     * @param result All eligible files under the path
     * @return
     */
    default Status globList(String remotePath, List<RemoteFile> result) {
        return globList(remotePath, result, true);
    }

    /**
     * List files in remotePath by wildcard <br/>
     * @param remotePath remote path
     * @param result All eligible files under the path
     * @param fileNameOnly for {@link RemoteFile}'name: whether the full path is included.<br/>
     *                     true: only contains file name, false: contains full path<br/>
     * @return
     */
    Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly);

    default Status listDirectories(String remotePath, Set<String> result) {
        throw new UnsupportedOperationException("Unsupported operation list directories on current file system.");
    }
}
