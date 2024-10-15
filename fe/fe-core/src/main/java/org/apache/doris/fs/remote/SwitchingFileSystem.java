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

import org.apache.doris.backup.Status;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemCache;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SwitchingFileSystem implements FileSystem {

    private final ExternalMetaCacheMgr extMetaCacheMgr;

    private final String bindBrokerName;

    private final Map<String, String> properties;

    public SwitchingFileSystem(ExternalMetaCacheMgr extMetaCacheMgr, String bindBrokerName,
            Map<String, String> properties) {
        this.extMetaCacheMgr = extMetaCacheMgr;
        this.bindBrokerName = bindBrokerName;
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Status exists(String remotePath) {
        return fileSystem(remotePath).exists(remotePath);
    }

    @Override
    public Status directoryExists(String dir) {
        return fileSystem(dir).directoryExists(dir);
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        return fileSystem(remoteFilePath).downloadWithFileSize(remoteFilePath, localFilePath, fileSize);
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        return fileSystem(localPath).upload(localPath, remotePath);
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        return fileSystem(remoteFile).directUpload(content, remoteFile);
    }

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        return fileSystem(origFilePath).rename(origFilePath, destFilePath);
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath) {
        return fileSystem(origFilePath).renameDir(origFilePath, destFilePath);
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath, Runnable runWhenPathNotExist) {
        return fileSystem(origFilePath).renameDir(origFilePath, destFilePath, runWhenPathNotExist);
    }

    @Override
    public Status delete(String remotePath) {
        return fileSystem(remotePath).delete(remotePath);
    }

    @Override
    public Status deleteDirectory(String absolutePath) {
        return fileSystem(absolutePath).deleteDirectory(absolutePath);
    }

    @Override
    public Status makeDir(String remotePath) {
        return fileSystem(remotePath).makeDir(remotePath);
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        return fileSystem(remotePath).listFiles(remotePath, recursive, result);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result) {
        return fileSystem(remotePath).globList(remotePath, result);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        return fileSystem(remotePath).globList(remotePath, result, fileNameOnly);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        return fileSystem(remotePath).listDirectories(remotePath, result);
    }

    public FileSystem fileSystem(String location) {
        return extMetaCacheMgr.getFsCache().getRemoteFileSystem(
                new FileSystemCache.FileSystemCacheKey(
                        LocationPath.getFSIdentity(location,
                                bindBrokerName), properties, bindBrokerName));
    }
}

