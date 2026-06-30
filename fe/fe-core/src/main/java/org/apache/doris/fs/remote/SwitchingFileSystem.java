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
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemCache;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SwitchingFileSystem implements FileSystem {

    private final ExternalMetaCacheMgr extMetaCacheMgr;

    private final Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    public SwitchingFileSystem(ExternalMetaCacheMgr extMetaCacheMgr,
                               Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        this.extMetaCacheMgr = extMetaCacheMgr;
        this.storagePropertiesMap = storagePropertiesMap;
    }

    @Override
    public Map<String, String> getProperties() {
        //fixme need this ?
        return null;
    }

    @Override
    public Status exists(String remotePath) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).exists(normalizedPath);
    }

    @Override
    public Status directoryExists(String dir) {
        String normalizedPath = normalizeLocation(dir);
        return fileSystem(normalizedPath).directoryExists(normalizedPath);
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        String normalizedPath = normalizeLocation(remoteFilePath);
        return fileSystem(normalizedPath).downloadWithFileSize(normalizedPath, localFilePath, fileSize);
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).upload(localPath, normalizedPath);
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        String normalizedPath = normalizeLocation(remoteFile);
        return fileSystem(normalizedPath).directUpload(content, normalizedPath);
    }

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        String normalizedOrigPath = normalizeLocation(origFilePath);
        String normalizedDestPath = normalizeLocation(destFilePath);
        return fileSystem(normalizedOrigPath).rename(normalizedOrigPath, normalizedDestPath);
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath) {
        String normalizedOrigPath = normalizeLocation(origFilePath);
        String normalizedDestPath = normalizeLocation(destFilePath);
        return fileSystem(normalizedOrigPath).renameDir(normalizedOrigPath, normalizedDestPath);
    }

    @Override
    public Status renameDir(String origFilePath, String destFilePath, Runnable runWhenPathNotExist) {
        String normalizedOrigPath = normalizeLocation(origFilePath);
        String normalizedDestPath = normalizeLocation(destFilePath);
        return fileSystem(normalizedOrigPath).renameDir(normalizedOrigPath, normalizedDestPath, runWhenPathNotExist);
    }

    @Override
    public Status delete(String remotePath) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).delete(normalizedPath);
    }

    @Override
    public Status deleteDirectory(String absolutePath) {
        String normalizedPath = normalizeLocation(absolutePath);
        return fileSystem(normalizedPath).deleteDirectory(normalizedPath);
    }

    @Override
    public Status makeDir(String remotePath) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).makeDir(normalizedPath);
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).listFiles(normalizedPath, recursive, result);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).globList(normalizedPath, result);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).globList(normalizedPath, result, fileNameOnly);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        String normalizedPath = normalizeLocation(remotePath);
        return fileSystem(normalizedPath).listDirectories(normalizedPath, result);
    }

    public FileSystem fileSystem(String location) {
        LocationPath path = LocationPath.of(location, storagePropertiesMap);
        FileSystemCache.FileSystemCacheKey fileSystemCacheKey = new FileSystemCache.FileSystemCacheKey(
                path.getFsIdentifier(), path.getStorageProperties()
        );
        return extMetaCacheMgr.getFsCache().getRemoteFileSystem(fileSystemCacheKey);
    }

    private String normalizeLocation(String location) {
        if (storagePropertiesMap == null) {
            return location;
        }
        return LocationPath.of(location, storagePropertiesMap).getNormalizedLocation();
    }
}
