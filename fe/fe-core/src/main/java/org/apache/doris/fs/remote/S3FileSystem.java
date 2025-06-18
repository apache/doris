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
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.S3ObjStorage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class S3FileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(S3FileSystem.class);
    private final AbstractS3CompatibleProperties s3Properties;

    public S3FileSystem(AbstractS3CompatibleProperties s3Properties) {
        super(StorageBackend.StorageType.S3.name(), StorageBackend.StorageType.S3,
                new S3ObjStorage(s3Properties));
        this.s3Properties = s3Properties;
        initFsProperties();
    }

    @Override
    public StorageProperties getStorageProperties() {
        return s3Properties;
    }

    private void initFsProperties() {
        this.properties.putAll(s3Properties.getOrigProps());
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        return objStorage.listFiles(remotePath, recursive, result);
    }

    // broker file pattern glob is too complex, so we use hadoop directly
    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        return objStorage.globList(remotePath, result, fileNameOnly);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        return objStorage.listDirectories(remotePath, result);
    }

    @Override
    public boolean connectivityTest(List<String> filePaths) throws UserException {
        if (filePaths == null || filePaths.isEmpty()) {
            throw new UserException("File paths cannot be null or empty for connectivity test.");
        }
        S3ObjStorage objStorage = (S3ObjStorage) this.objStorage;
        try {
            S3Client s3Client = objStorage.getClient();
            Set<String> bucketNames = new HashSet<>();
            boolean usePathStyle = Boolean.parseBoolean(s3Properties.getUsePathStyle());
            boolean forceParsingByStandardUri = Boolean.parseBoolean(s3Properties.getForceParsingByStandardUrl());
            for (String filePath : filePaths) {
                S3URI s3uri;
                s3uri = S3URI.create(filePath, usePathStyle, forceParsingByStandardUri);
                bucketNames.add(s3uri.getBucket());
            }
            bucketNames.forEach(bucketName -> s3Client.headBucket(b -> b.bucket(bucketName)));
            return true;
        } catch (Exception e) {
            LOG.warn("S3 connectivityTest error: {}", e.getMessage(), e);
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                objStorage.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
