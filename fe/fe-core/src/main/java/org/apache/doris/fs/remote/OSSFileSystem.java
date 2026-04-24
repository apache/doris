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

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.GlobListResult;
import org.apache.doris.fs.obj.OSSObjStorage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RemoteFileSystem backed by the Alibaba Cloud OSS Java SDK (aliyun-sdk-oss:3.15.0).
 *
 * Handles all credential types correctly using OSS-native signing:
 *   - Static ak/sk (permanent credentials)
 *   - Static ak/sk + session_token (STS temporary credentials) — fixes AWS SDK signing mismatch
 *   - role_arn via ECS RAM role (ECS metadata → STS AssumeRole)
 *   - ECS direct (via ALIBABA_CLOUD_ECS_METADATA env var)
 *
 * Follows the same pattern as AzureFileSystem: extends ObjFileSystem, wraps OSSObjStorage.
 */
public class OSSFileSystem extends ObjFileSystem {
    private static final Logger LOG = LogManager.getLogger(OSSFileSystem.class);

    private final OSSProperties ossProperties;

    public OSSFileSystem(OSSProperties ossProperties) {
        super(StorageType.S3.name(), StorageType.S3, new OSSObjStorage(ossProperties));
        this.ossProperties = ossProperties;
        // Populate properties map so callers using getProperties() see the OSS config.
        // Mirrors S3FileSystem.initFsProperties() and AzureFileSystem constructor behaviour.
        this.properties.putAll(ossProperties.getOrigProps());
    }

    @Override
    public StorageProperties getStorageProperties() {
        return ossProperties;
    }

    @Override
    public Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result) {
        return getOssObjStorage().listFiles(remotePath, recursive, result);
    }

    @Override
    public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        return getOssObjStorage().globList(remotePath, result, fileNameOnly);
    }

    @Override
    public GlobListResult globListWithLimit(String remotePath, List<RemoteFile> result,
            String startFile, long fileSizeLimit, long fileNumLimit) {
        return getOssObjStorage().globListWithLimit(remotePath, result, startFile, fileSizeLimit, fileNumLimit);
    }

    @Override
    public Status listDirectories(String remotePath, Set<String> result) {
        return getOssObjStorage().listDirectories(remotePath, result);
    }

    // Connectivity test using doesBucketExist() per bucket
    @Override
    public boolean connectivityTest(List<String> filePaths) throws UserException {
        if (filePaths == null || filePaths.isEmpty()) {
            throw new UserException("File paths cannot be null or empty for connectivity test.");
        }
        try {
            OSSObjStorage ossObjStorage = getOssObjStorage();
            // OSS only supports virtual-hosted style — always parse with usePathStyle=false
            Set<String> bucketNames = new HashSet<>();
            for (String filePath : filePaths) {
                S3URI uri = S3URI.create(filePath, false,
                        Boolean.parseBoolean(ossProperties.getForceParsingByStandardUrl()));
                bucketNames.add(uri.getBucket());
            }
            for (String bucketName : bucketNames) {
                // doesBucketExist() confirmed in OSS SDK: boolean doesBucketExist(String bucketName)
                boolean exists = ossObjStorage.getClient().doesBucketExist(bucketName);
                if (!exists) {
                    LOG.warn("OSSFileSystem connectivityTest: bucket {} does not exist", bucketName);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOG.warn("OSSFileSystem connectivityTest error: {}", e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void completeMultipartUpload(String bucket, String key, String uploadId,
            Map<Integer, String> parts) {
        getOssObjStorage().completeMultipartUpload(bucket, key, uploadId, parts);
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

    private OSSObjStorage getOssObjStorage() {
        return (OSSObjStorage) this.objStorage;
    }
}
