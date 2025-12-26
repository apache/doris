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

package org.apache.doris.common.plugin;

import org.apache.doris.backup.Status;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.S3ObjStorage;

import com.google.common.base.Strings;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple cloud plugin downloader for UDF and JDBC drivers.
 */
public class CloudPluginDownloader {

    public enum PluginType {
        JDBC_DRIVERS,
        JAVA_UDF,
        CONNECTORS,     // Reserved, not supported yet
        HADOOP_CONF     // Reserved, not supported yet
    }

    /**
     * Download plugin from cloud storage to local path
     */
    public static synchronized String downloadFromCloud(PluginType type, String name, String localPath) {
        validateInput(type, name);
        try {
            Cloud.ObjectStoreInfoPB objInfo = getCloudStorageInfo();
            String remotePath = buildS3Path(objInfo, type, name);
            return doDownload(objInfo, remotePath, localPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download plugin: " + e.getMessage(), e);
        }
    }

    /**
     * Validate input parameters
     */
    static void validateInput(PluginType type, String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Plugin name cannot be empty");
        }

        if (type != PluginType.JDBC_DRIVERS && type != PluginType.JAVA_UDF) {
            throw new UnsupportedOperationException("Plugin type " + type + " is not supported yet");
        }
    }

    /**
     * Get cloud storage info from MetaService
     * Package-private for testing
     */
    static Cloud.ObjectStoreInfoPB getCloudStorageInfo() throws Exception {
        Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance()
                .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());

        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new RuntimeException("Failed to get storage info: " + response.getStatus().getMsg());
        }

        if (response.getObjInfoList().isEmpty()) {
            throw new RuntimeException("Only SaaS cloud storage is supported currently");
        }

        return response.getObjInfo(0);
    }

    /**
     * Build complete S3 path from objInfo
     * Package-private for testing
     */
    static String buildS3Path(Cloud.ObjectStoreInfoPB objInfo, PluginType type, String name) {
        String bucket = objInfo.getBucket();
        String prefix = objInfo.hasPrefix() ? objInfo.getPrefix() : "";
        String relativePath = String.format("plugins/%s/%s", type.name().toLowerCase(), name);

        String fullPath;
        if (Strings.isNullOrEmpty(prefix)) {
            fullPath = bucket + "/" + relativePath;
        } else {
            fullPath = bucket + "/" + prefix + "/" + relativePath;
        }

        return "s3://" + fullPath;
    }

    /**
     * Execute download with S3ObjStorage
     */
    private static String doDownload(Cloud.ObjectStoreInfoPB objInfo, String remotePath, String localPath)
            throws Exception {
        // Create parent directory
        Path parentDir = Paths.get(localPath).getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // Delete existing file if present
        File localFile = new File(localPath);
        if (localFile.exists() && !localFile.delete()) {
            throw new RuntimeException("Failed to delete existing file: " + localPath);
        }

        // Create S3ObjStorage and download
        try (S3ObjStorage s3Storage = createS3Storage(objInfo)) {
            Status status = s3Storage.getObject(remotePath, localFile);
            if (!status.ok()) {
                throw new RuntimeException("Download failed: " + status.getErrMsg());
            }
            return localPath;
        }
    }

    /**
     * Create S3ObjStorage from objInfo
     */
    private static S3ObjStorage createS3Storage(Cloud.ObjectStoreInfoPB objInfo) {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", objInfo.getEndpoint());
        props.put("s3.region", objInfo.getRegion());
        props.put("s3.access_key", objInfo.getAk());
        props.put("s3.secret_key", objInfo.getSk());
        props.put("s3.bucket", objInfo.getBucket());

        // Auto-detect storage type (S3, COS, OSS, etc.)
        StorageProperties storageProps;
        try {
            storageProps = StorageProperties.createAll(props).stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Failed to create storage properties"));
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        return new S3ObjStorage((AbstractS3CompatibleProperties) storageProps);
    }
}
