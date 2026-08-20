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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.foundation.fs.FsStorageType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * A lightweight POJO that describes a persistent file system configuration.
 *
 * <p>Serialized format (JSON):
 * <pre>{@code
 * {
 *   "fs_type": "S3",
 *   "fs_name": "my-s3-repo",
 *   "fs_props": { "AWS_ACCESS_KEY": "...", "AWS_SECRET_KEY": "..." }
 * }
 * }</pre>
 */
public class FileSystemDescriptor {

    @SerializedName("fs_type")
    private final FsStorageType storageType;

    @SerializedName("fs_name")
    private final String name;

    @SerializedName("fs_props")
    private final Map<String, String> properties;

    public FileSystemDescriptor(FsStorageType storageType, String name, Map<String, String> properties) {
        this.storageType = storageType;
        this.name = name;
        this.properties = Maps.newHashMap(properties);
    }

    public FsStorageType getStorageType() {
        return storageType;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns the Thrift storage type corresponding to this descriptor's storage type.
     * Used to populate {@code storage_backend} in BE snapshot/download task RPCs.
     */
    public StorageBackend.StorageType getThriftStorageType() {
        return FsStorageTypeAdapter.toThrift(storageType);
    }

    /**
     * Returns the backend configuration properties (AK, SK, endpoint, etc.) needed
     * by BE to initialize its storage client for snapshot/upload/download tasks.
     */
    public Map<String, String> getBackendConfigProperties() {
        return StorageAdapter.of(properties).getBackendConfigProperties();
    }

    /**
     * Creates a FileSystemDescriptor from a {@link StorageAdapter} facade.
     * The {@code fsName} is the broker name for BROKER type, or empty for others.
     */
    public static FileSystemDescriptor fromStorageAdapter(StorageAdapter storageAdapter, String fsName) {
        FsStorageType storageType = storageNameToFsType(storageAdapter.getStorageName());
        return new FileSystemDescriptor(storageType, fsName, storageAdapter.getOrigProps());
    }

    private static FsStorageType storageNameToFsType(String name) {
        switch (name) {
            case "S3":      return FsStorageType.S3;
            case "HDFS":    return FsStorageType.HDFS;
            case "BROKER":  return FsStorageType.BROKER;
            case "AZURE":   return FsStorageType.AZURE;
            case "OSSHDFS": return FsStorageType.OSS_HDFS;
            default: throw new IllegalArgumentException("Unsupported storage type name: " + name);
        }
    }
}
