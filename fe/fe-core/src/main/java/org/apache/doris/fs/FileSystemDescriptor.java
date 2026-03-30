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

import org.apache.doris.foundation.fs.FsStorageType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * A lightweight POJO that describes a persistent file system configuration.
 *
 * <p>This class replaces direct serialization of concrete {@link PersistentFileSystem}
 * subclasses (S3FileSystem, DFSFileSystem, etc.) in the backup/restore metadata.
 * By separating the description (type + properties) from the live object,
 * {@code GsonUtils} no longer needs compile-time references to concrete implementation classes.
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

    /** Creates a FileSystemDescriptor from an existing PersistentFileSystem (migration helper). */
    public static FileSystemDescriptor fromPersistentFileSystem(PersistentFileSystem fs) {
        return new FileSystemDescriptor(fs.getStorageType(), fs.getName(), fs.getProperties());
    }
}
