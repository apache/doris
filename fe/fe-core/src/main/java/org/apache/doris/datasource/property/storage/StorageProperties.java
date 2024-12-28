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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.CatalogProperties;

import lombok.Getter;

import java.util.Map;

public class StorageProperties extends CatalogProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";

    public enum Type {
        HDFS,
        S3,
        UNKNOWN
    }

    @Getter
    protected Type type;

    public StorageProperties create(Map<String, String> origProps) throws UserException {
        if (isFsSupport(origProps, FS_HDFS_SUPPORT)) {
            return new HDFSProperties(origProps);
        } else if (isFsSupport(origProps, FS_S3_SUPPORT)) {
            return new S3Properties(origProps);
        } else if (isFsSupport(origProps, FS_GCS_SUPPORT)) {
            throw new UserException("Unsupported native GCS filesystem");
        } else if (isFsSupport(origProps, FS_AZURE_SUPPORT)) {
            throw new UserException("Unsupported native AZURE filesystem");
        }

        // In previous version, we don't support fs.xxx.support properties.
        // So we need to "guess" this info from the properties.
        
    }

    protected StorageProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }

    private boolean isFsSupport(Map<String, String> origProps, String fsEnable) {
        return origProps.getOrDefault(fsEnable, "false").equalsIgnoreCase("true");
    }
}
