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
import org.apache.doris.datasource.property.ConnectionProperties;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public abstract class StorageProperties extends ConnectionProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";
    public static final String FS_OSS_SUPPORT = "fs.oss.support";
    public static final String FS_OBS_SUPPORT = "fs.obs.support";
    public static final String FS_COS_SUPPORT = "fs.cos.support";

    public enum Type {
        HDFS,
        S3,
        OSS,
        OBS,
        COS,
        UNKNOWN
    }

    public abstract Map<String, String> getBackendConfigProperties();

    @Getter
    protected Type type;

    /**
     * The purpose of this method is to create a list of StorageProperties based on the user specified properties.
     *
     * @param origProps
     * @return
     * @throws UserException
     */
    public static List<StorageProperties> create(Map<String, String> origProps) {
        List<StorageProperties> storageProperties = Lists.newArrayList();
        // 1. parse the storage properties by user specified fs.xxx.support properties
        if (isFsSupport(origProps, FS_HDFS_SUPPORT)) {
            storageProperties.add(new HDFSProperties(origProps));
        } else {
            // always try to add hdfs properties, because in previous version, we don't have fs.xxx.support properties,
            // the hdfs properties may be loaded from the configuration file.
            // so there is no way to guess the storage type.
            storageProperties.add(new HDFSProperties(origProps));
        }

        if (isFsSupport(origProps, FS_S3_SUPPORT) || S3Properties.guessIsMe(origProps)) {
            storageProperties.add(new S3Properties(origProps));
        }
        // FIXME: This logic directly checks for FS types (OSS, OBS, COS) here, which is intrusive.
        // We should refactor this so that the plugins themselves provide a method to check if they are supported,
        // thus decoupling the logic and making the system more extensible.
        if (isFsSupport(origProps, FS_OSS_SUPPORT)) {
            storageProperties.add(new OSSProperties(origProps));
        }
        if (isFsSupport(origProps, FS_OBS_SUPPORT)) {
            storageProperties.add(new OBSProperties(origProps));
        }
        if (isFsSupport(origProps, FS_COS_SUPPORT)) {
            storageProperties.add(new COSProperties(origProps));
        }

        if (isFsSupport(origProps, FS_GCS_SUPPORT)) {
            throw new RuntimeException("Unsupported native GCS filesystem");
        }
        if (isFsSupport(origProps, FS_AZURE_SUPPORT)) {
            throw new RuntimeException("Unsupported native AZURE filesystem");
        }

        if (storageProperties.isEmpty()) {
            throw new RuntimeException("Unknown storage type");
        } else {
            for (StorageProperties storageProperty : storageProperties) {
                storageProperty.normalizedAndCheckProps();
            }
        }
        return storageProperties;
    }

    public static StorageProperties createStorageProperties(Map<String, String> origProps) {
        StorageProperties storageProperties = null;
        // 1. parse the storage properties by user specified fs.xxx.support properties
        if (isFsSupport(origProps, FS_HDFS_SUPPORT)) {
            storageProperties = new HDFSProperties(origProps);
        }

        if (isFsSupport(origProps, FS_S3_SUPPORT) || S3Properties.guessIsMe(origProps)) {
            storageProperties = new S3Properties(origProps);
        }
        if (isFsSupport(origProps, FS_OSS_SUPPORT) || OSSProperties.guessIsMe(origProps)) {
            storageProperties = new OSSProperties(origProps);
        }
        if (isFsSupport(origProps, FS_OBS_SUPPORT) || OBSProperties.guessIsMe(origProps)) {
            storageProperties = new OBSProperties(origProps);
        }
        if (isFsSupport(origProps, FS_COS_SUPPORT) || COSProperties.guessIsMe(origProps)) {
            storageProperties = new COSProperties(origProps);
        }
        if (null == storageProperties) {
            throw new RuntimeException("not support this fs");
        }
        storageProperties.normalizedAndCheckProps();
        //load from default file
        return storageProperties;

    }


    protected StorageProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }

    private static boolean isFsSupport(Map<String, String> origProps, String fsEnable) {
        return origProps.getOrDefault(fsEnable, "false").equalsIgnoreCase("true");
    }

    protected static boolean checkIdentifierKey(Map<String, String> origProps, List<Field> fields) {
        for (Field field : fields) {
            field.setAccessible(true);
            ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
            for (String key : annotation.names()) {
                if (origProps.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

}
