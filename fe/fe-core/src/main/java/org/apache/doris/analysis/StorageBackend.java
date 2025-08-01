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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TStorageBackendType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

public class StorageBackend implements ParseNode {
    private String location;
    private StorageDesc storageDesc;

    public static void checkPath(String path, StorageBackend.StorageType type, String exceptionMsg)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException(exceptionMsg == null ? "No destination path specified." : exceptionMsg);
        }
        //checkUri(URI.create(path), type);
    }


    public StorageBackend(String storageName, String location,
                          StorageType storageType, Map<String, String> properties) {
        this.storageDesc = new StorageDesc(storageName, storageType, properties);
        this.location = location;
        /*boolean convertedToS3 = BosProperties.tryConvertBosToS3(properties, storageType);
        if (convertedToS3) {
            this.storageDesc.setStorageType(StorageBackend.StorageType.S3);
            this.location = BosProperties.convertPathToS3(location);
        } else {
            this.location = location;
        }*/
    }

    public void setStorageDesc(StorageDesc storageDesc) {
        this.storageDesc = storageDesc;
    }

    public StorageDesc getStorageDesc() {
        return storageDesc;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public void analyze() throws UserException {
        StorageBackend.StorageType storageType = storageDesc.getStorageType();
        if (storageType != StorageType.BROKER && StringUtils.isEmpty(storageDesc.getName())) {
            storageDesc.setName(storageType.name());
        }
        if (storageType != StorageType.BROKER && storageType != StorageType.S3
                && storageType != StorageType.HDFS) {
            throw new NotImplementedException(storageType.toString() + " is not support now.");
        }
        FeNameFormat.checkCommonName("repository", storageDesc.getName());

        if (Strings.isNullOrEmpty(location)) {
            throw new AnalysisException("You must specify a location on the repository");
        }
        checkPath(location, storageType, null);
    }

    public void validate() throws UserException {
        StorageBackend.StorageType storageType = storageDesc.getStorageType();
        if (storageType != StorageType.BROKER && StringUtils.isEmpty(storageDesc.getName())) {
            storageDesc.setName(storageType.name());
        }
        if (storageType != StorageType.BROKER && storageType != StorageType.S3
                && storageType != StorageType.HDFS) {
            throw new NotImplementedException(storageType.toString() + " is not support now.");
        }
        FeNameFormat.checkCommonName("repository", storageDesc.getName());

        if (Strings.isNullOrEmpty(location)) {
            throw new AnalysisException("You must specify a location on the repository");
        }
        checkPath(location, storageType, null);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        StorageBackend.StorageType storageType = storageDesc.getStorageType();
        sb.append(storageType.name());
        if (storageType == StorageType.BROKER) {
            sb.append(" `").append(storageDesc.getName()).append("`");
        }
        sb.append(" ON LOCATION ").append(location).append(" PROPERTIES(")
                .append(new PrintableMap<>(storageDesc.getProperties(), " = ", true, false, true))
                .append(")");
        return sb.toString();
    }

    public enum StorageType {
        BROKER("Doris Broker"),
        S3("Amazon S3 Simple Storage Service"),
        HDFS("Hadoop Distributed File System"),
        LOCAL("Local file system"),
        OFS("Tencent CHDFS"),
        GFS("Tencent Goose File System"),
        JFS("Juicefs"),
        STREAM("Stream load pipe"),
        AZURE("MicroSoft Azure Blob");

        @SerializedName("desc")
        private final String description;

        StorageType(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }

        public TStorageBackendType toThrift() {
            switch (this) {
                case S3:
                    return TStorageBackendType.S3;
                case HDFS:
                    return TStorageBackendType.HDFS;
                case OFS:
                    return TStorageBackendType.OFS;
                case JFS:
                    return TStorageBackendType.JFS;
                case LOCAL:
                    return TStorageBackendType.LOCAL;
                // deprecated
                case AZURE:
                    return TStorageBackendType.AZURE;
                default:
                    return TStorageBackendType.BROKER;
            }
        }

        /**
         * A set of storage types that currently support parameter refactoring.
         * <p>
         * Includes: S3 (referring to all systems compatible with the S3 protocol),
         * HDFS, OFS, JFS, and AZURE. For S3, this is a generalized type that matches
         * any system whose storage type name is returned as "s3" (or compatible)
         * by {@link org.apache.doris.datasource.property.storage.StorageProperties#getStorageName()}.
         * <p>
         * This set is a temporary solution. Once parameter refactoring is fully supported
         * across all storage systems, this class can be removed.
         */
        public static final Set<StorageType> REFACTOR_STORAGE_TYPES =
                ImmutableSet.of(StorageType.S3, StorageType.HDFS, StorageType.OFS, StorageType.JFS, StorageType.AZURE);

        public static StorageType convertToStorageType(String storageName) {
            switch (storageName.toLowerCase()) {
                case "hdfs":
                    return StorageType.HDFS;
                case "s3":
                    return StorageType.S3;
                case "jfs":
                    return StorageType.JFS;
                case "local":
                    return StorageType.LOCAL;
                default:
                    throw new IllegalArgumentException("Invalid storage type: " + storageName);
            }
        }
    }

}
