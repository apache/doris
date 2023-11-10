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
import org.apache.doris.common.util.URI;
import org.apache.doris.datasource.property.constants.BosProperties;
import org.apache.doris.thrift.TStorageBackendType;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class StorageBackend implements ParseNode {
    private String location;
    private StorageDesc storageDesc;

    public static void checkPath(String path, StorageBackend.StorageType type) throws AnalysisException {
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("No destination path specified.");
        }
        checkUri(URI.create(path), type);
    }

    public static void checkUri(URI uri, StorageBackend.StorageType type) throws AnalysisException {
        String schema = uri.getScheme();
        if (schema == null) {
            throw new AnalysisException(
                    "Invalid export path, there is no schema of URI found. please check your path.");
        }
        if (type == StorageBackend.StorageType.BROKER) {
            if (!schema.equalsIgnoreCase("bos")
                    && !schema.equalsIgnoreCase("afs")
                    && !schema.equalsIgnoreCase("hdfs")
                    && !schema.equalsIgnoreCase("viewfs")
                    && !schema.equalsIgnoreCase("ofs")
                    && !schema.equalsIgnoreCase("obs")
                    && !schema.equalsIgnoreCase("oss")
                    && !schema.equalsIgnoreCase("s3a")
                    && !schema.equalsIgnoreCase("cosn")
                    && !schema.equalsIgnoreCase("gfs")
                    && !schema.equalsIgnoreCase("jfs")
                    && !schema.equalsIgnoreCase("gs")) {
                throw new AnalysisException("Invalid broker path. please use valid 'hdfs://', 'viewfs://', 'afs://',"
                        + " 'bos://', 'ofs://', 'obs://', 'oss://', 's3a://', 'cosn://', 'gfs://', 'gs://'"
                        + " or 'jfs://' path.");
            }
        } else if (type == StorageBackend.StorageType.S3 && !schema.equalsIgnoreCase("s3")) {
            throw new AnalysisException("Invalid export path. please use valid 's3://' path.");
        } else if (type == StorageBackend.StorageType.HDFS && !schema.equalsIgnoreCase("hdfs")
                && !schema.equalsIgnoreCase("viewfs")) {
            throw new AnalysisException("Invalid export path. please use valid 'HDFS://' or 'viewfs://' path.");
        } else if (type == StorageBackend.StorageType.LOCAL && !schema.equalsIgnoreCase("file")) {
            throw new AnalysisException(
                    "Invalid export path. please use valid '" + OutFileClause.LOCAL_FILE_PREFIX + "' path.");
        }
    }

    public StorageBackend(String storageName, String location,
            StorageType storageType, Map<String, String> properties) {
        this.storageDesc = new StorageDesc(storageName, storageType, properties);
        this.location = location;
        boolean convertedToS3 = BosProperties.tryConvertBosToS3(properties, storageType);
        if (convertedToS3) {
            this.storageDesc.setStorageType(StorageBackend.StorageType.S3);
            this.location = BosProperties.convertPathToS3(location);
        } else {
            this.location = location;
        }
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
    public void analyze(Analyzer analyzer) throws UserException {
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
        checkPath(location, storageType);
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
            .append(new PrintableMap<>(storageDesc.getProperties(), " = ", true, false))
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
        STREAM("Stream load pipe");

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
                default:
                    return TStorageBackendType.BROKER;
            }
        }
    }

}
