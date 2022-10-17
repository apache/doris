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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StorageBackend extends StorageDesc implements ParseNode {
    private static final Logger LOG = LoggerFactory.getLogger(StorageBackend.class);

    private String location;
    private StorageType storageType;
    private Map<String, String> properties;

    public StorageBackend(String storageName, String location,
            StorageType storageType, Map<String, String> properties) {
        this.name = storageName;
        this.location = location;
        this.storageType = storageType;
        this.properties = properties;
        tryConvertToS3();
        this.location = convertPathToS3(location);
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public String getStorageName() {
        return name;
    }

    public void setStorageName(String storageName) {
        this.name = storageName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (this.storageType != StorageType.BROKER && StringUtils.isEmpty(name)) {
            name = this.storageType.name();
        }
        if (this.storageType != StorageType.BROKER && this.storageType != StorageType.S3
                && this.storageType != StorageType.HDFS) {
            throw new NotImplementedException(this.storageType.toString() + " is not support now.");
        }
        FeNameFormat.checkCommonName("repository", name);

        if (Strings.isNullOrEmpty(location)) {
            throw new AnalysisException("You must specify a location on the repository");
        }
        location = ExportStmt.checkPath(location, storageType);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(storageType.name());
        if (storageType == StorageType.BROKER) {
            sb.append(" `").append(name).append("`");
        }
        sb.append(" ON LOCATION ").append(location).append(" PROPERTIES(")
            .append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }

    public enum StorageType {
        BROKER("Doris Broker"),
        S3("Amazon S3 Simple Storage Service"),
        HDFS("Hadoop Distributed File System"),
        LOCAL("Local file system"),
        OFS("Tencent CHDFS"),
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
                case LOCAL:
                    return TStorageBackendType.LOCAL;
                default:
                    return TStorageBackendType.BROKER;
            }
        }
    }

}
