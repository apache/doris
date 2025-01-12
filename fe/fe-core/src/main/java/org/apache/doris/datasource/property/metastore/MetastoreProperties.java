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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectionProperties;

import lombok.Getter;

import java.util.Map;

/**
 *
 */
public class MetastoreProperties extends ConnectionProperties {
    public static final String METASTORE_TYPE = "metastore.type";

    public enum Type {
        HMS,
        GLUE,
        DLF,
        ICEBERG_REST,
        DATAPROC,
        FILE_SYSTEM,
        UNKNOWN
    }

    @Getter
    protected MetastoreProperties.Type type;

    public static MetastoreProperties create(Map<String, String> origProps) {
        // "hive.metastore.type" = "dlf",
        // "hive.metastore.type" = "glue",

        // 'iceberg.catalog.type' = 'glue',
        // 'iceberg.catalog.type'='hms',
        // 'iceberg.catalog.type'='rest',

        // 'type'='hms',
        // "paimon.catalog.type" = "hms",dlf, filesystem
        if (origProps.containsKey(METASTORE_TYPE)) {
            String type = origProps.get(METASTORE_TYPE);
            switch (type) {
                case "hms":
                    return new HMSProperties(origProps);
                case "glue":
                    return new AWSGlueProperties(origProps);
                case "dlf":
                    return new AliyunDLFProperties(origProps);
                case "rest":
                    return new IcebergRestProperties(origProps);
                case "dataproc":
                    return new DataProcProperties(origProps);
                case "filesystem":
                    return new FileMetastoreProperties(origProps);
                default:
                    throw new IllegalArgumentException("Unknown metastore type: " + type);
            }
        } else if (origProps.containsKey("hive.metastore.type")) {
            String type = origProps.get("hive.metastore.type");
            switch (type) {
                case "hms":
                    return new HMSProperties(origProps);
                case "glue":
                    return new AWSGlueProperties(origProps);
                case "dlf":
                    return new AliyunDLFProperties(origProps);
                default:
                    throw new IllegalArgumentException("Unknown metastore type: " + type);
            }
        } else if (origProps.containsKey("iceberg.catalog.type")) {
            String type = origProps.get("iceberg.catalog.type");
            switch (type) {
                case "hms":
                    return new HMSProperties(origProps);
                case "glue":
                    return new AWSGlueProperties(origProps);
                case "rest":
                    return new IcebergRestProperties(origProps);
                case "hadoop":
                    return new FileMetastoreProperties(origProps);
                default:
                    throw new IllegalArgumentException("Unknown iceberg catalog type: " + type);
            }
        } else if (origProps.containsKey("paimon.catalog.type")) {
            String type = origProps.get("paimon.catalog.type");
            switch (type) {
                case "hms":
                    return new HMSProperties(origProps);
                case "dlf":
                    return new AliyunDLFProperties(origProps);
                default:
                    // default is "filesystem"
                    return new FileMetastoreProperties(origProps);
            }
        } else if (origProps.containsKey("type")) {
            String type = origProps.get("type");
            switch (type) {
                case "hms":
                    return new HMSProperties(origProps);
                default:
                    throw new IllegalArgumentException("Unknown metastore type: " + type);
            }
        }
        throw new IllegalArgumentException("Can not find metastore type in properties");
    }

    protected MetastoreProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }
}
