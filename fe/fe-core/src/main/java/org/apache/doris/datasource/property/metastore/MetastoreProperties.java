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
        Type msType = Type.UNKNOWN;
        if (origProps.containsKey(METASTORE_TYPE)) {
            String type = origProps.get(METASTORE_TYPE);
            switch (type) {
                case "hms":
                    msType = Type.HMS;
                case "glue":
                    msType = Type.GLUE;
                case "dlf":
                    msType = Type.DLF;
                case "rest":
                    msType = Type.ICEBERG_REST;
                case "dataproc":
                    msType = Type.DATAPROC;
                case "filesystem":
                    msType = Type.FILE_SYSTEM;
                default:
                    throw new IllegalArgumentException("Unknown 'metastore.type': " + type);
            }
        } else if (origProps.containsKey("hive.metastore.type")) {
            String type = origProps.get("hive.metastore.type");
            switch (type) {
                case "hms":
                    msType = Type.HMS;
                case "glue":
                    msType = Type.GLUE;
                case "dlf":
                    msType = Type.DLF;
                default:
                    throw new IllegalArgumentException("Unknown 'hive.metastore.type': " + type);
            }
        } else if (origProps.containsKey("iceberg.catalog.type")) {
            String type = origProps.get("iceberg.catalog.type");
            switch (type) {
                case "hms":
                    msType = Type.HMS;
                case "glue":
                    msType = Type.GLUE;
                case "rest":
                    msType = Type.ICEBERG_REST;
                case "hadoop":
                    msType = Type.FILE_SYSTEM;
                default:
                    throw new IllegalArgumentException("Unknown 'iceberg.catalog.type': " + type);
            }
        } else if (origProps.containsKey("paimon.catalog.type")) {
            String type = origProps.get("paimon.catalog.type");
            switch (type) {
                case "hms":
                    msType = Type.HMS;
                case "dlf":
                    msType = Type.DLF;
                default:
                    // default is "filesystem"
                    msType = Type.FILE_SYSTEM;
            }
        } else if (origProps.containsKey("type")) {
            String type = origProps.get("type");
            switch (type) {
                case "hms":
                    msType = Type.HMS;
                default:
                    throw new IllegalArgumentException("Unknown metastore 'type': " + type);
            }
        }

        return MetastoreProperties.create(msType, origProps);
    }

    public static MetastoreProperties create(Type type, Map<String, String> origProps) {
        MetastoreProperties metastoreProperties;
        switch (type) {
            case HMS:
                metastoreProperties = new HMSProperties(origProps);
                break;
            case GLUE:
                metastoreProperties = new AWSGlueProperties(origProps);
                break;
            case DLF:
                metastoreProperties = new AliyunDLFProperties(origProps);
                break;
            case ICEBERG_REST:
                metastoreProperties = new IcebergRestProperties(origProps);
                break;
            case DATAPROC:
                metastoreProperties = new DataProcProperties(origProps);
                break;
            case FILE_SYSTEM:
                metastoreProperties = new FileMetastoreProperties(origProps);
                break;
            default:
                throw new IllegalArgumentException("Unknown metastore type: " + type);
        }
        metastoreProperties.normalizedAndCheckProps();
        return metastoreProperties;
    }

    protected MetastoreProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }
}
