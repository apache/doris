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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public enum SchemaTypeMapper {

    S3("s3", StorageProperties.Type.S3, FileSystemType.S3, TFileType.FILE_S3),
    S3A("s3a", StorageProperties.Type.S3, FileSystemType.S3, TFileType.FILE_S3),
    S3N("s3n", StorageProperties.Type.S3, FileSystemType.S3, TFileType.FILE_S3),
    COSN("cosn", StorageProperties.Type.COS, FileSystemType.S3, TFileType.FILE_S3),
    OFS("ofs", StorageProperties.Type.BROKER, FileSystemType.OFS, TFileType.FILE_BROKER),
    GFS("gfs", StorageProperties.Type.BROKER, FileSystemType.HDFS, TFileType.FILE_BROKER),
    JFS("jfs", StorageProperties.Type.BROKER, FileSystemType.JFS, TFileType.FILE_BROKER),
    VIEWFS("viewfs", StorageProperties.Type.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS),
    //LAKEFS("lakefs", StorageProperties.Type.LAKEFS),
    //GCS("gs", StorageProperties.Type.S3),
    //BOS("bos", StorageProperties.Type.BOS),
    FILE("file", StorageProperties.Type.LOCAL, FileSystemType.FILE, TFileType.FILE_LOCAL),

    OSS("oss", StorageProperties.Type.OSS, FileSystemType.S3, TFileType.FILE_S3),
    OBS("obs", StorageProperties.Type.OBS, FileSystemType.S3, TFileType.FILE_S3),
    COS("cos", StorageProperties.Type.COS, FileSystemType.S3, TFileType.FILE_S3),
    //MINIO("minio", StorageProperties.Type.MINIO),
    /*
     * Only secure protocols are supported to ensure safe access to Azure storage services.
     * This implementation allows only "abfss" and "wasbs" schemes, which operate over HTTPS.
     * Insecure or deprecated schemes such as "abfs", "wasb", and "adl" are explicitly unsupported.
     * */
    ABFSS("abfss", StorageProperties.Type.AZURE, FileSystemType.S3, TFileType.FILE_S3),
    WASBS("wasbs", StorageProperties.Type.AZURE, FileSystemType.S3, TFileType.FILE_S3),
    AZURE("azure", StorageProperties.Type.AZURE, FileSystemType.S3, TFileType.FILE_S3),
    HDFS("hdfs", StorageProperties.Type.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS),
    LOCAL("local", StorageProperties.Type.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
    private final String schema;
    private final StorageProperties.Type storageType;
    private final FileSystemType fileSystemType;
    private final TFileType fileType;

    SchemaTypeMapper(String schema, StorageProperties.Type storageType, FileSystemType fileSystemType,
                     TFileType fileType) {
        this.schema = schema;
        this.storageType = storageType;
        this.fileSystemType = fileSystemType;
        this.fileType = fileType;
    }


    private static final Map<String, StorageProperties.Type> SCHEMA_TO_TYPE_MAP = new HashMap<>();

    static {
        for (SchemaTypeMapper mapper : values()) {
            SCHEMA_TO_TYPE_MAP.put(mapper.schema.toLowerCase(), mapper.storageType);
        }
    }

    private static final Map<String, FileSystemType> SCHEMA_TO_FS_TYPE_MAP = new HashMap<>();

    static {
        for (SchemaTypeMapper mapper : values()) {
            SCHEMA_TO_FS_TYPE_MAP.put(mapper.schema.toLowerCase(), mapper.fileSystemType);
        }
    }

    private static final Map<String, TFileType> SCHEMA_TO_FILE_TYPE_MAP = new HashMap<>();

    static {
        for (SchemaTypeMapper mapper : values()) {
            SCHEMA_TO_FILE_TYPE_MAP.put(mapper.schema.toLowerCase(), mapper.fileType);
        }
    }

    /*
     * Compatibility note:
     * When processing HDFS paths, if the URI lacks a schema (protocol),
     * it is assumed to be of "hdfs" type by default. This is a compatibility sacrifice
     * made to support legacy behaviors.
     *
     * Legacy systems often omitted the schema in HDFS paths, e.g. "/user/hadoop/data"
     * instead of "hdfs:///user/hadoop/data". To avoid breaking existing code,
     * this default assumption is applied for smoother compatibility and migration.
     */
    public static StorageProperties.Type fromSchema(String schema) {
        if (StringUtils.isBlank(schema)) {
            return StorageProperties.Type.HDFS;
        }
        return SCHEMA_TO_TYPE_MAP.get(schema.toLowerCase());
    }

    public static FileSystemType fromSchemaToFileSystemType(String schema) {
        if (schema == null) {
            return FileSystemType.HDFS;
        }
        return SCHEMA_TO_FS_TYPE_MAP.get(schema.toLowerCase());
    }

    public static TFileType fromSchemaToFileType(String schema) {
        if (schema == null) {
            return TFileType.FILE_HDFS;
        }
        return SCHEMA_TO_FILE_TYPE_MAP.get(schema.toLowerCase());
    }
}
