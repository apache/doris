package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.StorageProperties;

import java.util.HashMap;
import java.util.Map;

public enum SchemaTypeMapper {
    
    S3("s3", StorageProperties.Type.S3),
    S3A("s3a", StorageProperties.Type.S3),
    S3N("s3n", StorageProperties.Type.S3),
    //GCS("gs", StorageProperties.Type.S3),
    //BOS("bos", StorageProperties.Type.BOS),
    COSN("cosn", StorageProperties.Type.COS),
    //LAKEFS("lakefs", StorageProperties.Type.LAKEFS),
    OFS("ofs", StorageProperties.Type.HDFS),
/*    GFS("gfs", StorageProperties.Type.GFS),
    JFS("jfs", StorageProperties.Type.JFS),
    FILE("file", StorageProperties.Type.FILE),*/
        
    OSS("oss", StorageProperties.Type.OSS),
    OBS("obs", StorageProperties.Type.OBS),
    COS("cos", StorageProperties.Type.COS),
    MINIO("minio", StorageProperties.Type.MINIO),
    AZURE("azure", StorageProperties.Type.AZURE),
    HDFS("hdfs", StorageProperties.Type.HDFS);
    private final String schema;
    private final StorageProperties.Type storageType;
    
    SchemaTypeMapper(String schema, StorageProperties.Type storageType) {
        this.schema = schema;
        this.storageType = storageType;
    }


    private static final Map<String, StorageProperties.Type> SCHEMA_TO_TYPE_MAP = new HashMap<>();

    static {
        for (SchemaTypeMapper mapper : values()) {
            SCHEMA_TO_TYPE_MAP.put(mapper.schema.toLowerCase(), mapper.storageType);
        }
    }
    
    public static StorageProperties.Type fromSchema(String schema) {
        if (schema == null) {
            return null;
        }
        return SCHEMA_TO_TYPE_MAP.get(schema.toLowerCase());
    }
}
