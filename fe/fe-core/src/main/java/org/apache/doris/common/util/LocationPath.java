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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.fs.SchemaTypeMapper;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class LocationPath {
    private static final Logger LOG = LogManager.getLogger(LocationPath.class);


    // Return true if this location is with oss-hdfs
    public static boolean isHdfsOnOssEndpoint(String location) {
        // example: cn-shanghai.oss-dls.aliyuncs.com contains the "oss-dls.aliyuncs".
        // https://www.alibabacloud.com/help/en/e-mapreduce/latest/oss-kusisurumen
        return location.contains("oss-dls.aliyuncs");
    }

    /**
     * provide file type for BE.
     *
     * @param location the location is from fs.listFile
     * @return on BE, we will use TFileType to get the suitable client to access storage.
     */
    public static TFileType getTFileTypeForBE(String location) {
        if (location == null || location.isEmpty()) {
            return null;
        }
        LocationPath locationPath = LocationPath.of(location);
        return locationPath.getTFileTypeForBE();
    }

    public static String getTempWritePath(String loc, String prefix) {
        Path tempRoot = new Path(loc, prefix);
        Path tempPath = new Path(tempRoot, UUID.randomUUID().toString().replace("-", ""));
        return tempPath.toString();
    }

    public TFileType getTFileTypeForBE() {
        return SchemaTypeMapper.fromSchemaToFileType(schema);
    }

    /**
     * The converted path is used for BE
     *
     * @return BE scan range path
     */
    public Path toStorageLocation() {
        return new Path(normalizedLocation);
    }


    public FileSystemType getFileSystemType() {
        return SchemaTypeMapper.fromSchemaToFileSystemType(schema);
    }


    /**
     * URI schema, e.g., "s3", "hdfs", "file"
     */
    private final String schema;

    /**
     * Normalized and validated location URI
     */
    private final String normalizedLocation;

    /**
     * Unique filesystem identifier, typically "scheme://authority"
     */
    private final String fsIdentifier;

    /**
     * Storage properties associated with this schema
     */
    private final StorageProperties storageProperties;

    /**
     * Private constructor to enforce creation through the factory method.
     */
    private LocationPath(String schema,
                         String normalizedLocation,
                         String fsIdentifier,
                         StorageProperties storageProperties) {
        this.schema = schema;
        this.normalizedLocation = normalizedLocation;
        this.fsIdentifier = fsIdentifier;
        this.storageProperties = storageProperties;
    }

    /**
     * Static factory method to create a LocationPath2 instance.
     *
     * @param location             the input URI location string
     * @param storagePropertiesMap map of schema type to corresponding storage properties
     * @return a new LocationPath2 instance
     * @throws UserException if validation fails or required data is missing
     */
    public static LocationPath of(String location,
                                  Map<StorageProperties.Type, StorageProperties> storagePropertiesMap,
                                  boolean normalize) throws UserException {
        String schema = extractScheme(location);

        String normalizedLocation = location;
        StorageProperties storageProperties = null;

        if (normalize) {
            StorageProperties.Type type = SchemaTypeMapper.fromSchema(schema);
            storageProperties = storagePropertiesMap.get(type);
            if (storageProperties == null) {
                if (type == StorageProperties.Type.S3
                        && storagePropertiesMap.containsKey(StorageProperties.Type.MINIO)) {
                    storageProperties = storagePropertiesMap.get(StorageProperties.Type.MINIO);
                } else {
                    throw new UserException("No StorageProperties found for schema: " + schema);
                }
            }
            normalizedLocation = storageProperties.validateAndNormalizeUri(location);
            if (StringUtils.isBlank(normalizedLocation)) {
                throw new UserException("Invalid location: " + location + ", normalized location is null");
            }
        }

        URI uri = URI.create(normalizedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());

        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageProperties);
    }

    public static LocationPath of(String location) {
        String schema = extractScheme(location);
        URI uri = URI.create(location);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return new LocationPath(schema, location, fsIdentifier, null);
    }

    /**
     * Static factory method to create a LocationPath2 instance.
     *
     * @param location             the input URI location string
     * @param storagePropertiesMap map of schema type to corresponding storage properties
     * @return a new LocationPath2 instance
     * @throws UserException if validation fails or required data is missing
     */
    public static LocationPath of(String location,
                                  Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        try {
            return LocationPath.of(location, storagePropertiesMap, true);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the URI scheme (e.g., "s3", "hdfs") from the location string.
     *
     * @param location the input URI string
     * @return the extracted scheme
     * @throws IllegalArgumentException if the scheme is missing or URI is malformed
     */
    private static String extractScheme(String location) {
        if (Strings.isNullOrEmpty(location)) {
            return null;
        }
        Path path = new Path(location);
        return path.toUri().getScheme();
    }

    // Getters (optional, if needed externally)
    public String getSchema() {
        return schema;
    }

    public String getNormalizedLocation() {
        return normalizedLocation;
    }

    public String getFsIdentifier() {
        return fsIdentifier;
    }

    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    public Path getPath() {
        return new Path(normalizedLocation);
    }
}
