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
import org.apache.doris.datasource.property.storage.AzurePropertyUtils;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.fs.SchemaTypeMapper;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

/**
 * LocationPath is a utility class for parsing, validating, and normalizing storage location URIs.
 * It supports various storage backends such as HDFS, S3, OSS, and local file systems.
 * <p>
 * Core responsibilities include:
 * - Extracting the schema (e.g., "s3", "hdfs", "file") from a location string.
 * - Normalizing the location path using the corresponding {@link StorageProperties} for the schema.
 * - Deriving the file system identifier (e.g., "s3://bucket") to uniquely identify a storage endpoint.
 * - Mapping the schema to corresponding {@link TFileType} and {@link FileSystemType} for backend access.
 * <p>
 * Special handling:
 * - Supports both standard ("scheme://") and nonstandard ("scheme:/") URI formats.
 * - If the schema is missing (e.g., for local paths), it gracefully falls back to treating the path as local/HDFS.
 * - Includes fallback compatibility logic for legacy schema mappings (e.g., S3 vs COS vs MinIO).
 * <p>
 * This class is often used by Frontend to pass normalized locations and storage metadata to Backend (BE).
 */
public class LocationPath {
    private static final Logger LOG = LogManager.getLogger(LocationPath.class);
    private static final String SCHEME_DELIM = "://";
    private static final String NONSTANDARD_SCHEME_DELIM = ":/";

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

    private static String parseScheme(String finalLocation) {
        // Use indexOf instead of split for better performance
        int schemeDelimIndex = finalLocation.indexOf(SCHEME_DELIM);
        if (schemeDelimIndex > 0) {
            return finalLocation.substring(0, schemeDelimIndex);
        }

        int nonstandardDelimIndex = finalLocation.indexOf(NONSTANDARD_SCHEME_DELIM);
        if (nonstandardDelimIndex > 0) {
            return finalLocation.substring(0, nonstandardDelimIndex);
        }

        // if not get scheme, need consider /path/to/local to no scheme
        try {
            Paths.get(finalLocation);
        } catch (InvalidPathException exception) {
            throw new IllegalArgumentException("Fail to parse scheme, invalid location: " + finalLocation);
        }

        return "";
    }

    /**
     * Static factory method to create a LocationPath instance.
     *
     * @param location             the input URI location string
     * @param storagePropertiesMap map of schema type to corresponding storage properties
     * @return a new LocationPath instance
     * @throws UserException if validation fails or required data is missing
     */
    public static LocationPath of(String location,
                                  Map<StorageProperties.Type, StorageProperties> storagePropertiesMap,
                                  boolean normalize) throws UserException {
        String schema = extractScheme(location);
        String normalizedLocation = location;
        StorageProperties storageProperties = null;
        StorageProperties.Type type = fromSchemaWithContext(location, schema);
        if (StorageProperties.Type.LOCAL.equals(type)) {
            normalize = false;
        }
        if (normalize) {
            storageProperties = findStorageProperties(type, schema, storagePropertiesMap);

            if (storageProperties == null) {
                throw new UserException("No storage properties found for schema: " + schema);
            }
            normalizedLocation = storageProperties.validateAndNormalizeUri(location);
            if (StringUtils.isBlank(normalizedLocation)) {
                throw new IllegalArgumentException("Invalid location: " + location + ", normalized location is null");
            }
        }
        String encodedLocation = encodedLocation(normalizedLocation);
        URI uri = URI.create(encodedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());

        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageProperties);
    }

    public static StorageProperties.Type fromSchemaWithContext(String location, String schema) {
        if (isHdfsOnOssEndpoint(location)) {
            return StorageProperties.Type.OSS_HDFS;
        }
        return SchemaTypeMapper.fromSchema(schema); // fallback to default
    }

    public static LocationPath of(String location) {
        String schema = extractScheme(location);
        String encodedLocation = encodedLocation(location);
        URI uri = URI.create(encodedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return new LocationPath(schema, location, fsIdentifier, null);
    }

    /**
     * Static factory method to create a LocationPath instance.
     *
     * @param location             the input URI location string
     * @param storagePropertiesMap map of schema type to corresponding storage properties
     * @return a new LocationPath instance
     */
    public static LocationPath of(String location,
                                  Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        try {
            return LocationPath.of(location, storagePropertiesMap, true);
        } catch (UserException e) {
            throw new StoragePropertiesException("Failed to create LocationPath for location: " + location, e);
        }
    }

    public static LocationPath of(String location,
                                  StorageProperties storageProperties) {
        try {
            String schema = extractScheme(location);
            String normalizedLocation = storageProperties.validateAndNormalizeUri(location);
            String encodedLocation = encodedLocation(normalizedLocation);
            URI uri = URI.create(encodedLocation);
            String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://"
                    + Strings.nullToEmpty(uri.getAuthority());
            return new LocationPath(schema, normalizedLocation, fsIdentifier, storageProperties);
        } catch (UserException e) {
            throw new StoragePropertiesException("Failed to create LocationPath for location: " + location, e);
        }
    }

    /**
     * Ultra-fast factory method that directly constructs LocationPath without any parsing.
     * This is used when the normalized location is already known (e.g., from prefix transformation).
     *
     * @param normalizedLocation the already-normalized location string
     * @param schema             pre-computed schema
     * @param fsIdentifier       pre-computed filesystem identifier
     * @param storageProperties  the storage properties (can be null)
     * @return a new LocationPath instance
     */
    public static LocationPath ofDirect(String normalizedLocation,
                                        String schema,
                                        String fsIdentifier,
                                        StorageProperties storageProperties) {
        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageProperties);
    }

    /**
     * Fast factory method that reuses pre-computed schema and fsIdentifier.
     * This is optimized for batch processing where many files share the same bucket/prefix.
     *
     * @param location           the input URI location string
     * @param storageProperties  pre-computed storage properties for normalization
     * @param cachedSchema       pre-computed schema (can be null to compute)
     * @param cachedFsIdPrefix   pre-computed fsIdentifier prefix like "s3://" (can be null to compute)
     * @return a new LocationPath instance
     */
    public static LocationPath ofWithCache(String location,
                                           StorageProperties storageProperties,
                                           String cachedSchema,
                                           String cachedFsIdPrefix) {
        try {
            String normalizedLocation = storageProperties.validateAndNormalizeUri(location);

            String fsIdentifier;
            if (cachedFsIdPrefix != null && normalizedLocation.startsWith(cachedFsIdPrefix)) {
                // Fast path: extract authority from normalized location without full URI parsing
                int authorityStart = cachedFsIdPrefix.length();
                int authorityEnd = normalizedLocation.indexOf('/', authorityStart);
                if (authorityEnd == -1) {
                    authorityEnd = normalizedLocation.length();
                }
                String authority = normalizedLocation.substring(authorityStart, authorityEnd);
                if (authority.isEmpty()) {
                    throw new StoragePropertiesException("Invalid location, missing authority: " + normalizedLocation);
                }
                fsIdentifier = cachedFsIdPrefix + authority;
            } else {
                // Fallback to full URI parsing
                String encodedLocation = encodedLocation(normalizedLocation);
                URI uri = URI.create(encodedLocation);
                String authority = uri.getAuthority();
                if (Strings.isNullOrEmpty(authority)) {
                    throw new StoragePropertiesException("Invalid location, missing authority: " + normalizedLocation);
                }
                fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://"
                        + authority;
            }

            String schema = cachedSchema != null ? cachedSchema : extractScheme(location);
            return new LocationPath(schema, normalizedLocation, fsIdentifier, storageProperties);
        } catch (UserException e) {
            throw new StoragePropertiesException("Failed to create LocationPath for location: " + location, e);
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
        return parseScheme(location);
    }

    /**
     * Finds the appropriate {@link StorageProperties} configuration for a given storage type and schema.
     * <p>
     * This method attempts to locate the storage properties using the following logic:
     * <p>
     * 1. Direct match by type: Attempts to retrieve the properties from the map using the given {@code type}.
     * 2. S3-Minio fallback: If the requested type is S3 and no properties are found, try to fall back to MinIO
     * configuration,
     * assuming it is compatible with S3.
     * 3. Compatibility fallback based on schema:
     * In older configurations, the schema name might not strictly match the actual storage type.
     * For example, a COS storage might use the "s3" schema, or an S3 storage might use the "cos" schema.
     * To handle such legacy inconsistencies, we try to find any storage configuration with the name "s3"
     * if the schema maps to a file type of FILE_S3.
     *
     * @param type                 the storage type to search for
     * @param schema               the schema string used in the original request (e.g., "s3://bucket/file")
     * @param storagePropertiesMap a map of available storage types to their configuration
     * @return a matching {@link StorageProperties} if found; otherwise, {@code null}
     */
    private static StorageProperties findStorageProperties(StorageProperties.Type type, String schema,
                                                           Map<StorageProperties.Type, StorageProperties>
                                                                   storagePropertiesMap) {
        // Step 1: Try direct match by type
        StorageProperties props = storagePropertiesMap.get(type);
        if (props != null) {
            return props;
        }

        // Step 2: Fallback - if type is S3 and MinIO is configured, assume it's compatible
        if (type == StorageProperties.Type.S3
                && storagePropertiesMap.containsKey(StorageProperties.Type.MINIO)) {
            return storagePropertiesMap.get(StorageProperties.Type.MINIO);
        }

        // Step 3: Compatibility fallback based on schema
        // In previous configurations, the schema name may not strictly match the actual storage type.
        // For example, a COS storage might use the "s3" schema, or an S3 storage might use the "cos" schema.
        // To handle such legacy inconsistencies, we try to find a storage configuration whose name is "s3".
        if (TFileType.FILE_S3.equals(SchemaTypeMapper.fromSchemaToFileType(schema))) {
            return storagePropertiesMap.values().stream()
                    .filter(p -> "s3".equalsIgnoreCase(p.getStorageName()))
                    .findFirst()
                    .orElse(null);
        }

        // Not found
        return null;
    }


    private static String encodedLocation(String location) {
        try {
            return URLEncoder.encode(location, StandardCharsets.UTF_8.name())
                    .replace("%2F", "/")
                    .replace("%3A", ":");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to encode location: " + location, e);
        }
    }


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
        if (StringUtils.isBlank(location)) {
            return null;
        }
        if (isHdfsOnOssEndpoint(location)) {
            return TFileType.FILE_HDFS;
        }
        LocationPath locationPath = LocationPath.of(location);
        return locationPath.getTFileTypeForBE();
    }

    public static String getTempWritePath(String loc, String prefix) {
        // If prefix is relative, it is resolved under loc; if absolute, it is used as the base path.
        Path tempRoot = new Path(loc, prefix);
        Path tempPath = new Path(tempRoot, UUID.randomUUID().toString().replace("-", ""));
        return tempPath.toString();
    }

    public TFileType getTFileTypeForBE() {
        if ((SchemaTypeMapper.ABFS.getSchema().equals(schema) || SchemaTypeMapper.ABFSS.getSchema()
                .equals(schema)) && AzurePropertyUtils.isOneLakeLocation(normalizedLocation)) {
            return TFileType.FILE_HDFS;
        }
        if (StringUtils.isNotBlank(normalizedLocation) && isHdfsOnOssEndpoint(normalizedLocation)) {
            return TFileType.FILE_HDFS;
        }
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
        if ((SchemaTypeMapper.ABFS.getSchema().equals(schema) || SchemaTypeMapper.ABFSS.getSchema()
                .equals(schema)) && AzurePropertyUtils.isOneLakeLocation(normalizedLocation)) {
            return FileSystemType.HDFS;
        }
        return SchemaTypeMapper.fromSchemaToFileSystemType(schema);
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
