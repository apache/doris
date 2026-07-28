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
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageRegistry;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.datasource.storage.StorageUriUtils;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

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
 * - Normalizing the location path using the corresponding {@link StorageAdapter} for the schema.
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
     * Storage facade binding associated with this schema (null when created without
     * normalization context, e.g. {@link #of(String)}).
     */
    private final StorageAdapter storageAdapter;

    /**
     * Private constructor to enforce creation through the factory methods.
     */
    private LocationPath(String schema,
                         String normalizedLocation,
                         String fsIdentifier,
                         StorageAdapter storageAdapter) {
        this.schema = schema;
        this.normalizedLocation = normalizedLocation;
        this.fsIdentifier = fsIdentifier;
        this.storageAdapter = storageAdapter;
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
     * Static factory method to create a LocationPath instance over a type-keyed adapter map.
     *
     * @param location           the input URI location string
     * @param storageAdaptersMap map of storage type id to corresponding facade binding
     * @return a new LocationPath instance
     * @throws UserException if validation fails or required data is missing
     */
    public static LocationPath ofAdapters(String location,
                                          Map<StorageTypeId, StorageAdapter> storageAdaptersMap,
                                          boolean normalize) throws UserException {
        String schema = extractScheme(location);
        String normalizedLocation = location;
        StorageAdapter storageAdapter = null;
        StorageTypeId type = fromSchemaWithContext(location, schema);
        if (StorageTypeId.LOCAL.equals(type)) {
            normalize = false;
        }
        if (normalize) {
            storageAdapter = findStorageAdapter(type, schema, storageAdaptersMap);

            if (storageAdapter == null) {
                throw new UserException("No storage properties found for schema: " + schema);
            }
            normalizedLocation = storageAdapter.validateAndNormalizeUri(location);
            if (StringUtils.isBlank(normalizedLocation)) {
                throw new IllegalArgumentException("Invalid location: " + location + ", normalized location is null");
            }
        }
        String encodedLocation = encodedLocation(normalizedLocation);
        URI uri = URI.create(encodedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        if (StringUtils.isBlank(schema)) {
            schema = Strings.nullToEmpty(uri.getScheme());
        }

        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageAdapter);
    }

    public static StorageTypeId fromSchemaWithContext(String location, String schema) {
        if (isHdfsOnOssEndpoint(location)) {
            return StorageTypeId.OSS_HDFS;
        }
        return StorageRegistry.fromScheme(schema); // fallback to default
    }

    public static LocationPath of(String location) {
        String schema = extractScheme(location);
        String encodedLocation = encodedLocation(location);
        URI uri = URI.create(encodedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());
        return new LocationPath(schema, location, fsIdentifier, null);
    }

    /** Normalizing factory over a type-keyed adapter map (unchecked wrapper). */
    public static LocationPath ofAdapters(String location,
                                          Map<StorageTypeId, StorageAdapter> storageAdaptersMap) {
        try {
            return LocationPath.ofAdapters(location, storageAdaptersMap, true);
        } catch (UserException | StoragePropertiesException e) {
            // the facade throws unchecked StoragePropertiesException where legacy threw checked
            // UserException — keep the legacy location context in the wrapped message
            throw new StoragePropertiesException("Failed to create LocationPath for location: " + location, e);
        }
    }

    /** Normalizing factory over a single pre-resolved facade binding. */
    public static LocationPath of(String location,
                                  StorageAdapter storageAdapter) {
        String schema = extractScheme(location);
        String normalizedLocation = storageAdapter.validateAndNormalizeUri(location);
        String encodedLocation = encodedLocation(normalizedLocation);
        URI uri = URI.create(encodedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://"
                + Strings.nullToEmpty(uri.getAuthority());
        if (StringUtils.isBlank(schema)) {
            schema = Strings.nullToEmpty(uri.getScheme());
        }
        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageAdapter);
    }

    /**
     * Ultra-fast factory method that directly constructs LocationPath without any parsing.
     * This is used when the normalized location is already known (e.g., from prefix transformation).
     *
     * @param normalizedLocation the already-normalized location string
     * @param schema             pre-computed schema
     * @param fsIdentifier       pre-computed filesystem identifier
     * @param storageAdapter     the facade binding (can be null)
     * @return a new LocationPath instance
     */
    public static LocationPath ofDirect(String normalizedLocation,
                                        String schema,
                                        String fsIdentifier,
                                        StorageAdapter storageAdapter) {
        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageAdapter);
    }

    /**
     * Fast factory method that reuses pre-computed schema and fsIdentifier.
     * This is optimized for batch processing where many files share the same bucket/prefix.
     *
     * @param location         the input URI location string
     * @param storageAdapter   pre-computed facade binding for normalization
     * @param cachedSchema     pre-computed schema (can be null to compute)
     * @param cachedFsIdPrefix pre-computed fsIdentifier prefix like "s3://" (can be null to compute)
     * @return a new LocationPath instance
     */
    public static LocationPath ofWithCache(String location,
                                           StorageAdapter storageAdapter,
                                           String cachedSchema,
                                           String cachedFsIdPrefix) {
        String normalizedLocation = storageAdapter.validateAndNormalizeUri(location);

        String fsIdentifier;
        String schema = cachedSchema;
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
            if (StringUtils.isBlank(schema)) {
                schema = cachedFsIdPrefix.substring(0, cachedFsIdPrefix.length() - SCHEME_DELIM.length());
            }
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
            if (StringUtils.isBlank(schema)) {
                schema = Strings.nullToEmpty(uri.getScheme());
            }
        }

        return new LocationPath(schema, normalizedLocation, fsIdentifier, storageAdapter);
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
     * Finds the appropriate {@link StorageAdapter} binding for a given storage type and schema.
     * <p>
     * This method attempts to locate the binding using the following logic:
     * <p>
     * 1. Direct match by type: Attempts to retrieve the binding from the map using the given {@code type}.
     * 2. S3-Minio fallback: If the requested type is S3 and no binding is found, try to fall back to MinIO
     * (or Ozone) configuration, assuming it is compatible with S3.
     * 3. Compatibility fallback based on schema:
     * In older configurations, the schema name might not strictly match the actual storage type.
     * For example, a COS storage might use the "s3" schema, or an S3 storage might use the "cos" schema.
     * To handle such legacy inconsistencies, we try to find any storage configuration with the name "s3"
     * if the schema maps to a file type of FILE_S3.
     *
     * @param type               the storage type to search for
     * @param schema             the schema string used in the original request (e.g., "s3://bucket/file")
     * @param storageAdaptersMap a map of available storage types to their bindings
     * @return a matching {@link StorageAdapter} if found; otherwise, {@code null}
     */
    private static StorageAdapter findStorageAdapter(StorageTypeId type, String schema,
                                                     Map<StorageTypeId, StorageAdapter> storageAdaptersMap) {
        // Step 1: Try direct match by type
        StorageAdapter adapter = storageAdaptersMap.get(type);
        if (adapter != null) {
            return adapter;
        }

        // Step 2: Fallback - if type is S3 and MinIO is configured, assume it's compatible
        if (type == StorageTypeId.S3
                && storageAdaptersMap.containsKey(StorageTypeId.MINIO)) {
            return storageAdaptersMap.get(StorageTypeId.MINIO);
        }
        if (type == StorageTypeId.S3
                && storageAdaptersMap.containsKey(StorageTypeId.OZONE)) {
            return storageAdaptersMap.get(StorageTypeId.OZONE);
        }

        // Step 3: Compatibility fallback based on schema
        // In previous configurations, the schema name may not strictly match the actual storage type.
        // For example, a COS storage might use the "s3" schema, or an S3 storage might use the "cos" schema.
        // To handle such legacy inconsistencies, we try to find a storage configuration whose name is "s3".
        if (TFileType.FILE_S3.equals(StorageRegistry.fromSchemeToFileType(schema))) {
            return storageAdaptersMap.values().stream()
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
        if (("abfs".equals(schema) || "abfss".equals(schema))
                && StorageUriUtils.isOneLakeLocation(normalizedLocation)) {
            return TFileType.FILE_HDFS;
        }
        if (StringUtils.isNotBlank(normalizedLocation) && isHdfsOnOssEndpoint(normalizedLocation)) {
            return TFileType.FILE_HDFS;
        }
        return StorageRegistry.fromSchemeToFileType(schema);
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
        if (("abfs".equals(schema) || "abfss".equals(schema))
                && StorageUriUtils.isOneLakeLocation(normalizedLocation)) {
            return FileSystemType.HDFS;
        }
        return StorageRegistry.fromSchemeToFileSystemType(schema);
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

    public StorageAdapter getStorageAdapter() {
        return storageAdapter;
    }

    public Path getPath() {
        return new Path(normalizedLocation);
    }
}
