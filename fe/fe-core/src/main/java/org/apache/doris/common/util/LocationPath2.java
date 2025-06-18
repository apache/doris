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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.fs.SchemaTypeMapper;
import org.apache.doris.thrift.TFileType;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class LocationPath2 {

    /** URI schema, e.g., "s3", "hdfs", "file" */
    private final String schema;

    /** Normalized and validated location URI */
    private final String normalizedLocation;

    /** Unique filesystem identifier, typically "scheme://authority" */
    private final String fsIdentifier;

    /** Storage properties associated with this schema */
    private final StorageProperties storageProperties;

    /**
     * Private constructor to enforce creation through the factory method.
     */
    private LocationPath2(String schema,
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
     * @param location the input URI location string
     * @param storagePropertiesMap map of schema type to corresponding storage properties
     * @return a new LocationPath2 instance
     * @throws UserException if validation fails or required data is missing
     */
    public static LocationPath2 of(String location,
                                   Map<StorageProperties.Type, StorageProperties> storagePropertiesMap)
            throws UserException {
        String schema = extractScheme(location);

        StorageProperties storageProperties = storagePropertiesMap.get(SchemaTypeMapper.fromSchema(schema));
        if (storageProperties == null) {
            throw new UserException("No StorageProperties found for schema: " + schema);
        }

        String normalizedLocation = storageProperties.validateAndNormalizeUri(location);
        if (StringUtils.isBlank(normalizedLocation)) {
            throw new UserException("Invalid location: " + location + ", normalized location is null");
        }

        URI uri = URI.create(normalizedLocation);
        String fsIdentifier = Strings.nullToEmpty(uri.getScheme()) + "://" + Strings.nullToEmpty(uri.getAuthority());

        return new LocationPath2(schema, normalizedLocation, fsIdentifier, storageProperties);
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
        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new IllegalArgumentException("Invalid location: " + location + ", extract scheme is null");
            }
            return scheme;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid location: " + location, e);
        }
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
}
