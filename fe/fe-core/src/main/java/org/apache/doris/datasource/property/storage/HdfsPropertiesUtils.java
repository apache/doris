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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class HdfsPropertiesUtils {
    private static final String URI_KEY = "uri";
    private static final String STANDARD_HDFS_PREFIX = "hdfs://";
    private static final String EMPTY_HDFS_PREFIX = "hdfs:///";
    private static final String BROKEN_HDFS_PREFIX = "hdfs:/";
    private static final String SCHEME_DELIM = "://";
    private static final String NONSTANDARD_SCHEME_DELIM = ":/";

    public static String validateAndGetUri(Map<String, String> props, String host, String defaultFs,
                                           Set<String> supportSchemas) throws UserException {
        if (props.isEmpty()) {
            throw new UserException("props is empty");
        }
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            throw new StoragePropertiesException("props must contain uri");
        }
        return validateAndNormalizeUri(uriStr, host, defaultFs, supportSchemas);
    }

    public static boolean validateUriIsHdfsUri(Map<String, String> props,
                                               Set<String> supportSchemas) {
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            return false;
        }
        URI uri = URI.create(uriStr);
        String schema = uri.getScheme();
        if (StringUtils.isBlank(schema)) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr + ", extract schema is null");
        }
        return isSupportedSchema(schema, supportSchemas);
    }

    public static String extractDefaultFsFromPath(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return null;
        }
        URI uri = URI.create(filePath);
        return uri.getScheme() + "://" + uri.getAuthority();
    }

    public static String extractDefaultFsFromUri(Map<String, String> props, Set<String> supportSchemas) {
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            return null;
        }
        URI uri = URI.create(uriStr);
        if (!isSupportedSchema(uri.getScheme(), supportSchemas)) {
            return null;
        }
        return uri.getScheme() + "://" + uri.getAuthority();
    }

    public static String convertUrlToFilePath(String uriStr, String host,
                                              String defaultFs, Set<String> supportSchemas) {
        return validateAndNormalizeUri(uriStr, host, defaultFs, supportSchemas);
    }

    public static String convertUrlToFilePath(String uriStr, String host, Set<String> supportSchemas) {
        return validateAndNormalizeUri(uriStr, host, null, supportSchemas);
    }

    /*
     * Extracts the URI value from the given properties.
     * If multiple URIs are specified (separated by commas), this method returns null.
     * Note: Some storage systems may support multiple URIs (e.g., for load balancing or multi-host),
     * but in the HDFS scenario, fs.defaultFS only supports a single URI.
     * Therefore, such a format is considered invalid for HDFS. so, just return null.
     */
    private static String getUri(Map<String, String> props) {
        String uriValue = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
        if (uriValue == null) {
            return null;
        }
        String[] uris = uriValue.split(",");
        if (uris.length > 1) {
            return null;
        }
        return uriValue;
    }

    private static boolean isSupportedSchema(String schema, Set<String> supportSchema) {
        return schema != null && supportSchema.contains(schema.toLowerCase());
    }

    public static String validateAndNormalizeUri(String location, Set<String> supportedSchemas) {
        return validateAndNormalizeUri(location, null, null, supportedSchemas);
    }

    public static String validateAndNormalizeUri(String location, String host, String defaultFs,
                                                  Set<String> supportedSchemas) {
        if (StringUtils.isBlank(location)) {
            throw new IllegalArgumentException("Property 'uri' is required.");
        }
        if (!(location.contains(SCHEME_DELIM) || location.contains(NONSTANDARD_SCHEME_DELIM))
                && StringUtils.isNotBlank(defaultFs)) {
            location = defaultFs + location;
        }
        try {
            // Encode the location string, but keep '/' and ':' unescaped to preserve URI structure
            String newLocation = URLEncoder.encode(location, StandardCharsets.UTF_8.name())
                    .replace("%2F", "/")
                    .replace("%3A", ":");

            URI uri = new URI(newLocation).normalize();

            boolean isSupportedSchema = isSupportedSchema(uri.getScheme(), supportedSchemas);
            if (!isSupportedSchema) {
                throw new IllegalArgumentException("Unsupported schema: " + uri.getScheme());
            }
            // compatible with 'hdfs:///' or 'hdfs:/'
            if (StringUtils.isEmpty(uri.getHost())) {
                newLocation = URLDecoder.decode(newLocation, StandardCharsets.UTF_8.name());
                if (newLocation.startsWith(BROKEN_HDFS_PREFIX) && !newLocation.startsWith(STANDARD_HDFS_PREFIX)) {
                    newLocation = newLocation.replace(BROKEN_HDFS_PREFIX, STANDARD_HDFS_PREFIX);
                }
                if (StringUtils.isNotEmpty(host)) {
                    // Replace 'hdfs://key/' to 'hdfs://name_service/key/'
                    // Or hdfs:///abc to hdfs://name_service/abc
                    if (newLocation.startsWith(EMPTY_HDFS_PREFIX)) {
                        return newLocation.replace(STANDARD_HDFS_PREFIX, STANDARD_HDFS_PREFIX + host);
                    } else {
                        return newLocation.replace(STANDARD_HDFS_PREFIX, STANDARD_HDFS_PREFIX + host + "/");
                    }
                } else {
                    // 'hdfs://null/' equals the 'hdfs:///'
                    if (newLocation.startsWith(EMPTY_HDFS_PREFIX)) {
                        // Do not support hdfs:///location
                        throw new RuntimeException("Invalid location with empty host: " + newLocation);
                    } else {
                        // Replace 'hdfs://key/' to '/key/', try access local NameNode on BE.
                        return newLocation.replace(STANDARD_HDFS_PREFIX, "/");
                    }
                }
            }
            // Normal case: decode and return the fully-qualified URI
            return URLDecoder.decode(newLocation, StandardCharsets.UTF_8.name());

        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new StoragePropertiesException("Failed to parse URI: " + location, e);
        }
    }
}
