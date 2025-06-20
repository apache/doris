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

    public static String validateAndGetUri(Map<String, String> props, String host, Set<String> supportSchemas)
            throws UserException {
        if (props.isEmpty()) {
            throw new UserException("props is empty");
        }
        String uriStr = getUri(props);
        if (StringUtils.isBlank(uriStr)) {
            throw new StoragePropertiesException("props must contain uri");
        }
        return validateAndNormalizeUri(uriStr, host, supportSchemas);
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

    public static String convertUrlToFilePath(String uriStr, String host, Set<String> supportSchemas) {
        return validateAndNormalizeUri(uriStr, host, supportSchemas);
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
        return validateAndNormalizeUri(location, null, supportedSchemas);
    }

    public static String validateAndNormalizeUri(String location, String host, Set<String> supportedSchemas) {
        if (StringUtils.isBlank(location)) {
            throw new IllegalArgumentException("Property 'uri' is required.");
        }

        try {
            // Encode the location string, but keep '/' and ':' unescaped to preserve URI structure
            String encodedLocation = URLEncoder.encode(location, StandardCharsets.UTF_8.name())
                    .replace("%2F", "/")
                    .replace("%3A", ":");

            URI uri = new URI(encodedLocation).normalize();
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();

            if (StringUtils.isBlank(scheme)) {
                throw new IllegalArgumentException("Invalid URI: no schema found in " + location);
            }

            if (!isSupportedSchema(scheme, supportedSchemas)) {
                throw new IllegalArgumentException("Unsupported schema: " + scheme
                        + ". Supported schemas are: " + String.join(", ", supportedSchemas));
            }

            // If no authority/host is present, handle cases like 'hdfs:///' or 'hdfs:/'
            if (StringUtils.isEmpty(authority)) {
                String decoded = URLDecoder.decode(encodedLocation, StandardCharsets.UTF_8.name());

                // Fix broken scheme like 'hdfs:/...' into 'hdfs://...'
                if (decoded.startsWith("hdfs:/") && !decoded.startsWith("hdfs://")) {
                    decoded = decoded.replaceFirst("hdfs:/", "hdfs://");
                }

                if (StringUtils.isNotBlank(host)) {
                    // Host is provided, inject it into the URI
                    if (decoded.startsWith("hdfs:///")) {
                        return "hdfs://" + host + decoded.substring("hdfs:///".length() - 1);
                    } else {
                        return "hdfs://" + host + "/" + decoded.substring("hdfs://".length());
                    }
                } else {
                    // If not enabled, convert to local path style for backend access
                    if (decoded.startsWith("hdfs:///")) {
                        throw new IllegalArgumentException("Invalid location with empty host: " + decoded);
                    }
                    return decoded.replaceFirst("hdfs://", "/");

                }
            }

            // Normal case: decode and return the fully-qualified URI
            return URLDecoder.decode(encodedLocation, StandardCharsets.UTF_8.name());

        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new StoragePropertiesException("Failed to parse URI: " + location, e);
        }
    }
}
