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
import org.apache.doris.common.util.URI;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

public class HdfsPropertiesUtils {
    private static final String URI_KEY = "uri";

    // Supported URI schemes
    private static final Set<String> SUPPORTED_SCHEMES = ImmutableSet.of("hdfs", "viewfs");

    /**
     * Validates that the 'uri' property exists in the provided map, and normalizes it.
     *
     * @param props the map of properties that must include a 'uri' entry
     * @return a normalized URI string such as 'hdfs://host/path'
     * @throws UserException if the map is empty or missing the required 'uri' key
     */
    public static String validateAndGetUri(Map<String, String> props) throws UserException {
        String uriStr = extractUriIgnoreCase(props);
        if (StringUtils.isBlank(uriStr)) {
            throw new UserException("props must contain uri");
        }
        return validateAndNormalizeUri(uriStr);
    }

    /**
     * Extracts the default filesystem URI (scheme + authority) from a URI in props.
     *
     * @param props map containing a valid 'uri' entry
     * @return default fs string like 'hdfs://host:port', or null if invalid
     */
    public static String extractDefaultFsFromUri(Map<String, String> props) {
        String uriStr = extractUriIgnoreCase(props);
        if (!isSupportedScheme(uriStr)) {
            return null;
        }
        return getSchemeAndAuthority(uriStr);
    }

    /**
     * Extracts default fs (scheme + authority) from a full URI string.
     *
     * @param path full URI string
     * @return 'scheme://authority' or throws if invalid
     */
    public static String extractDefaultFsFromPath(String path) {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path is empty");
        }
        return getSchemeAndAuthority(path);
    }

    /**
     * Validates if the URI in props is of a supported HDFS scheme.
     *
     * @param props map containing 'uri'
     * @return true if valid and supported, false otherwise
     */
    public static boolean validateUriIsHdfsUri(Map<String, String> props) {
        String uriStr = extractUriIgnoreCase(props);
        if (StringUtils.isBlank(uriStr)) {
            return false;
        }
        return isSupportedScheme(uriStr);
    }

    /**
     * Converts a URI string to a normalized HDFS-style path.
     *
     * @param uriStr raw URI string
     * @return normalized path like 'hdfs://host/path'
     * @throws UserException if invalid or unsupported
     */
    public static String convertUrlToFilePath(String uriStr) throws UserException {
        return validateAndNormalizeUri(uriStr);
    }

    /**
     * Constructs default fs URI (scheme + authority) from 'uri' in props.
     *
     * @param props map containing 'uri'
     * @return default fs like 'hdfs://host:port', or null if invalid
     */
    public static String constructDefaultFsFromUri(Map<String, String> props) {
        String uriStr = extractUriIgnoreCase(props);
        if (!isSupportedScheme(uriStr)) {
            return null;
        }
        return getSchemeAndAuthority(uriStr);
    }

    // ----------------------- Private Helper Methods -----------------------

    /**
     * Extracts the 'uri' value from props map ignoring case sensitivity.
     */
    private static String extractUriIgnoreCase(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            return null;
        }
        return props.entrySet().stream()
                .filter(e -> URI_KEY.equalsIgnoreCase(e.getKey()))
                .map(Map.Entry::getValue)
                .filter(StringUtils::isNotBlank)
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns true if the URI string has a supported scheme (hdfs or viewfs).
     */
    private static boolean isSupportedScheme(String uriStr) {
        try {
            URI uri = URI.create(uriStr);
            String scheme = uri.getScheme();
            return StringUtils.isNotBlank(scheme) && SUPPORTED_SCHEMES.contains(scheme.toLowerCase());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Extracts 'scheme://authority' from the given URI string.
     */
    private static String getSchemeAndAuthority(String uriStr) {
        try {
            URI uri = URI.create(uriStr);
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();
            if (StringUtils.isBlank(scheme)) {
                throw new IllegalArgumentException("Invalid uri: " + uriStr + ", extract schema is null");
            }
            return scheme + "://" + authority;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid uri: " + uriStr, e);
        }
    }

    /**
     * Validates and normalizes the full URI string.
     */
    private static String validateAndNormalizeUri(String uriStr) throws UserException {
        if (StringUtils.isBlank(uriStr)) {
            throw new UserException("Property 'uri' is required");
        }
        try {
            URI uri = URI.create(uriStr);
            String scheme = uri.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new IllegalArgumentException("Invalid uri: " + uriStr + ", extract schema is null");
            }
            if (!SUPPORTED_SCHEMES.contains(scheme.toLowerCase())) {
                throw new IllegalArgumentException("Invalid export path: " + scheme
                        + ", please use valid 'hdfs://' or 'viewfs://' path.");
            }
            return scheme + "://" + uri.getAuthority() + uri.getPath();
        } catch (Exception e) {
            throw new UserException("Invalid uri: " + uriStr, e);
        }
    }
}
