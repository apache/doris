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
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

public class S3PropertyUtils {
    private static final Logger LOG = LogManager.getLogger(S3PropertyUtils.class);

    private static final String URI_KEY = "uri";

    /**
     * Constructs the S3 endpoint from a given URI in the props map.
     *
     * @param props                           the map containing the S3 URI, keyed by URI_KEY
     * @param stringUsePathStyle              whether to use path-style access ("true"/"false")
     * @param stringForceParsingByStandardUri whether to force parsing using the standard URI format ("true"/"false")
     * @return the extracted S3 endpoint or null if URI is invalid or parsing fails
     * <p>
     * Example:
     * Input URI: "https://s3.us-west-1.amazonaws.com/my-bucket/my-key"
     * Output: "s3.us-west-1.amazonaws.com"
     */
    public static String constructEndpointFromUrl(Map<String, String> props,
                                                  String stringUsePathStyle,
                                                  String stringForceParsingByStandardUri) {
        Optional<String> uriOptional = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .findFirst();

        if (!uriOptional.isPresent()) {
            return null;
        }
        String uri = uriOptional.get();
        if (StringUtils.isBlank(uri)) {
            return null;
        }
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        S3URI s3uri;
        try {
            s3uri = S3URI.create(uri, usePathStyle, forceParsingByStandardUri);
        } catch (UserException e) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri + ",usePathStyle: " + usePathStyle
                    + " forceParsingByStandardUri: " + forceParsingByStandardUri, e);
        }
        return s3uri.getEndpoint().orElse(null);
    }

    /**
     * Extracts the S3 region from a URI in the given props map.
     *
     * @param props                           the map containing the S3 URI, keyed by URI_KEY
     * @param stringUsePathStyle              whether to use path-style access ("true"/"false")
     * @param stringForceParsingByStandardUri whether to force parsing using the standard URI format ("true"/"false")
     * @return the extracted S3 region or null if URI is invalid or parsing fails
     * <p>
     * Example:
     * Input URI: "https://s3.us-west-1.amazonaws.com/my-bucket/my-key"
     * Output: "us-west-1"
     */
    public static String constructRegionFromUrl(Map<String, String> props,
                                                String stringUsePathStyle,
                                                String stringForceParsingByStandardUri) {
        Optional<String> uriOptional = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .findFirst();

        if (!uriOptional.isPresent()) {
            return null;
        }
        String uri = uriOptional.get();
        if (StringUtils.isBlank(uri)) {
            return null;
        }
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        S3URI s3uri = null;
        try {
            s3uri = S3URI.create(uri, usePathStyle, forceParsingByStandardUri);
        } catch (UserException e) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri + ",usePathStyle: " + usePathStyle
                    + " forceParsingByStandardUri: " + forceParsingByStandardUri, e);
        }
        return s3uri.getRegion().orElse(null);
    }

    /**
     * Validates and normalizes the given path into a standard S3 URI.
     * If the input already starts with "s3://", it is returned as-is.
     * Otherwise, it is parsed and converted into an S3-compatible URI format.
     *
     * @param path                            the raw S3-style path or full URI
     * @param stringUsePathStyle              whether to use path-style access ("true"/"false")
     * @param stringForceParsingByStandardUri whether to force parsing using the standard URI format ("true"/"false")
     * @return normalized S3 URI string like "s3://bucket/key"
     * @throws UserException if the input path is blank or invalid
     *                       <p>
     *                       Example:
     *                       Input: "https://s3.us-west-1.amazonaws.com/my-bucket/my-key"
     *                       Output: "s3://my-bucket/my-key"
     */
    public static String validateAndNormalizeUri(String path,
                                                 String stringUsePathStyle,
                                                 String stringForceParsingByStandardUri) throws UserException {
        if (StringUtils.isBlank(path)) {
            throw new StoragePropertiesException("path is null");
        }
        if (path.startsWith("s3://")) {
            return path;
        }

        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        S3URI s3uri = S3URI.create(path, usePathStyle, forceParsingByStandardUri);
        return "s3" + S3URI.SCHEME_DELIM + s3uri.getBucket() + S3URI.PATH_DELIM + s3uri.getKey();
    }

    /**
     * Extracts and returns the raw URI string from the given props map.
     *
     * @param props the map expected to contain a 'uri' entry
     * @return the URI string from props
     * @throws UserException if the map is empty or does not contain 'uri'
     *                       <p>
     *                       Example:
     *                       Input: {"uri": "s3://my-bucket/my-key"}
     *                       Output: "s3://my-bucket/my-key"
     */
    public static String validateAndGetUri(Map<String, String> props) {
        if (props.isEmpty()) {
            throw new StoragePropertiesException("props is empty");
        }
        Optional<String> uriOptional = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .findFirst();

        if (!uriOptional.isPresent()) {
            throw new StoragePropertiesException("props must contain uri");
        }
        return uriOptional.get();
    }
}
