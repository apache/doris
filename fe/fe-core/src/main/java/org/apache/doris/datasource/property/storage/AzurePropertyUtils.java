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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.regex.Pattern;

public class AzurePropertyUtils {

    /**
     * Validates and normalizes an Azure Blob Storage URI into a unified {@code s3://}-style format.
     * <p>
     * This method supports the following URI formats:
     * <ul>
     *   <li>HDFS-style Azure URIs: {@code wasb://}, {@code wasbs://}, {@code abfs://}, {@code abfss://}</li>
     *   <li>HTTPS-style Azure Blob URLs: {@code https://<account>.blob.core.windows.net/<container>/<path>}</li>
     * </ul>
     * <p>
     * The normalized output will always be in the form of:
     * <pre>{@code
     * s3://<container>/<path>
     * }</pre>
     * <p>
     * Examples:
     * <ul>
     *   <li>{@code wasbs://container@account.blob.core.windows.net/data/file.txt}
     *       → {@code s3://container/data/file.txt}</li>
     *   <li>{@code https://account.blob.core.windows.net/container/file.csv}
     *       → {@code s3://container/file.csv}</li>
     * </ul>
     *
     * @param path the input Azure URI string to be validated and normalized
     * @return a normalized {@code s3://}-style URI
     * @throws StoragePropertiesException if the URI is blank, invalid, or unsupported
     */
    public static String validateAndNormalizeUri(String path) throws UserException {

        if (StringUtils.isBlank(path)) {
            throw new StoragePropertiesException("Path cannot be null or empty");
        }
        // Only accept Azure Blob Storage-related URI schemes
        if (!(path.startsWith("wasb://") || path.startsWith("wasbs://")
                || path.startsWith("abfs://") || path.startsWith("abfss://")
                || path.startsWith("https://") || path.startsWith("http://")
                || path.startsWith("s3://"))) {
            throw new StoragePropertiesException("Unsupported Azure URI scheme: " + path);
        }
        if (isOneLakeLocation(path)) {
            return path;
        }
        return convertToS3Style(path);
    }

    private static final Pattern ONELAKE_PATTERN = Pattern.compile(
            "abfs[s]?://([^@]+)@([^/]+)\\.dfs\\.fabric\\.microsoft\\.com(/.*)?", Pattern.CASE_INSENSITIVE);


    /**
     * Converts an Azure Blob Storage URI into a unified {@code s3://<container>/<path>} format.
     * <p>
     * This method recognizes both:
     * <ul>
     *   <li>HDFS-style Azure URIs ({@code wasb://}, {@code wasbs://}, {@code abfs://}, {@code abfss://})</li>
     *   <li>HTTPS-style Azure Blob URLs ({@code https://<account>.blob.core.windows.net/...})</li>
     * </ul>
     * <p>
     * It throws an exception if the URI is invalid or does not match Azure Blob Storage patterns.
     *
     * @param uri the original Azure URI string
     * @return the normalized {@code s3://<container>/<path>} string
     * @throws StoragePropertiesException if the URI is invalid or unsupported
     */
    private static String convertToS3Style(String uri) {
        if (StringUtils.isBlank(uri)) {
            throw new StoragePropertiesException("URI is blank");
        }
        if (uri.startsWith("s3://")) {
            return uri;
        }
        // Handle Azure HDFS-style URIs (wasb://, wasbs://, abfs://, abfss://)
        if (uri.startsWith("wasb://") || uri.startsWith("wasbs://")
                || uri.startsWith("abfs://") || uri.startsWith("abfss://")) {

            // Example: wasbs://container@account.blob.core.windows.net/path/file.txt
            String schemeRemoved = uri.replaceFirst("^[a-z]+s?://", "");
            int atIndex = schemeRemoved.indexOf('@');
            if (atIndex < 0) {
                throw new StoragePropertiesException("Invalid Azure URI, missing '@': " + uri);
            }

            // Extract container name (before '@')
            String container = schemeRemoved.substring(0, atIndex);

            // Extract remaining part after '@'
            String remainder = schemeRemoved.substring(atIndex + 1);
            int slashIndex = remainder.indexOf('/');

            // Extract the path part if it exists
            String path = (slashIndex != -1) ? remainder.substring(slashIndex + 1) : "";

            // Normalize to s3-style URI: s3://<container>/<path>
            return StringUtils.isBlank(path)
                    ? String.format("s3://%s", container)
                    : String.format("s3://%s/%s", container, path);
        }

        // ② Handle HTTPS/HTTP Azure Blob Storage URLs
        if (uri.startsWith("https://") || uri.startsWith("http://")) {
            try {
                URI parsed = new URI(uri);
                String host = parsed.getHost();
                String path = parsed.getPath();

                if (StringUtils.isBlank(host)) {
                    throw new StoragePropertiesException("Invalid Azure HTTPS URI, missing host: " + uri);
                }

                // Typical Azure Blob domain: <account>.blob.core.windows.net
                if (!host.contains(".blob.core.windows.net")) {
                    throw new StoragePropertiesException("Not an Azure Blob URL: " + uri);
                }

                // Path usually looks like: /<container>/<path>
                String[] parts = path.split("/", 3);
                if (parts.length < 2) {
                    throw new StoragePropertiesException("Invalid Azure Blob URL, missing container: " + uri);
                }

                String container = parts[1];
                String remainder = (parts.length == 3) ? parts[2] : "";

                // Convert HTTPS URL to s3-style format
                return StringUtils.isBlank(remainder)
                        ? String.format("s3://%s", container)
                        : String.format("s3://%s/%s", container, remainder);

            } catch (URISyntaxException e) {
                throw new StoragePropertiesException("Invalid HTTPS URI: " + uri, e);
            }
        }

        throw new StoragePropertiesException("Unsupported Azure URI scheme: " + uri);
    }

    /**
     * Extracts and validates the "uri" entry from a properties map.
     *
     * <p>Example:
     * <pre>
     * Input : {"uri": "wasb://container@account.blob.core.windows.net/dir/file.txt"}
     * Output: "wasb://container@account.blob.core.windows.net/dir/file.txt"
     * </pre>
     *
     * @param props the configuration map expected to contain a "uri" key
     * @return the URI string from the map
     * @throws StoragePropertiesException if the map is empty or missing the "uri" key
     */
    public static String validateAndGetUri(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            throw new StoragePropertiesException("Properties map cannot be null or empty");
        }

        return props.entrySet().stream()
                .filter(e -> StorageProperties.URI_KEY.equalsIgnoreCase(e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new StoragePropertiesException("Properties must contain 'uri' key"));
    }

    public static boolean isOneLakeLocation(String location) {
        return ONELAKE_PATTERN.matcher(location).matches();
    }
}
