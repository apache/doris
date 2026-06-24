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

package org.apache.doris.filesystem.properties;

/**
 * Shared typed accessors for S3-compatible object storage properties.
 *
 * <p>Provider implementations may live in different plugin modules, but callers
 * that only need common S3-compatible settings can depend on this API-level
 * contract. The interface intentionally contains only JDK types.</p>
 */
public interface S3CompatibleFileSystemProperties extends FileSystemProperties {

    String SKIP_LIST_FOR_DETERMINISTIC_PATH = "s3_skip_list_for_deterministic_path";
    String HEAD_REQUEST_MAX_PATHS = "s3_head_request_max_paths";
    int DEFAULT_HEAD_REQUEST_MAX_PATHS = 100;

    /** Returns the service endpoint. */
    String getEndpoint();

    /** Returns the signing region. */
    String getRegion();

    /** Returns the static access key, or an empty value when static credentials are not used. */
    String getAccessKey();

    /** Returns the static secret key, or an empty value when static credentials are not used. */
    String getSecretKey();

    /** Returns the session token used with temporary static credentials. */
    String getSessionToken();

    /** Returns the IAM role ARN used for AssumeRole access. */
    String getRoleArn();

    /** Returns the external ID used for AssumeRole trust policy validation. */
    String getExternalId();

    /** Returns the default bucket configured for object-storage helper operations. */
    String getBucket();

    /** Returns the root path prefix inside the bucket. */
    String getRootPath();

    /** Returns the maximum connection count as a provider property value. */
    String getMaxConnections();

    /** Returns the request timeout in milliseconds as a provider property value. */
    String getRequestTimeoutMs();

    /** Returns the connection timeout in milliseconds as a provider property value. */
    String getConnectionTimeoutMs();

    /** Returns whether path-style bucket addressing is enabled, as a raw provider property value. */
    String getUsePathStyle();

    /** Returns whether deterministic path patterns should use HEAD requests instead of ListObjects. */
    String getSkipListForDeterministicPath();

    /** Returns the maximum deterministic object key count to resolve through HEAD requests. */
    int getHeadRequestMaxPaths();

    /**
     * Returns path-style bucket addressing as a parsed boolean (single conversion point).
     *
     * <p>Blank or absent means {@code false}. Any other value than {@code true}/{@code false}
     * (case-insensitive) is rejected instead of being silently coerced to {@code false}, so a
     * typo cannot accidentally disable path-style addressing. Providers call
     * {@link #hasInvalidUsePathStyle()} from {@code validate()}, so an invalid value fails
     * fast at property-binding time rather than on first use.
     */
    default boolean isUsePathStyle() {
        String value = getUsePathStyle();
        if (value == null || value.isBlank()) {
            return false;
        }
        String normalized = value.trim();
        if ("true".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized)) {
            return false;
        }
        throw new IllegalArgumentException(
                "Invalid use_path_style value: '" + value + "' (expected true or false)");
    }

    /** Returns true when the raw use_path_style value cannot be parsed by {@link #isUsePathStyle()}. */
    default boolean hasInvalidUsePathStyle() {
        try {
            isUsePathStyle();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    default boolean isSkipListForDeterministicPath() {
        String value = getSkipListForDeterministicPath();
        if (value == null || value.isBlank()) {
            return true;
        }
        String normalized = value.trim();
        if ("true".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized)) {
            return false;
        }
        throw new IllegalArgumentException(
                "Invalid " + SKIP_LIST_FOR_DETERMINISTIC_PATH + " value: '" + value
                        + "' (expected true or false)");
    }

    default boolean hasInvalidSkipListForDeterministicPath() {
        try {
            isSkipListForDeterministicPath();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    /** Returns true when a static AK/SK credential pair is present. */
    default boolean hasStaticCredentials() {
        String ak = getAccessKey();
        String sk = getSecretKey();
        return ak != null && !ak.isBlank() && sk != null && !sk.isBlank();
    }

    /** Returns true when AssumeRole (IAM role ARN) access is configured. */
    default boolean hasAssumeRole() {
        String arn = getRoleArn();
        return arn != null && !arn.isBlank();
    }
}
