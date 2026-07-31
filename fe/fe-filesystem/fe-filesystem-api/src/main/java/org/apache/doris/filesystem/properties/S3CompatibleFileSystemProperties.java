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

import java.util.Set;

/**
 * Shared typed accessors for S3-compatible object storage properties.
 *
 * <p>Provider implementations may live in different plugin modules, but callers
 * that only need common S3-compatible settings can depend on this API-level
 * contract. The interface intentionally contains only JDK types.</p>
 */
public interface S3CompatibleFileSystemProperties extends FileSystemProperties {

    // ------------------------------------------------------------------
    // User-namespace property key contract (s3.* spelling). These literals are wire/DDL/image
    // contracts shared by every S3-compatible dialect and by fe-core's persisted-map glue
    // (thrift/PB adapters, frozen Resource entities). Single source of the spellings — do not
    // redefine them elsewhere and never change the values.
    // ------------------------------------------------------------------
    String PROP_ENDPOINT = "s3.endpoint";
    String PROP_REGION = "s3.region";
    String PROP_ACCESS_KEY = "s3.access_key";
    String PROP_SECRET_KEY = "s3.secret_key";
    String PROP_SESSION_TOKEN = "s3.session_token";
    String PROP_ROOT_PATH = "s3.root.path";
    String PROP_BUCKET = "s3.bucket";
    String PROP_EXTERNAL_ENDPOINT = "s3.external_endpoint";
    String PROP_ROLE_ARN = "s3.role_arn";
    String PROP_EXTERNAL_ID = "s3.external_id";
    String PROP_CREDENTIALS_PROVIDER_TYPE = "s3.credentials_provider_type";
    String PROP_MAX_CONNECTIONS = "s3.connection.maximum";
    String PROP_REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    String PROP_CONNECTION_TIMEOUT_MS = "s3.connection.timeout";
    String PROP_USE_PATH_STYLE = "use_path_style";
    String PROP_PROVIDER = "provider";
    String DEFAULT_MAX_CONNECTIONS_VALUE = "50";
    String DEFAULT_REQUEST_TIMEOUT_MS_VALUE = "3000";
    String DEFAULT_CONNECTION_TIMEOUT_MS_VALUE = "1000";


    /**
     * Ledger 2.4-1: every S3-compatible dialect reports the "S3" storage family to fe-core
     * consumers (legacy getStorageName() collapsed OSS/COS/OBS/GCS/MinIO/Ozone to "S3").
     */
    @Override
    default String storageFamilyName() {
        return "S3";
    }


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

    /**
     * Returns the URI schemes this provider accepts, lower-cased (e.g. {@code {s3, s3a, oss}}).
     *
     * <p>This is the single source of truth for the provider's scheme identity: URI parsing
     * rejects any scheme not in this set (so a COS provider refuses {@code oss://}), and
     * scheme-to-storage routing can read the same set. Every S3-compatible provider accepts the
     * historically normalized {@code s3}/{@code s3a} form in addition to its native scheme(s).
     */
    Set<String> getSupportedSchemes();

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
