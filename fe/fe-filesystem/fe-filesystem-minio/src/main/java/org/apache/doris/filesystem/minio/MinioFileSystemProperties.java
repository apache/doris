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

package org.apache.doris.filesystem.minio;

import org.apache.doris.filesystem.s3.AbstractDelegatingS3Properties;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;
import java.util.Set;

/**
 * Provider-owned MinIO properties (S3-compatible, static HMAC credentials only).
 *
 * <p>The public aliases, ordering, defaults, and endpoint requirement follow fe-core
 * MinioProperties. AWS-only credential mechanisms (role ARN, instance profile) are rejected.
 */
public final class MinioFileSystemProperties extends AbstractDelegatingS3Properties {

    public static final String ENDPOINT = "minio.endpoint";
    public static final String REGION = "minio.region";
    public static final String ACCESS_KEY = "minio.access_key";
    public static final String SECRET_KEY = "minio.secret_key";
    public static final String SESSION_TOKEN = "minio.session_token";
    public static final String MAX_CONNECTIONS = "minio.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "minio.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "minio.connection.timeout";
    public static final String USE_PATH_STYLE = "minio.use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI =
            "minio.force_parsing_by_standard_uri";

    public static final String DEFAULT_REGION = "us-east-1";

    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of MinIO.")
    private String endpoint = "";

    @ConnectorProperty(names = {REGION, "s3.region", "AWS_REGION", "region", "REGION"},
            required = false,
            isRegionField = true,
            description = "The region of MinIO.")
    private String region = DEFAULT_REGION;

    @ConnectorProperty(names = {ACCESS_KEY, "s3.access-key-id", "AWS_ACCESS_KEY", "ACCESS_KEY",
            "access_key", "s3.access_key"},
            required = false,
            description = "The access key of MinIO.")
    private String accessKey = "";

    @ConnectorProperty(names = {SECRET_KEY, "s3.secret-access-key", "s3.secret_key",
            "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The secret key of MinIO.")
    private String secretKey = "";

    @ConnectorProperty(names = {SESSION_TOKEN, "s3.session-token", "s3.session_token",
            "session_token", "AWS_TOKEN"},
            required = false,
            sensitive = true,
            description = "The session token of MinIO.")
    private String sessionToken = "";

    @ConnectorProperty(names = {MAX_CONNECTIONS, "s3.connection.maximum", "AWS_MAX_CONNECTIONS"},
            required = false,
            description = "Maximum number of connections.")
    private String maxConnections = "100";

    @ConnectorProperty(names = {REQUEST_TIMEOUT_MS, "s3.connection.request.timeout",
            "AWS_REQUEST_TIMEOUT_MS"},
            required = false,
            description = "Request timeout in milliseconds.")
    private String requestTimeoutMs = "10000";

    @ConnectorProperty(names = {CONNECTION_TIMEOUT_MS, "s3.connection.timeout",
            "AWS_CONNECTION_TIMEOUT_MS"},
            required = false,
            description = "Connection timeout in milliseconds.")
    private String connectionTimeoutMs = "10000";

    @ConnectorProperty(names = {USE_PATH_STYLE, "use_path_style", "s3.path-style-access"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    private String usePathStyle = "false";

    @ConnectorProperty(names = {FORCE_PARSING_BY_STANDARD_URI, "force_parsing_by_standard_uri"},
            required = false,
            description = "Whether to force standard URI parsing.")
    private String forceParsingByStandardUrl = "false";

    private MinioFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties);
    }

    public static MinioFileSystemProperties of(Map<String, String> properties) {
        MinioFileSystemProperties props = new MinioFileSystemProperties(properties);
        props.bindAndCollect();
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        validateHmacDialect("minio", "MinIO");
    }

    @Override
    public String providerName() {
        return "MINIO";
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public String getSessionToken() {
        return sessionToken;
    }

    @Override
    public String getMaxConnections() {
        return maxConnections;
    }

    @Override
    public String getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    @Override
    public String getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    @Override
    public String getUsePathStyle() {
        return usePathStyle;
    }

    @Override
    public Set<String> getSupportedSchemes() {
        return Set.of("s3", "s3a");
    }

    public String getForceParsingByStandardUrl() {
        return forceParsingByStandardUrl;
    }
}
