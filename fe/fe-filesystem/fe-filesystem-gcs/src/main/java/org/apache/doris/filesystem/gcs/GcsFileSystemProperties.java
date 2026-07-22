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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.s3.AbstractDelegatingS3Properties;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Provider-owned GCS properties (S3 interoperability / XML API, HMAC credentials only).
 *
 * <p>The public aliases, ordering, defaults, and static-credential validation follow fe-core
 * GCSProperties. The Google native SDK (JSON API) is intentionally NOT used: it only accepts
 * OAuth2 credentials, while Doris GCS configurations carry HMAC keys. AWS-only credential
 * mechanisms (role ARN, instance profile) are rejected at binding time.
 */
public final class GcsFileSystemProperties extends AbstractDelegatingS3Properties {

    public static final String ENDPOINT = "gs.endpoint";
    public static final String ACCESS_KEY = "gs.access_key";
    public static final String SECRET_KEY = "gs.secret_key";
    public static final String SESSION_TOKEN = "gs.session_token";
    public static final String MAX_CONNECTIONS = "gs.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "gs.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "gs.connection.timeout";
    public static final String USE_PATH_STYLE = "gs.use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI =
            "gs.force_parsing_by_standard_uri";

    public static final String DEFAULT_ENDPOINT = "https://storage.googleapis.com";
    public static final String DEFAULT_REGION = "us-east1";

    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            required = false,
            description = "The endpoint of GCS.")
    private String endpoint = DEFAULT_ENDPOINT;

    // Not a user-facing property: GCS maps the region internally by bucket, so legacy
    // GCSProperties keeps it as a plain constant with no @ConnectorProperty binding. The value
    // still has to exist because the S3-compatible API needs a region for request signing.
    private final String region = DEFAULT_REGION;

    @ConnectorProperty(names = {ACCESS_KEY, "s3.access_key", "AWS_ACCESS_KEY", "access_key",
            "ACCESS_KEY"},
            required = false,
            description = "The HMAC access key of GCS.")
    private String accessKey = "";

    @ConnectorProperty(names = {SECRET_KEY, "s3.secret_key", "AWS_SECRET_KEY", "secret_key",
            "SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The HMAC secret key of GCS.")
    private String secretKey = "";

    @ConnectorProperty(names = {SESSION_TOKEN, "s3.session_token", "session_token", "AWS_TOKEN"},
            required = false,
            sensitive = true,
            description = "The session token of GCS.")
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

    private GcsFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties);
    }

    public static GcsFileSystemProperties of(Map<String, String> properties) {
        GcsFileSystemProperties props = new GcsFileSystemProperties(properties);
        props.bindAndCollect();
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        validateHmacDialect("gs", "GCS");
    }

    @Override
    public String providerName() {
        return "GCS";
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> kv = new HashMap<>(toS3CompatibleKv());
        // Legacy GCSProperties.getBackendConfigProperties() tags GCS for the BE S3 client.
        kv.put("provider", "GCP");
        return Collections.unmodifiableMap(kv);
    }

    @Override
    protected void customizeHadoopConfiguration(Map<String, String> cfg) {
        cfg.put("fs.gs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        if (!hasStaticCredentials()) {
            cfg.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
        }
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
        return Set.of("gs", "s3", "s3a");
    }

    public String getForceParsingByStandardUrl() {
        return forceParsingByStandardUrl;
    }

    @Override
    public Set<String> legacyCacheSchemes() {
        return Set.of("gs");
    }

}
