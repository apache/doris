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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider-owned typed properties for AWS S3 and S3-compatible object storage.
 * Binding uses {@link ConnectorProperty} aliases so legacy key names can continue
 * to work while callers migrate to canonical s3.* names.
 */
public final class S3FileSystemProperties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties,
                S3CompatibleFileSystemProperties {

    public static final String ENDPOINT = "s3.endpoint";
    public static final String REGION = "s3.region";
    public static final String ACCESS_KEY = "s3.access_key";
    public static final String SECRET_KEY = "s3.secret_key";
    public static final String SESSION_TOKEN = "s3.session_token";
    public static final String ROLE_ARN = "s3.role_arn";
    public static final String EXTERNAL_ID = "s3.external_id";
    public static final String BUCKET = "s3.bucket";
    public static final String ROOT_PATH = "s3.root.path";
    public static final String MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "s3.connection.timeout";
    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String CREDENTIALS_PROVIDER_TYPE = "s3.credentials_provider_type";

    public static final String DEFAULT_MAX_CONNECTIONS = "50";
    public static final String DEFAULT_REQUEST_TIMEOUT_MS = "3000";
    public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "1000";
    public static final String DEFAULT_CREDENTIALS_PROVIDER_TYPE = "DEFAULT";
    public static final String DEFAULT_REGION = "us-east-1";

    private static final Pattern[] ENDPOINT_PATTERNS = new Pattern[] {
            Pattern.compile(
                    "^(?:https?://)?(?:"
                            + "s3(?:[-.]fips)?(?:[-.]dualstack)?[-.]([a-z0-9-]+)|"
                            + "s3express-control\\.([a-z0-9-]+)|"
                            + "s3express-[a-z0-9-]+\\.([a-z0-9-]+)"
                            + ")\\.amazonaws\\.com(?:/.*)?$",
                    Pattern.CASE_INSENSITIVE),
            Pattern.compile(
                    "^(?:https?://)?glue(?:-fips)?\\.([a-z0-9-]+)\\.(amazonaws\\.com(?:\\.cn)?|api\\.aws)$",
                    Pattern.CASE_INSENSITIVE)
    };

    @Getter
    @ConnectorProperty(names = {ENDPOINT, "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
            "glue.endpoint", "aws.glue.endpoint"},
            required = false,
            description = "The endpoint of S3.")
    private String endpoint = "";

    @Getter
    @ConnectorProperty(names = {REGION, "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
            "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region"},
            required = false,
            isRegionField = true,
            description = "The region of S3.")
    private String region = "";

    @Getter
    @ConnectorProperty(names = {ACCESS_KEY, "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY",
            "glue.access_key", "aws.glue.access-key",
            "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
            "s3.access-key-id"},
            required = false,
            description = "The access key of S3.")
    private String accessKey = "";

    @Getter
    @ConnectorProperty(names = {SECRET_KEY, "AWS_SECRET_KEY", "secret_key", "SECRET_KEY",
            "glue.secret_key", "aws.glue.secret-key",
            "client.credentials-provider.glue.secret_key", "iceberg.rest.secret-access-key",
            "s3.secret-access-key"},
            required = false,
            sensitive = true,
            description = "The secret key of S3.")
    private String secretKey = "";

    @Getter
    @ConnectorProperty(names = {SESSION_TOKEN, "AWS_TOKEN", "session_token",
            "s3.session-token", "iceberg.rest.session-token"},
            required = false,
            sensitive = true,
            description = "The session token of S3.")
    private String sessionToken = "";

    @Getter
    @ConnectorProperty(names = {ROLE_ARN, "AWS_ROLE_ARN", "glue.role_arn"},
            required = false,
            description = "The IAM role ARN for AssumeRole-based access.")
    private String roleArn = "";

    @Getter
    @ConnectorProperty(names = {EXTERNAL_ID, "AWS_EXTERNAL_ID", "glue.external_id"},
            required = false,
            description = "The external ID for AssumeRole trust policy.")
    private String externalId = "";

    @Getter
    @ConnectorProperty(names = {BUCKET, "AWS_BUCKET"},
            required = false,
            description = "The default bucket name.")
    private String bucket = "";

    @Getter
    @ConnectorProperty(names = {ROOT_PATH, "AWS_ROOT_PATH"},
            required = false,
            description = "The root path prefix inside the bucket.")
    private String rootPath = "";

    @Getter
    @ConnectorProperty(names = {MAX_CONNECTIONS, "AWS_MAX_CONNECTIONS"},
            required = false,
            description = "The maximum number of connections to S3.")
    private String maxConnections = DEFAULT_MAX_CONNECTIONS;

    @Getter
    @ConnectorProperty(names = {REQUEST_TIMEOUT_MS, "AWS_REQUEST_TIMEOUT_MS"},
            required = false,
            description = "The request timeout of S3 in milliseconds.")
    private String requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

    @Getter
    @ConnectorProperty(names = {CONNECTION_TIMEOUT_MS, "AWS_CONNECTION_TIMEOUT_MS"},
            required = false,
            description = "The connection timeout of S3 in milliseconds.")
    private String connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;

    @Getter
    @ConnectorProperty(names = {USE_PATH_STYLE, "s3.path-style-access"},
            required = false,
            description = "Whether to use path-style bucket addressing.")
    private String usePathStyle = "false";

    @ConnectorProperty(names = {CREDENTIALS_PROVIDER_TYPE, "AWS_CREDENTIALS_PROVIDER_TYPE",
            "glue.credentials_provider_type", "iceberg.rest.credentials_provider_type"},
            required = false,
            description = "The credentials provider type.")
    private String credentialsProviderType = DEFAULT_CREDENTIALS_PROVIDER_TYPE;

    private final Map<String, String> rawProperties;
    private final Map<String, String> matchedProperties;

    private S3FileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        normalizeForLegacyS3Compatibility();
    }

    /** Binds and validates raw properties. */
    public static S3FileSystemProperties of(Map<String, String> properties) {
        S3FileSystemProperties props = new S3FileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        new ParamRules()
                .requireTogether(new String[] {accessKey, secretKey},
                        "s3.access_key and s3.secret_key must be set together")
                .requireAllIfPresent(sessionToken, new String[] {accessKey, secretKey},
                        "s3.session_token requires s3.access_key and s3.secret_key")
                .requireAllIfPresent(externalId, new String[] {roleArn},
                        "s3.external_id must be used together with s3.role_arn")
                .check(this::hasUnsupportedCredentialsProviderType,
                        "Unsupported s3.credentials_provider_type: " + credentialsProviderType)
                .check(() -> StringUtils.isBlank(endpoint) && StringUtils.isBlank(region),
                        "Either s3.endpoint or s3.region must be set")
                .check(this::hasInvalidUsePathStyle,
                        "use_path_style must be true or false, got: '" + getUsePathStyle() + "'")
                .validate("Invalid S3 filesystem properties");
    }

    @Override
    public String providerName() {
        return "S3";
    }

    @Override
    public StorageKind kind() {
        return StorageKind.OBJECT_STORAGE;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.S3;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return matchedProperties;
    }

    /**
     * Returns canonical {@code AWS_*} keys consumed by {@link S3ObjStorage}.
     * This preserves compatibility with the existing map-based path.
     */
    public Map<String, String> toFileSystemKv() {
        Map<String, String> kv = new HashMap<>();
        putIfNotBlank(kv, "AWS_ENDPOINT", endpoint);
        putIfNotBlank(kv, "AWS_REGION", region);
        putIfNotBlank(kv, "AWS_ACCESS_KEY", accessKey);
        putIfNotBlank(kv, "AWS_SECRET_KEY", secretKey);
        putIfNotBlank(kv, "AWS_TOKEN", sessionToken);
        putIfNotBlank(kv, "AWS_ROLE_ARN", roleArn);
        putIfNotBlank(kv, "AWS_EXTERNAL_ID", externalId);
        putIfNotBlank(kv, "AWS_BUCKET", bucket);
        putIfNotBlank(kv, "AWS_ROOT_PATH", rootPath);
        kv.put("AWS_MAX_CONNECTIONS", maxConnections);
        kv.put("AWS_REQUEST_TIMEOUT_MS", requestTimeoutMs);
        kv.put("AWS_CONNECTION_TIMEOUT_MS", connectionTimeoutMs);
        kv.put(USE_PATH_STYLE, usePathStyle);
        kv.put("AWS_CREDENTIALS_PROVIDER_TYPE", getCredentialsProviderType().getMode());
        return Collections.unmodifiableMap(kv);
    }

    @Override
    public Optional<BackendStorageProperties> toBackendProperties() {
        return Optional.of(this);
    }

    @Override
    public Optional<HadoopStorageProperties> toHadoopProperties() {
        return Optional.of(this);
    }

    @Override
    public BackendStorageKind backendKind() {
        return BackendStorageKind.S3_COMPATIBLE;
    }

    @Override
    public Map<String, String> toMap() {
        return toFileSystemKv();
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3.impl.disable.cache", "true");
        cfg.put("fs.s3a.impl.disable.cache", "true");
        putIfNotBlank(cfg, "fs.s3a.endpoint", endpoint);
        putIfNotBlank(cfg, "fs.s3a.endpoint.region", region);
        if (StringUtils.isNotBlank(accessKey)) {
            cfg.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            cfg.put("fs.s3a.access.key", accessKey);
            cfg.put("fs.s3a.secret.key", secretKey);
            putIfNotBlank(cfg, "fs.s3a.session.token", sessionToken);
        }
        if (StringUtils.isNotBlank(roleArn)) {
            cfg.put("fs.s3a.assumed.role.arn", roleArn);
            cfg.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
            cfg.put("fs.s3a.assumed.role.credentials.provider",
                    S3CredentialsProviderFactory.hadoopClassName(getCredentialsProviderType(), false));
            putIfNotBlank(cfg, "fs.s3a.assumed.role.external.id", externalId);
        } else if (StringUtils.isBlank(accessKey)) {
            cfg.put("fs.s3a.aws.credentials.provider",
                    S3CredentialsProviderFactory.hadoopClassName(getCredentialsProviderType(), true));
        }
        cfg.put("fs.s3a.connection.maximum", maxConnections);
        cfg.put("fs.s3a.connection.request.timeout", requestTimeoutMs);
        cfg.put("fs.s3a.connection.timeout", connectionTimeoutMs);
        cfg.put("fs.s3a.path.style.access", usePathStyle);
        return Collections.unmodifiableMap(cfg);
    }

    public S3CredentialsProviderType getCredentialsProviderType() {
        return S3CredentialsProviderType.fromString(credentialsProviderType);
    }

    public boolean hasStaticCredentials() {
        return StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey);
    }

    public boolean hasAssumeRole() {
        return StringUtils.isNotBlank(roleArn);
    }

    public boolean isDirectoryBucketEndpoint() {
        return StringUtils.containsIgnoreCase(endpoint, "s3express-control.")
                || StringUtils.containsIgnoreCase(endpoint, "s3express-");
    }

    private static void putIfNotBlank(Map<String, String> map, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            map.put(key, value);
        }
    }

    private boolean hasUnsupportedCredentialsProviderType() {
        try {
            getCredentialsProviderType();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    private static Map<String, String> collectMatchedProperties(Map<String, String> rawProperties) {
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(S3FileSystemProperties.class)) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, rawProperties);
            if (StringUtils.isNotBlank(matchedName)) {
                matched.put(matchedName, rawProperties.get(matchedName));
            }
        }
        return matched;
    }

    private void normalizeForLegacyS3Compatibility() {
        if (StringUtils.isBlank(endpoint) && StringUtils.isNotBlank(region)) {
            endpoint = buildS3Endpoint(region);
        }
        if (StringUtils.isBlank(region) && StringUtils.isNotBlank(endpoint)) {
            region = extractRegion(endpoint).orElse(DEFAULT_REGION);
        }
        if (StringUtils.containsIgnoreCase(endpoint, "glue") && StringUtils.isNotBlank(region)) {
            endpoint = buildS3Endpoint(region);
        }
    }

    private static String buildS3Endpoint(String region) {
        return "https://s3." + region + ".amazonaws.com";
    }

    private static Optional<String> extractRegion(String endpoint) {
        String lowerEndpoint = endpoint.toLowerCase();
        for (Pattern pattern : ENDPOINT_PATTERNS) {
            Matcher matcher = pattern.matcher(lowerEndpoint);
            if (!matcher.matches()) {
                continue;
            }
            for (int i = 1; i <= matcher.groupCount(); i++) {
                String group = matcher.group(i);
                if (StringUtils.isNotBlank(group)) {
                    return Optional.of(group);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
