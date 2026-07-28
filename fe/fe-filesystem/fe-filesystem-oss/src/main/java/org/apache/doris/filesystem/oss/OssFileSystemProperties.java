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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.LegacyS3Uri;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider-owned OSS properties.
 *
 * <p>The public aliases, ordering, defaults, endpoint region detection, and
 * static credential validation follow fe-core OSSProperties.
 */
public final class OssFileSystemProperties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties,
                S3CompatibleFileSystemProperties {

    public static final String ENDPOINT = "oss.endpoint";
    public static final String REGION = "oss.region";
    public static final String ACCESS_KEY = "oss.access_key";
    public static final String SECRET_KEY = "oss.secret_key";
    public static final String SESSION_TOKEN = "oss.session_token";
    public static final String MAX_CONNECTIONS = "oss.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "oss.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "oss.connection.timeout";
    public static final String USE_PATH_STYLE = "oss.use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI =
            "oss.force_parsing_by_standard_uri";

    public static final String DEFAULT_MAX_CONNECTIONS = "100";
    public static final String DEFAULT_REQUEST_TIMEOUT_MS = "10000";
    public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "10000";

    private static final Pattern ENDPOINT_PATTERN =
            Pattern.compile("^(?:https?://)?(?:s3\\.)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com$");

    private static final String JINDO_OSS_FILE_SYSTEM_IMPL =
            "com.aliyun.jindodata.oss.JindoOssFileSystem";
    private static final String JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL =
            "com.aliyun.jindodata.oss.JindoOSS";

    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint", "OSS_ENDPOINT"},
            required = false,
            description = "The endpoint of OSS.")
    private String endpoint = "";

    @ConnectorProperty(names = {ACCESS_KEY, "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY",
            "access_key", "ACCESS_KEY", "dlf.access_key", "dlf.catalog.accessKeyId",
            "fs.oss.accessKeyId", "OSS_ACCESS_KEY"},
            required = false,
            description = "The access key of OSS.")
    private String accessKey = "";

    @ConnectorProperty(names = {SECRET_KEY, "s3.secret_key", "s3.secret-access-key",
            "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "dlf.secret_key",
            "dlf.catalog.secret_key", "fs.oss.accessKeySecret", "OSS_SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The secret key of OSS.")
    private String secretKey = "";

    @ConnectorProperty(names = {REGION, "s3.region", "AWS_REGION", "region", "REGION",
            "dlf.region", "iceberg.rest.signing-region", "OSS_REGION"},
            required = false,
            isRegionField = true,
            description = "The region of OSS.")
    private String region = "";

    @ConnectorProperty(names = {"dlf.access.public", "dlf.catalog.accessPublic"},
            required = false,
            description = "Enable public access to Aliyun DLF.")
    private String dlfAccessPublic = "false";

    @ConnectorProperty(names = {SESSION_TOKEN, "s3.session_token", "s3.session-token",
            "session_token", "fs.oss.securityToken", "OSS_SESSION_TOKEN", "OSS_TOKEN", "AWS_TOKEN"},
            required = false,
            sensitive = true,
            description = "The session token of OSS.")
    private String sessionToken = "";

    @ConnectorProperty(names = {MAX_CONNECTIONS, "s3.connection.maximum"},
            required = false,
            description = "Maximum number of connections.")
    private String maxConnections = DEFAULT_MAX_CONNECTIONS;

    @ConnectorProperty(names = {REQUEST_TIMEOUT_MS, "s3.connection.request.timeout"},
            required = false,
            description = "Request timeout in seconds.")
    private String requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

    @ConnectorProperty(names = {CONNECTION_TIMEOUT_MS, "s3.connection.timeout"},
            required = false,
            description = "Connection timeout in seconds.")
    private String connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;

    @ConnectorProperty(names = {USE_PATH_STYLE, "use_path_style", "s3.path-style-access"},
            required = false,
            description = "Whether to use path style URL for the storage.")
    private String usePathStyle = "false";

    @ConnectorProperty(names = {FORCE_PARSING_BY_STANDARD_URI, "force_parsing_by_standard_uri"},
            required = false,
            description = "Whether to force standard URI parsing.")
    private String forceParsingByStandardUrl = "false";

    @ConnectorProperty(names = {"OSS_BUCKET", "AWS_BUCKET"},
            required = false,
            description = "The default bucket name.")
    private String bucket = "";

    @ConnectorProperty(names = {"OSS_ROLE_ARN", "AWS_ROLE_ARN"},
            required = false,
            description = "The OSS role ARN for AssumeRole access.")
    private String roleArn = "";

    @ConnectorProperty(names = {"AWS_EXTERNAL_ID"},
            required = false,
            description = "The external ID for AssumeRole trust policy.")
    private String externalId = "";

    @ConnectorProperty(names = {"AWS_ROOT_PATH"},
            required = false,
            description = "The root path prefix inside the bucket.")
    private String rootPath = "";

    private final Map<String, String> rawProperties;
    private final Map<String, String> matchedProperties;

    private OssFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        normalize();
    }

    public static OssFileSystemProperties of(Map<String, String> properties) {
        OssFileSystemProperties props = new OssFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        new ParamRules()
                .requireTogether(new String[] {accessKey, secretKey},
                        "Both the access key and the secret key must be set.")
                .check(() -> StringUtils.isBlank(region),
                        "Region is not set. If you are using a standard endpoint, the region "
                                + "will be detected automatically. Otherwise, please specify it explicitly.")
                .check(() -> StringUtils.isBlank(endpoint),
                        "Endpoint is not set. Please specify it explicitly.")
                .check(this::hasInvalidUsePathStyle,
                        "use_path_style must be true or false, got: '" + getUsePathStyle() + "'")
                .validate("Invalid OSS filesystem properties");
    }

    @Override
    public String providerName() {
        return "OSS";
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
        return toBackendKv();
    }

    private Map<String, String> toBackendKv() {
        Map<String, String> kv = new HashMap<>();
        putIfNotBlank(kv, "AWS_ENDPOINT", endpoint);
        putIfNotBlank(kv, "AWS_REGION", region);
        putIfNotBlank(kv, "AWS_ACCESS_KEY", accessKey);
        putIfNotBlank(kv, "AWS_SECRET_KEY", secretKey);
        putIfNotBlank(kv, "AWS_TOKEN", sessionToken);
        putIfNotBlank(kv, "AWS_BUCKET", bucket);
        putIfNotBlank(kv, "AWS_ROLE_ARN", roleArn);
        putIfNotBlank(kv, "AWS_EXTERNAL_ID", externalId);
        putIfNotBlank(kv, "AWS_ROOT_PATH", rootPath);
        kv.put("AWS_MAX_CONNECTIONS", maxConnections);
        kv.put("AWS_REQUEST_TIMEOUT_MS", requestTimeoutMs);
        kv.put("AWS_CONNECTION_TIMEOUT_MS", connectionTimeoutMs);
        kv.put("use_path_style", usePathStyle);
        return Collections.unmodifiableMap(kv);
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3.impl.disable.cache", "true");
        cfg.put("fs.s3a.impl.disable.cache", "true");
        cfg.put("fs.s3a.endpoint", endpoint);
        cfg.put("fs.s3a.endpoint.region", region);
        if (StringUtils.isNotBlank(accessKey)) {
            cfg.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            cfg.put("fs.s3a.access.key", accessKey);
            cfg.put("fs.s3a.secret.key", secretKey);
            putIfNotBlank(cfg, "fs.s3a.session.token", sessionToken);
        }
        cfg.put("fs.s3a.connection.maximum", maxConnections);
        cfg.put("fs.s3a.connection.request.timeout", requestTimeoutMs);
        cfg.put("fs.s3a.connection.timeout", connectionTimeoutMs);
        cfg.put("fs.s3a.path.style.access", usePathStyle);
        cfg.put("fs.oss.impl", JINDO_OSS_FILE_SYSTEM_IMPL);
        cfg.put("fs.AbstractFileSystem.oss.impl", JINDO_OSS_ABSTRACT_FILE_SYSTEM_IMPL);
        cfg.put("fs.oss.accessKeyId", accessKey);
        cfg.put("fs.oss.accessKeySecret", secretKey);
        putIfNotBlank(cfg, "fs.oss.securityToken", sessionToken);
        cfg.put("fs.oss.endpoint", endpoint);
        cfg.put("fs.oss.region", region);
        return Collections.unmodifiableMap(cfg);
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
    public String getRoleArn() {
        return roleArn;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getRootPath() {
        return rootPath;
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
        return Set.of("oss", "s3", "s3a");
    }

    public String getForceParsingByStandardUrl() {
        return forceParsingByStandardUrl;
    }

    private void normalize() {
        // Legacy AbstractS3CompatibleProperties.setEndpointIfPossible leg 2 (inherited by fe-core
        // OSSProperties): derive the endpoint from the raw "uri" property when no endpoint key is
        // set; parse failures are swallowed exactly like fe-core. Runs before the endpoint/region
        // derivation so a uri-derived endpoint feeds region extraction and the standard-endpoint
        // rewrite below, matching the legacy ordering.
        if (StringUtils.isBlank(endpoint)) {
            String derived = LegacyS3Uri.deriveEndpointQuietly(rawProperties, usePathStyle,
                    forceParsingByStandardUrl);
            if (StringUtils.isNotBlank(derived)) {
                endpoint = derived;
            }
        }
        if (StringUtils.isBlank(region) && StringUtils.isNotBlank(endpoint)) {
            region = extractRegion(endpoint).orElse("");
        }
        if (StringUtils.isBlank(endpoint) && StringUtils.isNotBlank(region)) {
            endpoint = getOssEndpoint(region, Boolean.parseBoolean(dlfAccessPublic));
        }
        // Align fe-core OSSProperties.initNormalizeAndCheckProps: any endpoint that is not a
        // standard OSS endpoint (e.g. the S3-compatible s3.<region>.aliyuncs.com spelling) is
        // rewritten to oss-<region>[-internal].aliyuncs.com. Guarded on a non-blank region:
        // with a blank region validate() throws first, exactly like fe-core.
        if (StringUtils.isNotBlank(region)
                && (StringUtils.isBlank(endpoint) || !ENDPOINT_PATTERN.matcher(endpoint).matches())) {
            endpoint = getOssEndpoint(region, Boolean.parseBoolean(dlfAccessPublic));
        }
    }

    private static String getOssEndpoint(String region, boolean publicAccess) {
        String suffix = ".aliyuncs.com";
        if (!publicAccess) {
            suffix = "-internal" + suffix;
        }
        return "oss-" + region + suffix;
    }

    private static Optional<String> extractRegion(String endpoint) {
        Matcher matcher = ENDPOINT_PATTERN.matcher(endpoint.toLowerCase(Locale.ROOT));
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }

    private static Map<String, String> collectMatchedProperties(Map<String, String> rawProperties) {
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(OssFileSystemProperties.class)) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, rawProperties);
            if (StringUtils.isNotBlank(matchedName)) {
                matched.put(matchedName, rawProperties.get(matchedName));
            }
        }
        return matched;
    }

    private static void putIfNotBlank(Map<String, String> map, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            map.put(key, value);
        }
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }

    @Override
    public Set<String> legacyCacheSchemes() {
        return Set.of("oss");
    }

}
