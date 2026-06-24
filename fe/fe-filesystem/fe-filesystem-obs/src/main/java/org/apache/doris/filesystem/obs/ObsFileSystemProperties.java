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

package org.apache.doris.filesystem.obs;

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

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider-owned OBS properties.
 *
 * <p>The public aliases, ordering, defaults, endpoint region detection, and
 * static credential validation follow fe-core OBSProperties.
 */
public final class ObsFileSystemProperties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties,
                S3CompatibleFileSystemProperties {

    public static final String ENDPOINT = "obs.endpoint";
    public static final String REGION = "obs.region";
    public static final String ACCESS_KEY = "obs.access_key";
    public static final String SECRET_KEY = "obs.secret_key";
    public static final String SESSION_TOKEN = "obs.session_token";
    public static final String MAX_CONNECTIONS = "obs.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "obs.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "obs.connection.timeout";
    public static final String USE_PATH_STYLE = "obs.use_path_style";
    public static final String FORCE_PARSING_BY_STANDARD_URI =
            "obs.force_parsing_by_standard_uri";
    public static final String SKIP_LIST_FOR_DETERMINISTIC_PATH =
            S3CompatibleFileSystemProperties.SKIP_LIST_FOR_DETERMINISTIC_PATH;
    public static final String HEAD_REQUEST_MAX_PATHS = S3CompatibleFileSystemProperties.HEAD_REQUEST_MAX_PATHS;

    public static final String DEFAULT_MAX_CONNECTIONS = "100";
    public static final String DEFAULT_REQUEST_TIMEOUT_MS = "10000";
    public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "10000";

    private static final Pattern ENDPOINT_PATTERN =
            Pattern.compile("^(?:https?://)?obs\\.([a-z0-9-]+)\\.myhuaweicloud\\.com$");

    private static final boolean OBS_FILE_SYSTEM_AVAILABLE =
            isClassAvailable("org.apache.hadoop.fs.obs.OBSFileSystem");

    @ConnectorProperty(names = {ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "OBS_ENDPOINT"},
            required = false,
            description = "The endpoint of OBS.")
    private String endpoint = "";

    @ConnectorProperty(names = {ACCESS_KEY, "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY",
            "access_key", "ACCESS_KEY", "OBS_ACCESS_KEY"},
            required = false,
            description = "The access key of OBS.")
    private String accessKey = "";

    @ConnectorProperty(names = {SECRET_KEY, "s3.secret_key", "s3.secret-access-key",
            "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "OBS_SECRET_KEY"},
            required = false,
            sensitive = true,
            description = "The secret key of OBS.")
    private String secretKey = "";

    @ConnectorProperty(names = {REGION, "s3.region", "AWS_REGION", "region", "REGION", "OBS_REGION"},
            required = false,
            isRegionField = true,
            description = "The region of OBS.")
    private String region = "";

    @ConnectorProperty(names = {SESSION_TOKEN, "s3.session_token", "s3.session-token",
            "session_token", "OBS_SESSION_TOKEN", "OBS_TOKEN", "AWS_TOKEN"},
            required = false,
            sensitive = true,
            description = "The session token of OBS.")
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

    @ConnectorProperty(names = {SKIP_LIST_FOR_DETERMINISTIC_PATH},
            required = false,
            description = "Whether deterministic S3-compatible paths should use HEAD requests instead of ListObjects.")
    private String skipListForDeterministicPath = "true";

    @ConnectorProperty(names = {HEAD_REQUEST_MAX_PATHS},
            required = false,
            description = "Maximum deterministic S3-compatible object keys to resolve with HEAD "
                    + "before falling back to ListObjects.")
    private int headRequestMaxPaths = S3CompatibleFileSystemProperties.DEFAULT_HEAD_REQUEST_MAX_PATHS;

    @ConnectorProperty(names = {"OBS_BUCKET", "AWS_BUCKET"},
            required = false,
            description = "The default bucket name.")
    private String bucket = "";

    @ConnectorProperty(names = {"OBS_AGENCY_NAME"},
            required = false,
            description = "The OBS agency name for STS access.")
    private String agencyName = "";

    @ConnectorProperty(names = {"OBS_DOMAIN_NAME"},
            required = false,
            description = "The OBS domain name for STS access.")
    private String domainName = "";

    @ConnectorProperty(names = {"OBS_ROLE_ARN", "AWS_ROLE_ARN"},
            required = false,
            description = "The OBS role ARN for compatibility.")
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

    private ObsFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
        this.matchedProperties = Collections.unmodifiableMap(collectMatchedProperties(rawProperties));
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        normalize();
    }

    public static ObsFileSystemProperties of(Map<String, String> properties) {
        ObsFileSystemProperties props = new ObsFileSystemProperties(properties);
        props.validate();
        return props;
    }

    @Override
    public void validate() {
        new ParamRules()
                .check(() -> StringUtils.isBlank(endpoint),
                        "Property obs.endpoint is required.")
                .requireTogether(new String[] {accessKey, secretKey},
                        "Both the access key and the secret key must be set.")
                .check(() -> StringUtils.isBlank(region),
                        "Region is not set. If you are using a standard endpoint, the region "
                                + "will be detected automatically. Otherwise, please specify it explicitly.")
                .check(this::hasInvalidUsePathStyle,
                        "use_path_style must be true or false, got: '" + getUsePathStyle() + "'")
                .check(this::hasInvalidSkipListForDeterministicPath,
                        SKIP_LIST_FOR_DETERMINISTIC_PATH + " must be true or false, got: '"
                                + skipListForDeterministicPath + "'")
                .check(() -> headRequestMaxPaths < 0,
                        HEAD_REQUEST_MAX_PATHS + " must be greater than or equal to 0, got: "
                                + headRequestMaxPaths)
                .validate("Invalid OBS filesystem properties");
    }

    @Override
    public String providerName() {
        return "OBS";
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
        if (OBS_FILE_SYSTEM_AVAILABLE) {
            cfg.put("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
            cfg.put("fs.AbstractFileSystem.obs.impl", "org.apache.hadoop.fs.obs.OBS");
        } else {
            cfg.put("fs.obs.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        cfg.put("fs.obs.access.key", accessKey);
        cfg.put("fs.obs.secret.key", secretKey);
        cfg.put("fs.obs.endpoint", endpoint);
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
    public String getSkipListForDeterministicPath() {
        return skipListForDeterministicPath;
    }

    @Override
    public int getHeadRequestMaxPaths() {
        return headRequestMaxPaths;
    }

    public String getForceParsingByStandardUrl() {
        return forceParsingByStandardUrl;
    }

    public String getAgencyName() {
        return agencyName;
    }

    public String getDomainName() {
        return domainName;
    }

    private void normalize() {
        if (StringUtils.isBlank(region) && StringUtils.isNotBlank(endpoint)) {
            region = extractRegion(endpoint).orElse("");
        }
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
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(ObsFileSystemProperties.class)) {
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

    private static boolean isClassAvailable(String className) {
        try {
            Class.forName(className, false, ObsFileSystemProperties.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
