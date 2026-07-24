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

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for S3-compatible dialect properties. Concrete dialects live in their own plugin
 * modules (fe-filesystem-gcs, fe-filesystem-minio, fe-filesystem-ozone) and depend on this one.
 *
 * <p>A dialect differs from plain S3 only in property spelling (aliases), defaults, and the
 * allowed credential surface (static HMAC keys only). Subclasses declare the annotated alias
 * fields and dialect-specific validation; this class owns the shared plumbing: binding,
 * matched-property collection, the canonical {@code AWS_*} delegation map consumed by
 * {@link S3FileSystemProperties#of(Map)}, the Hadoop s3a configuration, and masked rendering.
 */
public abstract class AbstractDelegatingS3Properties
        implements FileSystemProperties, BackendStorageProperties, HadoopStorageProperties,
                S3CompatibleFileSystemProperties {

    // Alias order matches legacy AbstractS3CompatibleProperties.getBucket(), which reads
    // s3.bucket first and falls back to AWS_BUCKET, for every S3-compatible dialect.
    @ConnectorProperty(names = {"s3.bucket", "AWS_BUCKET"},
            required = false,
            description = "The default bucket name.")
    protected String bucket = "";

    @ConnectorProperty(names = {"AWS_ROOT_PATH"},
            required = false,
            description = "The root path prefix inside the bucket.")
    protected String rootPath = "";

    @ConnectorProperty(names = {S3FileSystemProperties.CLIENT_HTTP_SCHEME},
            required = false,
            description = "The default scheme for endpoints without an explicit scheme.")
    private String clientHttpScheme = "https";

    private final Map<String, String> rawProperties;
    private Map<String, String> matchedProperties = Collections.emptyMap();

    protected AbstractDelegatingS3Properties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
    }

    /**
     * Binds raw properties onto the annotated fields (this class and the concrete subclass;
     * {@link ConnectorPropertiesUtils} walks the hierarchy) and collects matched keys.
     *
     * <p>MUST be called from the subclass's static {@code of()} factory AFTER construction:
     * subclass field initializers run after this base constructor, so binding inside the
     * constructor would be silently overwritten by the dialect defaults.
     */
    protected final void bindAndCollect() {
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        Map<String, String> matched = new HashMap<>();
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(getClass())) {
            String matchedName = ConnectorPropertiesUtils.getMatchedPropertyName(field, rawProperties);
            if (StringUtils.isNotBlank(matchedName)) {
                matched.put(matchedName, rawProperties.get(matchedName));
            }
        }
        this.matchedProperties = Collections.unmodifiableMap(matched);
    }

    /**
     * Shared validation for HMAC-only dialects. {@code aliasPrefix} is the dialect key prefix
     * without the dot (e.g. {@code gs}); {@code displayName} the human-readable storage name.
     */
    protected final void validateHmacDialect(String aliasPrefix, String displayName) {
        new ParamRules()
                .requireTogether(new String[] {getAccessKey(), getSecretKey()},
                        aliasPrefix + ".access_key and " + aliasPrefix
                                + ".secret_key must be set together")
                .requireAllIfPresent(getSessionToken(), new String[] {getAccessKey(), getSecretKey()},
                        aliasPrefix + ".session_token requires " + aliasPrefix + ".access_key and "
                                + aliasPrefix + ".secret_key")
                .check(() -> StringUtils.isBlank(getEndpoint()),
                        "Property " + aliasPrefix + ".endpoint is required.")
                .check(this::hasInvalidUsePathStyle,
                        "use_path_style must be true or false, got: '" + getUsePathStyle() + "'")
                .check(() -> S3CompatSignals.hasAwsOnlyCredentialOptions(rawProperties),
                        displayName + " supports only HMAC access_key/secret_key credentials; "
                                + "role_arn/instance-profile style options are not supported")
                .validate("Invalid " + displayName + " filesystem properties");
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
    public BackendStorageKind backendKind() {
        return BackendStorageKind.S3_COMPATIBLE;
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
    public Map<String, String> toMap() {
        return toS3CompatibleKv();
    }

    /**
     * Canonical {@code AWS_*} keys accepted by {@link S3FileSystemProperties#of(Map)}; this is
     * how a dialect delegates to the shared S3 interoperability client.
     */
    public final Map<String, String> toS3CompatibleKv() {
        Map<String, String> kv = new HashMap<>();
        kv.put("AWS_ENDPOINT", getEndpoint());
        kv.put("AWS_REGION", getRegion());
        putIfNotBlank(kv, "AWS_ACCESS_KEY", getAccessKey());
        putIfNotBlank(kv, "AWS_SECRET_KEY", getSecretKey());
        putIfNotBlank(kv, "AWS_TOKEN", getSessionToken());
        putIfNotBlank(kv, "AWS_BUCKET", bucket);
        putIfNotBlank(kv, "AWS_ROOT_PATH", rootPath);
        kv.put(S3FileSystemProperties.CLIENT_HTTP_SCHEME, clientHttpScheme);
        kv.put("AWS_MAX_CONNECTIONS", getMaxConnections());
        kv.put("AWS_REQUEST_TIMEOUT_MS", getRequestTimeoutMs());
        kv.put("AWS_CONNECTION_TIMEOUT_MS", getConnectionTimeoutMs());
        kv.put("use_path_style", getUsePathStyle());
        // With no HMAC pair configured, legacy AbstractS3CompatibleProperties defaults to the
        // ANONYMOUS marker; without it the shared S3 client falls back to the AWS default provider
        // chain and signs requests with ambient env credentials. But preserve an explicitly
        // requested mode (validation restricts it to DEFAULT/ANONYMOUS): a user asking for DEFAULT
        // keeps the credential chain instead of being silently downgraded to unsigned ANONYMOUS.
        if (!hasStaticCredentials()) {
            String requested = S3CompatSignals.requestedCredentialsProviderType(rawProperties);
            kv.put("AWS_CREDENTIALS_PROVIDER_TYPE", requested != null ? requested : "ANONYMOUS");
        }
        customizeS3CompatibleKv(kv);
        return Collections.unmodifiableMap(kv);
    }

    /** Hook for dialect-specific canonical keys. */
    protected void customizeS3CompatibleKv(Map<String, String> kv) {
    }

    @Override
    public Map<String, String> toHadoopConfigurationMap() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        cfg.put("fs.s3.impl.disable.cache", "true");
        cfg.put("fs.s3a.impl.disable.cache", "true");
        cfg.put("fs.s3a.endpoint", getEndpoint());
        cfg.put("fs.s3a.endpoint.region", getRegion());
        if (StringUtils.isNotBlank(getAccessKey())) {
            cfg.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            cfg.put("fs.s3a.access.key", getAccessKey());
            cfg.put("fs.s3a.secret.key", getSecretKey());
            putIfNotBlank(cfg, "fs.s3a.session.token", getSessionToken());
        }
        cfg.put("fs.s3a.connection.maximum", getMaxConnections());
        cfg.put("fs.s3a.connection.request.timeout", getRequestTimeoutMs());
        cfg.put("fs.s3a.connection.timeout", getConnectionTimeoutMs());
        cfg.put("fs.s3a.path.style.access", getUsePathStyle());
        customizeHadoopConfiguration(cfg);
        return Collections.unmodifiableMap(cfg);
    }

    /** Hook for dialect-specific Hadoop configuration (e.g. the GCS fs.gs.impl mapping). */
    protected void customizeHadoopConfiguration(Map<String, String> cfg) {
    }

    @Override
    public String getRoleArn() {
        return "";
    }

    @Override
    public String getExternalId() {
        return "";
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getRootPath() {
        return rootPath;
    }

    protected static void putIfNotBlank(Map<String, String> map, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            map.put(key, value);
        }
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
