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

package org.apache.doris.property.storage;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.property.common.AwsCredentialsProviderMode;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract base class for object storage system properties. This class provides common configuration
 * settings for object storage systems and supports conversion of these properties into configuration
 * maps for different protocols, such as AWS S3. All object storage systems should extend this class
 * to inherit the common configuration properties and methods.
 * <p>
 * The properties include connection settings (e.g., timeouts and maximum connections) and a flag to
 * determine if path-style URLs should be used for the storage system.
 */
public abstract class AbstractS3CompatibleProperties extends StorageProperties implements ObjectStorageProperties {
    private static final Logger LOG = LogManager.getLogger(AbstractS3CompatibleProperties.class);

    /**
     * Constructor to initialize the object storage properties with the provided type and original properties map.
     *
     * @param type      the type of object storage system.
     * @param origProps the original properties map.
     */
    protected AbstractS3CompatibleProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    /**
     * Generates a map of AWS S3 configuration properties specifically for Backend (BE) service usage.
     * This configuration includes endpoint, region, access credentials, timeouts, and connection settings.
     * The map is typically used to initialize S3-compatible storage access for the backend.
     *
     * @param maxConnections      the maximum number of allowed S3 connections.
     * @param requestTimeoutMs    request timeout in milliseconds.
     * @param connectionTimeoutMs connection timeout in milliseconds.
     * @param usePathStyle        whether to use path-style access (true/false).
     * @return a map containing AWS S3 configuration properties.
     */
    protected Map<String, String> generateBackendS3Configuration(String maxConnections,
                                                                 String requestTimeoutMs,
                                                                 String connectionTimeoutMs,
                                                                 String usePathStyle) {
        return doBuildS3Configuration(maxConnections, requestTimeoutMs, connectionTimeoutMs, usePathStyle);
    }

    /**
     * Overloaded version of {@link #generateBackendS3Configuration(String, String, String, String)}
     * that uses default values
     * from the current object context for connection settings.
     *
     * @return a map containing AWS S3 configuration properties.
     */
    protected Map<String, String> generateBackendS3Configuration() {
        return doBuildS3Configuration(getMaxConnections(), getRequestTimeoutS(), getConnectionTimeoutS(),
                getUsePathStyle());
    }

    /**
     * Internal method to centralize S3 configuration property assembly.
     */
    private Map<String, String> doBuildS3Configuration(String maxConnections,
                                                       String requestTimeoutMs,
                                                       String connectionTimeoutMs,
                                                       String usePathStyle) {
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("AWS_ENDPOINT", getEndpoint());
        s3Props.put("AWS_REGION", getRegion());
        s3Props.put("AWS_ACCESS_KEY", getAccessKey());
        s3Props.put("AWS_SECRET_KEY", getSecretKey());
        s3Props.put("AWS_MAX_CONNECTIONS", maxConnections);
        s3Props.put("AWS_REQUEST_TIMEOUT_MS", requestTimeoutMs);
        s3Props.put("AWS_CONNECTION_TIMEOUT_MS", connectionTimeoutMs);
        s3Props.put("use_path_style", usePathStyle);
        if (StringUtils.isNotBlank(getSessionToken())) {
            s3Props.put("AWS_TOKEN", getSessionToken());
        }
        String credentialsProviderType = getAwsCredentialsProviderTypeForBackend();
        if (StringUtils.isNotBlank(credentialsProviderType)) {
            s3Props.put("AWS_CREDENTIALS_PROVIDER_TYPE", credentialsProviderType);
        }
        return s3Props;
    }

    protected String getAwsCredentialsProviderTypeForBackend() {
        if (StringUtils.isBlank(getAccessKey()) && StringUtils.isBlank(getSecretKey())) {
            return AwsCredentialsProviderMode.ANONYMOUS.name();
        }
        return null;
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        return generateBackendS3Configuration();
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        setEndpointIfPossible();
        setRegionIfPossible();
        // NOTE (fe-property leniency — matches the connector storage-config port): region/endpoint are NOT
        // required here. setEndpointIfPossible/setRegionIfPossible derive them when possible; if still blank, the
        // corresponding fs.s3a.endpoint[.region] key is simply omitted (see appendS3HdfsProperties) instead of
        // failing fast. Legacy fe-core threw; the connector emits conditionally, and that behavior is preserved so
        // a connector catalog that delegates parsing here keeps its current (lenient) runtime behavior.
    }

    /**
     * Checks and validates the configured endpoint.
     * <p>
     * All object storage implementations must have an explicitly set endpoint.
     * However, for compatibility with legacy behavior—especially when using DLF
     * as the catalog—some logic may derive the endpoint based on the region.
     * <p>
     * To support such cases, this method is exposed as {@code protected} to allow
     * subclasses to override it with custom logic if necessary.
     * <p>
     * That said, we strongly recommend users to explicitly configure both
     * {@code endpoint} and {@code region} to ensure predictable behavior
     * across all storage backends.
     *
     * @throws IllegalArgumentException if the endpoint format is invalid
     */
    protected void setEndpointIfPossible() {
        if (StringUtils.isNotBlank(getEndpoint())) {
            return;
        }
        // 1. try getting endpoint region
        String endpoint = getEndpointFromRegion();
        if (StringUtils.isNotBlank(endpoint)) {
            setEndpoint(endpoint);
            return;
        }
        // 2. try getting endpoint from uri
        try {
            endpoint = S3PropertyUtils.constructEndpointFromUrl(origProps, getUsePathStyle(),
                    getForceParsingByStandardUrl());
            if (StringUtils.isNotBlank(endpoint)) {
                setEndpoint(endpoint);
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to construct endpoint from url: {}", e.getMessage(), e);
            }
        }
    }

    private void setRegionIfPossible() {
        if (StringUtils.isNotBlank(getRegion())) {
            return;
        }
        String endpoint = getEndpoint();
        if (endpoint == null || endpoint.isEmpty()) {
            return;
        }
        Optional<String> regionOptional = extractRegion(endpoint);
        if (regionOptional.isPresent()) {
            setRegion(regionOptional.get());
        }
    }

    private Optional<String> extractRegion(String endpoint) {
        return extractRegion(endpointPatterns(), endpoint);
    }

    public static Optional<String> extractRegion(Set<Pattern> endpointPatterns, String endpoint) {
        for (Pattern pattern : endpointPatterns) {
            Matcher matcher = pattern.matcher(endpoint.toLowerCase());
            if (matcher.matches()) {
                // Check all possible groups for region (group 1, 2, or 3)
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    String group = matcher.group(i);
                    if (StringUtils.isNotBlank(group)) {
                        return Optional.of(group);
                    }
                }
            }
        }
        return Optional.empty();
    }

    protected abstract Set<Pattern> endpointPatterns();

    // This method should be overridden by subclasses to provide a default endpoint based on the region.
    // Because for aws s3, only region is needed, the endpoint can be constructed from the region.
    // But for other s3 compatible storage, the endpoint may need to be specified explicitly.
    protected String getEndpointFromRegion() {
        return "";
    }

    @Override
    public String validateAndNormalizeUri(String uri) throws StoragePropertiesException {
        return S3PropertyUtils.validateAndNormalizeUri(uri, getUsePathStyle(), getForceParsingByStandardUrl());

    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws StoragePropertiesException {
        return S3PropertyUtils.validateAndGetUri(loadProps);
    }

    @Override
    public void initializeHadoopStorageConfig() {
        hadoopConfigMap = new LinkedHashMap<>();
        // Compatibility note: Due to historical reasons, even when the underlying
        // storage is OSS, OBS, etc., users may still configure the schema as "s3://".
        // To ensure backward compatibility, we append S3-related properties by default.
        appendS3HdfsProperties(hadoopConfigMap);
    }

    private void appendS3HdfsProperties(Map<String, String> hadoopConfigMap) {
        hadoopConfigMap.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfigMap.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // endpoint/region emitted only when present (lenient; matches the connector port that omitted them when
        // blank rather than asserting non-null).
        if (StringUtils.isNotBlank(getEndpoint())) {
            hadoopConfigMap.put("fs.s3a.endpoint", getEndpoint());
        }
        if (StringUtils.isNotBlank(getRegion())) {
            hadoopConfigMap.put("fs.s3a.endpoint.region", getRegion());
        }
        hadoopConfigMap.put("fs.s3.impl.disable.cache", "true");
        hadoopConfigMap.put("fs.s3a.impl.disable.cache", "true");
        if (StringUtils.isNotBlank(getAccessKey())) {
            hadoopConfigMap.put("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            hadoopConfigMap.put("fs.s3a.access.key", getAccessKey());
            hadoopConfigMap.put("fs.s3a.secret.key", getSecretKey());
            if (StringUtils.isNotBlank(getSessionToken())) {
                hadoopConfigMap.put("fs.s3a.session.token", getSessionToken());
            }
        }
        hadoopConfigMap.put("fs.s3a.connection.maximum", getMaxConnections());
        hadoopConfigMap.put("fs.s3a.connection.request.timeout", getRequestTimeoutS());
        hadoopConfigMap.put("fs.s3a.connection.timeout", getConnectionTimeoutS());
        hadoopConfigMap.put("fs.s3a.path.style.access", getUsePathStyle());
    }

    /**
     * Searches for a region value from the given properties map by scanning all known
     * S3-compatible subclass region field annotations.
     * <p>
     * This method iterates through all known subclasses of {@link AbstractS3CompatibleProperties},
     * finds fields annotated with {@code @ConnectorProperty(isRegionField = true)},
     * and checks if any of the annotation's {@code names} exist in the provided properties map.
     *
     * @param props the property map to search for region values
     * @return the region value if found, or {@code null} if no region property is present
     */
    public static String getRegionFromProperties(Map<String, String> props) {
        List<Class<? extends AbstractS3CompatibleProperties>> subClasses = Arrays.asList(
                S3Properties.class, OSSProperties.class, COSProperties.class,
                OBSProperties.class, MinioProperties.class);
        for (Class<?> clazz : subClasses) {
            List<Field> fields = ConnectorPropertiesUtils.getConnectorProperties(clazz);
            for (Field field : fields) {
                ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
                if (annotation != null && annotation.isRegionField()) {
                    for (String name : annotation.names()) {
                        String value = props.get(name);
                        if (StringUtils.isNotBlank(value)) {
                            return value;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public String getStorageName() {
        return "S3";
    }

    /** Returns the bucket name from the connector properties map. */
    public String getBucket() {
        String bucket = origProps.get("s3.bucket");
        if (bucket == null) {
            bucket = origProps.get("AWS_BUCKET");
        }
        return bucket != null ? bucket : "";
    }
}
