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

import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.property.ConnectionProperties;

import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class StorageProperties extends ConnectionProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_MINIO_SUPPORT = "fs.minio.support";
    public static final String FS_OZONE_SUPPORT = "fs.ozone.support";
    public static final String FS_BROKER_SUPPORT = "fs.broker.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";
    public static final String FS_OSS_SUPPORT = "fs.oss.support";
    public static final String FS_OBS_SUPPORT = "fs.obs.support";
    public static final String FS_COS_SUPPORT = "fs.cos.support";
    public static final String FS_OSS_HDFS_SUPPORT = "fs.oss-hdfs.support";
    public static final String FS_LOCAL_SUPPORT = "fs.local.support";
    public static final String FS_HTTP_SUPPORT = "fs.http.support";

    public static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";
    protected static final String URI_KEY = "uri";

    public static final String FS_PROVIDER_KEY = "provider";

    protected final String userFsPropsPrefix = "fs.";

    public enum Type {
        HDFS,
        S3,
        OSS,
        OBS,
        COS,
        GCS,
        OSS_HDFS,
        MINIO,
        OZONE,
        AZURE,
        BROKER,
        LOCAL,
        HTTP,
        UNKNOWN
    }

    public abstract Map<String, String> getBackendConfigProperties();

    /**
     * Normalized Hadoop-style storage configuration as a flat key/value map (e.g. {@code fs.s3a.*},
     * {@code fs.cosn.*}, {@code fs.obs.*}, {@code fs.azure.*}, {@code dfs.*}). This module deliberately does NOT
     * build a live Hadoop {@code Configuration} object: it only produces the keys to set. Consumers (fe-core, SPI
     * connectors) overlay this map onto their own {@code Configuration} (e.g.
     * {@code getHadoopConfigMap().forEach(conf::set)}) using their own hadoop dependency.
     * <p>
     * A {@code null} value means this storage backend contributes no Hadoop config (e.g. HTTP), preserving the
     * legacy semantics where {@code hadoopConfigMap} was left null and the user-fs/disable-cache overlay was
     * skipped.
     */
    @Getter
    protected Map<String, String> hadoopConfigMap;

    /**
     * Get backend configuration properties with optional runtime properties.
     * This method allows passing runtime properties (like vended credentials)
     * that should be merged with the base configuration.
     *
     * @param runtimeProperties additional runtime properties to merge, can be null
     * @return Map of backend properties including runtime properties
     */
    public Map<String, String> getBackendConfigProperties(Map<String, String> runtimeProperties) {
        Map<String, String> properties = new HashMap<>(getBackendConfigProperties());
        if (runtimeProperties != null && !runtimeProperties.isEmpty()) {
            properties.putAll(runtimeProperties);
        }
        return properties;
    }

    @Getter
    protected Type type;


    /**
     * Creates a list of StorageProperties instances based on the provided properties.
     * <p>
     * This method iterates through all registered storage providers and constructs one
     * {@link StorageProperties} instance for each provider that recognizes the given properties.
     * <p>
     * If no HDFSProperties is explicitly configured, a default HDFSProperties will be added
     * automatically. The default HDFSProperties is inserted at index 0 to ensure that:
     * <ul>
     *   <li>The list preserves a deterministic order (it is an ordered List).</li>
     *   <li>The default HDFS configuration does not override or shadow explicitly configured
     *       object storage providers, which are appended after detection.</li>
     * </ul>
     *
     * @param origProps the raw property map used to initialize each StorageProperties instance
     * @return an ordered list of StorageProperties instances
     */
    public static List<StorageProperties> createAll(Map<String, String> origProps) throws StoragePropertiesException {
        List<StorageProperties> result = new ArrayList<>();
        // If the user has explicitly specified any fs.xx.support=true, disable guessIsMe heuristics
        // for all providers to avoid false-positive matches from ambiguous endpoint strings.
        boolean useGuess = !hasAnyExplicitFsSupport(origProps);
        for (BiFunction<Map<String, String>, Boolean, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps, useGuess);
            if (p != null) {
                result.add(p);
            }
        }
        // When no explicit fs.xx.support flag is set, add a default HDFS storage as fallback.
        // When the user has explicitly declared providers via fs.xx.support=true, skip the
        // default HDFS to avoid injecting an unwanted provider into the result.
        if (useGuess && result.stream().noneMatch(HdfsProperties.class::isInstance)) {
            result.add(0, new HdfsProperties(origProps, false));
        }

        for (StorageProperties storageProperties : result) {
            storageProperties.initNormalizeAndCheckProps();
            storageProperties.buildHadoopStorageConfig();
        }
        return result;
    }

    /**
     * Creates a primary StorageProperties instance based on the provided properties.
     * <p>
     * This method iterates through the list of supported storage types and returns the first
     * matching StorageProperties instance. If no supported type is found, an exception is thrown.
     *
     * @param origProps the original properties map to create the StorageProperties instance
     * @return a StorageProperties instance for the primary storage type
     * @throws RuntimeException if no supported storage type is found
     */
    public static StorageProperties createPrimary(Map<String, String> origProps) {
        // If the user has explicitly specified any fs.xx.support=true, disable guessIsMe heuristics
        // for all providers to avoid false-positive matches from ambiguous endpoint strings.
        boolean useGuess = !hasAnyExplicitFsSupport(origProps);
        for (BiFunction<Map<String, String>, Boolean, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps, useGuess);
            if (p != null) {
                p.initNormalizeAndCheckProps();
                p.buildHadoopStorageConfig();
                return p;
            }
        }
        throw new StoragePropertiesException("No supported storage type found. Please check your configuration.");
    }

    /**
     * Connector-facing helper: builds the merged Hadoop object-storage config map ({@code fs.s3a.*}/{@code fs.oss.*}/
     * {@code fs.cosn.*}/{@code fs.obs.*}) for whatever object-store backends the raw properties configure. Unlike
     * {@link #createAll}, it injects NO default HDFS fallback and does NOT fail when no object store is present
     * (returns an empty map). HDFS / local / broker / http contribute nothing here: a connector overlays the raw
     * {@code fs.*}/{@code dfs.*}/{@code hadoop.*} passthrough itself. Used by SPI connectors that build their own
     * {@link java.util.Map}-backed Hadoop config (e.g. paimon) instead of importing fe-core's StorageProperties.
     *
     * @param origProps the raw user property map
     * @return the merged object-storage Hadoop config keys (possibly empty), never null
     */
    public static Map<String, String> buildObjectStorageHadoopConfig(Map<String, String> origProps) {
        Map<String, String> merged = new LinkedHashMap<>();
        boolean useGuess = !hasAnyExplicitFsSupport(origProps);
        for (BiFunction<Map<String, String>, Boolean, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps, useGuess);
            if (p == null || !isObjectStorage(p.getType())) {
                continue;
            }
            p.initNormalizeAndCheckProps();
            p.buildHadoopStorageConfig();
            if (p.getHadoopConfigMap() != null) {
                merged.putAll(p.getHadoopConfigMap());
            }
        }
        return merged;
    }

    /** Whether the given type is an object-storage backend (vs HDFS / broker / local / http / unknown). */
    private static boolean isObjectStorage(Type type) {
        switch (type) {
            case S3:
            case OSS:
            case OBS:
            case COS:
            case GCS:
            case OSS_HDFS:
            case MINIO:
            case OZONE:
            case AZURE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Registry of all supported storage provider detection functions.
     * <p>
     * Each entry is a {@link BiFunction} that takes:
     * <ul>
     *   <li>{@code props} — the user-supplied property map</li>
     *   <li>{@code guess} — whether heuristic-based {@code guessIsMe} detection is enabled.
     *       When {@code false}, only explicit {@code fs.xx.support=true} flags are honored,
     *       preventing endpoint-based heuristics from causing false-positive matches
     *       across providers (e.g., an {@code aliyuncs.com} endpoint accidentally
     *       matching both OSS and S3).</li>
     * </ul>
     * Returns a {@link StorageProperties} instance if the provider matches, or {@code null} otherwise.
     */
    private static final List<BiFunction<Map<String, String>, Boolean, StorageProperties>> PROVIDERS =
            Arrays.asList(
                    (props, guess) -> (isFsSupport(props, FS_HDFS_SUPPORT)
                            || (guess && HdfsProperties.guessIsMe(props))) ? new HdfsProperties(props) : null,
                    (props, guess) -> {
                        // OSS-HDFS and OSS are mutually exclusive - check OSS-HDFS first
                        if ((isFsSupport(props, FS_OSS_HDFS_SUPPORT)
                                || isFsSupport(props, DEPRECATED_OSS_HDFS_SUPPORT))
                                || (guess && OSSHdfsProperties.guessIsMe(props))) {
                            return new OSSHdfsProperties(props);
                        }
                        // Only check for regular OSS if OSS-HDFS is not enabled
                        if (isFsSupport(props, FS_OSS_SUPPORT)
                                || (guess && OSSProperties.guessIsMe(props))) {
                            return new OSSProperties(props);
                        }
                        return null;
                    },
                    (props, guess) -> (isFsSupport(props, FS_S3_SUPPORT)
                            || (guess && S3Properties.guessIsMe(props))) ? new S3Properties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_OBS_SUPPORT)
                            || (guess && OBSProperties.guessIsMe(props))) ? new OBSProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_COS_SUPPORT)
                            || (guess && COSProperties.guessIsMe(props))) ? new COSProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_GCS_SUPPORT)
                            || (guess && GCSProperties.guessIsMe(props))) ? new GCSProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_AZURE_SUPPORT)
                            || (guess && AzureProperties.guessIsMe(props))) ? new AzureProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_MINIO_SUPPORT)
                            || (guess && MinioProperties.guessIsMe(props))) ? new MinioProperties(props) : null,
                    (props, guess) -> isFsSupport(props, FS_OZONE_SUPPORT) ? new OzoneProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_BROKER_SUPPORT)
                            || (guess && BrokerProperties.guessIsMe(props))) ? new BrokerProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_LOCAL_SUPPORT)
                            || (guess && LocalProperties.guessIsMe(props))) ? new LocalProperties(props) : null,
                    (props, guess) -> (isFsSupport(props, FS_HTTP_SUPPORT)
                            || (guess && HttpProperties.guessIsMe(props))) ? new HttpProperties(props) : null
            );

    protected StorageProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }

    private static boolean isFsSupport(Map<String, String> origProps, String fsEnable) {
        return origProps.getOrDefault(fsEnable, "false").equalsIgnoreCase("true");
    }

    /**
     * Checks whether the user has explicitly set any {@code fs.xx.support=true} property.
     * <p>
     * When at least one explicit {@code fs.xx.support} flag is present, the system should
     * rely solely on these flags for provider matching and skip the heuristic-based
     * {@code guessIsMe} inference. This prevents ambiguous endpoint strings (e.g.,
     * {@code aliyuncs.com}) from accidentally triggering multiple providers (e.g.,
     * both OSS and S3) at the same time.
     *
     * @param props the raw property map from user configuration
     * @return {@code true} if any {@code fs.xx.support} property is explicitly set to "true"
     */
    private static boolean hasAnyExplicitFsSupport(Map<String, String> props) {
        return isFsSupport(props, FS_HDFS_SUPPORT)
                || isFsSupport(props, FS_S3_SUPPORT)
                || isFsSupport(props, FS_GCS_SUPPORT)
                || isFsSupport(props, FS_MINIO_SUPPORT)
                || isFsSupport(props, FS_BROKER_SUPPORT)
                || isFsSupport(props, FS_AZURE_SUPPORT)
                || isFsSupport(props, FS_OSS_SUPPORT)
                || isFsSupport(props, FS_OBS_SUPPORT)
                || isFsSupport(props, FS_COS_SUPPORT)
                || isFsSupport(props, FS_OSS_HDFS_SUPPORT)
                || isFsSupport(props, FS_LOCAL_SUPPORT)
                || isFsSupport(props, FS_HTTP_SUPPORT)
                || isFsSupport(props, FS_OZONE_SUPPORT)
                || isFsSupport(props, DEPRECATED_OSS_HDFS_SUPPORT);
    }

    protected static boolean checkIdentifierKey(Map<String, String> origProps, List<Field> fields) {
        for (Field field : fields) {
            field.setAccessible(true);
            ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
            for (String key : annotation.names()) {
                if (origProps.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Validates the given URL string and returns a normalized URI in the format: scheme://authority/path.
     * <p>
     * This method checks that the input is non-empty, the scheme is present and supported (e.g., hdfs, viewfs),
     * and converts it into a canonical URI string.
     *
     * @param url the raw URL string to validate and normalize
     * @return a normalized URI string with validated scheme and authority
     * @throws StoragePropertiesException if the URL is empty, lacks a valid scheme, or contains an unsupported scheme
     */
    public abstract String validateAndNormalizeUri(String url) throws StoragePropertiesException;

    /**
     * Extracts the URI string from the provided properties map, validates it, and returns the normalized URI.
     * <p>
     * This method checks that the 'uri' key exists in the property map, retrieves the value,
     * and then delegates to {@link #validateAndNormalizeUri(String)} for further validation and normalization.
     *
     * @param loadProps the map containing load-related properties, including the URI under the key 'uri'
     * @return a normalized and validated URI string
     * @throws StoragePropertiesException if the 'uri' property is missing, empty, or invalid
     */
    public abstract String validateAndGetUri(Map<String, String> loadProps) throws StoragePropertiesException;

    public abstract String getStorageName();

    private void buildHadoopStorageConfig() {
        initializeHadoopStorageConfig();
        if (null == hadoopConfigMap) {
            return;
        }
        appendUserFsConfig(origProps);
        ensureDisableCache(hadoopConfigMap, origProps);
    }

    private void appendUserFsConfig(Map<String, String> userProps) {
        userProps.forEach((k, v) -> {
            if (k.startsWith(userFsPropsPrefix) && StringUtils.isNotBlank(v)) {
                hadoopConfigMap.put(k, v);
            }
        });
    }

    protected abstract void initializeHadoopStorageConfig();

    protected abstract Set<String> schemas();

    /**
     * By default, Hadoop caches FileSystem instances per scheme and authority (e.g. s3a://bucket/), meaning that all
     * subsequent calls using the same URI will reuse the same FileSystem object.
     * In multi-tenant or dynamic credential environments — where different users may access the same bucket using
     * different access keys or tokens — this cache reuse can lead to cross-credential contamination.
     * <p>
     * Specifically, if the cache is not disabled, a FileSystem instance initialized with one set of credentials may
     * be reused by another session targeting the same bucket but with a different AK/SK. This results in:
     * <p>
     * Incorrect authentication (using stale credentials)
     * <p>
     * Unexpected permission errors or access denial
     * <p>
     * Potential data leakage between users
     * <p>
     * To avoid such risks, the configuration property
     * fs.<schema>.impl.disable.cache
     * must be set to true for all object storage backends (e.g., S3A, OSS, COS, OBS), ensuring that each new access
     * creates an isolated FileSystem instance with its own credentials and configuration context.
     */
    private void ensureDisableCache(Map<String, String> conf, Map<String, String> origProps) {
        for (String schema : schemas()) {
            String key = "fs." + schema + ".impl.disable.cache";
            String userValue = origProps.get(key);
            if (StringUtils.isNotBlank(userValue)) {
                conf.put(key, String.valueOf(BooleanUtils.toBoolean(userValue)));
            } else {
                conf.put(key, "true");
            }
        }
    }
}
