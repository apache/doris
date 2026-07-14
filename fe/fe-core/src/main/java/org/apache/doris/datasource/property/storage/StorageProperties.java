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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectionProperties;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;

import lombok.Getter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

    /**
     * Reserved Hadoop configuration property carrying a credential fingerprint. The
     * Doris-patched {@code org.apache.hadoop.fs.FileSystem} shipped in hadoop-deps.jar on the
     * BE classpath mixes this value into its {@code FileSystem.CACHE} key, so FileSystem
     * instances created for the same scheme://authority but with different credentials no
     * longer collide in the cache. Vanilla (unpatched) Hadoop ignores this property, and an
     * absent/empty value keeps the vanilla cache-key semantics.
     */
    public static final String FS_CACHE_KEY_PROPERTY = "doris.fs.cache.key";

    private volatile String fsCacheFingerprint;

    /**
     * Backend-bound configuration properties. The returned map is a defensive copy and always
     * carries {@link #FS_CACHE_KEY_PROPERTY} identifying the credential set it was built from,
     * see {@link #getFsCacheFingerprint()}.
     */
    public final Map<String, String> getBackendConfigProperties() {
        Map<String, String> props = new HashMap<>(doGetBackendConfigProperties());
        props.put(FS_CACHE_KEY_PROPERTY, getFsCacheFingerprint());
        return props;
    }

    protected abstract Map<String, String> doGetBackendConfigProperties();

    /**
     * Stable fingerprint of this storage identity: SHA-256 over the concrete class name and the
     * sorted user-supplied properties this instance matched ({@code matchedProperties}), which
     * include the credentials. The same definition always yields the same fingerprint (cache
     * hits are preserved across queries); any credential or config change yields a new one.
     */
    public String getFsCacheFingerprint() {
        if (fsCacheFingerprint == null) {
            fsCacheFingerprint = fingerprintOf(getClass().getName(), matchedProperties);
        }
        return fsCacheFingerprint;
    }

    private static String fingerprintOf(String salt, Map<String, String> props) {
        StringBuilder sb = new StringBuilder(salt);
        new TreeMap<>(props).forEach((k, v) -> sb.append('\n').append(k).append('=').append(v == null ? "" : v));
        return DigestUtils.sha256Hex(sb.toString()).substring(0, 32);
    }

    /**
     * Fingerprint for a property map merged from several StorageProperties (a catalog can hold
     * more than one storage type). Order-independent combination of the individual fingerprints.
     */
    public static String combinedFsCacheFingerprint(Collection<StorageProperties> spList) {
        List<String> fps = new ArrayList<>();
        for (StorageProperties sp : spList) {
            fps.add(sp.getFsCacheFingerprint());
        }
        if (fps.size() == 1) {
            return fps.get(0);
        }
        fps.sort(String::compareTo);
        return DigestUtils.sha256Hex(String.join("\n", fps)).substring(0, 32);
    }

    /**
     * Merging several storages' hadoop configs (addResource / put-all loops) keeps only the
     * last storage's {@link #FS_CACHE_KEY_PROPERTY}; call this after such a merge to replace
     * it with the order-independent combined fingerprint. No-op for an empty storage list.
     */
    public static void setCombinedFsCacheKey(Configuration conf, Collection<StorageProperties> spList) {
        if (conf != null && spList != null && !spList.isEmpty()) {
            conf.set(FS_CACHE_KEY_PROPERTY, combinedFsCacheFingerprint(spList));
        }
    }

    /**
     * Map flavor of {@link #setCombinedFsCacheKey(Configuration, Collection)}.
     */
    public static void setCombinedFsCacheKey(Map<String, String> props, Collection<StorageProperties> spList) {
        if (props != null && spList != null && !spList.isEmpty()) {
            props.put(FS_CACHE_KEY_PROPERTY, combinedFsCacheFingerprint(spList));
        }
    }

    /**
     * Hadoop storage configuration used for interacting with HDFS-based systems.
     * <p>
     * Currently, some underlying APIs in Hive and Iceberg still rely on the HDFS protocol directly.
     * Because of this, we must introduce an additional storage layer conversion here to adapt
     * our system's storage abstraction to the HDFS protocol.
     * <p>
     * In the future, once we have unified the storage access layer by implementing our own
     * FileIO abstraction (a custom, unified interface for file system access),
     * this conversion layer will no longer be necessary. The FileIO abstraction
     * will provide seamless and consistent access to different storage backends,
     * eliminating the need to rely on HDFS protocol specifics.
     * <p>
     * This approach will simplify the integration and improve maintainability
     * by standardizing the way storage systems are accessed.
     */
    @Getter
    public Configuration hadoopStorageConfig;

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
            // Runtime properties may carry per-session credentials (e.g. vended credentials),
            // so the fingerprint of the base definition alone would wrongly share cache slots.
            properties.put(FS_CACHE_KEY_PROPERTY,
                    fingerprintOf(getFsCacheFingerprint(), runtimeProperties));
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
    public static List<StorageProperties> createAll(Map<String, String> origProps) throws UserException {
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
     * @throws UserException if the URL is empty, lacks a valid scheme, or contains an unsupported scheme
     */
    public abstract String validateAndNormalizeUri(String url) throws UserException;

    /**
     * Extracts the URI string from the provided properties map, validates it, and returns the normalized URI.
     * <p>
     * This method checks that the 'uri' key exists in the property map, retrieves the value,
     * and then delegates to {@link #validateAndNormalizeUri(String)} for further validation and normalization.
     *
     * @param loadProps the map containing load-related properties, including the URI under the key 'uri'
     * @return a normalized and validated URI string
     * @throws UserException if the 'uri' property is missing, empty, or invalid
     */
    public abstract String validateAndGetUri(Map<String, String> loadProps) throws UserException;

    public abstract String getStorageName();

    private void buildHadoopStorageConfig() {
        initializeHadoopStorageConfig();
        if (null == hadoopStorageConfig) {
            return;
        }
        appendUserFsConfig(origProps);
        applyUserFsCacheOverrides(hadoopStorageConfig, origProps);
        // Covers the channels that bake this Configuration into BE-bound artifacts
        // (Hudi hadoop_conf.*, Paimon/Iceberg FileIO options); see FS_CACHE_KEY_PROPERTY.
        hadoopStorageConfig.set(FS_CACHE_KEY_PROPERTY, getFsCacheFingerprint());
    }

    private void appendUserFsConfig(Map<String, String> userProps) {
        userProps.forEach((k, v) -> {
            if (k.startsWith(userFsPropsPrefix) && StringUtils.isNotBlank(v)) {
                hadoopStorageConfig.set(k, v);
            }
        });
    }

    protected abstract void initializeHadoopStorageConfig();

    protected abstract Set<String> schemas();

    /**
     * Hadoop caches FileSystem instances per scheme/authority/UGI, which used to cause cross-credential
     * contamination when different catalogs/TVFs accessed the same bucket or namenode with different
     * credentials. Doris therefore used to force fs.&lt;schema&gt;.impl.disable.cache=true everywhere.
     * <p>
     * That blanket disable is gone: both FE and BE now load a patched {@link org.apache.hadoop.fs.FileSystem}
     * whose cache key additionally carries {@link #FS_CACHE_KEY_PROPERTY} (a per-storage credential
     * fingerprint injected right after this method runs), so instances with different credentials never
     * collide while identical definitions safely share one cached instance.
     * <p>
     * Users can still opt out per schema by explicitly setting fs.&lt;schema&gt;.impl.disable.cache;
     * this method only forwards such explicit choices.
     */
    private void applyUserFsCacheOverrides(Configuration conf, Map<String, String> origProps) {
        for (String schema : schemas()) {
            String key = "fs." + schema + ".impl.disable.cache";
            String userValue = origProps.get(key);
            if (StringUtils.isNotBlank(userValue)) {
                conf.setBoolean(key, BooleanUtils.toBoolean(userValue));
            }
        }
    }
}
