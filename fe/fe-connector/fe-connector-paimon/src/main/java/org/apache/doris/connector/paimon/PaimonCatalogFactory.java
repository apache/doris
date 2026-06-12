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

package org.apache.doris.connector.paimon;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.FileSystemCatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Pure, testable assembly core for the Paimon connector flavor switch.
 *
 * <p>Mirrors the role of {@code MCConnectorClientFactory}: a stateless static holder that
 * (a) fail-fast {@link #validate(Map) validates} catalog properties at CREATE CATALOG time,
 * and (b) {@link #buildCatalogOptions(Map) builds} the Paimon {@link Options} for a flavor.
 *
 * <p>The option-key logic ports the legacy fe-core {@code AbstractPaimonProperties} +
 * each {@code Paimon*MetaStoreProperties}. {@code buildCatalogOptions} is PURE — it reads only
 * the supplied props (no env, no clock) — which is what makes it unit-testable offline.
 *
 * <p>B1 also adds three PURE Hadoop config builders ({@link #buildHadoopConfiguration},
 * {@link #buildHmsHiveConf}, {@link #buildDlfHiveConf}) that reconstruct, from the raw property
 * map alone, the {@code Configuration}/{@code HiveConf} that the live HiveCatalog needs. These
 * replace the fe-core {@code StorageProperties.getHadoopStorageConfig()} /
 * {@code HMSBaseProperties.getHiveConf()} / {@code PaimonAliyunDLFMetaStoreProperties.buildHiveConf()}
 * with a minimal, fe-core-free reconstruction. They are still pure (Map in, conf out) so they are
 * unit-testable offline; only the {@code CatalogFactory.createCatalog} call in
 * {@code PaimonConnector} needs a live metastore.
 */
public final class PaimonCatalogFactory {

    private static final String USER_PROPERTY_PREFIX = "paimon.";
    private static final String PAIMON_REST_PROPERTY_PREFIX = "paimon.rest.";
    private static final String JDBC_PREFIX = "jdbc.";

    private static final Set<String> KNOWN_FLAVORS = new HashSet<>(Arrays.asList(
            PaimonConnectorProperties.FILESYSTEM,
            PaimonConnectorProperties.HMS,
            PaimonConnectorProperties.REST,
            PaimonConnectorProperties.JDBC,
            PaimonConnectorProperties.DLF));

    /**
     * Storage-config prefixes that are intentionally excluded from the catalog Options
     * passthrough — they belong in the Hadoop Configuration (see {@link #buildHadoopConfiguration}),
     * mirroring legacy {@code AbstractPaimonProperties.userStoragePrefixes}.
     */
    private static final String[] USER_STORAGE_PREFIXES = {
            "paimon.s3.", "paimon.s3a.", "paimon.fs.s3.", "paimon.fs.oss."};

    /** Hadoop S3A standard prefix (legacy {@code AbstractPaimonProperties.FS_S3A_PREFIX}). */
    private static final String FS_S3A_PREFIX = "fs.s3a.";

    // Canonical Doris storage aliases (ported from fe-core S3Properties / OSSProperties
    // @ConnectorProperty names), listed in legacy priority order. Kept as literal strings to avoid
    // importing fe-core StorageProperties. Added by FIX-STORAGE-CREDS: before this, a catalog
    // created with the DOCUMENTED canonical keys (s3.access_key / oss.access_key / AWS_*) had every
    // credential silently dropped by applyStorageConfig (only paimon.* / raw fs.* were recognized),
    // so a private S3/OSS bucket was hit with no credentials. These are translated to the Hadoop
    // fs.s3a.* / fs.oss.* keys the live FileIO actually reads.
    private static final String[] S3_ACCESS_KEY_ALIASES = {
            "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY", "s3.access-key-id"};
    private static final String[] S3_SECRET_KEY_ALIASES = {
            "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "s3.secret-access-key"};
    private static final String[] S3_SESSION_TOKEN_ALIASES = {
            "s3.session_token", "session_token", "s3.session-token", "AWS_TOKEN"};
    private static final String[] S3_ENDPOINT_ALIASES = {
            "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
    private static final String[] S3_REGION_ALIASES = {
            "s3.region", "AWS_REGION", "region", "REGION"};

    private static final String[] OSS_ACCESS_KEY_ALIASES = {
            "oss.access_key", "fs.oss.accessKeyId", "dlf.access_key"};
    private static final String[] OSS_SECRET_KEY_ALIASES = {
            "oss.secret_key", "fs.oss.accessKeySecret", "dlf.secret_key"};
    private static final String[] OSS_SESSION_TOKEN_ALIASES = {
            "oss.session_token", "fs.oss.securityToken"};
    private static final String[] OSS_ENDPOINT_ALIASES = {
            "oss.endpoint", "fs.oss.endpoint"};
    private static final String[] OSS_REGION_ALIASES = {"oss.region", "dlf.region"};

    // S3A connection-tuning aliases (ported from each legacy *Properties @ConnectorProperty names). NOTE the
    // defaults DIVERGE by backend: S3Properties = 50/3000/1000, while OSS/COS/OBS = 100/10000/10000. Emitting
    // one shared default would silently mis-tune AWS S3 (round-3 re-review, FIX-FECONF-STORAGE-PARITY).
    private static final String[] S3_MAX_CONN_ALIASES = {"s3.connection.maximum", "AWS_MAX_CONNECTIONS"};
    private static final String[] S3_REQ_TIMEOUT_ALIASES = {
            "s3.connection.request.timeout", "AWS_REQUEST_TIMEOUT_MS"};
    private static final String[] S3_CONN_TIMEOUT_ALIASES = {"s3.connection.timeout", "AWS_CONNECTION_TIMEOUT_MS"};
    private static final String[] S3_PATH_STYLE_ALIASES = {"use_path_style", "s3.path-style-access"};

    private static final String[] OSS_MAX_CONN_ALIASES = {"oss.connection.maximum", "s3.connection.maximum"};
    private static final String[] OSS_REQ_TIMEOUT_ALIASES = {
            "oss.connection.request.timeout", "s3.connection.request.timeout"};
    private static final String[] OSS_CONN_TIMEOUT_ALIASES = {"oss.connection.timeout", "s3.connection.timeout"};
    private static final String[] OSS_PATH_STYLE_ALIASES = {
            "oss.use_path_style", "use_path_style", "s3.path-style-access"};

    // COS aliases (ported from COSProperties @ConnectorProperty names). Detection is independent of these
    // (cos.* key OR a "myqcloud.com" endpoint/warehouse), so the value lists may safely include the shared
    // s3.*/AWS_* aliases legacy COSProperties accepts.
    private static final String[] COS_ACCESS_KEY_ALIASES = {
            "cos.access_key", "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"};
    private static final String[] COS_SECRET_KEY_ALIASES = {
            "cos.secret_key", "s3.secret_key", "s3.secret-access-key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"};
    private static final String[] COS_SESSION_TOKEN_ALIASES = {
            "cos.session_token", "s3.session_token", "s3.session-token", "session_token"};
    private static final String[] COS_ENDPOINT_ALIASES = {
            "cos.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
    private static final String[] COS_REGION_ALIASES = {
            "cos.region", "s3.region", "AWS_REGION", "region", "REGION"};
    private static final String[] COS_MAX_CONN_ALIASES = {"cos.connection.maximum", "s3.connection.maximum"};
    private static final String[] COS_REQ_TIMEOUT_ALIASES = {
            "cos.connection.request.timeout", "s3.connection.request.timeout"};
    private static final String[] COS_CONN_TIMEOUT_ALIASES = {"cos.connection.timeout", "s3.connection.timeout"};
    private static final String[] COS_PATH_STYLE_ALIASES = {
            "cos.use_path_style", "use_path_style", "s3.path-style-access"};

    // OBS aliases (ported from OBSProperties @ConnectorProperty names).
    private static final String[] OBS_ACCESS_KEY_ALIASES = {
            "obs.access_key", "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY"};
    private static final String[] OBS_SECRET_KEY_ALIASES = {
            "obs.secret_key", "s3.secret_key", "s3.secret-access-key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"};
    private static final String[] OBS_SESSION_TOKEN_ALIASES = {
            "obs.session_token", "s3.session_token", "s3.session-token", "session_token"};
    private static final String[] OBS_ENDPOINT_ALIASES = {
            "obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
    private static final String[] OBS_REGION_ALIASES = {
            "obs.region", "s3.region", "AWS_REGION", "region", "REGION"};
    private static final String[] OBS_MAX_CONN_ALIASES = {"obs.connection.maximum", "s3.connection.maximum"};
    private static final String[] OBS_REQ_TIMEOUT_ALIASES = {
            "obs.connection.request.timeout", "s3.connection.request.timeout"};
    private static final String[] OBS_CONN_TIMEOUT_ALIASES = {"obs.connection.timeout", "s3.connection.timeout"};
    private static final String[] OBS_PATH_STYLE_ALIASES = {
            "obs.use_path_style", "use_path_style", "s3.path-style-access"};

    // Per-backend tuning defaults (legacy *Properties field defaults).
    private static final String S3_DEFAULT_MAX_CONN = "50";
    private static final String S3_DEFAULT_REQ_TIMEOUT = "3000";
    private static final String S3_DEFAULT_CONN_TIMEOUT = "1000";
    private static final String OBJ_STORE_DEFAULT_MAX_CONN = "100";
    private static final String OBJ_STORE_DEFAULT_REQ_TIMEOUT = "10000";
    private static final String OBJ_STORE_DEFAULT_CONN_TIMEOUT = "10000";
    private static final String DEFAULT_PATH_STYLE = "false";

    private static final String S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    private static final String S3A_SIMPLE_CRED_PROVIDER =
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
    // JindoOSS impls (literals; avoid the Aliyun compile dep, same pattern as appendDlfOptions).
    private static final String JINDO_OSS_IMPL = "com.aliyun.jindodata.oss.JindoOssFileSystem";
    private static final String JINDO_OSS_ABSTRACT_IMPL = "com.aliyun.jindodata.oss.JindoOSS";
    // Native Huawei OBS impls (literals; avoid the hadoop-obs compile dep). Used only when classpath-available.
    private static final String OBS_NATIVE_IMPL = "org.apache.hadoop.fs.obs.OBSFileSystem";
    private static final String OBS_NATIVE_ABSTRACT_IMPL = "org.apache.hadoop.fs.obs.OBS";

    private PaimonCatalogFactory() {
    }

    /** Resolves the lower-cased flavor, defaulting to {@code filesystem}. */
    public static String resolveFlavor(Map<String, String> props) {
        return props.getOrDefault(
                PaimonConnectorProperties.PAIMON_CATALOG_TYPE,
                PaimonConnectorProperties.DEFAULT_CATALOG_TYPE).toLowerCase(Locale.ROOT);
    }

    /**
     * Returns the first non-blank value among the given keys, or {@code null} if none is set.
     * Mirrors the alias-priority semantics of the legacy {@code @ConnectorProperty(names=...)}.
     */
    public static String firstNonBlank(Map<String, String> props, String... keys) {
        for (String key : keys) {
            String value = props.get(key);
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    /**
     * Resolves a JDBC {@code driver_url} to a full, scheme-bearing URL string. A value already
     * carrying a scheme ({@code "://"}) is used as-is; an absolute path (starting with {@code "/"})
     * is returned unchanged; otherwise it is treated as a bare jar file name and resolved against
     * the engine's configured {@code jdbc_drivers_dir} (defaulting to
     * {@code $DORIS_HOME/plugins/jdbc_drivers}), mirroring the minimal {@code JdbcResource.getFullDriverUrl}
     * resolution (no file-existence / legacy old-dir / cloud-download handling).
     *
     * <p>Shared by {@code PaimonConnector} (FE {@code URLClassLoader} driver registration) and
     * {@code PaimonScanPlanProvider.getBackendPaimonOptions} (the BE-bound options, where BE does
     * {@code new URL(value)} and a bare {@code "mysql.jar"} would throw {@code MalformedURLException})
     * so BOTH sides resolve a given {@code driver_url} <em>identically</em>. Security validation
     * (format / {@code jdbc_driver_url_white_list} / {@code jdbc_driver_secure_path}) is enforced
     * separately at CREATE CATALOG via {@code PaimonConnector.preCreateValidation}.
     *
     * @param driverUrl the raw driver_url; must be non-null and non-blank (the caller's responsibility —
     *                  both call sites guard with {@code firstNonBlank}/non-null checks before calling)
     * @param env the engine environment map (e.g. {@code jdbc_drivers_dir}, {@code doris_home}); never null
     */
    public static String resolveDriverUrl(String driverUrl, Map<String, String> env) {
        if (driverUrl.contains("://")) {
            return driverUrl;
        }
        if (driverUrl.startsWith("/")) {
            // Absolute path, no scheme: legacy returns it as-is (no driversDir prepend).
            return driverUrl;
        }
        String driversDir = env.get("jdbc_drivers_dir");
        if (StringUtils.isBlank(driversDir)) {
            String dorisHome = env.getOrDefault("doris_home", ".");
            driversDir = dorisHome + "/plugins/jdbc_drivers";
        }
        return "file://" + driversDir + "/" + driverUrl;
    }

    /**
     * Fail-fast validation, mirroring the legacy per-flavor rules. Throws
     * {@link IllegalArgumentException} (style consistent with MaxCompute), which the caller
     * ({@code PluginDrivenExternalCatalog.checkProperties}) wraps into a DdlException.
     */
    public static void validate(Map<String, String> props) {
        String flavor = resolveFlavor(props);
        if (!KNOWN_FLAVORS.contains(flavor)) {
            throw new IllegalArgumentException("Unknown paimon.catalog.type value: " + flavor);
        }

        // warehouse required for ALL flavors, REST included (legacy parity): the base
        // AbstractPaimonProperties declares @ConnectorProperty(names={"warehouse"}) and
        // ConnectorProperty.required() defaults to true; PaimonRestMetaStoreProperties does NOT
        // override it, so legacy rejects a REST catalog without warehouse.
        if (StringUtils.isBlank(props.get(PaimonConnectorProperties.WAREHOUSE))) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }

        switch (flavor) {
            case PaimonConnectorProperties.HMS:
                if (firstNonBlank(props, PaimonConnectorProperties.HMS_URI) == null) {
                    throw new IllegalArgumentException("hive.metastore.uris or uri is required");
                }
                break;
            case PaimonConnectorProperties.REST:
                if (firstNonBlank(props, PaimonConnectorProperties.REST_URI) == null) {
                    throw new IllegalArgumentException("paimon.rest.uri or uri is required");
                }
                if ("dlf".equalsIgnoreCase(props.get(PaimonConnectorProperties.REST_TOKEN_PROVIDER))
                        && (StringUtils.isBlank(props.get(PaimonConnectorProperties.REST_DLF_ACCESS_KEY_ID))
                            || StringUtils.isBlank(props.get(PaimonConnectorProperties.REST_DLF_ACCESS_KEY_SECRET)))) {
                    throw new IllegalArgumentException(
                            "DLF token provider requires 'paimon.rest.dlf.access-key-id' "
                                    + "and 'paimon.rest.dlf.access-key-secret'");
                }
                break;
            case PaimonConnectorProperties.JDBC:
                if (firstNonBlank(props, PaimonConnectorProperties.JDBC_URI) == null) {
                    throw new IllegalArgumentException("uri or paimon.jdbc.uri is required");
                }
                if (firstNonBlank(props, PaimonConnectorProperties.JDBC_DRIVER_URL) != null
                        && firstNonBlank(props, PaimonConnectorProperties.JDBC_DRIVER_CLASS) == null) {
                    throw new IllegalArgumentException(
                            "jdbc.driver_class or paimon.jdbc.driver_class is required when "
                                    + "jdbc.driver_url or paimon.jdbc.driver_url is specified");
                }
                break;
            case PaimonConnectorProperties.DLF:
                if (firstNonBlank(props, PaimonConnectorProperties.DLF_ACCESS_KEY) == null) {
                    throw new IllegalArgumentException("dlf.access_key is required");
                }
                if (firstNonBlank(props, PaimonConnectorProperties.DLF_SECRET_KEY) == null) {
                    throw new IllegalArgumentException("dlf.secret_key is required");
                }
                // Legacy derives the endpoint from the region when endpoint is blank; if both are
                // blank it throws. We do not derive here (the derivation happens in buildDlfHiveConf,
                // where the endpoint is consumed), but we keep the same fail-fast contract.
                if (firstNonBlank(props, PaimonConnectorProperties.DLF_ENDPOINT) == null
                        && StringUtils.isBlank(props.get(PaimonConnectorProperties.DLF_REGION))) {
                    throw new IllegalArgumentException("dlf.endpoint is required.");
                }
                break;
            default:
                // filesystem: warehouse-only, already checked above.
                break;
        }
    }

    /**
     * Builds the Paimon catalog {@link Options} for the resolved flavor. PURE: depends only on
     * {@code props}. Ports {@code AbstractPaimonProperties.appendCatalogOptions()} (common) plus
     * each flavor's {@code appendCustomCatalogOptions()}.
     */
    public static Options buildCatalogOptions(Map<String, String> props) {
        Options options = new Options();
        String flavor = resolveFlavor(props);

        appendCommonOptions(props, options, flavor);

        switch (flavor) {
            case PaimonConnectorProperties.HMS:
                appendHmsOptions(props, options);
                break;
            case PaimonConnectorProperties.REST:
                appendRestOptions(props, options);
                break;
            case PaimonConnectorProperties.JDBC:
                appendJdbcOptions(props, options);
                break;
            case PaimonConnectorProperties.DLF:
                appendDlfOptions(options);
                break;
            default:
                // filesystem: nothing custom.
                break;
        }
        return options;
    }

    private static void appendCommonOptions(Map<String, String> props, Options options, String flavor) {
        String warehouse = props.get(PaimonConnectorProperties.WAREHOUSE);
        if (StringUtils.isNotBlank(warehouse)) {
            options.set(CatalogOptions.WAREHOUSE.key(), warehouse);
        }
        options.set(CatalogOptions.METASTORE.key(), metastoreIdentifier(flavor));

        // FIXME(cmy): Rethink these custom properties (ported from AbstractPaimonProperties).
        // Re-key generic paimon.* props by stripping the prefix, excluding storage prefixes which
        // belong in the Hadoop Configuration (see buildHadoopConfiguration).
        props.forEach((k, v) -> {
            if (k.toLowerCase(Locale.ROOT).startsWith(USER_PROPERTY_PREFIX)) {
                String newKey = k.substring(USER_PROPERTY_PREFIX.length());
                if (StringUtils.isNotBlank(newKey) && !isStoragePrefixed(k)) {
                    options.set(newKey, v);
                }
            }
        });
    }

    private static String metastoreIdentifier(String flavor) {
        switch (flavor) {
            case PaimonConnectorProperties.FILESYSTEM:
                return FileSystemCatalogFactory.IDENTIFIER;
            case PaimonConnectorProperties.JDBC:
                return JdbcCatalogFactory.IDENTIFIER;
            case PaimonConnectorProperties.REST:
                return "rest";
            case PaimonConnectorProperties.HMS:
            case PaimonConnectorProperties.DLF:
                // = org.apache.paimon.hive.HiveCatalogOptions.IDENTIFIER; kept as a literal to
                // mirror the existing rest/jdbc style (this is a pure option string, not a type ref).
                return "hive";
            default:
                throw new IllegalArgumentException("Unknown paimon.catalog.type value: " + flavor);
        }
    }

    private static boolean isStoragePrefixed(String key) {
        for (String prefix : USER_STORAGE_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static void appendHmsOptions(Map<String, String> props, Options options) {
        String pool = props.getOrDefault(
                PaimonConnectorProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                PaimonConnectorProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        String location = props.getOrDefault(
                PaimonConnectorProperties.LOCATION_IN_PROPERTIES,
                PaimonConnectorProperties.LOCATION_IN_PROPERTIES_DEFAULT);
        options.set(PaimonConnectorProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS, pool);
        options.set(PaimonConnectorProperties.LOCATION_IN_PROPERTIES, location);
        options.set("uri", firstNonBlank(props, PaimonConnectorProperties.HMS_URI));
    }

    private static void appendRestOptions(Map<String, String> props, Options options) {
        options.set("uri", firstNonBlank(props, PaimonConnectorProperties.REST_URI));
        props.forEach((k, v) -> {
            if (k.startsWith(PAIMON_REST_PROPERTY_PREFIX)) {
                options.set(k.substring(PAIMON_REST_PROPERTY_PREFIX.length()), v);
            }
        });
    }

    private static void appendJdbcOptions(Map<String, String> props, Options options) {
        options.set(CatalogOptions.URI.key(), firstNonBlank(props, PaimonConnectorProperties.JDBC_URI));
        String user = firstNonBlank(props, PaimonConnectorProperties.JDBC_USER);
        if (StringUtils.isNotBlank(user)) {
            options.set("jdbc.user", user);
        }
        String password = firstNonBlank(props, PaimonConnectorProperties.JDBC_PASSWORD);
        if (StringUtils.isNotBlank(password)) {
            options.set("jdbc.password", password);
        }
        // Pass through any raw jdbc.* key not already set (legacy appendRawJdbcCatalogOptions).
        props.forEach((k, v) -> {
            if (k != null && k.startsWith(JDBC_PREFIX) && !options.keySet().contains(k)) {
                options.set(k, v);
            }
        });
    }

    private static void appendDlfOptions(Options options) {
        // String literal avoids the Aliyun datalake compile dep (the live SDK ships at runtime).
        options.set("metastore.client.class", "com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient");
        options.set("client-pool-cache.keys", "conf:dlf.catalog.id");
    }

    // ---------------------------------------------------------------------
    // Hadoop Configuration / HiveConf builders (PURE — functions of props only)
    // ---------------------------------------------------------------------

    /**
     * Builds a minimal Hadoop {@link Configuration} for the storage layer (HDFS / S3 / OSS),
     * reconstructed from the raw property map. This replaces the fe-core
     * {@code StorageProperties.getHadoopStorageConfig()} + {@code AbstractPaimonProperties
     * .normalizeS3Config()/appendUserHadoopConfig()} with a fe-core-free port:
     *
     * <ul>
     *   <li>canonical {@code s3.*}/{@code AWS_*} and {@code oss.*}/{@code fs.oss.*}/{@code dlf.*}
     *       aliases are translated to {@code fs.s3a.*} / Jindo {@code fs.oss.*} (ported legacy
     *       {@code appendS3HdfsProperties} + {@code OSSProperties.initializeHadoopStorageConfig};
     *       see {@link #applyStorageConfig});</li>
     *   <li>{@code paimon.s3.*} / {@code paimon.s3a.*} / {@code paimon.fs.s3.*} / {@code paimon.fs.oss.*}
     *       are normalized to the Hadoop S3A prefix {@code fs.s3a.} (strip the matched prefix,
     *       re-key as {@code fs.s3a.} + remainder), matching legacy {@code normalizeS3Config};</li>
     *   <li>raw {@code fs.*} / {@code dfs.*} / {@code hadoop.*} keys are copied verbatim (these are
     *       already Hadoop-recognized keys the user passed through).</li>
     * </ul>
     *
     * <p>PURE: depends only on {@code props}.
     */
    public static Configuration buildHadoopConfiguration(Map<String, String> props) {
        Configuration conf = new Configuration();
        // Pin the Configuration's classloader to the plugin loader (FIX-PAIMON-HADOOP-CLASSLOADER).
        // Hadoop resolves filesystem impls via Configuration.getClass("fs.<scheme>.impl", ...), which
        // loads through Configuration.classLoader (defaults to the thread-context CL = parent 'app').
        // With hadoop-aws (S3AFileSystem) bundled child-first, that default would still resolve
        // S3AFileSystem from the parent and fail the cast to the child-loaded FileSystem. Resolving
        // through the plugin loader keeps the whole FS class graph in one loader.
        conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader());
        applyStorageConfig(props, conf::set);
        return conf;
    }

    /**
     * Applies the normalized storage config via the given setter. Shared by
     * {@link #buildHadoopConfiguration} and the HiveConf builders (which overlay the same storage
     * config onto the HiveConf, mirroring legacy {@code appendUserHadoopConfig(hiveConf)} +
     * {@code ossProps.getHadoopStorageConfig()}). Three steps, in legacy precedence order:
     *
     * <ol>
     *   <li>canonical {@code s3.*}/{@code AWS_*} aliases -&gt; {@code fs.s3a.*} (ported legacy
     *       {@code AbstractS3CompatibleProperties.appendS3HdfsProperties}, credential subset);</li>
     *   <li>canonical {@code oss.*}/{@code fs.oss.*}/{@code dlf.*} aliases -&gt; Jindo {@code fs.oss.*}
     *       (ported legacy {@code OSSProperties.initializeHadoopStorageConfig});</li>
     *   <li>the original {@code paimon.s3./s3a./fs.s3./fs.oss.} re-key + raw {@code fs./dfs./hadoop.}
     *       passthrough, which run LAST and overlay the canonical translation (last-write-wins =
     *       legacy {@code addResource(getHadoopStorageConfig())} then {@code appendUserHadoopConfig}).</li>
     * </ol>
     */
    private static void applyStorageConfig(Map<String, String> props, BiConsumer<String, String> setter) {
        applyCanonicalS3Config(props, setter);
        applyCanonicalOssConfig(props, setter);
        applyCanonicalCosConfig(props, setter);
        applyCanonicalObsConfig(props, setter);
        props.forEach((key, value) -> {
            for (String prefix : USER_STORAGE_PREFIXES) {
                if (key.startsWith(prefix)) {
                    setter.accept(FS_S3A_PREFIX + key.substring(prefix.length()), value);
                    return; // stop after the first matching prefix (legacy normalizeS3Config)
                }
            }
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                setter.accept(key, value);
            }
        });
    }

    /**
     * Translates the canonical {@code s3.*}/{@code AWS_*} credential aliases into the Hadoop
     * {@code fs.s3a.*} keys the live S3AFileSystem reads. Port of the credential-bearing subset of
     * legacy {@code AbstractS3CompatibleProperties.appendS3HdfsProperties}: it intentionally omits
     * the connection/timeout/path-style keys (not credentials; Hadoop S3A supplies its own
     * defaults — the pre-fix code set none of them either). The {@code SimpleAWSCredentialsProvider}
     * + access/secret/token are emitted only when an access key is present (matches legacy's
     * {@code isNotBlank(accessKey)} guard, so anonymous public buckets stay anonymous).
     */
    private static void applyCanonicalS3Config(Map<String, String> props, BiConsumer<String, String> setter) {
        String ak = firstNonBlank(props, S3_ACCESS_KEY_ALIASES);
        String sk = firstNonBlank(props, S3_SECRET_KEY_ALIASES);
        String endpoint = firstNonBlank(props, S3_ENDPOINT_ALIASES);
        String region = firstNonBlank(props, S3_REGION_ALIASES);
        String token = firstNonBlank(props, S3_SESSION_TOKEN_ALIASES);
        // Only emit S3A config when the user actually configured an S3-style storage key.
        if (ak == null && endpoint == null && region == null) {
            return;
        }
        // Endpoint-from-region (legacy S3Properties.getEndpointFromRegion): a region-only AWS S3 catalog
        // (no explicit endpoint) derives https://s3.<region>.amazonaws.com so the FE FileIO can resolve it.
        if (StringUtils.isBlank(endpoint) && StringUtils.isNotBlank(region)) {
            endpoint = "https://s3." + region + ".amazonaws.com";
        }
        applyS3aBaseConfig(setter, ak, sk, token, endpoint, region,
                firstNonBlankOrDefault(props, S3_DEFAULT_MAX_CONN, S3_MAX_CONN_ALIASES),
                firstNonBlankOrDefault(props, S3_DEFAULT_REQ_TIMEOUT, S3_REQ_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, S3_DEFAULT_CONN_TIMEOUT, S3_CONN_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, DEFAULT_PATH_STYLE, S3_PATH_STYLE_ALIASES));
    }

    /**
     * Port of legacy {@code AbstractS3CompatibleProperties.appendS3HdfsProperties} — the S3A base block that
     * S3/OSS/COS/OBS all inherit via {@code super.initializeHadoopStorageConfig()}. The caller resolves the
     * credentials AND the 4 tuning values from its OWN scheme aliases/defaults (so a pure-{@code oss.*} catalog
     * never re-reads {@code s3.*} keys, and AWS S3 gets its 50/3000/1000 defaults while OSS/COS/OBS get
     * 100/10000/10000); this helper only emits. {@code fs.s3a.endpoint}/{@code endpoint.region} are CONDITIONAL
     * here — legacy emits them unconditionally via {@code Preconditions.checkNotNull}, but the connector has no
     * {@code setRegionIfPossible} throw-guard, so it omits them when blank (matches the existing connector style).
     */
    private static void applyS3aBaseConfig(BiConsumer<String, String> setter, String ak, String sk,
            String token, String endpoint, String region, String maxConnections, String requestTimeoutMs,
            String connectionTimeoutMs, String usePathStyle) {
        setter.accept("fs.s3.impl", S3A_IMPL);
        setter.accept("fs.s3a.impl", S3A_IMPL);
        setter.accept("fs.s3.impl.disable.cache", "true");
        setter.accept("fs.s3a.impl.disable.cache", "true");
        if (StringUtils.isNotBlank(endpoint)) {
            setter.accept("fs.s3a.endpoint", endpoint);
        }
        if (StringUtils.isNotBlank(region)) {
            setter.accept("fs.s3a.endpoint.region", region);
        }
        if (StringUtils.isNotBlank(ak)) {
            setter.accept("fs.s3a.aws.credentials.provider", S3A_SIMPLE_CRED_PROVIDER);
            setter.accept("fs.s3a.access.key", ak);
            setter.accept("fs.s3a.secret.key", nullToEmpty(sk));
            if (StringUtils.isNotBlank(token)) {
                setter.accept("fs.s3a.session.token", token);
            }
        }
        setter.accept("fs.s3a.connection.maximum", maxConnections);
        setter.accept("fs.s3a.connection.request.timeout", requestTimeoutMs);
        setter.accept("fs.s3a.connection.timeout", connectionTimeoutMs);
        setter.accept("fs.s3a.path.style.access", usePathStyle);
    }

    /**
     * Translates the canonical {@code oss.*}/{@code fs.oss.*}/{@code dlf.*} credential aliases into
     * the Jindo {@code fs.oss.*} keys the live OSS FileIO reads. Port of legacy
     * {@code OSSProperties.initializeHadoopStorageConfig} OSS block. Detection keys off OSS-specific
     * aliases only (NOT {@code s3.*}), so a pure-{@code s3.*} catalog does not trigger the Jindo
     * block (it is an S3 catalog, covered by {@link #applyCanonicalS3Config}); a pure-{@code oss.*}
     * catalog triggers this block.
     */
    private static void applyCanonicalOssConfig(Map<String, String> props, BiConsumer<String, String> setter) {
        String ak = firstNonBlank(props, OSS_ACCESS_KEY_ALIASES);
        String sk = firstNonBlank(props, OSS_SECRET_KEY_ALIASES);
        String endpoint = firstNonBlank(props, OSS_ENDPOINT_ALIASES);
        String region = firstNonBlank(props, OSS_REGION_ALIASES);
        String token = firstNonBlank(props, OSS_SESSION_TOKEN_ALIASES);
        if (ak == null && endpoint == null && region == null) {
            return;
        }
        // Endpoint-from-region (legacy OSSProperties.initNormalizeAndCheckProps -> getOssEndpoint): when no
        // explicit oss.endpoint is given, derive oss-<region>[-internal].aliyuncs.com. publicAccess defaults
        // to false (=> -internal), sourced from dlf.access.public/dlf.catalog.accessPublic (the only legacy
        // dlfAccessPublic aliases). This is the SAME derivation the DLF flavor used (its former DLF-local
        // block in buildDlfHiveConf is now removed) and that the legacy HMS+OSS path got via OSSProperties.of().
        if (StringUtils.isBlank(endpoint) && StringUtils.isNotBlank(region)) {
            boolean publicAccess = BooleanUtils.toBoolean(
                    firstNonBlank(props, "dlf.access.public", "dlf.catalog.accessPublic"));
            endpoint = "oss-" + region + (publicAccess ? "" : "-internal") + ".aliyuncs.com";
        }
        // Emit the S3A base too (legacy OSS inherits it via super.appendS3HdfsProperties) for s3://-over-OSS.
        applyS3aBaseConfig(setter, ak, sk, token, endpoint, region,
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_MAX_CONN, OSS_MAX_CONN_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_REQ_TIMEOUT, OSS_REQ_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_CONN_TIMEOUT, OSS_CONN_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, DEFAULT_PATH_STYLE, OSS_PATH_STYLE_ALIASES));
        // Jindo OSS keys (legacy OSSProperties.initializeHadoopStorageConfig).
        setter.accept("fs.oss.impl", JINDO_OSS_IMPL);
        setter.accept("fs.AbstractFileSystem.oss.impl", JINDO_OSS_ABSTRACT_IMPL);
        if (StringUtils.isNotBlank(ak)) {
            setter.accept("fs.oss.accessKeyId", ak);
            setter.accept("fs.oss.accessKeySecret", nullToEmpty(sk));
        }
        if (StringUtils.isNotBlank(token)) {
            setter.accept("fs.oss.securityToken", token);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            setter.accept("fs.oss.endpoint", endpoint);
        }
        if (StringUtils.isNotBlank(region)) {
            setter.accept("fs.oss.region", region);
        }
    }

    /**
     * Translates the canonical {@code cos.*}/{@code s3.*} aliases into the {@code fs.cosn.*} keys the Tencent
     * COS FileIO reads. Port of legacy {@code COSProperties.initializeHadoopStorageConfig}, which emits the S3A
     * base via {@code super} FIRST, then the cosn keys. Detection mirrors legacy {@code COSProperties.guessIsMe}
     * (endpoint/uri PATTERN, not the scheme key), augmented with the {@code cos.*} key signal: fire when any
     * {@code cos.*} key is present OR a resolved endpoint/warehouse value contains {@code myqcloud.com}. The
     * {@code fs.cosn.*} keys are emitted UNCONDITIONALLY (legacy parity — an empty value is written, not absent).
     */
    private static void applyCanonicalCosConfig(Map<String, String> props, BiConsumer<String, String> setter) {
        String endpoint = firstNonBlank(props, COS_ENDPOINT_ALIASES);
        if (!anyKeyStartsWith(props, "cos.")
                && !containsToken(endpoint, "myqcloud.com")
                && !containsToken(props.get(PaimonConnectorProperties.WAREHOUSE), "myqcloud.com")) {
            return;
        }
        String ak = firstNonBlank(props, COS_ACCESS_KEY_ALIASES);
        String sk = firstNonBlank(props, COS_SECRET_KEY_ALIASES);
        String region = firstNonBlank(props, COS_REGION_ALIASES);
        String token = firstNonBlank(props, COS_SESSION_TOKEN_ALIASES);
        applyS3aBaseConfig(setter, ak, sk, token, endpoint, region,
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_MAX_CONN, COS_MAX_CONN_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_REQ_TIMEOUT, COS_REQ_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_CONN_TIMEOUT, COS_CONN_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, DEFAULT_PATH_STYLE, COS_PATH_STYLE_ALIASES));
        setter.accept("fs.cos.impl", S3A_IMPL);
        setter.accept("fs.cosn.impl", S3A_IMPL);
        setter.accept("fs.cosn.bucket.region", nullToEmpty(region));
        setter.accept("fs.cosn.userinfo.secretId", nullToEmpty(ak));
        setter.accept("fs.cosn.userinfo.secretKey", nullToEmpty(sk));
    }

    /**
     * Translates the canonical {@code obs.*}/{@code s3.*} aliases into the {@code fs.obs.*} keys the Huawei OBS
     * FileIO reads. Port of legacy {@code OBSProperties.initializeHadoopStorageConfig}: S3A base via {@code super}
     * FIRST, then the obs keys, preferring the native {@code OBSFileSystem} when it is on the classpath, else the
     * S3A fallback. Detection mirrors legacy {@code OBSProperties.guessIsMe}: any {@code obs.*} key OR a resolved
     * endpoint/warehouse containing {@code myhuaweicloud.com}. The {@code fs.obs.*} keys are UNCONDITIONAL.
     */
    private static void applyCanonicalObsConfig(Map<String, String> props, BiConsumer<String, String> setter) {
        String endpoint = firstNonBlank(props, OBS_ENDPOINT_ALIASES);
        if (!anyKeyStartsWith(props, "obs.")
                && !containsToken(endpoint, "myhuaweicloud.com")
                && !containsToken(props.get(PaimonConnectorProperties.WAREHOUSE), "myhuaweicloud.com")) {
            return;
        }
        String ak = firstNonBlank(props, OBS_ACCESS_KEY_ALIASES);
        String sk = firstNonBlank(props, OBS_SECRET_KEY_ALIASES);
        String region = firstNonBlank(props, OBS_REGION_ALIASES);
        String token = firstNonBlank(props, OBS_SESSION_TOKEN_ALIASES);
        applyS3aBaseConfig(setter, ak, sk, token, endpoint, region,
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_MAX_CONN, OBS_MAX_CONN_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_REQ_TIMEOUT, OBS_REQ_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, OBJ_STORE_DEFAULT_CONN_TIMEOUT, OBS_CONN_TIMEOUT_ALIASES),
                firstNonBlankOrDefault(props, DEFAULT_PATH_STYLE, OBS_PATH_STYLE_ALIASES));
        // obs is not s3a-compatible; prefer the native OBSFileSystem when it is on the classpath (legacy
        // OBSProperties.isClassAvailable). The connector's child-first loader delegates this non-plugin class
        // to the host parent, so the answer matches legacy's.
        if (isClassAvailable(OBS_NATIVE_IMPL)) {
            setter.accept("fs.obs.impl", OBS_NATIVE_IMPL);
            setter.accept("fs.AbstractFileSystem.obs.impl", OBS_NATIVE_ABSTRACT_IMPL);
        } else {
            setter.accept("fs.obs.impl", S3A_IMPL);
        }
        setter.accept("fs.obs.access.key", nullToEmpty(ak));
        setter.accept("fs.obs.secret.key", nullToEmpty(sk));
        setter.accept("fs.obs.endpoint", nullToEmpty(endpoint));
    }

    /**
     * Builds the {@link HiveConf} for the {@code hms} flavor, reconstructed from the raw property
     * map. Replaces fe-core {@code HMSBaseProperties.getHiveConf()} minimally: sets all {@code hive.*}
     * keys verbatim, the metastore uri, the present auth keys, the kerberos-conditional metastore
     * SASL/service-principal/auth_to_local keys, the metastore client socket timeout default, then
     * overlays the storage config.
     *
     * <p>NOTE (B1, post-fix I-2): the kerberos-conditional metastore keys legacy
     * {@code HMSBaseProperties.initHadoopAuthenticator}/{@code checkAndInit} sets ARE now handled
     * here — {@code hive.metastore.sasl.enabled=true} + {@code hadoop.security.authentication=kerberos}
     * (when the auth type is kerberos), the metastore SERVICE principal
     * {@code hive.metastore.kerberos.principal} (sourced from {@code hive.metastore.service.principal}
     * or {@code hive.metastore.kerberos.principal}), and {@code hadoop.security.auth_to_local}.
     * An external hive-site.xml FILE ({@code hive.conf.resources}) is loaded FE-side via
     * {@code ConnectorContext.loadHiveConfResources} (legacy {@code CatalogConfigFileUtils}, which the
     * connector cannot import) and passed in to the 2-arg overload as the {@code HiveConf} BASE — see
     * {@link #buildHmsHiveConf(Map, Map)}. The real Kerberos UGI {@code doAs} is injected by the FE via
     * {@code ConnectorContext.executeAuthenticated}; here we only carry the auth keys into the conf
     * (legacy additionally built a {@code HadoopAuthenticator} from them).
     *
     * <p>PURE: depends only on {@code props} (and the pre-resolved file keys in the overload).
     */
    public static HiveConf buildHmsHiveConf(Map<String, String> props) {
        return buildHmsHiveConf(props, java.util.Collections.emptyMap());
    }

    /**
     * As {@link #buildHmsHiveConf(Map)}, but seeds {@code hiveConfResources} (the pre-resolved
     * key/values of an external {@code hive.conf.resources} hive-site.xml, loaded FE-side via
     * {@code ConnectorContext.loadHiveConfResources}) as the {@code HiveConf} BASE, BEFORE the user
     * {@code hive.*} overrides — matching legacy {@code HMSBaseProperties.checkAndInit} precedence
     * (file is base, user {@code hive.*} and the resolved uri win). PURE: depends only on the two maps.
     */
    public static HiveConf buildHmsHiveConf(Map<String, String> props, Map<String, String> hiveConfResources) {
        HiveConf hiveConf = new HiveConf();
        // External hive-site.xml (hive.conf.resources) as the BASE (legacy checkAndInit loads the
        // file first); the user hive.* keys below then correctly OVERRIDE it.
        if (hiveConfResources != null) {
            hiveConfResources.forEach(hiveConf::set);
        }
        // All user-supplied hive.* keys verbatim (legacy initUserHiveConfig).
        props.forEach((k, v) -> {
            if (k.startsWith("hive.")) {
                hiveConf.set(k, v);
            }
        });
        // Metastore uri (legacy checkAndInit: hiveConf.set("hive.metastore.uris", uri)).
        String uri = firstNonBlank(props, PaimonConnectorProperties.HMS_URI);
        if (StringUtils.isNotBlank(uri)) {
            hiveConf.set("hive.metastore.uris", uri);
        }
        // Auth keys present in props (legacy HMSBaseProperties @ConnectorProperty fields). The real
        // UGI.doAs() is applied by ConnectorContext.executeAuthenticated; these keys just describe it.
        copyIfPresent(props, hiveConf, "hive.metastore.authentication.type");
        copyIfPresent(props, hiveConf, "hive.metastore.client.principal");
        copyIfPresent(props, hiveConf, "hive.metastore.client.keytab");
        copyIfPresent(props, hiveConf, "hadoop.security.authentication");
        copyIfPresent(props, hiveConf, "hadoop.kerberos.principal");
        copyIfPresent(props, hiveConf, "hadoop.kerberos.keytab");

        // Metastore client socket timeout default (legacy checkAndInit lines 204-208): when the user
        // did not override it, default to Config.hive_metastore_client_timeout_second (=10s). The
        // ConfVar key string is "hive.metastore.client.socket.timeout"; legacy expresses the value in
        // seconds via HiveConf.setVar(..., METASTORE_CLIENT_SOCKET_TIMEOUT, "10").
        if (StringUtils.isBlank(props.get("hive.metastore.client.socket.timeout"))) {
            hiveConf.set("hive.metastore.client.socket.timeout", "10");
        }

        // Overlay the storage config (legacy buildHiveConfiguration + appendUserHadoopConfig).
        applyStorageConfig(props, hiveConf::set);

        // Kerberos-conditional metastore keys, ported faithfully from HMSBaseProperties.initHadoopAuthenticator
        // (lines 152-185). This block runs LAST, AFTER the storage overlay, mirroring legacy's
        // initHadoopAuthenticator-last ordering: the raw hadoop.* passthrough in applyStorageConfig would
        // otherwise re-copy a user-supplied literal hadoop.security.authentication (e.g. a kerberized-HMS +
        // simple-HDFS catalog) and CLOBBER the forced "kerberos" back to "simple", leaving sasl.enabled=true
        // with auth=simple — an inconsistent HiveConf that breaks the live GSSAPI handshake.
        //   - the SERVICE principal hive.metastore.kerberos.principal is set UNCONDITIONALLY when a
        //     service principal is supplied (legacy field hiveMetastoreServicePrincipal, sourced from
        //     "hive.metastore.service.principal" OR "hive.metastore.kerberos.principal"); not gated on
        //     the auth type (legacy lines 153-155).
        String servicePrincipal = firstNonBlank(props,
                "hive.metastore.service.principal", "hive.metastore.kerberos.principal");
        if (StringUtils.isNotBlank(servicePrincipal)) {
            hiveConf.set("hive.metastore.kerberos.principal", servicePrincipal);
        }
        //   - hadoop.security.auth_to_local is set UNCONDITIONALLY when present (legacy lines 156-159).
        copyIfPresent(props, hiveConf, "hadoop.security.auth_to_local");
        //   - sasl.enabled + hadoop.security.authentication=kerberos are set when the HMS auth type is
        //     kerberos (legacy lines 160-167), OR — when the HMS auth type is NOT simple — when the
        //     HDFS auth type (hadoop.security.authentication) is kerberos (legacy fallback lines
        //     174-182). Matches legacy's branching exactly.
        String hmsAuthType = props.getOrDefault("hive.metastore.authentication.type", "none");
        String hdfsAuthType = props.get("hadoop.security.authentication");
        boolean hmsKerberos = "kerberos".equalsIgnoreCase(hmsAuthType);
        boolean hdfsFallbackKerberos = !"simple".equalsIgnoreCase(hmsAuthType)
                && !hmsKerberos
                && "kerberos".equalsIgnoreCase(hdfsAuthType);
        if (hmsKerberos || hdfsFallbackKerberos) {
            hiveConf.set("hadoop.security.authentication", "kerberos");
            hiveConf.set("hive.metastore.sasl.enabled", "true");
        }

        // Username (legacy HMSBaseProperties @ConnectorProperty(names={"hive.metastore.username",
        // "hadoop.username"}) -> hiveConf.set(HADOOP_USER_NAME="hadoop.username", hmsUserName)): resolve the
        // alias to hadoop.username, also after the storage overlay so the legacy @ConnectorProperty priority
        // is authoritative (same raw hadoop.* passthrough clobber reason as the kerberos block). The bare
        // pre-fix copyIfPresent also missed a user who set ONLY hive.metastore.username (it stayed an inert
        // verbatim hive.* key).
        String hmsUserName = firstNonBlank(props, "hive.metastore.username", "hadoop.username");
        if (StringUtils.isNotBlank(hmsUserName)) {
            hiveConf.set("hadoop.username", hmsUserName);
        }
        return hiveConf;
    }

    /**
     * Builds the {@link HiveConf} for the {@code dlf} flavor (Aliyun DLF adapted onto paimon's
     * "hive" metastore via the ProxyMetaStoreClient). Replaces fe-core
     * {@code PaimonAliyunDLFMetaStoreProperties.buildHiveConf()} + {@code AliyunDLFBaseProperties
     * .checkAndInit()} minimally.
     *
     * <p>reference: com.aliyun.datalake.metastore.common.DataLakeConfig.CATALOG_* (values verified
     * via javap) — the 8 keys set below are the literal values of those constants:
     * <pre>
     *   CATALOG_ACCESS_KEY_ID     = "dlf.catalog.accessKeyId"
     *   CATALOG_ACCESS_KEY_SECRET = "dlf.catalog.accessKeySecret"
     *   CATALOG_ENDPOINT          = "dlf.catalog.endpoint"
     *   CATALOG_REGION_ID         = "dlf.catalog.region"
     *   CATALOG_SECURITY_TOKEN    = "dlf.catalog.securityToken"
     *   CATALOG_USER_ID           = "dlf.catalog.uid"
     *   CATALOG_ID                = "dlf.catalog.id"
     *   CATALOG_PROXY_MODE        = "dlf.catalog.proxyMode"
     * </pre>
     *
     * <p>PURE: depends only on {@code props}.
     */
    public static HiveConf buildDlfHiveConf(Map<String, String> props) {
        String accessKey = firstNonBlank(props, PaimonConnectorProperties.DLF_ACCESS_KEY);
        String secretKey = firstNonBlank(props, PaimonConnectorProperties.DLF_SECRET_KEY);
        String sessionToken = firstNonBlank(props, PaimonConnectorProperties.DLF_SESSION_TOKEN);
        String region = props.get(PaimonConnectorProperties.DLF_REGION);
        String endpoint = firstNonBlank(props, PaimonConnectorProperties.DLF_ENDPOINT);
        String uid = firstNonBlank(props, PaimonConnectorProperties.DLF_UID);
        String catalogId = firstNonBlank(props, PaimonConnectorProperties.DLF_CATALOG_ID);
        String accessPublic = props.getOrDefault(
                PaimonConnectorProperties.DLF_ACCESS_PUBLIC[0],
                props.getOrDefault(PaimonConnectorProperties.DLF_ACCESS_PUBLIC[1],
                        PaimonConnectorProperties.DLF_ACCESS_PUBLIC_DEFAULT));
        String proxyMode = props.getOrDefault(
                PaimonConnectorProperties.DLF_PROXY_MODE[0],
                props.getOrDefault(PaimonConnectorProperties.DLF_PROXY_MODE[1],
                        PaimonConnectorProperties.DLF_PROXY_MODE_DEFAULT));

        // Endpoint/catalog-id normalization (legacy AliyunDLFBaseProperties.checkAndInit).
        if (StringUtils.isBlank(endpoint) && StringUtils.isNotBlank(region)) {
            endpoint = BooleanUtils.toBoolean(accessPublic)
                    ? "dlf." + region + ".aliyuncs.com"
                    : "dlf-vpc." + region + ".aliyuncs.com";
        }
        if (StringUtils.isBlank(endpoint)) {
            throw new IllegalStateException("dlf.endpoint is required.");
        }
        if (StringUtils.isBlank(catalogId)) {
            catalogId = uid;
        }

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("dlf.catalog.accessKeyId", nullToEmpty(accessKey));
        hiveConf.set("dlf.catalog.accessKeySecret", nullToEmpty(secretKey));
        hiveConf.set("dlf.catalog.endpoint", endpoint);
        hiveConf.set("dlf.catalog.region", nullToEmpty(region));
        hiveConf.set("dlf.catalog.securityToken", nullToEmpty(sessionToken));
        hiveConf.set("dlf.catalog.uid", nullToEmpty(uid));
        hiveConf.set("dlf.catalog.id", nullToEmpty(catalogId));
        hiveConf.set("dlf.catalog.proxyMode", proxyMode);
        // Overlay the OSS storage config (legacy ossProps.getHadoopStorageConfig + appendUserHadoopConfig).
        // The OSS endpoint-from-region derivation now lives in applyCanonicalOssConfig (shared with the
        // filesystem/hms flavors, using the same dlf.access.public source), so no DLF-local derivation is
        // needed here.
        applyStorageConfig(props, hiveConf::set);
        return hiveConf;
    }

    /**
     * Fails fast unless an OSS / OSS_HDFS object-store storage key is present, mirroring legacy
     * {@code PaimonAliyunDLFMetaStoreProperties.initializeCatalog}, which selected a
     * {@code StorageProperties} of {@code Type.OSS || Type.OSS_HDFS} (NOT a generic S3 backend) and
     * otherwise threw {@code "Paimon DLF metastore requires OSS storage properties."}. We cannot
     * import the fe-core {@code StorageProperties} enum, so we key off the OSS-only storage property
     * prefixes the user passes for a DLF catalog ({@code oss.} / {@code fs.oss.} / {@code paimon.fs.oss.}).
     * A misconfigured S3-only DLF catalog (only {@code s3.*}/{@code fs.s3a.*}/{@code paimon.s3.*} keys)
     * is therefore rejected, matching legacy.
     *
     * <p>PURE: depends only on {@code props}. Throws {@link IllegalStateException} with the exact
     * legacy message.
     */
    public static void requireOssStorageForDlf(Map<String, String> props) {
        for (String key : props.keySet()) {
            if (key.startsWith("oss.") || key.startsWith("fs.oss.") || key.startsWith("paimon.fs.oss.")) {
                return;
            }
        }
        throw new IllegalStateException("Paimon DLF metastore requires OSS storage properties.");
    }

    private static void copyIfPresent(Map<String, String> props, HiveConf hiveConf, String key) {
        String value = props.get(key);
        if (StringUtils.isNotBlank(value)) {
            hiveConf.set(key, value);
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /** As {@link #firstNonBlank}, but returns {@code defaultValue} (not null) when no key is set. */
    private static String firstNonBlankOrDefault(Map<String, String> props, String defaultValue, String... keys) {
        String value = firstNonBlank(props, keys);
        return value != null ? value : defaultValue;
    }

    private static boolean anyKeyStartsWith(Map<String, String> props, String prefix) {
        for (String key : props.keySet()) {
            if (key != null && key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsToken(String value, String token) {
        return value != null && value.contains(token);
    }

    /** Whether {@code className} is loadable (legacy {@code OBSProperties.isClassAvailable} parity). */
    private static boolean isClassAvailable(String className) {
        try {
            Class.forName(className, false, PaimonCatalogFactory.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
