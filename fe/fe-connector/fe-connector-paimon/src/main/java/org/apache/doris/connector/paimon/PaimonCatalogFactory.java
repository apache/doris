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

import org.apache.doris.property.storage.StorageProperties;

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
        // Canonical object-store alias -> fs.s3a.*/fs.oss.*/fs.cosn.*/fs.obs.* translation, delegated to the shared
        // fe-property module (replaces the hand-ported applyCanonicalS3/Minio/Oss/Cos/Obs blocks that diverged and
        // caused the MinIO bug). It detects each object-store family from the raw props and emits the same Hadoop
        // keys legacy did; HDFS contributes nothing here (handled by the raw passthrough below), matching legacy
        // (applyStorageConfig never had an HDFS canonical block).
        StorageProperties.buildObjectStorageHadoopConfig(props).forEach(setter);
        // Connector-specific (NOT in fe-property): paimon.* prefix re-key + raw fs./dfs./hadoop. passthrough,
        // run LAST so explicit fs.s3a.* keys overlay the canonical translation (last-write-wins).
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
        // The OSS endpoint-from-region derivation now lives in the shared fe-property OSSProperties (used by the
        // filesystem/hms flavors too, with the same dlf.access.public source), so no DLF-local OSS derivation is
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

}
