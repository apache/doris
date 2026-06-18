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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.FileSystemCatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Pure, testable assembly core for the Paimon connector flavor switch — the paimon-SDK-specific bits
 * that stay in the connector after the P2-T03 cutover.
 *
 * <p>Mirrors the role of {@code MCConnectorClientFactory}: a stateless static holder that
 * {@link #buildCatalogOptions(Map) builds} the Paimon {@link Options} for a flavor. The option-key
 * logic ports the legacy fe-core {@code AbstractPaimonProperties} + each {@code Paimon*MetaStoreProperties}
 * Options assembly. {@code buildCatalogOptions} is PURE — it reads only the supplied props (no env, no
 * clock) — which is what makes it unit-testable offline.
 *
 * <p>It also holds two PURE Hadoop config helpers: {@link #buildHadoopConfiguration} (the filesystem/jdbc
 * storage {@code Configuration} from the pre-computed canonical object-store config) and
 * {@link #assembleHiveConf} (layers the shared-parser HiveConf overrides over an optional hive-site.xml
 * base for the hms/dlf flavors). The {@code storageHadoopConfig} arg is assembled by
 * {@code PaimonConnector} from {@code ConnectorContext.getStorageProperties()} (fe-filesystem's
 * {@code toHadoopProperties().toHadoopConfigurationMap()}), so the helpers stay pure (Maps in, conf out)
 * and unit-testable offline; only the {@code CatalogFactory.createCatalog} call in
 * {@code PaimonConnector} needs a live metastore.
 *
 * <p>The metastore CONNECTION facts (validate rules, HMS/DLF HiveConf key sets, JDBC driver-url
 * resolution, alias arrays) were moved to the shared {@code fe-connector-metastore-spi}
 * ({@code MetaStoreProviders.bind} -&gt; {@code HmsMetaStoreProperties.toHiveConfOverrides()} /
 * {@code DlfMetaStoreProperties.toDlfCatalogConf()}; {@code JdbcDriverSupport.resolveDriverUrl}) — see P2-T03.
 */
public final class PaimonCatalogFactory {

    private static final String USER_PROPERTY_PREFIX = "paimon.";
    private static final String PAIMON_REST_PROPERTY_PREFIX = "paimon.rest.";
    private static final String JDBC_PREFIX = "jdbc.";

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
     * Builds a minimal Hadoop {@link Configuration} for the storage layer (HDFS / S3 / OSS), from the
     * raw property map plus the pre-computed object-store storage config:
     *
     * <ul>
     *   <li>{@code storageHadoopConfig} carries the canonical object-store translation
     *       ({@code s3.*}/{@code oss.*}/{@code cos.*}/{@code obs.*}/{@code AWS_*} -&gt; {@code fs.s3a.*} /
     *       Jindo {@code fs.oss.*} / etc.), computed upstream by the connector from
     *       {@code ConnectorContext.getStorageProperties()} via fe-filesystem's
     *       {@code toHadoopProperties().toHadoopConfigurationMap()} (P1-T03; replaces the legacy
     *       {@code StorageProperties.buildObjectStorageHadoopConfig(props)} call);</li>
     *   <li>{@code paimon.s3.*} / {@code paimon.s3a.*} / {@code paimon.fs.s3.*} / {@code paimon.fs.oss.*}
     *       are normalized to the Hadoop S3A prefix {@code fs.s3a.} (strip the matched prefix,
     *       re-key as {@code fs.s3a.} + remainder), matching legacy {@code normalizeS3Config};</li>
     *   <li>raw {@code fs.*} / {@code dfs.*} / {@code hadoop.*} keys are copied verbatim (these are
     *       already Hadoop-recognized keys the user passed through). HDFS contributes via this
     *       passthrough only — it is absent from {@code storageHadoopConfig} (fe-filesystem binds
     *       object stores only), matching legacy.</li>
     * </ul>
     *
     * <p>PURE: depends only on {@code props} and {@code storageHadoopConfig}.
     */
    public static Configuration buildHadoopConfiguration(Map<String, String> props,
            Map<String, String> storageHadoopConfig) {
        Configuration conf = new Configuration();
        // Pin the Configuration's classloader to the plugin loader (FIX-PAIMON-HADOOP-CLASSLOADER).
        // Hadoop resolves filesystem impls via Configuration.getClass("fs.<scheme>.impl", ...), which
        // loads through Configuration.classLoader (defaults to the thread-context CL = parent 'app').
        // With hadoop-aws (S3AFileSystem) bundled child-first, that default would still resolve
        // S3AFileSystem from the parent and fail the cast to the child-loaded FileSystem. Resolving
        // through the plugin loader keeps the whole FS class graph in one loader.
        conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader());
        applyStorageConfig(storageHadoopConfig, props, conf::set);
        return conf;
    }

    /**
     * Applies the storage config via the given setter. Shared by {@link #buildHadoopConfiguration} and
     * the HiveConf builders (which overlay the same storage config onto the HiveConf, mirroring legacy
     * {@code appendUserHadoopConfig(hiveConf)} + {@code ossProps.getHadoopStorageConfig()}). Two steps,
     * in legacy precedence order:
     *
     * <ol>
     *   <li>the pre-computed {@code storageHadoopConfig} (canonical object-store translation, produced
     *       upstream from {@code ConnectorContext.getStorageProperties()} via fe-filesystem's
     *       {@code toHadoopConfigurationMap()}; replaces the legacy
     *       {@code StorageProperties.buildObjectStorageHadoopConfig(props)} call);</li>
     *   <li>the original {@code paimon.s3./s3a./fs.s3./fs.oss.} re-key + raw {@code fs./dfs./hadoop.}
     *       passthrough, which run LAST and overlay the canonical translation (last-write-wins =
     *       legacy {@code addResource(getHadoopStorageConfig())} then {@code appendUserHadoopConfig}).</li>
     * </ol>
     */
    private static void applyStorageConfig(Map<String, String> storageHadoopConfig,
            Map<String, String> props, BiConsumer<String, String> setter) {
        // Pre-computed canonical object-store config (fs.s3a.*/fs.oss.*/fs.cosn.*/fs.obs.*), assembled by
        // PaimonConnector from ctx.getStorageProperties().toHadoopProperties().toHadoopConfigurationMap()
        // (fe-filesystem is now the single source of truth; P1-T03). HDFS is absent here (fe-filesystem
        // binds object stores only) and reaches the conf via the raw fs./dfs./hadoop. passthrough below,
        // matching legacy (applyStorageConfig never had an HDFS canonical block).
        storageHadoopConfig.forEach(setter);
        // Connector-specific (NOT in fe-filesystem): paimon.* prefix re-key + raw fs./dfs./hadoop. passthrough,
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
     * Assembles a {@link HiveConf} for the {@code hms}/{@code dlf} flavors from a neutral key map.
     * Seeds the optional {@code base} (e.g. an external {@code hive.conf.resources} hive-site.xml,
     * resolved FE-side via {@code ConnectorContext.loadHiveConfResources}) FIRST, then applies the
     * shared-parser {@code overrides} on top (last-write-wins), so the connection/user keys correctly
     * OVERRIDE the file — matching the legacy {@code HMSBaseProperties.checkAndInit} precedence (file
     * base, then overrides).
     *
     * <p>The {@code overrides} are produced by the shared metastore parsers
     * ({@code HmsMetaStoreProperties.toHiveConfOverrides()} — uri + verbatim {@code hive.*} + auth keys
     * + socket-timeout default + storage overlay + kerberos block last; or
     * {@code DlfMetaStoreProperties.toDlfCatalogConf()} — the 8 {@code dlf.catalog.*} keys + OSS storage
     * overlay), which own the ordering-sensitive logic (storage overlay BEFORE the kerberos block). This
     * method only layers the file base under those facts. The real Kerberos UGI {@code doAs} is injected
     * by the FE via {@code ConnectorContext.executeAuthenticated}; the keys here only describe it.
     *
     * <p>PURE: a function of the two maps (plus {@link HiveConf}'s own classpath defaults).
     *
     * @param base      optional base keys (e.g. a resolved hive-site.xml); may be {@code null}/empty
     * @param overrides the connection-fact overrides; never {@code null}
     */
    public static HiveConf assembleHiveConf(Map<String, String> base, Map<String, String> overrides) {
        HiveConf hiveConf = new HiveConf();
        if (base != null) {
            base.forEach(hiveConf::set);
        }
        overrides.forEach(hiveConf::set);
        return hiveConf;
    }

}
