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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.metastore.paimon.jdbc.PaimonJdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.ConnectorValidationContext;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.AuthenticationConfig;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegedCatalog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Paimon connector implementation managing the lifecycle of a
 * {@link org.apache.paimon.catalog.Catalog} instance.
 *
 * <p>The Paimon Catalog is lazily created on first metadata access.
 * It supports multiple catalog backends (filesystem, HMS, REST, JDBC)
 * determined by the {@code paimon.catalog.type} property. The per-flavor option
 * assembly lives in the pure {@link PaimonCatalogFactory}; this class drives the
 * live catalog creation.
 *
 * <p>B1 lands all five flavors live. filesystem/jdbc create a {@link CatalogContext} carrying a
 * minimal Hadoop {@link Configuration} (HDFS/S3 storage), rest is Options-only, and hms carries a
 * {@link HiveConf} (metastore=hive). All create calls are wrapped in
 * {@code ConnectorContext.executeAuthenticated} so the FE-injected Kerberos UGI (if any) applies;
 * the default is a no-op. The {@code Configuration}/{@code HiveConf} are assembled by the pure
 * builders in {@link PaimonCatalogFactory}.
 */
public class PaimonConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(PaimonConnector.class);

    /**
     * Caches {@link ClassLoader}s keyed by resolved driver URL so a given JDBC driver jar is
     * loaded at most once across catalogs, and tracks the (url#class) keys already registered with
     * the {@link java.sql.DriverManager}. Ported verbatim from the legacy
     * {@code PaimonJdbcMetaStoreProperties}.
     */
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    // FIX-4 (CI 973411): the legacy paimon table cache (meta.cache.paimon.table.*) governed BOTH the data
    // snapshot AND the schema; the SPI cutover dropped it (marked the keys dead). meta.cache.paimon.table.ttl-second
    // is restored here: it sizes the latest-snapshot cache below (data) AND, via schemaCacheTtlSecondOverride(),
    // the generic schema cache (schema). enable/capacity remain best-effort (capacity uses the legacy default).
    static final String TABLE_CACHE_TTL_SECOND = "meta.cache.paimon.table.ttl-second";
    // enable/capacity are not wired on the plugin path (see PaimonConnectorProvider), but their values are
    // still validated at CREATE/ALTER for legacy parity (reject non-boolean / out-of-range garbage).
    static final String TABLE_CACHE_ENABLE = "meta.cache.paimon.table.enable";
    static final String TABLE_CACHE_CAPACITY = "meta.cache.paimon.table.capacity";
    // Legacy default = Config.external_cache_expire_time_seconds_after_access (24h); the connector is isolated
    // from fe-core Config, so the legacy default is mirrored here (an explicit ttl-second always overrides it).
    static final long DEFAULT_TABLE_CACHE_TTL_SECOND = 86400L;
    // Legacy default = Config.max_external_table_cache_num.
    static final int DEFAULT_TABLE_CACHE_CAPACITY = 1000;

    // Catalog property key gating the plugin-side Kerberos authenticator (value matches AuthType.KERBEROS).
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    private final PaimonCatalogProperties catalogProps;
    private final ConnectorContext context;
    private volatile Catalog catalog;

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth; see TcclPinningConnectorContext).
    // null for a non-Kerberos catalog. Its doAs acts on the PLUGIN's UserGroupInformation copy — the one the
    // plugin's HDFS FileSystem reads — not the app-loader copy the FE-injected authenticator logs in.
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    // FIX-4: per-catalog (long-lived) cache of each table's latest snapshot id, sized by
    // meta.cache.paimon.table.ttl-second (<=0 disables -> always live, the no-cache catalog). getMetadata()
    // returns a fresh metadata per query, so this lives on the connector and is injected into the metadata so
    // beginQuerySnapshot pins a stable id across queries. Cleared wholesale on REFRESH CATALOG (connector rebuilt).
    private final PaimonLatestSnapshotCache latestSnapshotCache;
    private final CatalogMetaCache metaCache = new CatalogMetaCache();

    // FIX-B-MC2: connector-level (per-catalog, long-lived) second-level memo for the time-travel
    // schema-at-snapshot read. getMetadata() returns a FRESH metadata per query, so this must live on the
    // connector (not the metadata) to give the cross-query hit the legacy PaimonExternalMetaCache provided.
    // Cleared wholesale on REFRESH CATALOG (the connector is rebuilt). See PaimonSchemaAtMemo.
    private final PaimonSchemaAtMemo schemaAtMemo =
            new PaimonSchemaAtMemo(metaCache, PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);

    // PERF-06: cross-query DERIVED partition-view cache ("cache A", the generic ConnectorMetadataCache from
    // fe-connector-cache), layered ABOVE the raw remote catalog.listPartitions call (PaimonCatalogOps#listPartitions):
    // it memoizes the BUILT List<ConnectorPartitionInfo> (display-name rendering + null-sentinel normalization,
    // see PaimonConnectorMetadata#collectPartitions) keyed by (db, table, snapshotId, schemaId), so a repeated
    // query on a partitioned table skips the derived rebuild AND the remote catalog round-trip. ONE typed field
    // (unlike iceberg's two): paimon does not override getMvccPartitionView, so the generic MTMV model falls
    // back to its default listPartitions/LIST/timestamp path for paimon -- all three partition-enumeration
    // hooks (listPartitions/Names/Values) share it via PaimonConnectorMetadata#cachedPartitions. Unlike
    // iceberg, paimon has NO session=user / per-user credential-isolation
    // cache-disabling convention (a paimon catalog authenticates at catalog-creation time -- Kerberos UGI /
    // HMS principal -- not per-query session identity), so this is constructed unconditionally: never null on
    // a live connector (only PaimonConnectorMetadata's convenience/test constructors pass null).
    private final ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCache;

    // #65955: the catalog-level paimon.table-option.* defaults, extracted (and re-validated) once per
    // connector and overlaid on every table load by CatalogBackedPaimonCatalogOps.getTable.
    private final Map<String, String> tableOptions;

    public PaimonConnector(Map<String, String> properties, ConnectorContext context) {
        // Construct-time BIND, not validation: of() carries only what the connector cannot run without,
        // so a catalog created before a rule existed still comes back after an FE restart. The
        // CREATE/ALTER-only rules live behind checkCreateTimeOnlyRules(), which only the provider calls.
        this.catalogProps = PaimonCatalogProperties.of(properties);
        this.tableOptions = PaimonTableOptions.extractCompatible(properties);
        // Wrap the FE-injected context so every executeAuthenticated pins the TCCL to the plugin loader (the
        // paimon plugin bundles paimon-core + hadoop child-first) and, for a Kerberos catalog, runs the op
        // under a plugin-side UGI doAs (pluginAuthenticator): the plugin's FileSystem reads the plugin's own
        // UserGroupInformation copy, which the FE-injected app-side authenticator never logs in — so without
        // this a DDL/read against secured HDFS negotiates SIMPLE auth. See TcclPinningConnectorContext.
        this.context = new TcclPinningConnectorContext(context, getClass().getClassLoader(),
                this::pluginAuthenticator);
        this.latestSnapshotCache =
                new PaimonLatestSnapshotCache(
                        metaCache, resolveTableCacheTtlSecond(properties), DEFAULT_TABLE_CACHE_CAPACITY);
        // Reads its own meta.cache.paimon.partition_view.(enable|ttl-second|capacity) from the catalog
        // properties via the framework's CacheSpec (default ON / 24h / 1000).
        this.partitionViewCache = new ConnectorMetadataCache<>(
                metaCache, "paimon.partition-view", "paimon", "partition_view", properties);
    }

    /**
     * Lazily builds and memoizes the plugin-side Kerberos authenticator that {@link TcclPinningConnectorContext}
     * runs each op under, so remote HDFS access uses the PLUGIN's own {@code UserGroupInformation} copy (the one
     * the plugin's {@code FileSystem} reads). Returns {@code null} for a non-Kerberos catalog so the FE-injected
     * auth path is preserved unchanged. Construction is cheap — the keytab login is lazy in {@code getUGI()} on
     * the first {@code doAs}.
     */
    private HadoopAuthenticator pluginAuthenticator() {
        if (!pluginAuthComputed) {
            synchronized (this) {
                if (!pluginAuthComputed) {
                    pluginAuth = buildPluginAuthenticator(catalogProps.getRaw(), buildStorageHadoopConfig());
                    pluginAuthComputed = true;
                }
            }
        }
        return pluginAuth;
    }

    /**
     * Resolves only the storage-side Kerberos authenticator used by FileIO. HMS authentication is intentionally
     * resolved separately by {@link #buildHmsAuthenticator} and applied at the client-pool boundary.
     */
    static HadoopAuthenticator buildPluginAuthenticator(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        if ("kerberos".equalsIgnoreCase(properties.get(HADOOP_SECURITY_AUTHENTICATION))) {
            return HadoopAuthenticator.getHadoopAuthenticator(
                    PaimonCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig));
        }
        return null;
    }

    static HadoopAuthenticator buildHmsAuthenticator(Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(PaimonConnector.class.getClassLoader());
            if (!PaimonCatalogProperties.HMS.equals(PaimonCatalogProperties.of(properties).getFlavor())) {
                return null;
            }
            AbstractHmsMetaStoreProperties hms = (AbstractHmsMetaStoreProperties) MetaStoreProviders.bind(
                    properties, storageHadoopConfig);
            Optional<KerberosAuthSpec> spec = hms.kerberos();
            if (spec.isPresent() && spec.get().hasCredentials()) {
                Configuration conf = PaimonCatalogFactory.assembleHiveConf(
                        hms.getConfResources(), hms.toHiveConfOverrides(""));
                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hive.metastore.sasl.enabled", "true");
                return HadoopAuthenticator.getHadoopAuthenticator(
                        new KerberosAuthenticationConfig(
                                spec.get().getPrincipal(), spec.get().getKeytab(), conf));
            }
            if (hms.getAuthType() == AuthType.KERBEROS) {
                return null;
            }
            Configuration conf = PaimonCatalogFactory.assembleHiveConf(
                    hms.getConfResources(), hms.toHiveConfOverrides(""));
            return HadoopAuthenticator.getHadoopAuthenticator(
                    AuthenticationConfig.getSimpleAuthenticationConfig(conf));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Parses {@code meta.cache.paimon.table.ttl-second} (legacy default 24h; {@code <= 0} disables caching ->
     * the no-cache catalog reads live). An unparseable value falls back to the default rather than failing
     * catalog creation (validation of the knob is best-effort; the legacy CacheSpec check was dropped at cutover).
     */
    private static long resolveTableCacheTtlSecond(Map<String, String> properties) {
        String raw = properties.get(TABLE_CACHE_TTL_SECOND);
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}={}, falling back to default {}s",
                    TABLE_CACHE_TTL_SECOND, raw, DEFAULT_TABLE_CACHE_TTL_SECOND);
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new PaimonConnectorMetadata(
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog(), tableOptions),
                catalogProps, context, schemaAtMemo, latestSnapshotCache, partitionViewCache);
    }

    /**
     * True for a handle this connector produced (a {@link PaimonTableHandle}). Tested against this connector's
     * OWN in-loader type, so a gateway connector that embeds this one as a sibling can route a foreign paimon
     * handle here without casting it across the plugin classloader split. Returns false for any other
     * connector's handle, so the gateway keeps looking.
     *
     * <p>The default is {@code false}, which for a sibling means every one of the gateway's guards silently
     * fails open and the first cast throws a ClassCastException instead — so this is required of any connector
     * used as a sibling, not an optimization. Same implementation as the iceberg and hudi siblings behind the
     * hms gateway.
     */
    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof PaimonTableHandle;
    }

    @Override
    public void invalidateTable(String dbName, String tableName) {
        // REFRESH TABLE (and, via the generic PluginDrivenExternalCatalog DDL hook, a Doris-issued
        // DROP/CREATE of this name): drop the cached latest snapshot id so the next read goes live. Keyed by
        // the REMOTE db/table names, matching the key beginQuerySnapshot stores (PaimonTableHandle carries
        // remote names).
        // Also drop the time-travel schema memo for this table: unlike the snapshot cache it is keyed by
        // (db,table,sysTable,branch,schemaId) and would otherwise serve a stale schema-at-snapshot after a
        // drop+recreate that reuses a schemaId (the memo's narrow write-once-per-schemaId assumption breaks).
        // PERF-06: also drop this table's cached derived partition-view entries (every snapshotId cached for
        // it), so the next listPartitions re-enumerates live.
        metaCache.invalidateTable(dbName, tableName);
    }

    /**
     * REFRESH DATABASE hook (also reached by a Doris-issued {@code DROP DATABASE} via the generic
     * {@code PluginDrivenExternalCatalog} dropDb hook, and by the hive gateway's
     * {@code forEachBuiltSibling} for a paimon sibling): drop BOTH connector-owned caches for EVERY table
     * in one database — the latest-snapshot pin and the time-travel schema memo — so the next query
     * re-reads live. Db-scoped analogue of {@link #invalidateTable}; the name is the REMOTE db name.
     * Without this override paimon inherited the SPI no-op default, so REFRESH DATABASE and DROP DATABASE
     * (incl. its FORCE table cascade, which bypasses per-table invalidateTable) left both caches stale up
     * to the TTL.
     */
    @Override
    public void invalidateDb(String dbName) {
        metaCache.invalidateDatabase(dbName);
    }

    @Override
    public void invalidateAll() {
        metaCache.invalidateCatalog();
    }

    @Override
    public OptionalLong schemaCacheTtlSecondOverride() {
        // Restore the legacy single-knob semantics: meta.cache.paimon.table.ttl-second also governs the schema
        // cache (the SPI routes paimon schema to the generic schema cache keyed by schema.cache.ttl-second). So
        // the no-cache catalog (ttl-second=0) serves FRESH schema. Absent -> no override (engine default TTL).
        String raw = catalogProps.getRaw().get(TABLE_CACHE_TTL_SECOND);
        if (raw == null || raw.trim().isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        // FIX-B-R2-be: inject the SAME per-catalog schemaAtMemo getMetadata uses, so the schema-evolution
        // dict's per-schema-id reads are memoized across scans (and shared with the B-MC2 time-travel path).
        return new PaimonScanPlanProvider(catalogProps,
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog(), tableOptions),
                context, schemaAtMemo);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        return new PaimonWritePlanProvider(catalogProps,
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog(), tableOptions),
                context);
    }

    /**
     * Declares the E5 read-path capabilities paimon supports: MVCC snapshot pinning. The B5 fe-core
     * MvccTable wiring keys off this to call {@link PaimonConnectorMetadata#beginQuerySnapshot} /
     * {@code resolveTimeTravel}.
     * Write support is exposed through {@link #getWritePlanProvider()} rather than a capability flag.
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                // Paimon exposes per-partition stats (record/size/file count) via listPartitions,
                // so SHOW PARTITIONS renders the legacy 5-column result (D-045).
                ConnectorCapability.SUPPORTS_PARTITION_STATS,
                // Paimon tables are queryable via the generic SQL-driven ExternalAnalysisTask FULL path, so
                // they opt into background per-column auto-analyze (paimon was never wired into the legacy
                // instanceof-based whitelist; this is the parity-neutral mechanism wiring it in). Paimon is
                // already served by its connector plugin, so this is not inert: paimon background
                // auto-analyze activates on merge (parity-safe — manual ANALYZE already uses
                // the same doFull SQL path). NOT SUPPORTS_TOPN_LAZY_MATERIALIZE: paimon was never eligible for
                // Top-N lazy materialization.
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE,
                // Paimon's table properties (coreOptions incl. path) are user-facing and credential-free, so
                // SHOW CREATE TABLE renders LOCATION + PROPERTIES for paimon. This capability replaces the
                // legacy paimon-only engine-name gate in Env.getDdlStmt (the credential-leak guard now keyed
                // on a capability instead of an engine string). Paimon emits no partition/sort show.* keys, so
                // it renders no PARTITION BY / ORDER BY — byte-faithful with its prior SHOW CREATE output.
                ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL,
                // Paimon owns a relation-scoped scan-option vocabulary (CoreOptions scan.* keys), so it
                // accepts @options(...). fe-core's BindRelation consults this to reject the clause up front
                // for every other table type; the vocabulary itself is validated by PaimonScanParams while
                // resolveTimeTravel(Kind.OPTIONS) turns the options into an immutable pin. Declared
                // connector-wide: it holds for every paimon DATA table. The narrower question of which
                // SYSTEM table can honor the clause is answered per table by
                // PaimonScanPlanProvider.supportsSystemTableOptions.
                ConnectorCapability.SUPPORTS_SCAN_PARAM_OPTIONS);
    }

    /** Test-only: the derived listPartitions view cache (PERF-06). Never null (paimon has no session=user gate). */
    ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCacheForTest() {
        return partitionViewCache;
    }

    private Catalog ensureCatalog() {
        if (catalog == null) {
            synchronized (this) {
                if (catalog == null) {
                    catalog = createCatalog();
                }
            }
        }
        return catalog;
    }

    private Catalog createCatalog() {
        Options options = PaimonCatalogFactory.buildCatalogOptions(catalogProps);
        String flavor = catalogProps.getFlavor();
        // Canonical storage config from the FE-bound fe-filesystem StorageProperties (P1-T03), replacing
        // the legacy buildObjectStorageHadoopConfig path: object stores contribute their fs.s3a.*/fs.oss.*
        // /fs.cosn.*/fs.obs.* translation, and an HDFS-backed catalog contributes its hadoop.config.resources
        // XML + HA + auth keys (C2; the defaults-free fe-filesystem Hadoop map). Empty for REST (the server
        // owns storage) and for a catalog with no typed storage at all (it reaches the conf via the raw
        // fs./dfs./hadoop. passthrough).
        Map<String, String> storageHadoopConfig = buildStorageHadoopConfig();

        switch (flavor) {
            case PaimonCatalogProperties.FILESYSTEM: {
                // filesystem carries a Hadoop Configuration for HDFS/S3 storage.
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(
                        catalogProps.getRaw(), storageHadoopConfig);
                return createCatalogFromContext(CatalogContext.create(options, conf), flavor,
                        "Failed to create Paimon catalog with filesystem metastore");
            }
            case PaimonCatalogProperties.REST: {
                // rest is Options-only (no storage Configuration; the REST server owns storage).
                return createCatalogFromContext(CatalogContext.create(options), flavor,
                        "Failed to create Paimon catalog with REST metastore");
            }
            case PaimonCatalogProperties.JDBC: {
                maybeRegisterJdbcDriver();
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(
                        catalogProps.getRaw(), storageHadoopConfig);
                return createCatalogFromContext(CatalogContext.create(options, conf), flavor,
                        "Failed to create Paimon catalog with JDBC metastore");
            }
            case PaimonCatalogProperties.HMS: {
                // NOTE (B1/cutover-blocker P5-B7): the live metastore=hive path needs the Thrift
                // metastore client (org.apache.hadoop.hive.metastore.IMetaStoreClient /
                // HiveMetaStoreClient), which is NOT provided by this connector's compile deps
                // (paimon-hive-connector-3.1 keeps hive-exec/hive-metastore/hadoop-client at test
                // scope; hive-common only carries HiveConf). At cutover it must resolve from the FE
                // host's hive-catalog-shade. There is also a cross-classloader identity hazard: the
                // plugin loads child-first, so the bundled hadoop-common/hive-common Configuration/
                // HiveConf can diverge from the host shade's. Live-e2e MUST verify, before cutover,
                // that a real HMS-backed metastore=hive paimon catalog created through the plugin
                // throws neither NoClassDefFoundError (.../IMetaStoreClient) nor a Configuration/
                // HiveConf LinkageError/ClassCastException.
                // FIX-HMS-CONFRES: the external hive-site.xml (hive.conf.resources) is resolved by the
                // connector itself (PaimonCatalogFactory.addConfResources) and seeded as the HiveConf BASE,
                // so connection-critical settings present only in that file reach the live metastore client.
                // Shared parser produces the neutral HiveConf overrides (P2-T03); the connector seeds the
                // external hive-site.xml as the BASE first, then overlays the overrides (F2 ordering).
                AbstractHmsMetaStoreProperties hms = (AbstractHmsMetaStoreProperties)
                        MetaStoreProviders.bind(catalogProps.getRaw(), storageHadoopConfig);
                HiveConf hc = PaimonCatalogFactory.assembleHiveConf(
                        hms.getConfResources(),
                        hms.toHiveConfOverrides(PaimonConf.metastoreClientTimeoutSecond(context)));
                // Paimon's pool cache is JVM-static and URI-only by default. Configuration-derived identities
                // isolate catalogs without coupling the cache key to whichever storage UGI creates the catalog.
                options.set("client-pool-cache.keys", appendHmsCacheKeys(
                        options.get("client-pool-cache.keys")));
                HadoopAuthenticator hmsAuth = buildHmsAuthenticator(catalogProps.getRaw(), storageHadoopConfig);
                return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
                        hmsAuth, storageHadoopConfig,
                        "Failed to create Paimon catalog with HMS metastore");
            }
            default:
                throw new IllegalArgumentException("Unknown paimon.catalog.type value: " + flavor);
        }
    }

    /**
     * Assembles the canonical storage Hadoop config from the FE-bound storage properties (P1-T03).
     * fe-core binds the catalog's raw property map to fe-filesystem {@link StorageProperties} and hands
     * them over via {@link ConnectorStorageContext#getStorageProperties()}; here we merge each one's
     * {@code toHadoopProperties().toHadoopConfigurationMap()}: object stores contribute their
     * fs.s3a.* / Jindo fs.oss.* / fs.cosn.* / fs.obs.* translation, and an HDFS-backed catalog contributes
     * its hadoop.config.resources XML + HA + auth keys (C2; the fe-filesystem HDFS Hadoop map is
     * defaults-free so it never clobbers a co-bound object-store provider's tuned fs.s3a.* here). This
     * replaces the legacy {@code StorageProperties.buildObjectStorageHadoopConfig(properties)} call that
     * {@link PaimonCatalogFactory#buildHadoopConfiguration}/{@code buildHmsHiveConf}
     * used to make. Empty for REST (the server owns storage) and for a catalog with no typed storage (it
     * reaches the conf via the raw fs./dfs./hadoop. passthrough).
     */
    // Package-private (not private) so PaimonCatalogFactoryTest can drive the storage().getStorageProperties()
    // -> toHadoopProperties() -> Configuration wiring end-to-end (visible for testing).
    Map<String, String> buildStorageHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        for (StorageProperties sp : storage().getStorageProperties()) {
            sp.toHadoopProperties().ifPresent(h -> merged.putAll(h.toHadoopConfigurationMap()));
        }
        return merged;
    }

    private Catalog createCatalogFromContext(CatalogContext catalogContext, String flavor, String failureMessage) {
        return createCatalogFromContext(catalogContext, flavor, null, Collections.emptyMap(), failureMessage);
    }

    private Catalog createCatalogFromContext(CatalogContext catalogContext, String flavor,
            HadoopAuthenticator hmsAuth, Map<String, String> storageHadoopConfig, String failureMessage) {
        // Pin the thread-context classloader to the plugin loader for the duration of catalog
        // creation (FIX-PAIMON-HADOOP-CLASSLOADER). Hadoop's FileSystem ServiceLoader
        // (FileSystem.loadFileSystems -> ServiceLoader.load(FileSystem.class)) and SecurityUtil's
        // static init resolve classes via the thread-context CL; without the pin they read the parent
        // 'app' loader's service files / hadoop classes and split-brain against the child-loaded
        // FileSystem (which permanently poisons SecurityUtil.<clinit>). Mirrors JdbcConnectorClient /
        // ThriftHmsClient. The one-time FS class resolution + SecurityUtil init happen here on the
        // first FileSystem.get, so pinning creation is sufficient; later FS ops reuse loaded classes.
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            return context.executeAuthenticated(() -> {
                Catalog catalog = PaimonCatalogProperties.HMS.equals(flavor)
                        ? createHmsCatalog(catalogContext, hmsAuth, catalogProps.getRaw(),
                                storageHadoopConfig)
                        : CatalogFactory.createCatalog(catalogContext);
                return catalog;
            });
        } catch (Exception e) {
            throw new RuntimeException(failureMessage + " (flavor=" + flavor + "): " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    static Catalog createHmsCatalog(CatalogContext catalogContext, HadoopAuthenticator hmsAuth,
            Map<String, String> properties, Map<String, String> storageHadoopConfig) {
        HiveConf hiveConf = HiveCatalog.createHiveConf(catalogContext);
        Options options = catalogContext.options();
        String warehouse = options.get(CatalogOptions.WAREHOUSE);
        if (warehouse == null) {
            warehouse = hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    HiveConf.ConfVars.METASTOREWAREHOUSE.defaultStrVal);
        }
        Path warehousePath = new Path(warehouse);
        Path fileIoPath = warehousePath.toUri().getScheme() == null
                ? new Path(FileSystem.getDefaultUri(hiveConf)) : warehousePath;
        try {
            FileIO fileIO = FileIO.get(fileIoPath, catalogContext);
            // Paimon checks or creates the warehouse eagerly; it must retain the outer storage identity.
            fileIO.checkOrMkdirs(warehousePath);
            String clientClass = options.get(HiveCatalogOptions.METASTORE_CLIENT_CLASS);
            Catalog catalog = hmsAuth == null
                    ? new HiveCatalog(fileIO, hiveConf, clientClass, options, warehousePath.toUri().toString())
                    : hmsAuth.doAs(() -> new HiveCatalog(
                            fileIO, hiveConf, clientClass, options, warehousePath.toUri().toString()));
            catalog = PaimonHmsClientPool.install(catalog, hmsAuth);
            catalog = PaimonHmsCatalog.install(catalog, properties, storageHadoopConfig);
            catalog = CachingCatalog.tryToCreate(catalog, options);
            return PrivilegedCatalog.tryToCreate(catalog, options);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String appendHmsCacheKeys(String existing) {
        String keys = appendCacheKey(existing, "conf:hadoop.username");
        keys = appendCacheKey(keys, "conf:hive.metastore.client.principal");
        keys = appendCacheKey(keys, "conf:hive.metastore.kerberos.principal");
        keys = appendCacheKey(keys, "conf:hadoop.kerberos.principal");
        // Both settings are captured by the JVM-static SDK pool and must distinguish ALTER CATALOG generations.
        return appendCacheKey(keys, "conf:hive.metastore.sasl.enabled");
    }

    static String appendCacheKey(String existing, String required) {
        if (StringUtils.isBlank(existing)) {
            return required;
        }
        for (String element : existing.split(",")) {
            if (sameCacheKey(required, element.trim())) {
                return existing;
            }
        }
        return existing + "," + required;
    }

    private static boolean sameCacheKey(String required, String existing) {
        String prefix = "conf:";
        if (required.regionMatches(true, 0, prefix, 0, prefix.length())
                && existing.regionMatches(true, 0, prefix, 0, prefix.length())) {
            // Paimon accepts a case-insensitive marker, but Configuration property names remain case-sensitive.
            return required.substring(prefix.length()).equals(existing.substring(prefix.length()));
        }
        return required.equalsIgnoreCase(existing);
    }

    /**
     * Enforces JDBC driver-url security at CREATE CATALOG (rereview2 B-8b). For the JDBC flavor a
     * configured {@code driver_url} — read from either the {@code jdbc.driver_url} or the
     * {@code paimon.jdbc.driver_url} alias — is routed through the engine's
     * {@link ConnectorValidationContext#validateAndResolveDriverPath} hook, which applies the FE
     * format / {@code jdbc_driver_url_white_list} / {@code jdbc_driver_secure_path} gates (legacy
     * {@code JdbcResource.getFullDriverUrl}). A rejected url throws here, so CREATE CATALOG fails
     * before the jar is ever loaded into the FE JVM by {@link #maybeRegisterJdbcDriver}. Mirrors
     * {@code JdbcDorisConnector.preCreateValidation}; non-JDBC flavors are a no-op.
     */
    @Override
    public void preCreateValidation(ConnectorValidationContext validationContext) throws Exception {
        if (!PaimonCatalogProperties.JDBC.equals(catalogProps.getFlavor())) {
            return;
        }
        String driverUrl = PaimonJdbcMetaStoreProperties.of(catalogProps.getRaw()).getDriverUrl();
        if (StringUtils.isNotBlank(driverUrl)) {
            validationContext.validateAndResolveDriverPath(driverUrl);
        }
    }

    /**
     * If a JDBC driver_url is configured, dynamically load + register the driver before creating
     * the catalog. {@link java.sql.DriverManager#getConnection} does not consult the thread context
     * class loader, so the driver must be registered globally. Ported from the legacy
     * {@code PaimonJdbcMetaStoreProperties.registerJdbcDriver}, with the fe-core
     * {@code JdbcResource.getFullDriverUrl} dependency replaced by connector-side resolution
     * against {@code ConnectorContext.getEnvironment()}.
     */
    private void maybeRegisterJdbcDriver() {
        PaimonJdbcMetaStoreProperties jdbc = PaimonJdbcMetaStoreProperties.of(catalogProps.getRaw());
        String driverUrl = jdbc.getDriverUrl();
        if (StringUtils.isBlank(driverUrl)) {
            return;
        }
        registerJdbcDriver(driverUrl, jdbc.getDriverClass());
        LOG.info("Using dynamic JDBC driver for Paimon JDBC catalog from: {}", driverUrl);
    }

    /**
     * Resolves a driver_url to a full, scheme-bearing URL string for FE driver registration,
     * delegating to the shared {@link JdbcDriverSupport#resolveDriverUrl} so the FE registration
     * path and the BE-bound scan options ({@code PaimonScanPlanProvider.getBackendPaimonOptions})
     * resolve a given driver_url identically.
     *
     * <p>FE security validation (format / {@code jdbc_driver_url_white_list} /
     * {@code jdbc_driver_secure_path}) is enforced at CREATE CATALOG by {@link #preCreateValidation}
     * via the engine's {@code ConnectorValidationContext.validateAndResolveDriverPath} hook — a
     * rejected url fails catalog creation before this path is ever reached. Like the JDBC reference
     * connector ({@code JdbcDorisConnector}), validation is CREATE-time only; catalogs reloaded after
     * an FE restart or reconfigured via ALTER CATALOG are not re-validated against a since-tightened
     * allow-list (a pre-existing fe-core gap shared by all plugin connectors — see deviations-log).
     */
    private String resolveFullDriverUrl(String driverUrl) {
        return JdbcDriverSupport.resolveDriverUrl(driverUrl,
                PaimonConf.driversDir(context),
                PaimonConf.dorisHome(context));
    }

    private void registerJdbcDriver(String driverUrl, String driverClassName) {
        try {
            if (StringUtils.isBlank(driverClassName)) {
                throw new IllegalArgumentException(
                        "jdbc.driver_class or paimon.jdbc.driver_class is required when jdbc.driver_url "
                                + "or paimon.jdbc.driver_url is specified");
            }

            String fullDriverUrl = resolveFullDriverUrl(driverUrl);
            URL url = new URL(fullDriverUrl);
            String driverKey = fullDriverUrl + "#" + driverClassName;
            if (!REGISTERED_DRIVER_KEYS.add(driverKey)) {
                LOG.info("JDBC driver already registered for Paimon catalog: {} from {}",
                        driverClassName, fullDriverUrl);
                return;
            }
            try {
                ClassLoader classLoader = DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(url, u -> {
                    ClassLoader parent = getClass().getClassLoader();
                    return URLClassLoader.newInstance(new URL[] {u}, parent);
                });
                Class<?> loadedDriverClass = Class.forName(driverClassName, true, classLoader);
                java.sql.Driver driver = (java.sql.Driver) loadedDriverClass.getDeclaredConstructor().newInstance();
                java.sql.DriverManager.registerDriver(new DriverShim(driver));
                LOG.info("Successfully registered JDBC driver for Paimon catalog: {} from {}",
                        driverClassName, fullDriverUrl);
            } catch (ClassNotFoundException e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new IllegalArgumentException("Failed to load JDBC driver class: " + driverClassName, e);
            } catch (Exception e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid driver URL: " + driverUrl, e);
        } catch (IllegalArgumentException e) {
            throw e;
        }
    }

    private static class DriverShim implements java.sql.Driver {
        private final java.sql.Driver delegate;

        DriverShim(java.sql.Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.sql.Connection connect(String url, java.util.Properties info) throws java.sql.SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws java.sql.SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
                throws java.sql.SQLException {
            return delegate.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return delegate.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return delegate.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return delegate.jdbcCompliant();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }
    }

    @Override
    public void close() throws IOException {
        metaCache.close();
        Catalog cat = catalog;
        if (cat != null) {
            try {
                cat.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Paimon catalog", e);
            }
        }
    }

    /** This catalog's engine-owned storage services (see {@link ConnectorContext#getStorageContext()}). */
    private ConnectorStorageContext storage() {
        return context.getStorageContext();
    }
}
