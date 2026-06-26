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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.iceberg.dlf.DLFCatalog;
import org.apache.doris.connector.metastore.DlfMetaStoreProperties;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.S3TablesClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.s3tables.iceberg.S3TablesCatalog;
import software.amazon.s3tables.iceberg.S3TablesProperties;
import software.amazon.s3tables.iceberg.imports.HttpClientProperties;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Iceberg connector implementation. Manages the lifecycle of an Iceberg SDK
 * {@link Catalog} instance for all metadata operations.
 *
 * <p>Supports all Iceberg catalog backends: REST, HMS, Glue, DLF, JDBC,
 * Hadoop, and S3Tables. The backend is determined by the {@code iceberg.catalog.type}
 * property. The per-flavor catalog-property assembly lives in the pure
 * {@link IcebergCatalogFactory} (mirroring {@code PaimonCatalogFactory}); this class drives the
 * live catalog creation: it resolves the chosen storage + Hadoop {@code Configuration} / {@code HiveConf}
 * sinks, registers the JDBC driver, and wraps {@code CatalogUtil.buildIcebergCatalog} in the FE-injected
 * authentication context with the thread-context classloader pinned to the plugin loader.</p>
 *
 * <p>Phase 1 provides read-only metadata operations (list databases, list tables,
 * get schema). Write operations, scan planning, actions (compaction, snapshot
 * management), and transaction support remain in fe-core temporarily. {@code s3tables} uses its bespoke
 * 3-arg {@code S3TablesCatalog.initialize(name, opts, client)} path (P6-T06); {@code dlf} still falls through
 * to the generic {@code CatalogUtil} placeholder until its subtree port (P6-T07).</p>
 */
public class IcebergConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    /**
     * Caches {@link ClassLoader}s keyed by resolved driver URL so a given JDBC driver jar is loaded at
     * most once across catalogs, and tracks the (url#class) keys already registered with the
     * {@link java.sql.DriverManager}. Ported verbatim from the legacy
     * {@code IcebergJdbcMetaStoreProperties} (mirrors {@code PaimonConnector}).
     */
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    // T08 latest-snapshot cache knobs (mirror PaimonConnector). The TTL pins a STABLE snapshot across queries;
    // <= 0 means "no-cache catalog" (always live). Defaults mirror the legacy iceberg table cache
    // (Config.external_cache_expire_time_seconds_after_access = 24h, Config.max_external_table_cache_num).
    static final String TABLE_CACHE_TTL_SECOND = "meta.cache.iceberg.table.ttl-second";
    static final long DEFAULT_TABLE_CACHE_TTL_SECOND = 86400L;
    static final int DEFAULT_TABLE_CACHE_CAPACITY = 1000;

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog icebergCatalog;
    // T08 connector-internal caches (D6, 0 SPI). Final per-catalog fields: a REFRESH CATALOG rebuilds the
    // connector (PluginDrivenExternalCatalog.onClose nulls + recreates it) and thus drops both caches. The
    // manifest cache is path-keyed, no-TTL, capacity-bounded; it is consumed only when
    // meta.cache.iceberg.manifest.enable is set (default off → scan uses the SDK planFiles path).
    private final IcebergLatestSnapshotCache latestSnapshotCache;
    private final IcebergManifestCache manifestCache = new IcebergManifestCache();
    // commit-bridge supply (S4 part 2): per-catalog stash carrying a row-level DML's non-equality delete supply
    // across the scan->write seam — the scan provider fills it (keyed by queryId), the write provider drains it
    // into rewritable_delete_file_sets. Like the caches above, a REFRESH CATALOG rebuilds the connector and thus
    // drops it. Inert pre-cutover (iceberg scans/writes do not route through the providers until P6.6).
    private final IcebergRewritableDeleteStash rewritableDeleteStash = new IcebergRewritableDeleteStash();

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
        this.latestSnapshotCache = new IcebergLatestSnapshotCache(
                resolveTableCacheTtlSecond(this.properties), DEFAULT_TABLE_CACHE_CAPACITY);
    }

    /**
     * Resolves {@code meta.cache.iceberg.table.ttl-second} (default 24h); a blank/unparseable value falls back
     * to the default rather than failing catalog creation (best-effort, mirrors PaimonConnector). A value
     * {@code <= 0} disables caching (the no-cache catalog reads the latest snapshot live every query).
     */
    static long resolveTableCacheTtlSecond(Map<String, String> properties) {
        String raw = properties.get(TABLE_CACHE_TTL_SECOND);
        if (StringUtils.isBlank(raw)) {
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}='{}', falling back to default {}s", TABLE_CACHE_TTL_SECOND, raw,
                    DEFAULT_TABLE_CACHE_TTL_SECOND);
            return DEFAULT_TABLE_CACHE_TTL_SECOND;
        }
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        // Thread the listing-parity gating (mirrored from legacy IcebergMetadataOps) into the seam: nested
        // namespace recursion is REST-only and flag-gated; view filtering is REST-flag-gated; a configured
        // external_catalog.name roots namespaces (REST 3-level <catalog>.<db>.<table>).
        String flavor = IcebergCatalogFactory.resolveFlavor(properties);
        boolean restFlavor = IcebergConnectorProperties.TYPE_REST.equals(flavor);
        boolean nestedNamespaceEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_NESTED_NAMESPACE_ENABLED, "false"));
        boolean viewEnabled = Boolean.parseBoolean(properties.getOrDefault(
                IcebergConnectorProperties.REST_VIEW_ENABLED, "true"));
        Optional<String> externalCatalogName =
                Optional.ofNullable(properties.get(IcebergConnectorProperties.EXTERNAL_CATALOG_NAME));
        return new IcebergConnectorMetadata(
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(getOrCreateCatalog(),
                        restFlavor, nestedNamespaceEnabled, viewEnabled, externalCatalogName),
                properties, context, latestSnapshotCache);
    }

    /**
     * REFRESH TABLE hook: drop the cached latest snapshot for one table so the next query re-pins live
     * (mirrors PaimonConnector). The names are the REMOTE db/table (RefreshManager passes remote names, the
     * same form {@link IcebergConnectorMetadata#beginQuerySnapshot} keys on). The manifest cache is path-keyed
     * and intentionally NOT cleared here (legacy IcebergExternalMetaCache parity — db/table invalidation keeps
     * manifest entries; only a REFRESH CATALOG, i.e. connector rebuild, drops them).
     */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        latestSnapshotCache.invalidate(TableIdentifier.of(dbName, tableName));
    }

    @Override
    public void invalidateAll() {
        latestSnapshotCache.invalidateAll();
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        // Mirrors PaimonConnector.getScanPlanProvider: build a fresh provider per call over the lazily-built
        // live catalog. Scan planning only loadTable()s the table, so the listing-parity gating flags
        // (nested-namespace / view / external-catalog name) that getMetadata threads are irrelevant here —
        // the 1-arg CatalogBackedIcebergCatalogOps (with their defaults) suffices.
        return new IcebergScanPlanProvider(properties,
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(getOrCreateCatalog()), context, manifestCache,
                rewritableDeleteStash);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        // Mirrors getScanPlanProvider: a fresh provider per call over the lazily-built live catalog. The
        // provider builds the TIcebergTableSink and binds the write to the executor-opened
        // IcebergConnectorTransaction. Inert pre-cutover (iceberg writes do not route here until P6.6).
        return new IcebergWritePlanProvider(properties,
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(getOrCreateCatalog()), context,
                rewritableDeleteStash);
    }

    @Override
    public ConnectorProcedureOps getProcedureOps() {
        // Mirrors getWritePlanProvider: a fresh provider per call over the lazily-built live catalog. The
        // provider loadTable()s the target and runs the procedure body (P6.4-T03/T04). Inert pre-cutover —
        // iceberg ALTER TABLE EXECUTE routes to the legacy fe-core actions until iceberg enters
        // SPI_READY_TYPES (P6.6), so this is never reached pre-flip.
        return new IcebergProcedureOps(properties,
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(getOrCreateCatalog()), context);
    }

    /**
     * Iceberg exposes point-in-time snapshots, so it declares {@code SUPPORTS_MVCC_SNAPSHOT} (the gate for the
     * generic {@code PluginDrivenMvccExternalTable}, which drives {@code beginQuerySnapshot}/{@code
     * resolveTimeTravel}/{@code applySnapshot}) and {@code SUPPORTS_TIME_TRAVEL} (mirrors paimon). Inert
     * pre-cutover — the capability is consumed only on the plugin-driven path, which iceberg does not use until
     * it enters {@code SPI_READY_TYPES} (P6.6).
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        // SINK_REQUIRE_FULL_SCHEMA_ORDER: legacy bindIcebergTableSink ALWAYS projects the write child to
        // table.getFullSchema() order (regardless of the INSERT column list), and the BE iceberg writer
        // maps data columns positionally against the full schema. The generic bindConnectorTableSink
        // reproduces that projection only under this capability; without it a reordered/partial column
        // list (and the write-sort columnIndex, a full-schema position) would resolve against a
        // user-ordered sink output. Mirrors MaxComputeDorisConnector. Inert pre-cutover (P6.6).
        // SUPPORTS_PARALLEL_WRITE: legacy iceberg INSERT runs through PhysicalIcebergTableSink, whose
        // partition-hash branch is dead code (getPartitionNames() is the empty TableIf default for iceberg),
        // so at runtime every iceberg INSERT returns SINK_RANDOM_PARTITIONED (parallel writers). The generic
        // PhysicalConnectorTableSink reproduces that ONLY under this capability; without it it falls through
        // to GATHER (single writer), a parallelism regression. NOT SINK_REQUIRE_PARTITION_LOCAL_SORT: legacy
        // never sorts on write. Inert pre-cutover (P6.6).
        return EnumSet.of(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT, ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER, ConnectorCapability.SUPPORTS_PARALLEL_WRITE);
    }

    private Catalog getOrCreateCatalog() {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    icebergCatalog = createCatalog();
                }
            }
        }
        return icebergCatalog;
    }

    private Catalog createCatalog() {
        String flavor = IcebergCatalogFactory.resolveFlavor(properties);
        if (flavor == null) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }

        Optional<S3CompatibleFileSystemProperties> chosenS3 =
                IcebergCatalogFactory.chooseS3Compatible(context.getStorageProperties());
        String catalogName = IcebergCatalogFactory.resolveCatalogName(properties, flavor, context.getCatalogName());

        // s3tables is bespoke: it is NOT built via CatalogUtil.buildIcebergCatalog. Legacy
        // IcebergS3TablesMetaStoreProperties hand-builds an S3TablesClient and calls the 3-arg
        // S3TablesCatalog.initialize(name, opts, client). Routed before the CatalogUtil flavor switch.
        if (IcebergConnectorProperties.TYPE_S3_TABLES.equals(flavor)) {
            return createS3TablesCatalog(catalogName, chosenS3);
        }

        // dlf is bespoke too: legacy IcebergAliyunDLFMetaStoreProperties builds a hive-compatible DLFCatalog with
        // a DataLakeConfig-keyed Configuration and an OSS-backed S3FileIO, NOT via CatalogUtil.buildIcebergCatalog.
        if (IcebergConnectorProperties.TYPE_DLF.equals(flavor)) {
            return createDlfCatalog(catalogName, chosenS3);
        }

        Map<String, String> catalogProps =
                IcebergCatalogFactory.buildCatalogProperties(properties, flavor, chosenS3);
        Map<String, String> storageHadoopConfig = buildStorageHadoopConfig();

        Configuration conf;
        switch (flavor) {
            case IcebergConnectorProperties.TYPE_HMS: {
                // Reuse the shared metastore-spi parser (Q2=B bindForType): iceberg passes its own flavor token
                // so the metastore-spi never learns iceberg.catalog.type. Only toHiveConfOverrides is used
                // (iceberg HMS does NOT call paimon's validate(); it does not require a warehouse). The external
                // hive.conf.resources hive-site.xml is resolved FE-side and seeded as the HiveConf base.
                Map<String, String> hiveConfFiles = context.loadHiveConfResources(
                        IcebergCatalogFactory.firstNonBlank(properties, "hive.conf.resources"));
                HmsMetaStoreProperties hms = (HmsMetaStoreProperties) MetaStoreProviders.bindForType(
                        IcebergConnectorProperties.TYPE_HMS, properties, storageHadoopConfig);
                conf = IcebergCatalogFactory.assembleHiveConf(hiveConfFiles,
                        hms.toHiveConfOverrides(context.getEnvironment()
                                .getOrDefault("hive_metastore_client_timeout_second", "10")));
                break;
            }
            case IcebergConnectorProperties.TYPE_GLUE:
                // Legacy IcebergGlueMetaStoreProperties builds the catalog with conf=null.
                conf = null;
                break;
            case IcebergConnectorProperties.TYPE_JDBC:
                maybeRegisterJdbcDriver();
                conf = IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                break;
            default:
                // rest / hadoop: a storage Configuration from the fe-filesystem-bound storage + raw
                // fs./dfs./hadoop. passthrough.
                conf = IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                break;
        }

        LOG.info("Creating Iceberg catalog '{}' flavor='{}' impl='{}'",
                catalogName, flavor, catalogProps.get(CatalogProperties.CATALOG_IMPL));
        return buildCatalogAuthenticated(flavor,
                () -> CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf));
    }

    /**
     * Creates the bespoke {@code s3tables} catalog, mirroring legacy {@code IcebergS3TablesMetaStoreProperties}: a
     * hand-built {@link S3TablesClient} (region + credentials + optional {@code s3tables.endpoint} override + the
     * s3tables-SDK http config) is passed to the 3-arg {@code S3TablesCatalog.initialize(name, opts, client)} —
     * NOT to {@code CatalogUtil.buildIcebergCatalog}. The 2-arg {@code initialize(name, opts)} is intentionally
     * avoided: its {@code DefaultS3TablesAwsClientFactory} honors only a {@code client.credentials-provider} class
     * and would silently drop static {@code s3.access-key-id}/{@code s3.secret-access-key} (falling back to the
     * SDK default chain). A bound S3-compatible storage is required to derive region + credentials; a missing one
     * (or a blank region) fails loud here, before any AWS call.
     */
    private Catalog createS3TablesCatalog(String catalogName, Optional<S3CompatibleFileSystemProperties> chosenS3) {
        if (!chosenS3.isPresent()) {
            throw new DorisConnectorException(
                    "Iceberg s3tables catalog requires S3-compatible storage properties (region + credentials)");
        }
        S3CompatibleFileSystemProperties s3 = chosenS3.get();
        if (StringUtils.isBlank(s3.getRegion())) {
            throw new DorisConnectorException(
                    "Iceberg s3tables catalog requires a region (set s3.region or a region-bearing endpoint)");
        }
        Map<String, String> catalogProps =
                IcebergCatalogFactory.buildS3TablesCatalogProperties(properties, chosenS3);
        LOG.info("Creating Iceberg s3tables catalog '{}' region='{}'", catalogName, s3.getRegion());
        return buildCatalogAuthenticated(IcebergConnectorProperties.TYPE_S3_TABLES, () -> {
            S3TablesClient client = buildS3TablesClient(s3);
            S3TablesCatalog catalog = new S3TablesCatalog();
            catalog.initialize(catalogName, catalogProps, client);
            return catalog;
        });
    }

    /**
     * Creates the bespoke {@code dlf} catalog, mirroring legacy {@code IcebergAliyunDLFMetaStoreProperties}: the
     * DLF metastore connection {@link Configuration} is built from the shared metastore-spi
     * ({@code MetaStoreProviders.bindForType("dlf", ...)} -> {@code DlfMetaStoreProperties.toDlfCatalogConf()},
     * the {@code dlf.catalog.*} = {@code DataLakeConfig.CATALOG_*} keys) plus the two legacy hive keys (see
     * {@link IcebergCatalogFactory#buildDlfConfiguration}); the OSS-backed {@link DLFCatalog} then reads its
     * FileIO endpoint/region/credentials from the chosen fe-filesystem OSS storage (D-061). A bound
     * S3-compatible (OSS) storage is required; a missing one fails loud, before any metastore call.
     */
    private Catalog createDlfCatalog(String catalogName, Optional<S3CompatibleFileSystemProperties> chosenS3) {
        if (!chosenS3.isPresent()) {
            throw new DorisConnectorException("Iceberg dlf catalog requires OSS storage properties");
        }
        S3CompatibleFileSystemProperties oss = chosenS3.get();
        DlfMetaStoreProperties dlf = (DlfMetaStoreProperties) MetaStoreProviders.bindForType(
                IcebergConnectorProperties.TYPE_DLF, properties, buildStorageHadoopConfig());
        Configuration conf = IcebergCatalogFactory.buildDlfConfiguration(dlf.toDlfCatalogConf());
        Map<String, String> catalogProps = IcebergCatalogFactory.buildBaseCatalogProperties(properties);
        LOG.info("Creating Iceberg dlf catalog '{}'", catalogName);
        return buildCatalogAuthenticated(IcebergConnectorProperties.TYPE_DLF, () -> {
            DLFCatalog dlfCatalog = new DLFCatalog(oss);
            dlfCatalog.setConf(conf);
            dlfCatalog.initialize(catalogName, catalogProps);
            return dlfCatalog;
        });
    }

    /**
     * Hand-builds the control-plane {@link S3TablesClient}, mirroring legacy
     * {@code IcebergS3TablesMetaStoreProperties.buildS3TablesClient}: region + credentials provider + the optional
     * {@code s3tables.endpoint} override + the s3tables-SDK http-client tuning ({@link HttpClientProperties}). The
     * credentials provider is derived from the typed fe-filesystem storage by {@link #buildAwsCredentialsProvider}.
     */
    private S3TablesClient buildS3TablesClient(S3CompatibleFileSystemProperties s3) {
        S3TablesClientBuilder builder = S3TablesClient.builder()
                .region(Region.of(s3.getRegion()))
                .credentialsProvider(buildAwsCredentialsProvider(s3));
        String endpoint = properties.get(S3TablesProperties.S3TABLES_ENDPOINT);
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpointOverride(URI.create(endpoint));
        }
        new HttpClientProperties(properties).applyHttpClientConfigurations(builder);
        return builder.build();
    }

    /**
     * Derives the AWS SDK v2 credentials provider for the s3tables control-plane client from the typed
     * fe-filesystem storage, mirroring legacy
     * {@code IcebergAwsClientCredentialsProperties.createAwsCredentialsProvider}: static AK/SK ->
     * {@link StaticCredentialsProvider} (with a session token when present); a role ARN ->
     * {@link StsAssumeRoleCredentialsProvider} (role session name {@code aws-sdk-java-v2-fe}, optional external
     * id); otherwise the SDK default chain ({@link DefaultCredentialsProvider}).
     *
     * <p>DEVIATION (UT-invisible, P6.6 docker gate): legacy resolves the non-DEFAULT {@code PROVIDER_CHAIN}
     * provider modes through fe-core {@code AwsCredentialsProviderFactory.createV2}, which the connector may not
     * import; both the no-credential case and the STS base credentials therefore fall back to
     * {@link DefaultCredentialsProvider} (the common instance-profile / env case is unaffected). Same family as
     * the documented REST/glue {@code PROVIDER_CHAIN} gap.
     */
    private static AwsCredentialsProvider buildAwsCredentialsProvider(S3CompatibleFileSystemProperties s3) {
        if (s3.hasStaticCredentials()) {
            if (StringUtils.isBlank(s3.getSessionToken())) {
                return StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3.getAccessKey(), s3.getSecretKey()));
            }
            return StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(s3.getAccessKey(), s3.getSecretKey(), s3.getSessionToken()));
        }
        if (s3.hasAssumeRole()) {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(s3.getRegion()))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(b -> {
                        b.roleArn(s3.getRoleArn()).roleSessionName("aws-sdk-java-v2-fe");
                        if (StringUtils.isNotBlank(s3.getExternalId())) {
                            b.externalId(s3.getExternalId());
                        }
                    })
                    .build();
        }
        return DefaultCredentialsProvider.create();
    }

    /**
     * Assembles the canonical storage Hadoop config from the FE-bound storage properties (P1-T03), mirroring
     * {@code PaimonConnector.buildStorageHadoopConfig}: object stores contribute their fs.s3a.* / fs.oss.* /
     * fs.cosn.* / fs.obs.* translation, and an HDFS-backed catalog contributes its hadoop.config.resources XML +
     * HA + auth keys (C2; the defaults-free fe-filesystem HDFS map). Empty for a catalog with no typed storage.
     */
    private Map<String, String> buildStorageHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        for (StorageProperties sp : context.getStorageProperties()) {
            sp.toHadoopProperties().ifPresent(h -> merged.putAll(h.toHadoopConfigurationMap()));
        }
        return merged;
    }

    private Catalog buildCatalogAuthenticated(String flavor, Callable<Catalog> builder) {
        // Pin the thread-context classloader to the plugin loader for the duration of catalog creation
        // (FIX-PAIMON-HADOOP-CLASSLOADER parity): Hadoop's FileSystem ServiceLoader + SecurityUtil static init
        // resolve through the TCCL; without the pin they read the parent 'app' loader and split-brain against
        // the child-loaded classes. Mirrors PaimonConnector.createCatalogFromContext.
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            return context.executeAuthenticated(builder);
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Iceberg catalog (flavor=" + flavor + "): " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Enforces JDBC driver-url security at CREATE CATALOG (mirrors {@code PaimonConnector.preCreateValidation}):
     * for the jdbc flavor a configured {@code iceberg.jdbc.driver_url} is routed through the engine's
     * {@link ConnectorValidationContext#validateAndResolveDriverPath} hook (the FE format /
     * {@code jdbc_driver_url_white_list} / {@code jdbc_driver_secure_path} gates), so a rejected url fails
     * CREATE CATALOG before the jar is ever loaded by {@link #maybeRegisterJdbcDriver}. Non-jdbc flavors are
     * a no-op.
     */
    @Override
    public void preCreateValidation(ConnectorValidationContext validationContext) throws Exception {
        if (!IcebergConnectorProperties.TYPE_JDBC.equals(IcebergCatalogFactory.resolveFlavor(properties))) {
            return;
        }
        String driverUrl = IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_URL);
        if (StringUtils.isNotBlank(driverUrl)) {
            validationContext.validateAndResolveDriverPath(driverUrl);
        }
    }

    /**
     * If an {@code iceberg.jdbc.driver_url} is configured, dynamically load + register the driver before
     * creating the catalog. {@link java.sql.DriverManager#getConnection} does not consult the thread context
     * class loader, so the driver must be registered globally. Ported from the legacy
     * {@code IcebergJdbcMetaStoreProperties.registerJdbcDriver}, with the fe-core
     * {@code JdbcResource.getFullDriverUrl} dependency replaced by the shared
     * {@link JdbcDriverSupport#resolveDriverUrl} against {@code ConnectorContext.getEnvironment()}.
     */
    private void maybeRegisterJdbcDriver() {
        String driverUrl = IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_URL);
        if (StringUtils.isBlank(driverUrl)) {
            return;
        }
        String driverClass =
                IcebergCatalogFactory.firstNonBlank(properties, IcebergConnectorProperties.JDBC_DRIVER_CLASS);
        registerJdbcDriver(driverUrl, driverClass);
        LOG.info("Using dynamic JDBC driver for Iceberg JDBC catalog from: {}", driverUrl);
    }

    private void registerJdbcDriver(String driverUrl, String driverClassName) {
        try {
            if (StringUtils.isBlank(driverClassName)) {
                throw new IllegalArgumentException("driver_class is required when driver_url is specified");
            }
            Map<String, String> env = context != null ? context.getEnvironment() : Collections.emptyMap();
            String fullDriverUrl = JdbcDriverSupport.resolveDriverUrl(driverUrl, env);
            URL url = new URL(fullDriverUrl);
            String driverKey = fullDriverUrl + "#" + driverClassName;
            if (!REGISTERED_DRIVER_KEYS.add(driverKey)) {
                LOG.info("JDBC driver already registered for Iceberg catalog: {} from {}",
                        driverClassName, fullDriverUrl);
                return;
            }
            try {
                ClassLoader classLoader = DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(url,
                        u -> URLClassLoader.newInstance(new URL[] {u}, getClass().getClassLoader()));
                Class<?> loadedDriverClass = Class.forName(driverClassName, true, classLoader);
                java.sql.Driver driver = (java.sql.Driver) loadedDriverClass.getDeclaredConstructor().newInstance();
                java.sql.DriverManager.registerDriver(new DriverShim(driver));
                LOG.info("Successfully registered JDBC driver for Iceberg catalog: {} from {}",
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
        }
    }

    /**
     * A shim driver that wraps a driver loaded from a custom ClassLoader, because {@code DriverManager}
     * refuses to use a driver not loaded by the system classloader. Ported verbatim from the legacy
     * {@code IcebergJdbcMetaStoreProperties.DriverShim}.
     */
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
        Catalog c = icebergCatalog;
        if (c != null) {
            if (c instanceof java.io.Closeable) {
                ((java.io.Closeable) c).close();
            }
            icebergCatalog = null;
        }
    }
}
