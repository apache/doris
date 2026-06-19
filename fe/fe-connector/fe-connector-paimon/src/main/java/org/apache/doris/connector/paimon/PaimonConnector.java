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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.metastore.DlfMetaStoreProperties;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Paimon connector implementation managing the lifecycle of a
 * {@link org.apache.paimon.catalog.Catalog} instance.
 *
 * <p>The Paimon Catalog is lazily created on first metadata access.
 * It supports multiple catalog backends (filesystem, HMS, DLF, REST, JDBC)
 * determined by the {@code paimon.catalog.type} property. The per-flavor option
 * assembly lives in the pure {@link PaimonCatalogFactory}; this class drives the
 * live catalog creation.
 *
 * <p>B1 lands all five flavors live. filesystem/jdbc create a {@link CatalogContext} carrying a
 * minimal Hadoop {@link Configuration} (HDFS/S3 storage), rest is Options-only, and hms/dlf carry a
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

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog catalog;

    // FIX-B-MC2: connector-level (per-catalog, long-lived) second-level memo for the time-travel
    // schema-at-snapshot read. getMetadata() returns a FRESH metadata per query, so this must live on the
    // connector (not the metadata) to give the cross-query hit the legacy PaimonExternalMetaCache provided.
    // Cleared wholesale on REFRESH CATALOG (the connector is rebuilt). See PaimonSchemaAtMemo.
    private final PaimonSchemaAtMemo schemaAtMemo = new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE);

    public PaimonConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new PaimonConnectorMetadata(
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog()), properties, context,
                schemaAtMemo);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        // FIX-B-R2-be: inject the SAME per-catalog schemaAtMemo getMetadata uses, so the schema-evolution
        // dict's per-schema-id reads are memoized across scans (and shared with the B-MC2 time-travel path).
        return new PaimonScanPlanProvider(properties,
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog()), context, schemaAtMemo);
    }

    /**
     * Declares the E5 read-path capabilities paimon supports: MVCC snapshot pinning and time travel
     * (FOR TIME TRAVEL / FOR VERSION AS OF). The B5 fe-core MvccTable wiring keys off these to call
     * {@link PaimonConnectorMetadata#beginQuerySnapshot} / {@code resolveTimeTravel}.
     * No write capability is declared: paimon write is not migrated.
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                // Paimon exposes per-partition stats (record/size/file count) via listPartitions,
                // so SHOW PARTITIONS renders the legacy 5-column result (D-045).
                ConnectorCapability.SUPPORTS_PARTITION_STATS);
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
        Options options = PaimonCatalogFactory.buildCatalogOptions(properties);
        String flavor = PaimonCatalogFactory.resolveFlavor(properties);
        // Canonical storage config from the FE-bound fe-filesystem StorageProperties (P1-T03), replacing
        // the legacy buildObjectStorageHadoopConfig path: object stores contribute their fs.s3a.*/fs.oss.*
        // /fs.cosn.*/fs.obs.* translation, and an HDFS-backed catalog contributes its hadoop.config.resources
        // XML + HA + auth keys (C2; the defaults-free fe-filesystem Hadoop map). Empty for REST (the server
        // owns storage) and for a catalog with no typed storage at all (it reaches the conf via the raw
        // fs./dfs./hadoop. passthrough).
        Map<String, String> storageHadoopConfig = buildStorageHadoopConfig();

        switch (flavor) {
            case PaimonConnectorProperties.FILESYSTEM: {
                // filesystem carries a Hadoop Configuration for HDFS/S3 storage.
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                return createCatalogFromContext(CatalogContext.create(options, conf), flavor,
                        "Failed to create Paimon catalog with filesystem metastore");
            }
            case PaimonConnectorProperties.REST: {
                // rest is Options-only (no storage Configuration; the REST server owns storage).
                return createCatalogFromContext(CatalogContext.create(options), flavor,
                        "Failed to create Paimon catalog with REST metastore");
            }
            case PaimonConnectorProperties.JDBC: {
                maybeRegisterJdbcDriver();
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                return createCatalogFromContext(CatalogContext.create(options, conf), flavor,
                        "Failed to create Paimon catalog with JDBC metastore");
            }
            case PaimonConnectorProperties.HMS: {
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
                // FIX-HMS-CONFRES: resolve an external hive-site.xml (hive.conf.resources) FE-side
                // (the connector cannot import fe-core/fe-common's CatalogConfigFileUtils), then seed
                // its keys as the HiveConf BASE so connection-critical settings present only in that
                // file reach the live metastore client (legacy HMSBaseProperties parity).
                Map<String, String> hiveConfFiles = context.loadHiveConfResources(
                        PaimonCatalogFactory.firstNonBlank(properties, "hive.conf.resources"));
                // Shared parser produces the neutral HiveConf overrides (P2-T03); the connector seeds the
                // external hive-site.xml as the BASE first, then overlays the overrides (F2 ordering).
                HmsMetaStoreProperties hms = (HmsMetaStoreProperties)
                        MetaStoreProviders.bind(properties, storageHadoopConfig);
                HiveConf hc = PaimonCatalogFactory.assembleHiveConf(hiveConfFiles,
                        hms.toHiveConfOverrides(context.getEnvironment()
                                .getOrDefault("hive_metastore_client_timeout_second", "10")));
                return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
                        "Failed to create Paimon catalog with HMS metastore");
            }
            case PaimonConnectorProperties.DLF: {
                // Legacy parity: DLF metastore requires an OSS / OSS_HDFS backend specifically (not a
                // generic S3 one). This is now enforced at CREATE CATALOG by DlfMetaStoreProperties
                // .validate() (via PaimonConnectorProvider.validateProperties), so a misconfigured
                // S3-only DLF catalog never reaches this build path (P2-T03; replaces the old build-time
                // requireOssStorageForDlf call).
                // DLF storage is OSS (fe-filesystem-bound, in storageHadoopConfig); overlaid by the
                // shared parser inside toDlfCatalogConf.
                // NOTE (B1/cutover-blocker P5-B7): same metastore=hive runtime gap as the hms branch
                // above — the Thrift metastore client (IMetaStoreClient/HiveMetaStoreClient, here the
                // Aliyun ProxyMetaStoreClient) is host-provided via hive-catalog-shade at cutover, not
                // bundled; and the child-first Configuration/HiveConf cross-loader identity hazard
                // applies. Live-e2e MUST verify, before cutover, that a real DLF-backed
                // metastore=hive paimon catalog created through the plugin throws neither
                // NoClassDefFoundError (.../IMetaStoreClient) nor a Configuration/HiveConf
                // LinkageError/ClassCastException.
                DlfMetaStoreProperties dlf = (DlfMetaStoreProperties)
                        MetaStoreProviders.bind(properties, storageHadoopConfig);
                HiveConf hc = PaimonCatalogFactory.assembleHiveConf(null, dlf.toDlfCatalogConf());
                return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
                        "Failed to create Paimon catalog with DLF metastore");
            }
            default:
                throw new IllegalArgumentException("Unknown paimon.catalog.type value: " + flavor);
        }
    }

    /**
     * Assembles the canonical storage Hadoop config from the FE-bound storage properties (P1-T03).
     * fe-core binds the catalog's raw property map to fe-filesystem {@link StorageProperties} and hands
     * them over via {@link ConnectorContext#getStorageProperties()}; here we merge each one's
     * {@code toHadoopProperties().toHadoopConfigurationMap()}: object stores contribute their
     * fs.s3a.* / Jindo fs.oss.* / fs.cosn.* / fs.obs.* translation, and an HDFS-backed catalog contributes
     * its hadoop.config.resources XML + HA + auth keys (C2; the fe-filesystem HDFS Hadoop map is
     * defaults-free so it never clobbers a co-bound object-store provider's tuned fs.s3a.* here). This
     * replaces the legacy {@code StorageProperties.buildObjectStorageHadoopConfig(properties)} call that
     * {@link PaimonCatalogFactory#buildHadoopConfiguration}/{@code buildHmsHiveConf}/{@code buildDlfHiveConf}
     * used to make. Empty for REST (the server owns storage) and for a catalog with no typed storage (it
     * reaches the conf via the raw fs./dfs./hadoop. passthrough).
     */
    // Package-private (not private) so PaimonCatalogFactoryTest can drive the ctx.getStorageProperties()
    // -> toHadoopProperties() -> Configuration wiring end-to-end (visible for testing).
    Map<String, String> buildStorageHadoopConfig() {
        Map<String, String> merged = new HashMap<>();
        for (StorageProperties sp : context.getStorageProperties()) {
            sp.toHadoopProperties().ifPresent(h -> merged.putAll(h.toHadoopConfigurationMap()));
        }
        return merged;
    }

    private Catalog createCatalogFromContext(CatalogContext catalogContext, String flavor, String failureMessage) {
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
            return context.executeAuthenticated(() -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new RuntimeException(failureMessage + " (flavor=" + flavor + "): " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
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
        if (!PaimonConnectorProperties.JDBC.equals(PaimonCatalogFactory.resolveFlavor(properties))) {
            return;
        }
        String driverUrl = PaimonCatalogFactory.firstNonBlank(
                properties, PaimonConnectorProperties.JDBC_DRIVER_URL);
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
        String driverUrl = PaimonCatalogFactory.firstNonBlank(
                properties, PaimonConnectorProperties.JDBC_DRIVER_URL);
        if (StringUtils.isBlank(driverUrl)) {
            return;
        }
        String driverClass = PaimonCatalogFactory.firstNonBlank(
                properties, PaimonConnectorProperties.JDBC_DRIVER_CLASS);
        registerJdbcDriver(driverUrl, driverClass);
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
        Map<String, String> env = context != null ? context.getEnvironment() : Collections.emptyMap();
        return JdbcDriverSupport.resolveDriverUrl(driverUrl, env);
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
        Catalog cat = catalog;
        if (cat != null) {
            try {
                cat.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Paimon catalog", e);
            }
        }
    }
}
