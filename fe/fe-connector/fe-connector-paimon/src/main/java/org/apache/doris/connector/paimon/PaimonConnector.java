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
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

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

    public PaimonConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new PaimonConnectorMetadata(
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog()), properties, context);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new PaimonScanPlanProvider(properties,
                new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(ensureCatalog()));
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

        switch (flavor) {
            case PaimonConnectorProperties.FILESYSTEM: {
                // filesystem carries a Hadoop Configuration for HDFS/S3 storage.
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(properties);
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
                Configuration conf = PaimonCatalogFactory.buildHadoopConfiguration(properties);
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
                HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(properties);
                return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
                        "Failed to create Paimon catalog with HMS metastore");
            }
            case PaimonConnectorProperties.DLF: {
                // Legacy parity: DLF metastore requires an OSS / OSS_HDFS backend specifically (not a
                // generic S3 one). Enforced at catalog build, before the HiveConf is assembled,
                // matching legacy PaimonAliyunDLFMetaStoreProperties.initializeCatalog timing.
                PaimonCatalogFactory.requireOssStorageForDlf(properties);
                // NOTE (B1/cutover-blocker P5-B7): same metastore=hive runtime gap as the hms branch
                // above — the Thrift metastore client (IMetaStoreClient/HiveMetaStoreClient, here the
                // Aliyun ProxyMetaStoreClient) is host-provided via hive-catalog-shade at cutover, not
                // bundled; and the child-first Configuration/HiveConf cross-loader identity hazard
                // applies. Live-e2e MUST verify, before cutover, that a real DLF-backed
                // metastore=hive paimon catalog created through the plugin throws neither
                // NoClassDefFoundError (.../IMetaStoreClient) nor a Configuration/HiveConf
                // LinkageError/ClassCastException.
                HiveConf hc = PaimonCatalogFactory.buildDlfHiveConf(properties);
                return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
                        "Failed to create Paimon catalog with DLF metastore");
            }
            default:
                throw new IllegalArgumentException("Unknown paimon.catalog.type value: " + flavor);
        }
    }

    private Catalog createCatalogFromContext(CatalogContext catalogContext, String flavor, String failureMessage) {
        try {
            return context.executeAuthenticated(() -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new RuntimeException(failureMessage + " (flavor=" + flavor + "): " + e.getMessage(), e);
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
     * Resolves a driver_url to a full URL string. If it is already a URL (contains {@code "://"})
     * it is used as-is; an absolute path (starting with {@code "/"}) is returned unchanged;
     * otherwise it is treated as a bare jar file name and resolved against the engine's configured
     * {@code jdbc_drivers_dir} (defaulting to {@code $DORIS_HOME/plugins/jdbc_drivers}), mirroring
     * the minimal {@code JdbcResource.getFullDriverUrl} behavior.
     *
     * <p>NOTE (B1/cutover-blocker): legacy JdbcResource.getFullDriverUrl enforced FE security
     * allow-lists (jdbc_driver_url_white_list, jdbc_driver_secure_path) + jar-name format
     * validation. Those gates are NOT enforced here (the connector cannot import fe-core).
     * Before the jdbc driver_url path goes live at cutover (P5-B7), driver-url validation
     * must be routed through a ConnectorContext hook (cf. sanitizeJdbcUrl). Until then,
     * paimon is not in SPI_READY_TYPES so this path is not user-reachable.
     */
    private String resolveFullDriverUrl(String driverUrl) {
        if (driverUrl.contains("://")) {
            return driverUrl;
        }
        if (driverUrl.startsWith("/")) {
            // Absolute path, no scheme: legacy returns it as-is (no driversDir prepend).
            return driverUrl;
        }
        Map<String, String> env = context.getEnvironment();
        String driversDir = env.get("jdbc_drivers_dir");
        if (StringUtils.isBlank(driversDir)) {
            String dorisHome = env.getOrDefault("doris_home", ".");
            driversDir = dorisHome + "/plugins/jdbc_drivers";
        }
        return "file://" + driversDir + "/" + driverUrl;
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
