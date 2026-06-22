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
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorValidationContext;
import org.apache.doris.connector.api.DorisConnectorException;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * management), and transaction support remain in fe-core temporarily. {@code s3tables}/{@code dlf}
 * use the generic {@code CatalogUtil} path here; their bespoke instantiation lands in P6-T06/T07.</p>
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

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog icebergCatalog;

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new IcebergConnectorMetadata(
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(getOrCreateCatalog()), properties);
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
        Map<String, String> catalogProps =
                IcebergCatalogFactory.buildCatalogProperties(properties, flavor, chosenS3);
        String catalogName = IcebergCatalogFactory.resolveCatalogName(properties, flavor, context.getCatalogName());
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
                // rest / hadoop (and the s3tables/dlf placeholders): a storage Configuration from the
                // fe-filesystem-bound storage + raw fs./dfs./hadoop. passthrough.
                conf = IcebergCatalogFactory.buildHadoopConfiguration(properties, storageHadoopConfig);
                break;
        }

        LOG.info("Creating Iceberg catalog '{}' flavor='{}' impl='{}'",
                catalogName, flavor, catalogProps.get(CatalogProperties.CATALOG_IMPL));
        return buildCatalogAuthenticated(catalogName, catalogProps, conf, flavor);
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

    private Catalog buildCatalogAuthenticated(String catalogName, Map<String, String> catalogProps,
            Configuration conf, String flavor) {
        // Pin the thread-context classloader to the plugin loader for the duration of catalog creation
        // (FIX-PAIMON-HADOOP-CLASSLOADER parity): Hadoop's FileSystem ServiceLoader + SecurityUtil static init
        // resolve through the TCCL; without the pin they read the parent 'app' loader and split-brain against
        // the child-loaded classes. Mirrors PaimonConnector.createCatalogFromContext.
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            return context.executeAuthenticated(() -> CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, conf));
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
