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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergJdbcMetaStoreProperties extends AbstractIcebergProperties {
    private static final Logger LOG = LogManager.getLogger(IcebergJdbcMetaStoreProperties.class);

    private static final String JDBC_PREFIX = "jdbc.";
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();

    private Map<String, String> icebergJdbcCatalogProperties;

    @ConnectorProperty(
            names = {"uri", "iceberg.jdbc.uri"},
            required = true,
            description = "JDBC connection URI for the Iceberg JDBC catalog."
    )
    private String uri = "";

    @ConnectorProperty(
            names = {"iceberg.jdbc.user"},
            required = false,
            description = "Username for the Iceberg JDBC catalog."
    )
    private String jdbcUser;

    @ConnectorProperty(
            names = {"iceberg.jdbc.password"},
            required = false,
            sensitive = true,
            description = "Password for the Iceberg JDBC catalog."
    )
    private String jdbcPassword;

    @ConnectorProperty(
            names = {"iceberg.jdbc.init-catalog-tables"},
            required = false,
            description = "Whether to create catalog tables if they do not exist."
    )
    private String jdbcInitCatalogTables;

    @ConnectorProperty(
            names = {"iceberg.jdbc.schema-version"},
            required = false,
            description = "Iceberg JDBC catalog schema version (V0/V1)."
    )
    private String jdbcSchemaVersion;

    @ConnectorProperty(
            names = {"iceberg.jdbc.strict-mode"},
            required = false,
            description = "Whether to enforce strict JDBC catalog schema checks."
    )
    private String jdbcStrictMode;

    @ConnectorProperty(
            names = {"iceberg.jdbc.driver_url"},
            required = false,
            description = "JDBC driver JAR file path or URL. "
                    + "Can be a local file name (will look in $DORIS_HOME/plugins/jdbc_drivers/) "
                    + "or a full URL (http://, https://, file://)."
    )
    private String driverUrl;

    @ConnectorProperty(
            names = {"iceberg.jdbc.driver_class"},
            required = false,
            description = "JDBC driver class name. If not specified, will be auto-detected from the JDBC URI."
    )
    private String driverClass;

    public IcebergJdbcMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getIcebergCatalogType() {
        return IcebergExternalCatalog.ICEBERG_JDBC;
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        initIcebergJdbcCatalogProperties();
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
            List<StorageProperties> storagePropertiesList) {
        Map<String, String> fileIOProperties = Maps.newHashMap();
        Configuration conf = new Configuration();
        toFileIOProperties(storagePropertiesList, fileIOProperties, conf);

        Map<String, String> options = Maps.newHashMap(getIcebergJdbcCatalogProperties());
        options.putAll(fileIOProperties);

        // Support dynamic JDBC driver loading
        // We need to register the driver with DriverManager because Iceberg uses DriverManager.getConnection()
        // which doesn't respect Thread.contextClassLoader
        if (StringUtils.isNotBlank(driverUrl)) {
            registerJdbcDriver(driverUrl, driverClass);
            LOG.info("Using dynamic JDBC driver from: {}", driverUrl);
        }
        return CatalogUtil.buildIcebergCatalog(catalogName, options, conf);
    }

    /**
     * Register JDBC driver with DriverManager.
     * This is necessary because DriverManager.getConnection() doesn't use Thread.contextClassLoader,
     * it uses the caller's ClassLoader. By registering the driver, DriverManager can find it.
     *
     * @param driverUrl Path or URL to the JDBC driver JAR
     * @param driverClassName Driver class name to register
     */
    private void registerJdbcDriver(String driverUrl, String driverClassName) {
        try {
            String fullDriverUrl = JdbcResource.getFullDriverUrl(driverUrl);
            URL url = new URL(fullDriverUrl);

            ClassLoader classLoader = DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(url, u -> {
                ClassLoader parent = getClass().getClassLoader();
                return URLClassLoader.newInstance(new URL[]{u}, parent);
            });

            if (StringUtils.isBlank(driverClassName)) {
                throw new IllegalArgumentException("driver_class is required when driver_url is specified");
            }

            // Load the driver class and register it with DriverManager
            Class<?> driverClass = Class.forName(driverClassName, true, classLoader);
            java.sql.Driver driver = (java.sql.Driver) driverClass.getDeclaredConstructor().newInstance();

            // Wrap with a shim driver because DriverManager refuses to use a driver not loaded by system classloader
            java.sql.DriverManager.registerDriver(new DriverShim(driver));
            LOG.info("Successfully registered JDBC driver: {} from {}", driverClassName, fullDriverUrl);

        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid driver URL: " + driverUrl, e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load JDBC driver class: " + driverClassName, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
        }
    }

    /**
     * A shim driver that wraps the actual driver loaded from a custom ClassLoader.
     * This is needed because DriverManager refuses to use a driver that wasn't loaded by the system classloader.
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

    public Map<String, String> getIcebergJdbcCatalogProperties() {
        return Collections.unmodifiableMap(icebergJdbcCatalogProperties);
    }

    private void initIcebergJdbcCatalogProperties() {
        icebergJdbcCatalogProperties = new HashMap<>();
        icebergJdbcCatalogProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC);
        icebergJdbcCatalogProperties.put(CatalogProperties.URI, uri);
        if (StringUtils.isNotBlank(warehouse)) {
            icebergJdbcCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.user", jdbcUser);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.password", jdbcPassword);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.init-catalog-tables", jdbcInitCatalogTables);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.schema-version", jdbcSchemaVersion);
        addIfNotBlank(icebergJdbcCatalogProperties, "jdbc.strict-mode", jdbcStrictMode);

        if (origProps != null) {
            for (Map.Entry<String, String> entry : origProps.entrySet()) {
                String key = entry.getKey();
                if (key != null && key.startsWith(JDBC_PREFIX)
                        && !icebergJdbcCatalogProperties.containsKey(key)) {
                    icebergJdbcCatalogProperties.put(key, entry.getValue());
                }
            }
        }
    }

    private static void addIfNotBlank(Map<String, String> props, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            props.put(key, value);
        }
    }

    private static void toFileIOProperties(List<StorageProperties> storagePropertiesList,
            Map<String, String> fileIOProperties, Configuration conf) {
        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties instanceof AbstractS3CompatibleProperties) {
                toS3FileIOProperties((AbstractS3CompatibleProperties) storageProperties, fileIOProperties);
            }
            if (storageProperties.getHadoopStorageConfig() != null) {
                conf.addResource(storageProperties.getHadoopStorageConfig());
            }
        }
    }

    private static void toS3FileIOProperties(AbstractS3CompatibleProperties s3Properties,
            Map<String, String> options) {
        if (StringUtils.isNotBlank(s3Properties.getEndpoint())) {
            options.put(S3FileIOProperties.ENDPOINT, s3Properties.getEndpoint());
        }
        if (StringUtils.isNotBlank(s3Properties.getUsePathStyle())) {
            options.put(S3FileIOProperties.PATH_STYLE_ACCESS, s3Properties.getUsePathStyle());
        }
        if (StringUtils.isNotBlank(s3Properties.getRegion())) {
            options.put(AwsClientProperties.CLIENT_REGION, s3Properties.getRegion());
        }
        if (StringUtils.isNotBlank(s3Properties.getAccessKey())) {
            options.put(S3FileIOProperties.ACCESS_KEY_ID, s3Properties.getAccessKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSecretKey())) {
            options.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Properties.getSecretKey());
        }
        if (StringUtils.isNotBlank(s3Properties.getSessionToken())) {
            options.put(S3FileIOProperties.SESSION_TOKEN, s3Properties.getSessionToken());
        }
    }
}
