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
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.options.CatalogOptions;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PaimonJdbcMetaStoreProperties extends AbstractPaimonProperties {
    private static final Logger LOG = LogManager.getLogger(PaimonJdbcMetaStoreProperties.class);
    private static final String JDBC_PREFIX = "jdbc.";
    private static final String JDBC_DRIVER_URL = JDBC_PREFIX + JdbcResource.DRIVER_URL;
    private static final String JDBC_DRIVER_CLASS = JDBC_PREFIX + JdbcResource.DRIVER_CLASS;
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    @ConnectorProperty(
            names = {"uri", "paimon.jdbc.uri"},
            required = true,
            description = "JDBC connection URI for the Paimon JDBC catalog."
    )
    private String uri = "";

    @ConnectorProperty(
            names = {"paimon.jdbc.user", "jdbc.user"},
            required = false,
            description = "Username for the Paimon JDBC catalog."
    )
    private String jdbcUser;

    @ConnectorProperty(
            names = {"paimon.jdbc.password", "jdbc.password"},
            required = false,
            sensitive = true,
            description = "Password for the Paimon JDBC catalog."
    )
    private String jdbcPassword;

    @ConnectorProperty(
            names = {"paimon.jdbc.driver_url", "jdbc.driver_url"},
            required = false,
            description = "JDBC driver JAR file path or URL. "
                    + "Can be a local file name (will look in $DORIS_HOME/plugins/jdbc_drivers/) "
                    + "or a full URL (http://, https://, file://)."
    )
    private String driverUrl;

    @ConnectorProperty(
            names = {"paimon.jdbc.driver_class", "jdbc.driver_class"},
            required = false,
            description = "JDBC driver class name. If specified with paimon.jdbc.driver_url, "
                    + "the driver will be loaded dynamically."
    )
    private String driverClass;

    protected PaimonJdbcMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getPaimonCatalogType() {
        return PaimonExternalCatalog.PAIMON_JDBC;
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }
    }

    @Override
    public Catalog initializeCatalog(String catalogName, List<StorageProperties> storagePropertiesList) {
        buildCatalogOptions();
        Configuration conf = new Configuration();
        for (StorageProperties storageProperties : storagePropertiesList) {
            if (storageProperties.getHadoopStorageConfig() != null) {
                conf.addResource(storageProperties.getHadoopStorageConfig());
            }
            if (storageProperties.getType().equals(StorageProperties.Type.HDFS)) {
                this.executionAuthenticator = new HadoopExecutionAuthenticator(((HdfsProperties) storageProperties)
                        .getHadoopAuthenticator());
            }
        }
        appendUserHadoopConfig(conf);
        if (StringUtils.isNotBlank(driverUrl)) {
            registerJdbcDriver(driverUrl, driverClass);
            LOG.info("Using dynamic JDBC driver for Paimon JDBC catalog from: {}", driverUrl);
        }
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, conf);
        try {
            return this.executionAuthenticator.execute(() -> CatalogFactory.createCatalog(catalogContext));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Paimon catalog with JDBC metastore: " + e.getMessage(), e);
        }
    }

    @Override
    protected void appendCustomCatalogOptions() {
        catalogOptions.set(CatalogOptions.URI.key(), uri);
        addIfNotBlank("jdbc.user", jdbcUser);
        addIfNotBlank("jdbc.password", jdbcPassword);
        appendRawJdbcCatalogOptions();
    }

    @Override
    protected String getMetastoreType() {
        return JdbcCatalogFactory.IDENTIFIER;
    }

    private void addIfNotBlank(String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            catalogOptions.set(key, value);
        }
    }

    private void appendRawJdbcCatalogOptions() {
        origProps.forEach((key, value) -> {
            if (key != null && key.startsWith(JDBC_PREFIX) && !catalogOptions.keySet().contains(key)) {
                catalogOptions.set(key, value);
            }
        });
    }

    public Map<String, String> getBackendPaimonOptions() {
        if (StringUtils.isBlank(driverUrl)) {
            return Collections.emptyMap();
        }
        if (StringUtils.isBlank(driverClass)) {
            throw new IllegalArgumentException("jdbc.driver_class or paimon.jdbc.driver_class is required when "
                    + "jdbc.driver_url or paimon.jdbc.driver_url is specified");
        }
        Map<String, String> backendPaimonOptions = new HashMap<>();
        backendPaimonOptions.put(JDBC_DRIVER_URL, JdbcResource.getFullDriverUrl(driverUrl));
        backendPaimonOptions.put(JDBC_DRIVER_CLASS, driverClass);
        return backendPaimonOptions;
    }

    /**
     * Register JDBC driver with DriverManager.
     * This is necessary because DriverManager.getConnection() doesn't use Thread.contextClassLoader.
     */
    private void registerJdbcDriver(String driverUrl, String driverClassName) {
        try {
            if (StringUtils.isBlank(driverClassName)) {
                throw new IllegalArgumentException(
                        "jdbc.driver_class or paimon.jdbc.driver_class is required when jdbc.driver_url "
                                + "or paimon.jdbc.driver_url is specified");
            }

            String fullDriverUrl = JdbcResource.getFullDriverUrl(driverUrl);
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
}
