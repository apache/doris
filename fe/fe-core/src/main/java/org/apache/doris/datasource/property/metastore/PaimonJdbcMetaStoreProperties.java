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

import org.apache.doris.catalog.JdbcDriverLoader;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.jdbc.JdbcCatalogFactory;
import org.apache.paimon.options.CatalogOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonJdbcMetaStoreProperties extends AbstractPaimonProperties {
    private static final Logger LOG = LogManager.getLogger(PaimonJdbcMetaStoreProperties.class);
    private static final String JDBC_PREFIX = "jdbc.";
    public static final String PAIMON_JDBC_DRIVER_URL = "paimon.jdbc." + JdbcResource.DRIVER_URL;
    public static final String PAIMON_JDBC_DRIVER_CLASS = "paimon.jdbc." + JdbcResource.DRIVER_CLASS;
    public static final String PAIMON_JDBC_DRIVER_CHECKSUM = "paimon.jdbc.driver_checksum";
    public static final String JDBC_DRIVER_URL = JDBC_PREFIX + JdbcResource.DRIVER_URL;
    public static final String JDBC_DRIVER_CLASS = JDBC_PREFIX + JdbcResource.DRIVER_CLASS;
    public static final String JDBC_DRIVER_CHECKSUM = JDBC_PREFIX + "driver_checksum";

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
            names = {PAIMON_JDBC_DRIVER_URL, JDBC_DRIVER_URL},
            required = false,
            description = "JDBC driver JAR file path or URL. "
                    + "Can be a local file name (will look in $DORIS_HOME/plugins/jdbc_drivers/) "
                    + "or a full URL (http://, https://, file://)."
    )
    private String driverUrl;

    @ConnectorProperty(
            names = {PAIMON_JDBC_DRIVER_CLASS, JDBC_DRIVER_CLASS},
            required = false,
            description = "JDBC driver class name. If specified with paimon.jdbc.driver_url, "
                    + "the driver will be loaded dynamically."
    )
    private String driverClass;

    @ConnectorProperty(
            names = {PAIMON_JDBC_DRIVER_CHECKSUM, JDBC_DRIVER_CHECKSUM, JdbcResource.CHECK_SUM},
            required = false,
            description = "Expected checksum for the JDBC driver JAR."
    )
    private String driverChecksum;

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
            checkDriverClass();
            String fullDriverUrl = JdbcDriverLoader.registerDriver(
                    driverUrl, driverClass, getOrComputeDriverChecksum(), getClass().getClassLoader());
            LOG.info("Using dynamic JDBC driver for Paimon JDBC catalog from: {}", fullDriverUrl);
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
        checkDriverClass();
        Map<String, String> backendPaimonOptions = new HashMap<>();
        backendPaimonOptions.put(JDBC_DRIVER_URL, JdbcResource.getFullDriverUrl(driverUrl));
        backendPaimonOptions.put(JDBC_DRIVER_CLASS, driverClass);
        backendPaimonOptions.put(JDBC_DRIVER_CHECKSUM, getOrComputeDriverChecksum());
        return backendPaimonOptions;
    }

    private String getOrComputeDriverChecksum() {
        if (StringUtils.isNotBlank(driverChecksum)) {
            return driverChecksum;
        }
        try {
            return JdbcDriverLoader.validateDriverChecksum(driverUrl, driverChecksum);
        } catch (DdlException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private void checkDriverClass() {
        if (StringUtils.isBlank(driverClass)) {
            throw new IllegalArgumentException("jdbc.driver_class or paimon.jdbc.driver_class is required when "
                    + "jdbc.driver_url or paimon.jdbc.driver_url is specified");
        }
    }
}
