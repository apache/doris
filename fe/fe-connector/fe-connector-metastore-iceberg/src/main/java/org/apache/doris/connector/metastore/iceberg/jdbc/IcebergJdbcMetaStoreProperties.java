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

package org.apache.doris.connector.metastore.iceberg.jdbc;

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Iceberg JDBC catalog metastore backend — validation only (the catalog conf + dynamic driver loading are
 * connector-side in {@code IcebergCatalogFactory}/{@code IcebergConnector}). Parse-time rules (legacy
 * {@code IcebergJdbcMetaStoreProperties}: {@code uri}/{@code iceberg.jdbc.catalog_name} {@code required=true}
 * + the warehouse check), in fire order — §4 of the P6-T10 design. The lazy driver_class/url rules run at
 * initCatalog and are covered by the connector's {@code preCreateValidation}, NOT here.
 */
public final class IcebergJdbcMetaStoreProperties extends AbstractMetaStoreProperties {

    @ConnectorProperty(names = {"uri", "iceberg.jdbc.uri"}, required = false,
            description = "JDBC connection URI for the Iceberg JDBC catalog.")
    private String uri = "";

    @ConnectorProperty(names = {"iceberg.jdbc.catalog_name"}, required = false,
            description = "The Iceberg JDBC catalog_name used to isolate metadata in JDBC catalog tables.")
    private String jdbcCatalogName = "";

    @ConnectorProperty(names = {"iceberg.jdbc.user"}, required = false,
            description = "The user of the JDBC database holding the catalog tables.")
    private String user = "";

    @ConnectorProperty(names = {"iceberg.jdbc.password"}, required = false, sensitive = true,
            description = "The password of the JDBC database holding the catalog tables.")
    private String password = "";

    // Handed to the iceberg SDK verbatim, so all three stay Strings rather than being bound as
    // boolean/enum: a catalog created with a value the SDK tolerates must keep building.
    @ConnectorProperty(names = {"iceberg.jdbc.init-catalog-tables"}, required = false,
            description = "Whether to create the catalog tables when they do not exist yet.")
    private String initCatalogTables = "";

    @ConnectorProperty(names = {"iceberg.jdbc.schema-version"}, required = false,
            description = "The schema version of the JDBC catalog tables.")
    private String schemaVersion = "";

    @ConnectorProperty(names = {"iceberg.jdbc.strict-mode"}, required = false,
            description = "Whether the JDBC catalog rejects namespaces that do not exist.")
    private String strictMode = "";

    @ConnectorProperty(names = {"iceberg.jdbc.driver_url"}, required = false,
            description = "URL or file name of the JDBC driver jar to load dynamically.")
    private String driverUrl = "";

    @ConnectorProperty(names = {"iceberg.jdbc.driver_class"}, required = false,
            description = "Class name of the JDBC driver to register.")
    private String driverClass = "";

    private IcebergJdbcMetaStoreProperties(Map<String, String> raw) {
        super(raw);
    }

    public static IcebergJdbcMetaStoreProperties of(Map<String, String> raw) {
        IcebergJdbcMetaStoreProperties props = new IcebergJdbcMetaStoreProperties(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "JDBC";
    }

    // ---------------------------------------------------------------------
    // Assembly surface: the connector builds the jdbc catalog options, resolves the positional catalog name
    // and registers the driver from these, so the alias sets declared above are the single place a jdbc key
    // name lives.
    // ---------------------------------------------------------------------

    public String getUri() {
        return uri;
    }

    /** The positional catalog name the iceberg JdbcCatalog is built with, NOT the Doris catalog name. */
    public String getJdbcCatalogName() {
        return jdbcCatalogName;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getInitCatalogTables() {
        return initCatalogTables;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getStrictMode() {
        return strictMode;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    public String getDriverClass() {
        return driverClass;
    }

    @Override
    public void validate() {
        // Legacy: uri + iceberg.jdbc.catalog_name are required=true (checked by the base in field-declaration
        // order: uri first), then IcebergJdbcMetaStoreProperties.checkRequiredProperties adds warehouse.
        if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("Property uri is required.");
        }
        if (StringUtils.isBlank(jdbcCatalogName)) {
            throw new IllegalArgumentException("Property iceberg.jdbc.catalog_name is required.");
        }
        requireWarehouse();
    }
}
