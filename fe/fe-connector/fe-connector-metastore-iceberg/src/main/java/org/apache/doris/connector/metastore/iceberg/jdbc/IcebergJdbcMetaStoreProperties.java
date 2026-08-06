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
