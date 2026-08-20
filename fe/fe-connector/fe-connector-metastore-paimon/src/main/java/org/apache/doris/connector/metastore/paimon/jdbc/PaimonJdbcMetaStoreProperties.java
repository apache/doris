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

package org.apache.doris.connector.metastore.paimon.jdbc;

import org.apache.doris.connector.metastore.JdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Paimon JDBC catalog metastore backend facts. The getters return the raw, alias-resolved values; the
 * driver-url is resolved to a full URL by the consumer via
 * {@link JdbcDriverSupport#resolveDriverUrl(String, Map)} (which needs the engine environment),
 * exactly as the live FE registration and the BE-bound options do today.
 */
public final class PaimonJdbcMetaStoreProperties extends AbstractMetaStoreProperties
        implements JdbcMetaStoreProperties {

    @ConnectorProperty(names = {"uri", "paimon.jdbc.uri"}, required = false,
            description = "The JDBC connection URI.")
    private String uri = "";

    @ConnectorProperty(names = {"paimon.jdbc.user", "jdbc.user"}, required = false,
            description = "The JDBC user.")
    private String user = "";

    @ConnectorProperty(names = {"paimon.jdbc.password", "jdbc.password"}, required = false, sensitive = true,
            description = "The JDBC password.")
    private String password = "";

    @ConnectorProperty(names = {"paimon.jdbc.driver_url", "jdbc.driver_url"}, required = false,
            description = "The JDBC driver jar URL.")
    private String driverUrl = "";

    @ConnectorProperty(names = {"paimon.jdbc.driver_class", "jdbc.driver_class"}, required = false,
            description = "The JDBC driver class name.")
    private String driverClass = "";

    private PaimonJdbcMetaStoreProperties(Map<String, String> raw) {
        super(raw);
    }

    public static PaimonJdbcMetaStoreProperties of(Map<String, String> raw) {
        PaimonJdbcMetaStoreProperties props = new PaimonJdbcMetaStoreProperties(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public String providerName() {
        return "JDBC";
    }

    @Override
    public void validate() {
        requireWarehouse();
        if (StringUtils.isBlank(uri)) {
            throw new IllegalArgumentException("uri or paimon.jdbc.uri is required");
        }
        if (StringUtils.isNotBlank(driverUrl) && StringUtils.isBlank(driverClass)) {
            throw new IllegalArgumentException(
                    "jdbc.driver_class or paimon.jdbc.driver_class is required when "
                            + "jdbc.driver_url or paimon.jdbc.driver_url is specified");
        }
    }

    @Override
    public String getUri() {
        return uri;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDriverUrl() {
        return driverUrl;
    }

    @Override
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * The jdbc-flavor catalog options, as neutral keys the connector turns into paimon {@code Options}
     * (this module stays free of the paimon SDK). Mirrors the legacy {@code appendJdbcOptions}: the uri
     * unconditionally, user and password only when set.
     *
     * <p>The connector applies these BEFORE its raw {@code jdbc.*} passthrough, which only fills keys
     * not already present -- so an alias-resolved user still wins over a bare {@code jdbc.user} that
     * would otherwise be copied verbatim.
     */
    public Map<String, String> toCatalogOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("uri", uri);
        if (StringUtils.isNotBlank(user)) {
            options.put("jdbc.user", user);
        }
        if (StringUtils.isNotBlank(password)) {
            options.put("jdbc.password", password);
        }
        return options;
    }
}
