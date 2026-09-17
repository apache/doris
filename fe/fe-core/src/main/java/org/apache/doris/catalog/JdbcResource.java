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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.DefaultConnectorValidationContext;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * The legacy JDBC resource: {@code CREATE RESOURCE ... PROPERTIES ("type"="jdbc", ...)}.
 *
 * <p>A resource no longer backs anything: {@code CREATE CATALOG ... WITH RESOURCE} is disallowed by default
 * and, where allowed, reads nothing from a JDBC resource but its type. What is left is a named, grantable
 * property bag that {@code SHOW RESOURCES} lists. The class stays for two reasons: metadata images and edit
 * logs that hold one must keep replaying (the Gson tag and the persisted {@code configs} field are the
 * contract), and {@code CREATE RESOURCE type=jdbc} keeps working for deployments that still script it.
 * New work should use {@code CREATE CATALOG ... "type"="jdbc"}.</p>
 *
 * <p>Nothing here knows JDBC. Property validation and the driver-jar checksum are done by the jdbc connector
 * plugin, exactly as for a JDBC catalog; the property list and defaults below are only this object's own
 * persisted schema, kept so that a resource shows the same rows it always showed.</p>
 *
 * @deprecated Use JDBC Catalog instead.
 */
@Deprecated
public class JdbcResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(JdbcResource.class);

    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String DRIVER_URL = "driver_url";
    public static final String TYPE = "type";
    public static final String ONLY_SPECIFIED_DATABASE = "only_specified_database";
    public static final String CONNECTION_POOL_MIN_SIZE = "connection_pool_min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    public static final String CONNECTION_POOL_MAX_WAIT_TIME = "connection_pool_max_wait_time";
    public static final String CONNECTION_POOL_MAX_LIFE_TIME = "connection_pool_max_life_time";
    public static final String CONNECTION_POOL_KEEP_ALIVE = "connection_pool_keep_alive";
    public static final String CHECK_SUM = "checksum";
    public static final String CREATE_TIME = "create_time";
    public static final String TEST_CONNECTION = "test_connection";

    /** Every property a resource persists: what the user may set, plus what creation fills in. */
    private static final ImmutableList<String> ALL_PROPERTIES = new ImmutableList.Builder<String>().add(
            JDBC_URL,
            USER,
            PASSWORD,
            DRIVER_CLASS,
            DRIVER_URL,
            TYPE,
            CREATE_TIME,
            ONLY_SPECIFIED_DATABASE,
            ExternalCatalog.LOWER_CASE_META_NAMES,
            ExternalCatalog.META_NAMES_MAPPING,
            ExternalCatalog.INCLUDE_DATABASE_LIST,
            ExternalCatalog.EXCLUDE_DATABASE_LIST,
            CONNECTION_POOL_MIN_SIZE,
            CONNECTION_POOL_MAX_SIZE,
            CONNECTION_POOL_MAX_LIFE_TIME,
            CONNECTION_POOL_MAX_WAIT_TIME,
            CONNECTION_POOL_KEEP_ALIVE,
            TEST_CONNECTION,
            ExternalCatalog.USE_META_CACHE,
            CatalogProperty.ENABLE_MAPPING_VARBINARY,
            CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ
    ).build();

    // The default value of optional properties
    // if one optional property is not specified, will use default value
    private static final Map<String, String> OPTIONAL_PROPERTIES_DEFAULT_VALUE = Maps.newHashMap();

    static {
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ONLY_SPECIFIED_DATABASE, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.LOWER_CASE_META_NAMES, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.META_NAMES_MAPPING, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.INCLUDE_DATABASE_LIST, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.EXCLUDE_DATABASE_LIST, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MIN_SIZE, "1");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_SIZE, "30");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_LIFE_TIME, "1800000");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_WAIT_TIME, "5000");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_KEEP_ALIVE, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(TEST_CONNECTION, "true");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.USE_META_CACHE,
                String.valueOf(ExternalCatalog.DEFAULT_USE_META_CACHE));
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CatalogProperty.ENABLE_MAPPING_VARBINARY, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ, "false");
    }

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public JdbcResource() {
        super();
    }

    public JdbcResource(String name) {
        this(name, Maps.newHashMap());
    }

    public JdbcResource(String name, Map<String, String> configs) {
        super(name, ResourceType.JDBC);
        this.configs = configs;
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // modify properties
        for (String propertyKey : ALL_PROPERTIES) {
            replaceIfEffectiveValue(this.configs, propertyKey, properties.get(propertyKey));
        }
        super.modifyProperties(properties);
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        // check properties
        for (String propertyKey : ALL_PROPERTIES) {
            copiedProperties.remove(propertyKey);
        }
        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown JDBC catalog resource properties: " + copiedProperties);
        }
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.configs = Maps.newHashMap(properties);
        validateProperties(this.configs);
        validateThroughConnector(this.configs);
        applyDefaultProperties();
        String currentDateTime = TimeUtils.longToTimeString(System.currentTimeMillis());
        configs.put(CREATE_TIME, currentDateTime);
        // check properties
        for (String property : ALL_PROPERTIES) {
            String value = configs.get(property);
            if (value == null) {
                throw new DdlException("JdbcResource Missing " + property + " in properties");
            }
        }
        computeDriverChecksumThroughConnector(this.configs);
    }

    /**
     * This function used to handle optional arguments
     * eg: only_specified_database、lower_case_table_names
     */

    @Override
    public void applyDefaultProperties() {
        for (String s : OPTIONAL_PROPERTIES_DEFAULT_VALUE.keySet()) {
            if (!configs.containsKey(s)) {
                configs.put(s, OPTIONAL_PROPERTIES_DEFAULT_VALUE.get(s));
            }
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(configs);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            // it's dangerous to show password in show jdbc resource
            // so we use empty string to replace the real password
            if (entry.getKey().equals(PASSWORD)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), ""));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }

    public String getProperty(String propertiesKey) {
        // check the properties key
        return configs.get(propertiesKey);
    }

    public static String getDefaultPropertyValue(String propertyName) {
        return OPTIONAL_PROPERTIES_DEFAULT_VALUE.getOrDefault(propertyName, "");
    }

    public static void validateProperties(Map<String, String> properties) throws DdlException {
        for (String key : properties.keySet()) {
            if (!ALL_PROPERTIES.contains(key)) {
                throw new DdlException("JDBC resource Property of " + key + " is unknown");
            }
        }
    }

    /** The connector type the plugin that validates this resource answers to: the resource type's name. */
    private String connectorType() {
        return type.name().toLowerCase(Locale.ROOT);
    }

    /**
     * The value rules of the connector that serves a JDBC catalog — required keys, booleans, connection-pool
     * bounds, the driver_url grammar — applied to the resource's properties, so a resource is held to
     * exactly what a catalog is held to and the engine keeps no copy of those rules.
     */
    private void validateThroughConnector(Map<String, String> properties) throws DdlException {
        String connectorType = connectorType();
        if (!ConnectorFactory.findProvider(connectorType, properties).isPresent()) {
            throw new DdlException("JDBC resource requires the '" + connectorType
                    + "' connector plugin, which is not installed");
        }
        try {
            ConnectorFactory.validateProperties(connectorType, properties);
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    /**
     * Resolves the driver jar and records its checksum under {@link #CHECK_SUM}, through the same
     * pre-creation validation a JDBC catalog runs. The BE connectivity test that validation may request is
     * left unsent: a resource never tested connectivity. Skipped under unit tests, where no driver jar exists
     * (the checksum was likewise not computed there before).
     */
    private void computeDriverChecksumThroughConnector(Map<String, String> properties) throws DdlException {
        if (FeConstants.runningUnitTest) {
            properties.put(CHECK_SUM, "");
            return;
        }
        Connector connector = ConnectorFactory.createConnector(connectorType(), properties,
                DefaultConnectorContext.forCatalogCreationValidation(name, -1L, properties));
        if (connector == null) {
            throw new DdlException("JDBC resource requires the '" + connectorType()
                    + "' connector plugin, which is not installed");
        }
        try {
            connector.preCreateValidation(new DefaultConnectorValidationContext(-1L,
                    new CatalogProperty(null, properties)));
        } catch (DdlException e) {
            throw e;
        } catch (Exception e) {
            throw new DdlException(e.getMessage(), e);
        } finally {
            try {
                connector.close();
            } catch (IOException e) {
                LOG.warn("Failed to close the connector that validated resource {}", name, e);
            }
        }
    }
}
