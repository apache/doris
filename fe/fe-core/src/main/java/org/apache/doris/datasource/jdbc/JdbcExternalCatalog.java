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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Getter
public class JdbcExternalCatalog extends ExternalCatalog {
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            JdbcResource.JDBC_URL,
            JdbcResource.DRIVER_URL,
            JdbcResource.DRIVER_CLASS
    );

    // Must add "transient" for Gson to ignore this field,
    // or Gson will throw exception with HikariCP
    private transient JdbcClient jdbcClient;

    public JdbcExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment)
            throws DdlException {
        super(catalogId, name, InitCatalogLog.Type.JDBC, comment);
        this.catalogProperty = new CatalogProperty(resource, processCompatibleProperties(props));
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }

        Map<String, String> propertiesWithoutCheckSum = Maps.newHashMap(catalogProperty.getProperties());
        propertiesWithoutCheckSum.remove(JdbcResource.CHECK_SUM);
        JdbcResource.validateProperties(propertiesWithoutCheckSum);

        JdbcResource.checkBooleanProperty(JdbcResource.ONLY_SPECIFIED_DATABASE, getOnlySpecifiedDatabase());
        JdbcResource.checkBooleanProperty(JdbcResource.LOWER_CASE_TABLE_NAMES, getLowerCaseTableNames());
        JdbcResource.checkDatabaseListProperties(getOnlySpecifiedDatabase(), getIncludeDatabaseMap(),
                getExcludeDatabaseMap());
        JdbcResource.checkConnectionPoolProperties(getConnectionPoolMinSize(), getConnectionPoolMaxSize(),
                getConnectionPoolMaxWaitTime(), getConnectionPoolMaxLifeTime());
    }

    @Override
    public void onRefresh(boolean invalidCache) {
        super.onRefresh(invalidCache);
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    private Map<String, String> processCompatibleProperties(Map<String, String> props) throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            properties.put(StringUtils.removeStart(kv.getKey(), JdbcResource.JDBC_PROPERTIES_PREFIX), kv.getValue());
        }
        String jdbcUrl = properties.getOrDefault(JdbcResource.JDBC_URL, "");
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            jdbcUrl = JdbcResource.handleJdbcUrl(jdbcUrl);
            properties.put(JdbcResource.JDBC_URL, jdbcUrl);
        }
        return properties;
    }

    public String getDatabaseTypeName() {
        return jdbcClient.getDbType();
    }

    public String getJdbcUser() {
        return catalogProperty.getOrDefault(JdbcResource.USER, "");
    }

    public String getJdbcPasswd() {
        return catalogProperty.getOrDefault(JdbcResource.PASSWORD, "");
    }

    public String getJdbcUrl() {
        return catalogProperty.getOrDefault(JdbcResource.JDBC_URL, "");
    }

    public String getDriverUrl() {
        return catalogProperty.getOrDefault(JdbcResource.DRIVER_URL, "");
    }

    public String getDriverClass() {
        return catalogProperty.getOrDefault(JdbcResource.DRIVER_CLASS, "");
    }

    public String getCheckSum() {
        return catalogProperty.getOrDefault(JdbcResource.CHECK_SUM, "");
    }

    public String getOnlySpecifiedDatabase() {
        return catalogProperty.getOrDefault(JdbcResource.ONLY_SPECIFIED_DATABASE, JdbcResource.getDefaultPropertyValue(
                JdbcResource.ONLY_SPECIFIED_DATABASE));
    }

    public String getLowerCaseTableNames() {
        // Forced to true if Config.lower_case_table_names has a value of 1 or 2
        if (Config.lower_case_table_names == 1 || Config.lower_case_table_names == 2) {
            return "true";
        }

        // Otherwise, it defaults to false
        return catalogProperty.getOrDefault(JdbcResource.LOWER_CASE_TABLE_NAMES, JdbcResource.getDefaultPropertyValue(
                JdbcResource.LOWER_CASE_TABLE_NAMES));
    }

    public int getConnectionPoolMinSize() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MIN_SIZE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MIN_SIZE)));
    }

    public int getConnectionPoolMaxSize() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_SIZE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_SIZE)));
    }

    public int getConnectionPoolMaxWaitTime() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_WAIT_TIME, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_WAIT_TIME)));
    }

    public int getConnectionPoolMaxLifeTime() {
        return Integer.parseInt(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_LIFE_TIME, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_MAX_LIFE_TIME)));
    }

    public boolean isConnectionPoolKeepAlive() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(JdbcResource.CONNECTION_POOL_KEEP_ALIVE, JdbcResource
                .getDefaultPropertyValue(JdbcResource.CONNECTION_POOL_KEEP_ALIVE)));
    }

    @Override
    protected void initLocalObjectsImpl() {
        JdbcClientConfig jdbcClientConfig = new JdbcClientConfig()
                .setCatalog(this.name)
                .setUser(getJdbcUser())
                .setPassword(getJdbcPasswd())
                .setJdbcUrl(getJdbcUrl())
                .setDriverUrl(getDriverUrl())
                .setDriverClass(getDriverClass())
                .setOnlySpecifiedDatabase(getOnlySpecifiedDatabase())
                .setIsLowerCaseTableNames(getLowerCaseTableNames())
                .setIncludeDatabaseMap(getIncludeDatabaseMap())
                .setExcludeDatabaseMap(getExcludeDatabaseMap())
                .setConnectionPoolMinSize(getConnectionPoolMinSize())
                .setConnectionPoolMaxSize(getConnectionPoolMaxSize())
                .setConnectionPoolMaxLifeTime(getConnectionPoolMaxLifeTime())
                .setConnectionPoolMaxWaitTime(getConnectionPoolMaxWaitTime())
                .setConnectionPoolKeepAlive(isConnectionPoolKeepAlive());

        jdbcClient = JdbcClient.createJdbcClient(jdbcClientConfig);
    }

    protected List<String> listDatabaseNames() {
        return jdbcClient.getDatabaseNameList();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        JdbcExternalDatabase db = (JdbcExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (db != null && db.isInitialized()) {
            List<String> names = Lists.newArrayList();
            db.getTables().forEach(table -> names.add(table.getName()));
            return names;
        } else {
            return jdbcClient.getTablesNameList(dbName);
        }
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return jdbcClient.isTableExist(dbName, tblName);
    }

    @Override
    public void setDefaultPropsWhenCreating(boolean isReplay) throws DdlException {
        if (isReplay) {
            return;
        }
        Map<String, String> properties = catalogProperty.getProperties();
        if (properties.containsKey(JdbcResource.DRIVER_URL)) {
            String computedChecksum = JdbcResource.computeObjectChecksum(properties.get(JdbcResource.DRIVER_URL));
            if (properties.containsKey(JdbcResource.CHECK_SUM)) {
                String providedChecksum = properties.get(JdbcResource.CHECK_SUM);
                if (!providedChecksum.equals(computedChecksum)) {
                    throw new DdlException(
                            "The provided checksum (" + providedChecksum
                                    + ") does not match the computed checksum (" + computedChecksum
                                    + ") for the driver_url."
                    );
                }
            } else {
                catalogProperty.addProperty(JdbcResource.CHECK_SUM, computedChecksum);
            }
        }
    }
}
