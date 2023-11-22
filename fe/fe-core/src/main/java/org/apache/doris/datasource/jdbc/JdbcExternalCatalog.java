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

        if (properties.containsKey(JdbcResource.DRIVER_URL) && !properties.containsKey(JdbcResource.CHECK_SUM)) {
            properties.put(JdbcResource.CHECK_SUM,
                    JdbcResource.computeObjectChecksum(properties.get(JdbcResource.DRIVER_URL)));
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
        return catalogProperty.getOrDefault(JdbcResource.ONLY_SPECIFIED_DATABASE, "false");
    }

    public String getLowerCaseTableNames() {
        // Forced to true if Config.lower_case_table_names has a value of 1 or 2
        if (Config.lower_case_table_names == 1 || Config.lower_case_table_names == 2) {
            return "true";
        }

        // Otherwise, it defaults to false
        return catalogProperty.getOrDefault(JdbcResource.LOWER_CASE_TABLE_NAMES, "false");
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
                .setExcludeDatabaseMap(getExcludeDatabaseMap());

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
        Map<String, String> properties = Maps.newHashMap();
        if (properties.containsKey(JdbcResource.DRIVER_URL) && !properties.containsKey(JdbcResource.CHECK_SUM)) {
            properties.put(JdbcResource.CHECK_SUM,
                    JdbcResource.computeObjectChecksum(properties.get(JdbcResource.DRIVER_URL)));
        }
        String onlySpecifiedDatabase = getOnlySpecifiedDatabase();
        if (!onlySpecifiedDatabase.equalsIgnoreCase("true") && !onlySpecifiedDatabase.equalsIgnoreCase("false")) {
            throw new DdlException("only_specified_database must be true or false");
        }
        String lowerCaseTableNames = getLowerCaseTableNames();
        if (!lowerCaseTableNames.equalsIgnoreCase("true") && !lowerCaseTableNames.equalsIgnoreCase("false")) {
            throw new DdlException("lower_case_table_names must be true or false");
        }
        if (!onlySpecifiedDatabase.equalsIgnoreCase("true")) {
            Map<String, Boolean> includeDatabaseList = getIncludeDatabaseMap();
            Map<String, Boolean> excludeDatabaseList = getExcludeDatabaseMap();
            if ((includeDatabaseList != null && !includeDatabaseList.isEmpty())
                    || (excludeDatabaseList != null && !excludeDatabaseList.isEmpty())) {
                throw new DdlException("include_database_list and exclude_database_list can not be set when "
                        + "only_specified_database is false");
            }
        }
    }
}
