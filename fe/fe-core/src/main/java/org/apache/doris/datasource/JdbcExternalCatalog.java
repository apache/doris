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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.jdbc.JdbcClient;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

@Getter
public class JdbcExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalCatalog.class);

    private static final List<String> REQUIRED_PROPERTIES = Lists.newArrayList(
            JdbcResource.JDBC_URL,
            JdbcResource.DRIVER_URL,
            JdbcResource.DRIVER_CLASS
    );

    // Must add "transient" for Gson to ignore this field,
    // or Gson will throw exception with HikariCP
    private transient JdbcClient jdbcClient;

    public JdbcExternalCatalog(long catalogId, String name, String resource, Map<String, String> props)
            throws DdlException {
        super(catalogId, name);
        this.type = "jdbc";
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
        String oceanbaseMode = properties.getOrDefault(JdbcResource.OCEANBASE_MODE, "");
        if (!Strings.isNullOrEmpty(jdbcUrl)) {
            jdbcUrl = JdbcResource.handleJdbcUrl(jdbcUrl, oceanbaseMode);
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
        return catalogProperty.getOrDefault(JdbcResource.LOWER_CASE_TABLE_NAMES, "false");
    }

    public String getSpecifiedDatabaseList() {
        return catalogProperty.getOrDefault(JdbcResource.SPECIFIED_DATABASE_LIST, "");
    }

    public String getOceanBaseMode() {
        return catalogProperty.getOrDefault(JdbcResource.OCEANBASE_MODE, "");
    }

    @Override
    protected void initLocalObjectsImpl() {
        jdbcClient = new JdbcClient(getJdbcUser(), getJdbcPasswd(), getJdbcUrl(), getDriverUrl(), getDriverClass(),
                getOnlySpecifiedDatabase(), getLowerCaseTableNames(), getSpecifiedDatabaseMap(), getOceanBaseMode());
    }

    @Override
    protected void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.JDBC);
        List<String> allDatabaseNames = jdbcClient.getDatabaseNameList();
        for (String dbName : allDatabaseNames) {
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(dbName)) {
                dbId = dbNameToId.get(dbName);
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase db = idToDb.get(dbId);
                db.setUnInitialized(invalidCacheInInit);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(dbName, dbId);
                JdbcExternalDatabase db = new JdbcExternalDatabase(this, dbId, dbName);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, dbName);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        JdbcExternalDatabase db = (JdbcExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (db != null && db.isInitialized()) {
            List<String> names = Lists.newArrayList();
            db.getTables().stream().forEach(table -> names.add(table.getName()));
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
}
