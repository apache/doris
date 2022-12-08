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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.jdbc.JdbcClient;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
public class JdbcExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalCatalog.class);

    // Must add "transient" for Gson to ignore this field,
    // or Gson will throw exception with HikariCP
    private transient JdbcClient jdbcClient;
    private String databaseTypeName;
    private String jdbcUser;
    private String jdbcPasswd;
    private String jdbcUrl;
    private String driverUrl;
    private String driverClass;
    private String checkSum;

    public JdbcExternalCatalog(
            long catalogId, String name, String resource, Map<String, String> props) throws DdlException {
        this.id = catalogId;
        this.name = name;
        this.type = "jdbc";
        if (resource == null) {
            catalogProperty = new CatalogProperty(null, processCompatibleProperties(props));
        } else {
            catalogProperty = new CatalogProperty(resource, Collections.emptyMap());
            processCompatibleProperties(catalogProperty.getProperties());
        }
    }

    @Override
    public void onClose() {
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    private Map<String, String> processCompatibleProperties(Map<String, String> props) {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            properties.put(StringUtils.removeStart(kv.getKey(), JdbcResource.JDBC_PROPERTIES_PREFIX), kv.getValue());
        }
        jdbcUser = properties.getOrDefault(JdbcResource.USER, "");
        jdbcPasswd = properties.getOrDefault(JdbcResource.PASSWORD, "");
        jdbcUrl = properties.getOrDefault(JdbcResource.URL, "");
        handleJdbcUrl();
        properties.put(JdbcResource.URL, jdbcUrl);
        driverUrl = properties.getOrDefault(JdbcResource.DRIVER_URL, "");
        driverClass = properties.getOrDefault(JdbcResource.DRIVER_CLASS, "");
        return properties;
    }

    // `yearIsDateType` is a parameter of JDBC, and the default is `yearIsDateType=true`
    // We force the use of `yearIsDateType=false`
    private void handleJdbcUrl() {
        // delete all space in jdbcUrl
        jdbcUrl = jdbcUrl.replaceAll(" ", "");
        if (jdbcUrl.contains("yearIsDateType=false")) {
            return;
        } else if (jdbcUrl.contains("yearIsDateType=true")) {
            jdbcUrl = jdbcUrl.replaceAll("yearIsDateType=true", "yearIsDateType=false");
        } else {
            String yearIsDateType = "yearIsDateType=false";
            if (jdbcUrl.contains("?")) {
                if (jdbcUrl.charAt(jdbcUrl.length() - 1) != '?') {
                    jdbcUrl += "&";
                }
            } else {
                jdbcUrl += "?";
            }
            jdbcUrl += yearIsDateType;
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
        jdbcClient = new JdbcClient(jdbcUser, jdbcPasswd, jdbcUrl, driverUrl, driverClass);
        databaseTypeName = jdbcClient.getDbType();
        checkSum = jdbcClient.getCheckSum();
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

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (catalogProperty.getResource() == null) {
            catalogProperty.setProperties(processCompatibleProperties(catalogProperty.getProperties()));
        } else {
            processCompatibleProperties(catalogProperty.getProperties());
        }
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        return jdbcClient.getColumnsFromJdbc(dbName, tblName);
    }
}
