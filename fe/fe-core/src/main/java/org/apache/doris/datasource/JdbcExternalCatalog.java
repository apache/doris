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
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.external.jdbc.JdbcClient;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
public class JdbcExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalCatalog.class);

    public static final String PROP_USER = "jdbc.user";
    public static final String PROP_PASSWORD = "jdbc.password";
    public static final String PROP_JDBC_URL = "jdbc.jdbc_url";
    public static final String PROP_DRIVER_URL = "jdbc.driver_url";
    public static final String PROP_DRIVER_CLASS = "jdbc.driver_class";

    private JdbcClient jdbcClient;
    private String databaseTypeName;
    private String jdbcUser;
    private String jdbcPasswd;
    private String jdbcUrl;
    private String driverUrl;
    private String driverClass;
    private String checkSum;

    public JdbcExternalCatalog(long catalogId, String name, Map<String, String> props) {
        this.id = catalogId;
        this.name = name;
        this.type = "jdbc";
        setProperties(props);
        this.catalogProperty = new CatalogProperty();
        this.catalogProperty.setProperties(props);
    }

    @Override
    public void onClose() {
        if (jdbcClient != null) {
            jdbcClient.closeClient();
        }
    }

    private void setProperties(Map<String, String> props) {
        jdbcUser = props.getOrDefault(PROP_USER, "");
        jdbcPasswd = props.getOrDefault(PROP_PASSWORD, "");
        jdbcUrl = props.getOrDefault(PROP_JDBC_URL, "");
        handleJdbcUrl();
        driverUrl = props.getOrDefault(PROP_DRIVER_URL, "");
        driverClass = props.getOrDefault(PROP_DRIVER_CLASS, "");
    }

    // `yearIsDateType` is a parameter of JDBC, and the default is `yearIsDateType=true`
    // We force the use of `yearIsDateType=false`
    private void handleJdbcUrl() {
        // delete all space in jdbcUrl
        jdbcUrl.replaceAll(" ", "");
        if (jdbcUrl.contains("yearIsDateType=false")) {
            return;
        } else if (jdbcUrl.contains("yearIsDateType=true")) {
            jdbcUrl.replaceAll("yearIsDateType=true", "yearIsDateType=false");
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
                db.setUnInitialized();
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
        setProperties(this.catalogProperty.getProperties());
    }

    @Override
    public List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        return jdbcClient.getColumnsFromJdbc(dbName, tblName);
    }
}
