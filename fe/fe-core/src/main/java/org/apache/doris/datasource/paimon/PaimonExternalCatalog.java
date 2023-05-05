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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.PaimonExternalDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PaimonExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_HMS = "hms";
    protected String paimonCatalogType;
    protected Catalog catalog;

    public PaimonExternalCatalog(long catalogId, String name) {
        super(catalogId, name);
    }

    @Override
    protected void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.PAIMON);
        List<String> allDatabaseNames = listDatabaseNames();
        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();
        for (String dbName : allDatabaseNames) {
            // Exclude database map take effect with higher priority over include database map
            if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(dbName)) {
                continue;
            }
            if (!includeDatabaseMap.isEmpty() && includeDatabaseMap.containsKey(dbName)) {
                continue;
            }
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
                PaimonExternalDatabase db = new PaimonExternalDatabase(this, dbId, dbName);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, dbName);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    protected Configuration getConfiguration() {
        Configuration conf = new HdfsConfiguration();
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    public Catalog getCatalog() {
        makeSureInitialized();
        return catalog;
    }

    public String getPaimonCatalogType() {
        makeSureInitialized();
        return paimonCatalogType;
    }

    protected List<String> listDatabaseNames() {
        return catalog.listDatabases().stream()
            .map(e -> {
                String dbName = e.toString();
                try {
                    FeNameFormat.checkDbName(dbName);
                } catch (AnalysisException ex) {
                    Util.logAndThrowRuntimeException(LOG,
                            String.format("Not a supported  name format: %s", dbName), ex);
                }
                return dbName;
            })
            .collect(Collectors.toList());
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return new ArrayList<>(dbNameToId.keySet());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return catalog.tableExists(Identifier.create(dbName, tblName));
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<String> tableNames = null;
        try {
            tableNames = catalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            LOG.warn("DatabaseNotExistException", e);
        }
        return tableNames;
    }

    public org.apache.paimon.table.Table getPaimonTable(String dbName, String tblName) {
        makeSureInitialized();
        org.apache.paimon.table.Table table = null;
        try {
            table = catalog.getTable(Identifier.create(dbName, tblName));
        } catch (Catalog.TableNotExistException e) {
            LOG.warn("TableNotExistException", e);
        }
        return table;
    }
}
