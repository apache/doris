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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.IcebergExternalDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalCatalog.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    protected String icebergCatalogType;
    protected Catalog catalog;
    protected SupportsNamespaces nsCatalog;

    public IcebergExternalCatalog(long catalogId, String name) {
        super(catalogId, name);
    }

    @Override
    protected void init() {
        nsCatalog = (SupportsNamespaces) catalog;
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(InitCatalogLog.Type.ICEBERG);
        List<String> allDatabaseNames = listDatabaseNames();
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
                IcebergExternalDatabase db = new IcebergExternalDatabase(this, dbId, dbName);
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

    public SupportsNamespaces getNsCatalog() {
        makeSureInitialized();
        return nsCatalog;
    }

    public String getIcebergCatalogType() {
        makeSureInitialized();
        return icebergCatalogType;
    }

    protected List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
            .map(e -> {
                String dbName = e.toString();
                try {
                    FeNameFormat.checkDbName(dbName);
                } catch (AnalysisException ex) {
                    Util.logAndThrowRuntimeException(LOG,
                            String.format("Not a supported namespace name format: %s", dbName), ex);
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
        return catalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    public org.apache.iceberg.Table getIcebergTable(String dbName, String tblName) {
        makeSureInitialized();
        return catalog.loadTable(TableIdentifier.of(dbName, tblName));
    }
}
