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
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * External catalog for hive metastore compatible data sources.
 */
public class HMSExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(HMSExternalCatalog.class);

    // Cache of db name to db id.
    private Map<String, Long> dbNameToId;
    private Map<Long, HMSExternalDatabase> idToDb;
    protected HiveMetaStoreClient client;

    /**
     * Default constructor for HMSExternalCatalog.
     */
    public HMSExternalCatalog(long catalogId, String name, Map<String, String> props) {
        this.id = catalogId;
        this.name = name;
        this.type = "hms";
        this.catalogProperty = new CatalogProperty();
        this.catalogProperty.setProperties(props);
    }

    public String getHiveMetastoreUris() {
        return catalogProperty.getOrDefault("hive.metastore.uris", "");
    }

    private void init() {
        // Must set here. Because after replay from image, these 2 map will become null again.
        dbNameToId = Maps.newConcurrentMap();
        idToDb = Maps.newConcurrentMap();
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, getHiveMetastoreUris());
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            LOG.warn("Failed to create HiveMetaStoreClient: {}", e.getMessage());
        }
        List<String> allDatabases;
        try {
            allDatabases = client.getAllDatabases();
        } catch (MetaException e) {
            LOG.warn("Fail to init db name to id map. {}", e.getMessage());
            return;
        }
        // Update the db name to id map.
        if (allDatabases == null) {
            return;
        }
        for (String dbName : allDatabases) {
            long dbId = Env.getCurrentEnv().getNextId();
            dbNameToId.put(dbName, dbId);
            idToDb.put(dbId, new HMSExternalDatabase(this, dbId, dbName));
        }
    }

    /**
     * Catalog can't be init when creating because the external catalog may depend on third system.
     * So you have to make sure the client of third system is initialized before any method was called.
     */
    private synchronized void makeSureInitialized() {
        if (!initialized) {
            init();
            initialized = true;
        }
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.keySet());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        try {
            return client.getAllTables(getRealTableName(dbName));
        } catch (MetaException e) {
            LOG.warn("List Table Names failed. {}", e.getMessage());
        }
        return Lists.newArrayList();
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        try {
            return client.tableExists(getRealTableName(dbName), tblName);
        } catch (TException e) {
            LOG.warn("Check table exist failed. {}", e.getMessage());
        }
        return false;
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(String dbName) {
        makeSureInitialized();
        String realDbName = ClusterNamespace.getNameFromFullName(dbName);
        if (!dbNameToId.containsKey(realDbName)) {
            return null;
        }
        return idToDb.get(dbNameToId.get(realDbName));
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(long dbId) {
        makeSureInitialized();
        return idToDb.get(dbId);
    }

    @Override
    public List<Long> getDbIds() {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.values());
    }
}
