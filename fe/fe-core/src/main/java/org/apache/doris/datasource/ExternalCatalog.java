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
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.MasterCatalogExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The abstract class for all types of external catalogs.
 */
@Data
public abstract class ExternalCatalog implements CatalogIf<ExternalDatabase>, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalCatalog.class);

    // Unique id of this catalog, will be assigned after catalog is loaded.
    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected String type;
    // save properties of this catalog, such as hive meta store url.
    @SerializedName(value = "catalogProperty")
    protected CatalogProperty catalogProperty;
    @SerializedName(value = "initialized")
    private boolean initialized = false;
    @SerializedName(value = "idToDb")
    protected Map<Long, ExternalDatabase> idToDb = Maps.newConcurrentMap();
    // db name does not contains "default_cluster"
    protected Map<String, Long> dbNameToId = Maps.newConcurrentMap();
    private boolean objectCreated = false;

    private ExternalSchemaCache schemaCache;

    /**
     * @return names of database in this catalog.
     */
    public abstract List<String> listDatabaseNames(SessionContext ctx);

    /**
     * @param dbName
     * @return names of tables in specified database
     */
    public abstract List<String> listTableNames(SessionContext ctx, String dbName);

    /**
     * check if the specified table exist.
     *
     * @param dbName
     * @param tblName
     * @return true if table exists, false otherwise
     */
    public abstract boolean tableExist(SessionContext ctx, String dbName, String tblName);

    /**
     * Catalog can't be init when creating because the external catalog may depend on third system.
     * So you have to make sure the client of third system is initialized before any method was called.
     */
    public final synchronized void makeSureInitialized() {
        initLocalObjects();
        if (!initialized) {
            if (!Env.getCurrentEnv().isMaster()) {
                // Forward to master and wait the journal to replay.
                MasterCatalogExecutor remoteExecutor = new MasterCatalogExecutor();
                try {
                    remoteExecutor.forward(id, -1);
                } catch (Exception e) {
                    Util.logAndThrowRuntimeException(LOG,
                            String.format("failed to forward init catalog %s operation to master.", name), e);
                }
                return;
            }
            init();
            initialized = true;
        }
    }

    protected final void initLocalObjects() {
        if (!objectCreated) {
            initLocalObjectsImpl();
            objectCreated = true;
        }
    }

    // init some local objects such as:
    // hms client, read properties from hive-site.xml, es client
    protected abstract void initLocalObjectsImpl();

    // init schema related objects
    protected abstract void init();

    public void setUninitialized() {
        this.initialized = false;
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalogCache(id);
    }

    public ExternalDatabase getDbForReplay(long dbId) {
        return idToDb.get(dbId);
    }

    public abstract List<Column> getSchema(String dbName, String tblName);

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public List<String> getDbNames() {
        return listDatabaseNames(null);
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

    @Override
    public Map<String, String> getProperties() {
        return catalogProperty.getProperties();
    }

    @Override
    public void modifyCatalogName(String name) {
        this.name = name;
    }

    @Override
    public void modifyCatalogProps(Map<String, String> props) {
        catalogProperty.getProperties().putAll(props);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void replayInitCatalog(InitCatalogLog log) {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            ExternalDatabase db = getDbForReplay(log.getRefreshDbIds().get(i));
            db.setUnInitialized();
            tmpDbNameToId.put(db.getFullName(), db.getId());
            tmpIdToDb.put(db.getId(), db);
        }
        switch (log.getType()) {
            case HMS:
                for (int i = 0; i < log.getCreateCount(); i++) {
                    HMSExternalDatabase db = new HMSExternalDatabase(
                            this, log.getCreateDbIds().get(i), log.getCreateDbNames().get(i));
                    tmpDbNameToId.put(db.getFullName(), db.getId());
                    tmpIdToDb.put(db.getId(), db);
                }
                break;
            case ES:
                for (int i = 0; i < log.getCreateCount(); i++) {
                    EsExternalDatabase db = new EsExternalDatabase(
                            this, log.getCreateDbIds().get(i), log.getCreateDbNames().get(i));
                    tmpDbNameToId.put(db.getFullName(), db.getId());
                    tmpIdToDb.put(db.getId(), db);
                }
                break;
            case JDBC:
                for (int i = 0; i < log.getCreateCount(); i++) {
                    JdbcExternalDatabase db = new JdbcExternalDatabase(
                            this, log.getCreateDbIds().get(i), log.getCreateDbNames().get(i));
                    tmpDbNameToId.put(db.getFullName(), db.getId());
                    tmpIdToDb.put(db.getId(), db);
                }
                break;
            default:
                break;
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        initialized = true;
    }

    /**
     * External catalog has no cluster semantics.
     */
    protected static String getRealTableName(String tableName) {
        return ClusterNamespace.getNameFromFullName(tableName);
    }

    public static ExternalCatalog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalCatalog.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        dbNameToId = Maps.newConcurrentMap();
        for (ExternalDatabase db : idToDb.values()) {
            dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
            db.setExtCatalog(this);
            db.setTableExtCatalog(this);
        }
        objectCreated = false;
    }

    public void addDatabaseForTest(ExternalDatabase db) {
        idToDb.put(db.getId(), db);
        dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
    }
}
