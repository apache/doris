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
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.IcebergExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    protected boolean invalidCacheInInit = true;

    private ExternalSchemaCache schemaCache;
    private String comment;

    public ExternalCatalog(long catalogId, String name) {
        this.id = catalogId;
        this.name = name;
    }

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
     * check if the specified table exist in doris.
     *
     * @param dbName
     * @param tblName
     * @return true if table exists, false otherwise
     */
    public boolean tableExistInLocal(String dbName, String tblName) {
        throw new NotImplementedException("tableExistInLocal not implemented");
    }

    /**
     * Catalog can't be init when creating because the external catalog may depend on third system.
     * So you have to make sure the client of third system is initialized before any method was called.
     */
    public final synchronized void makeSureInitialized() {
        initLocalObjects();
        if (!initialized) {
            if (!Env.getCurrentEnv().isMaster()) {
                // Forward to master and wait the journal to replay.
                int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeout();
                MasterCatalogExecutor remoteExecutor = new MasterCatalogExecutor(waitTimeOut * 1000);
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

    public boolean isInitialized() {
        return this.initialized;
    }

    // init some local objects such as:
    // hms client, read properties from hive-site.xml, es client
    protected abstract void initLocalObjectsImpl();

    // check if all required properties are set when creating catalog
    public void checkProperties() throws DdlException {
        // check refresh parameter of catalog
        Map<String, String> properties = getCatalogProperty().getProperties();
        if (properties.containsKey(CatalogMgr.METADATA_REFRESH_INTERVAL_SEC)) {
            try {
                Integer.valueOf(properties.get(CatalogMgr.METADATA_REFRESH_INTERVAL_SEC));
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid properties: " + CatalogMgr.METADATA_REFRESH_INTERVAL_SEC);
            }
        }
    }

    /**
     * eg:
     * (
     * ""access_controller.class" = "org.apache.doris.mysql.privilege.RangerHiveAccessControllerFactory",
     * "access_controller.properties.prop1" = "xxx",
     * "access_controller.properties.prop2" = "yyy",
     * )
     */
    public void initAccessController() {
        Map<String, String> properties = getCatalogProperty().getProperties();
        // 1. get access controller class name
        String className = properties.getOrDefault(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "");
        if (Strings.isNullOrEmpty(className)) {
            // not set access controller, use internal access controller
            return;
        }

        // 2. get access controller properties
        Map<String, String> acProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP)) {
                acProperties.put(
                        StringUtils.removeStart(entry.getKey(), CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP),
                        entry.getValue());
            }
        }

        // 3. create access controller
        Env.getCurrentEnv().getAccessManager().createAccessController(name, className, acProperties);
    }

    // init schema related objects
    protected abstract void init();

    public void setUninitialized(boolean invalidCache) {
        this.objectCreated = false;
        this.initialized = false;
        this.invalidCacheInInit = invalidCache;
        if (invalidCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalogCache(id);
        }
    }

    public void updateDbList() {
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalogCache(id);
    }

    public ExternalDatabase getDbForReplay(long dbId) {
        return idToDb.get(dbId);
    }

    public final List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        Optional<ExternalDatabase> db = getDb(dbName);
        if (db.isPresent()) {
            Optional table = db.get().getTable(tblName);
            if (table.isPresent()) {
                return ((ExternalTable) table.get()).initSchema();
            }
        }
        // return one column with unsupported type.
        // not return empty to avoid some unexpected issue.
        return Lists.newArrayList(Column.UNSUPPORTED_COLUMN);
    }

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
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public List<String> getDbNames() {
        return listDatabaseNames(null);
    }

    @Override
    public List<String> getDbNamesOrEmpty() {
        if (initialized) {
            try {
                return getDbNames();
            } catch (Exception e) {
                LOG.warn("failed to get db names in catalog {}", getName(), e);
                return Lists.newArrayList();
            }
        } else {
            return Lists.newArrayList();
        }
    }

    @Override
    public String getResource() {
        return catalogProperty.getResource();
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(String dbName) {
        try {
            makeSureInitialized();
        } catch (Exception e) {
            LOG.warn("failed to get db {} in catalog {}", dbName, name, e);
            return null;
        }
        String realDbName = ClusterNamespace.getNameFromFullName(dbName);
        if (!dbNameToId.containsKey(realDbName)) {
            return null;
        }
        return idToDb.get(dbNameToId.get(realDbName));
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(long dbId) {
        try {
            makeSureInitialized();
        } catch (Exception e) {
            LOG.warn("failed to get db {} in catalog {}", dbId, name, e);
            return null;
        }
        return idToDb.get(dbId);
    }

    @Override
    public List<Long> getDbIds() {
        makeSureInitialized();
        return Lists.newArrayList(dbNameToId.values());
    }

    @Override
    public Map<String, String> getProperties() {
        return PropertyConverter.convertToMetaProperties(catalogProperty.getProperties());
    }

    @Override
    public void modifyCatalogName(String name) {
        this.name = name;
    }

    @Override
    public void modifyCatalogProps(Map<String, String> props) {
        modifyComment(props);
        catalogProperty.modifyCatalogProps(props);
        notifyPropertiesUpdated(props);
    }

    private void modifyComment(Map<String, String> props) {
        setComment(props.getOrDefault("comment", comment));
        props.remove("comment");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void onClose() {
        removeAccessController();
        CatalogIf.super.onClose();
    }

    private void removeAccessController() {
        Env.getCurrentEnv().getAccessManager().removeAccessController(name);
    }

    public void replayInitCatalog(InitCatalogLog log) {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase> tmpIdToDb = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            ExternalDatabase db = getDbForReplay(log.getRefreshDbIds().get(i));
            db.setUnInitialized(invalidCacheInInit);
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
            case ICEBERG:
                for (int i = 0; i < log.getCreateCount(); i++) {
                    IcebergExternalDatabase db = new IcebergExternalDatabase(
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
        if (idToDb == null) {
            // ExternalCatalog is loaded from meta with older version
            idToDb = Maps.newConcurrentMap();
        }
        dbNameToId = Maps.newConcurrentMap();
        for (ExternalDatabase db : idToDb.values()) {
            dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
            db.setExtCatalog(this);
            db.setTableExtCatalog(this);
        }
        objectCreated = false;
        if (this instanceof HMSExternalCatalog) {
            ((HMSExternalCatalog) this).setLastSyncedEventId(-1L);
        }
    }

    public void addDatabaseForTest(ExternalDatabase db) {
        idToDb.put(db.getId(), db);
        dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
    }

    public void dropDatabase(String dbName) {
        throw new NotImplementedException("dropDatabase not implemented");
    }

    public void createDatabase(long dbId, String dbName) {
        throw new NotImplementedException("createDatabase not implemented");
    }

    public Map getIncludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.INCLUDE_DATABASE_LIST);
    }

    public Map getExcludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.EXCLUDE_DATABASE_LIST);
    }

    public Map getSpecifiedDatabaseMap(String catalogPropertyKey) {
        String specifiedDatabaseList = catalogProperty.getOrDefault(catalogPropertyKey, "");
        Map<String, Boolean> specifiedDatabaseMap = Maps.newHashMap();
        specifiedDatabaseList = specifiedDatabaseList.trim();
        if (specifiedDatabaseList.isEmpty()) {
            return specifiedDatabaseMap;
        }
        String[] databaseList = specifiedDatabaseList.split(",");
        for (int i = 0; i < databaseList.length; i++) {
            String dbname = databaseList[i].trim();
            if (!dbname.isEmpty()) {
                specifiedDatabaseMap.put(dbname, true);
            }
        }
        return specifiedDatabaseMap;
    }
}
