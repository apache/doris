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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.DeltaLakeExternalDataBase;
import org.apache.doris.catalog.external.EsExternalDatabase;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.IcebergExternalDatabase;
import org.apache.doris.catalog.external.JdbcExternalDatabase;
import org.apache.doris.catalog.external.MaxComputeExternalDatabase;
import org.apache.doris.catalog.external.PaimonExternalDatabase;
import org.apache.doris.catalog.external.TestExternalDatabase;
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The abstract class for all types of external catalogs.
 */
@Data
public abstract class ExternalCatalog
            implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalCatalog.class);

    public static final String ENABLE_AUTO_ANALYZE = "enable.auto.analyze";

    // Unique id of this catalog, will be assigned after catalog is loaded.
    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    // TODO: Keep this to compatible with older version meta data. Need to remove after several DORIS versions.
    protected String type;
    @SerializedName(value = "logType")
    protected InitCatalogLog.Type logType;
    // save properties of this catalog, such as hive meta store url.
    @SerializedName(value = "catalogProperty")
    protected CatalogProperty catalogProperty;
    @SerializedName(value = "initialized")
    private boolean initialized = false;
    @SerializedName(value = "idToDb")
    protected Map<Long, ExternalDatabase<? extends ExternalTable>> idToDb = Maps.newConcurrentMap();
    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;
    // db name does not contains "default_cluster"
    protected Map<String, Long> dbNameToId = Maps.newConcurrentMap();
    private boolean objectCreated = false;
    protected boolean invalidCacheInInit = true;

    private ExternalSchemaCache schemaCache;
    private String comment;

    public ExternalCatalog() {
    }

    public ExternalCatalog(long catalogId, String name, InitCatalogLog.Type logType, String comment) {
        this.id = catalogId;
        this.name = name;
        this.logType = logType;
        this.comment = com.google.common.base.Strings.nullToEmpty(comment);
    }

    public Configuration getConfiguration() {
        Configuration conf = new HdfsConfiguration();
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    protected List<String> listDatabaseNames() {
        throw new UnsupportedOperationException("Unsupported operation: "
                + "listDatabaseNames from remote client when init catalog with " + logType.name());
    }

    public void setDefaultPropsWhenCreating(boolean isReplay) throws DdlException {
        // set some default properties when creating catalog
    }

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
                Integer metadataRefreshIntervalSec = Integer.valueOf(
                        properties.get(CatalogMgr.METADATA_REFRESH_INTERVAL_SEC));
                if (metadataRefreshIntervalSec < 0) {
                    throw new DdlException("Invalid properties: " + CatalogMgr.METADATA_REFRESH_INTERVAL_SEC);
                }
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
     * <p>
     * isDryRun: if true, it will try to create the custom access controller, but will not add it to the access manager.
     */
    public void initAccessController(boolean isDryRun) {
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
        Env.getCurrentEnv().getAccessManager().createAccessController(name, className, acProperties, isDryRun);
    }

    // init schema related objects
    protected void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase<? extends ExternalTable>> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(logType);
        List<String> allDatabases = listDatabaseNames();
        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();
        for (String dbName : allDatabases) {
            // Exclude database map take effect with higher priority over include database map
            if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(dbName)) {
                continue;
            }
            if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(dbName)) {
                continue;
            }
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(dbName)) {
                dbId = dbNameToId.get(dbName);
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase<? extends ExternalTable> db = idToDb.get(dbId);
                db.setUnInitialized(invalidCacheInInit);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase<? extends ExternalTable> db = getDbForInit(dbName, dbId, logType);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, dbName);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        lastUpdateTime = System.currentTimeMillis();
        initCatalogLog.setLastUpdateTime(lastUpdateTime);
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

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

    public final List<Column> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        Optional<ExternalDatabase<? extends ExternalTable>> db = getDb(dbName);
        if (db.isPresent()) {
            Optional<? extends ExternalTable> table = db.get().getTable(tblName);
            if (table.isPresent()) {
                return table.get().initSchemaAndUpdateTime();
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
        return logType.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @return names of database in this catalog.
     */
    @Override
    public List<String> getDbNames() {
        makeSureInitialized();
        return new ArrayList<>(dbNameToId.keySet());
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
    public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
        if (StringUtils.isEmpty(dbName)) {
            return null;
        }
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
    public ExternalDatabase<? extends ExternalTable> getDbNullable(long dbId) {
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
        catalogProperty.modifyCatalogProps(props);
        notifyPropertiesUpdated(props);
    }

    public void tryModifyCatalogProps(Map<String, String> props) {
        catalogProperty.modifyCatalogProps(props);
    }

    public void rollBackCatalogProps(Map<String, String> props) {
        catalogProperty.rollBackCatalogProps(props);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
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
        Map<Long,  ExternalDatabase<? extends ExternalTable>> tmpIdToDb = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            ExternalDatabase<? extends ExternalTable> db = getDbForReplay(log.getRefreshDbIds().get(i));
            db.setUnInitialized(invalidCacheInInit);
            tmpDbNameToId.put(db.getFullName(), db.getId());
            tmpIdToDb.put(db.getId(), db);
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            ExternalDatabase<? extends ExternalTable> db =
                    getDbForInit(log.getCreateDbNames().get(i), log.getCreateDbIds().get(i), log.getType());
            if (db != null) {
                tmpDbNameToId.put(db.getFullName(), db.getId());
                tmpIdToDb.put(db.getId(), db);
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        lastUpdateTime = log.getLastUpdateTime();
        initialized = true;
    }

    public  ExternalDatabase<? extends ExternalTable> getDbForReplay(long dbId) {
        return idToDb.get(dbId);
    }

    protected ExternalDatabase<? extends ExternalTable> getDbForInit(String dbName, long dbId,
                                                                     InitCatalogLog.Type logType) {
        switch (logType) {
            case HMS:
                return new HMSExternalDatabase(this, dbId, dbName);
            case ES:
                return new EsExternalDatabase(this, dbId, dbName);
            case JDBC:
                return new JdbcExternalDatabase(this, dbId, dbName);
            case ICEBERG:
                return new IcebergExternalDatabase(this, dbId, dbName);
            case MAX_COMPUTE:
                return new MaxComputeExternalDatabase(this, dbId, dbName);
            //case HUDI:
                //return new HudiExternalDatabase(this, dbId, dbName);
            case TEST:
                return new TestExternalDatabase(this, dbId, dbName);
            case PAIMON:
                return new PaimonExternalDatabase(this, dbId, dbName);
            case DELTALAKE:
                return new DeltaLakeExternalDataBase(this, dbId, dbName);
            default:
                break;
        }
        return null;
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
        for (ExternalDatabase<? extends ExternalTable> db : idToDb.values()) {
            dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
            db.setExtCatalog(this);
            db.setTableExtCatalog(this);
        }
        objectCreated = false;
        if (this instanceof HMSExternalCatalog) {
            ((HMSExternalCatalog) this).setLastSyncedEventId(-1L);
        }
        // TODO: This code is to compatible with older version of metadata.
        //  Could only remove after all users upgrate to the new version.
        if (logType == null) {
            if (type == null) {
                logType = InitCatalogLog.Type.UNKNOWN;
            } else {
                try {
                    logType = InitCatalogLog.Type.valueOf(type.toUpperCase(Locale.ROOT));
                } catch (Exception e) {
                    logType = InitCatalogLog.Type.UNKNOWN;
                }
            }
        }
    }

    public void addDatabaseForTest(ExternalDatabase<? extends ExternalTable> db) {
        idToDb.put(db.getId(), db);
        dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
    }

    public void dropDatabaseForReplay(String dbName) {
        throw new NotImplementedException("dropDatabase not implemented");
    }

    public void createDatabaseForReplay(long dbId, String dbName) {
        throw new NotImplementedException("createDatabase not implemented");
    }

    public Map<String, Boolean> getIncludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.INCLUDE_DATABASE_LIST);
    }

    public Map<String, Boolean> getExcludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.EXCLUDE_DATABASE_LIST);
    }

    public Map<String, Boolean> getSpecifiedDatabaseMap(String catalogPropertyKey) {
        String specifiedDatabaseList = catalogProperty.getOrDefault(catalogPropertyKey, "");
        Map<String, Boolean> specifiedDatabaseMap = Maps.newHashMap();
        specifiedDatabaseList = specifiedDatabaseList.trim();
        if (specifiedDatabaseList.isEmpty()) {
            return specifiedDatabaseMap;
        }
        String[] databaseList = specifiedDatabaseList.split(",");
        for (String database : databaseList) {
            String dbname = database.trim();
            if (!dbname.isEmpty()) {
                specifiedDatabaseMap.put(dbname, true);
            }
        }
        return specifiedDatabaseMap;
    }

    public boolean useSelfSplitter() {
        Map<String, String> properties = catalogProperty.getProperties();
        boolean ret = true;
        if (properties.containsKey(HMSExternalCatalog.ENABLE_SELF_SPLITTER)
                && properties.get(HMSExternalCatalog.ENABLE_SELF_SPLITTER).equalsIgnoreCase("false")) {
            ret = false;
        }
        return ret;
    }

    public String bindBrokerName() {
        Map<String, String> properties = catalogProperty.getProperties();
        if (properties.containsKey(HMSExternalCatalog.BIND_BROKER_NAME)) {
            return properties.get(HMSExternalCatalog.BIND_BROKER_NAME);
        }
        return null;
    }

    @Override
    public Collection<DatabaseIf<? extends TableIf>> getAllDbs() {
        makeSureInitialized();
        return new HashSet<>(idToDb.values());
    }

    @Override
    public boolean enableAutoAnalyze() {
        // By default, external catalog disables auto analyze, uses could set catalog property to enable it:
        // "enable.auto.analyze" = true
        Map<String, String> properties = catalogProperty.getProperties();
        boolean ret = false;
        if (properties.containsKey(ENABLE_AUTO_ANALYZE)
                && properties.get(ENABLE_AUTO_ANALYZE).equalsIgnoreCase("true")) {
            ret = true;
        }
        return ret;
    }

    @Override
    public ConcurrentHashMap<Long, DatabaseIf> getIdToDb() {
        return new ConcurrentHashMap<>(idToDb);
    }
}
