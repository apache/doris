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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.jdbc.JdbcExternalDatabase;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.doris.transaction.TransactionManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
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
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The abstract class for all types of external catalogs.
 */
@Data
public abstract class ExternalCatalog
        implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalCatalog.class);

    public static final String ENABLE_AUTO_ANALYZE = "enable.auto.analyze";
    public static final String DORIS_VERSION = "doris.version";
    public static final String DORIS_VERSION_VALUE = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
    public static final String USE_META_CACHE = "use_meta_cache";
    public static final String CREATE_TIME = "create_time";
    public static final boolean DEFAULT_USE_META_CACHE = true;

    // Properties that should not be shown in the `show create catalog` result
    public static final Set<String> HIDDEN_PROPERTIES = Sets.newHashSet(
            CREATE_TIME,
            USE_META_CACHE);

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
    protected boolean initialized = false;
    @SerializedName(value = "idToDb")
    protected Map<Long, ExternalDatabase<? extends ExternalTable>> idToDb = Maps.newConcurrentMap();
    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;
    // <db name, table name> to tableAutoAnalyzePolicy
    @SerializedName(value = "taap")
    protected Map<Pair<String, String>, String> tableAutoAnalyzePolicy = Maps.newHashMap();
    // db name does not contains "default_cluster"
    protected Map<String, Long> dbNameToId = Maps.newConcurrentMap();
    private boolean objectCreated = false;
    protected boolean invalidCacheInInit = true;
    protected ExternalMetadataOps metadataOps;
    protected TransactionManager transactionManager;

    private ExternalSchemaCache schemaCache;
    private String comment;
    // A cached and being converted properties for external catalog.
    // generated from catalog properties.
    private byte[] propLock = new byte[0];
    private Map<String, String> convertedProperties = null;

    protected Optional<Boolean> useMetaCache = Optional.empty();
    protected MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache;

    public ExternalCatalog() {
    }

    public ExternalCatalog(long catalogId, String name, InitCatalogLog.Type logType, String comment) {
        this.id = catalogId;
        this.name = name;
        this.logType = logType;
        this.comment = Strings.nullToEmpty(comment);
    }

    public Configuration getConfiguration() {
        Configuration conf = DFSFileSystem.getHdfsConf(ifNotSetFallbackToSimpleAuth());
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    /**
     * set some default properties when creating catalog
     * @return list of database names in this catalog
     */
    protected List<String> listDatabaseNames() {
        if (metadataOps == null) {
            throw new UnsupportedOperationException("Unsupported operation: "
                    + "listDatabaseNames from remote client when init catalog with " + logType.name());
        } else {
            return metadataOps.listDatabaseNames();
        }
    }

    // Will be called when creating catalog(so when as replaying)
    // to add some default properties if missing.
    public void setDefaultPropsIfMissing(boolean isReplay) {
        if (catalogProperty.getOrDefault(USE_META_CACHE, "").isEmpty()) {
            // If not setting USE_META_CACHE in replay logic,
            // set default value to false to be compatible with older version meta data.
            catalogProperty.addProperty(USE_META_CACHE, isReplay ? "false" : String.valueOf(DEFAULT_USE_META_CACHE));
        }
        useMetaCache = Optional.of(
                Boolean.valueOf(catalogProperty.getOrDefault(USE_META_CACHE, String.valueOf(DEFAULT_USE_META_CACHE))));
    }

    // we need check auth fallback for kerberos or simple
    public boolean ifNotSetFallbackToSimpleAuth() {
        return catalogProperty.getOrDefault(DFSFileSystem.PROP_ALLOW_FALLBACK_TO_SIMPLE_AUTH, "").isEmpty();
    }

    // Will be called when creating catalog(not replaying).
    // Subclass can override this method to do some check when creating catalog.
    public void checkWhenCreating() throws DdlException {
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
     * init some local objects such as:
     * hms client, read properties from hive-site.xml, es client
     */
    protected abstract void initLocalObjectsImpl();

    /**
     * check if the specified table exist in doris.
     * Currently only be used for hms event handler.
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
            if (useMetaCache.get()) {
                if (metaCache == null) {
                    metaCache = Env.getCurrentEnv().getExtMetaCacheMgr().buildMetaCache(
                            name,
                            OptionalLong.of(86400L),
                            OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60L),
                            Config.max_meta_object_cache_num,
                            ignored -> getFilteredDatabaseNames(),
                            dbName -> Optional.ofNullable(
                                    buildDbForInit(dbName, Util.genIdByName(name, dbName), logType)),
                            (key, value, cause) -> value.ifPresent(v -> v.setUnInitialized(invalidCacheInInit)));
                }
                setLastUpdateTime(System.currentTimeMillis());
            } else {
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
            }
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

        // if (properties.getOrDefault(ExternalCatalog.USE_META_CACHE, "true").equals("false")) {
        //     LOG.warn("force to set use_meta_cache to true for catalog: {} when creating", name);
        //     getCatalogProperty().addProperty(ExternalCatalog.USE_META_CACHE, "true");
        //     useMetaCache = Optional.of(true);
        // }
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
    private void init() {
        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase<? extends ExternalTable>> tmpIdToDb = Maps.newConcurrentMap();
        InitCatalogLog initCatalogLog = new InitCatalogLog();
        initCatalogLog.setCatalogId(id);
        initCatalogLog.setType(logType);
        List<String> allDatabases = getFilteredDatabaseNames();
        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();
        for (String dbName : allDatabases) {
            if (!dbName.equals(InfoSchemaDb.DATABASE_NAME) && !dbName.equals(MysqlDb.DATABASE_NAME)) {
                // Exclude database map take effect with higher priority over include database map
                if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(dbName)) {
                    continue;
                }
                if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(dbName)) {
                    continue;
                }
            }
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(dbName)) {
                dbId = dbNameToId.get(dbName);
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase<? extends ExternalTable> db = idToDb.get(dbId);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(dbName, dbId);
                ExternalDatabase<? extends ExternalTable> db = buildDbForInit(dbName, dbId, logType);
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

    @NotNull
    private List<String> getFilteredDatabaseNames() {
        List<String> allDatabases = Lists.newArrayList(listDatabaseNames());
        allDatabases.remove(InfoSchemaDb.DATABASE_NAME);
        allDatabases.add(InfoSchemaDb.DATABASE_NAME);
        allDatabases.remove(MysqlDb.DATABASE_NAME);
        allDatabases.add(MysqlDb.DATABASE_NAME);
        return allDatabases;
    }

    public void onRefresh(boolean invalidCache) {
        this.objectCreated = false;
        this.initialized = false;
        synchronized (this.propLock) {
            this.convertedProperties = null;
        }

        refreshOnlyCatalogCache(invalidCache);
    }

    public void onRefreshCache(boolean invalidCache) {
        refreshOnlyCatalogCache(invalidCache);
    }

    private void refreshOnlyCatalogCache(boolean invalidCache) {
        if (useMetaCache.isPresent()) {
            if (useMetaCache.get() && metaCache != null) {
                metaCache.invalidateAll();
            } else if (!useMetaCache.get()) {
                for (ExternalDatabase<? extends ExternalTable> db : idToDb.values()) {
                    db.setUnInitialized(invalidCache);
                }
            }
        }
        this.invalidCacheInInit = invalidCache;
        if (invalidCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalogCache(id);
        }
    }

    public final Optional<SchemaCacheValue> getSchema(String dbName, String tblName) {
        makeSureInitialized();
        Optional<ExternalDatabase<? extends ExternalTable>> db = getDb(dbName);
        if (db.isPresent()) {
            Optional<? extends ExternalTable> table = db.get().getTable(tblName);
            if (table.isPresent()) {
                return table.get().initSchemaAndUpdateTime();
            }
        }
        return Optional.empty();
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
     * Different from 'listDatabases()', this method will return dbnames from cache.
     * while 'listDatabases()' will return dbnames from remote datasource.
     * @return names of database in this catalog.
     */
    @Override
    public List<String> getDbNames() {
        makeSureInitialized();
        if (useMetaCache.get()) {
            return metaCache.listNames();
        } else {
            return new ArrayList<>(dbNameToId.keySet());
        }
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
    public TableName getTableNameByTableId(Long tableId) {
        throw new UnsupportedOperationException("External catalog does not support getTableNameByTableId() method."
                + ", table id: " + tableId);
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

        // information_schema db name is case-insensitive.
        // So, we first convert it to standard database name.
        if (realDbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            realDbName = InfoSchemaDb.DATABASE_NAME;
        } else if (realDbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            realDbName = MysqlDb.DATABASE_NAME;
        }

        if (useMetaCache.get()) {
            // must use full qualified name to generate id.
            // otherwise, if 2 catalogs have the same db name, the id will be the same.
            return metaCache.getMetaObj(realDbName, Util.genIdByName(getQualifiedName(realDbName))).orElse(null);
        } else {
            if (dbNameToId.containsKey(realDbName)) {
                return idToDb.get(dbNameToId.get(realDbName));
            }
            return null;
        }
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

        if (useMetaCache.get()) {
            return metaCache.getMetaObjById(dbId).orElse(null);
        } else {
            return idToDb.get(dbId);
        }
    }

    @Override
    public List<Long> getDbIds() {
        makeSureInitialized();
        if (useMetaCache.get()) {
            return getAllDbs().stream().map(DatabaseIf::getId).collect(Collectors.toList());
        } else {
            return Lists.newArrayList(dbNameToId.values());
        }
    }

    @Override
    public Map<String, String> getProperties() {
        // convert properties may be a heavy operation, so we cache the result.
        if (convertedProperties != null) {
            return convertedProperties;
        }
        synchronized (propLock) {
            if (convertedProperties != null) {
                return convertedProperties;
            }
            convertedProperties = PropertyConverter.convertToMetaProperties(catalogProperty.getProperties());
            return convertedProperties;
        }
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
            Optional<ExternalDatabase<? extends ExternalTable>> db = getDbForReplay(log.getRefreshDbIds().get(i));
            // Should not return null.
            // Because replyInitCatalog can only be called when `use_meta_cache` is false.
            // And if `use_meta_cache` is false, getDbForReplay() will not return null
            if (!db.isPresent()) {
                LOG.warn("met invalid db id {} in replayInitCatalog, catalog: {}, ignore it to skip bug.",
                        log.getRefreshDbIds().get(i), name);
                continue;
            }
            Preconditions.checkNotNull(db.get());
            tmpDbNameToId.put(db.get().getFullName(), db.get().getId());
            tmpIdToDb.put(db.get().getId(), db.get());
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            ExternalDatabase<? extends ExternalTable> db =
                    buildDbForInit(log.getCreateDbNames().get(i), log.getCreateDbIds().get(i), log.getType());
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

    public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(long dbId) {
        if (useMetaCache.get()) {
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.getMetaObjById(dbId);
        } else {
            return Optional.ofNullable(idToDb.get(dbId));
        }
    }

    protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String dbName, long dbId,
            InitCatalogLog.Type logType) {
        if (dbName.equals(InfoSchemaDb.DATABASE_NAME)) {
            return new ExternalInfoSchemaDatabase(this, dbId);
        }
        if (dbName.equals(MysqlDb.DATABASE_NAME)) {
            return new ExternalMysqlDatabase(this, dbId);
        }
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
            case LAKESOUL:
                return new LakeSoulExternalDatabase(this, dbId, dbName);
            case TEST:
                return new TestExternalDatabase(this, dbId, dbName);
            case PAIMON:
                return new PaimonExternalDatabase(this, dbId, dbName);
            case TRINO_CONNECTOR:
                return new TrinoConnectorExternalDatabase(this, dbId, dbName);
            default:
                break;
        }
        return null;
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
        this.propLock = new byte[0];
        this.initialized = false;
        setDefaultPropsIfMissing(true);
        if (tableAutoAnalyzePolicy == null) {
            tableAutoAnalyzePolicy = Maps.newHashMap();
        }
    }

    public void addDatabaseForTest(ExternalDatabase<? extends ExternalTable> db) {
        idToDb.put(db.getId(), db);
        dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            LOG.warn("createDb not implemented");
            return;
        }
        try {
            metadataOps.createDb(stmt);
        } catch (Exception e) {
            LOG.warn("Failed to create a database.", e);
            throw e;
        }
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            LOG.warn("dropDb not implemented");
            return;
        }
        try {
            metadataOps.dropDb(stmt);
        } catch (Exception e) {
            LOG.warn("Failed to drop a database.", e);
            throw e;
        }
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        makeSureInitialized();
        if (metadataOps == null) {
            LOG.warn("createTable not implemented");
            return false;
        }
        try {
            return metadataOps.createTable(stmt);
        } catch (Exception e) {
            LOG.warn("Failed to create a table.", e);
            throw e;
        }
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            LOG.warn("dropTable not implemented");
            return;
        }
        try {
            metadataOps.dropTable(stmt);
        } catch (Exception e) {
            LOG.warn("Failed to drop a table", e);
            throw e;
        }
    }

    public void unregisterDatabase(String dbName) {
        throw new NotImplementedException("unregisterDatabase not implemented");
    }

    public void registerDatabase(long dbId, String dbName) {
        throw new NotImplementedException("registerDatabase not implemented");
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

    public String bindBrokerName() {
        return catalogProperty.getProperties().get(HMSExternalCatalog.BIND_BROKER_NAME);
    }

    // ATTN: this method only return all cached databases.
    // will not visit remote datasource's metadata
    @Override
    public Collection<DatabaseIf<? extends TableIf>> getAllDbs() {
        makeSureInitialized();
        if (useMetaCache.get()) {
            Set<DatabaseIf<? extends TableIf>> dbs = Sets.newHashSet();
            List<String> dbNames = getDbNames();
            for (String dbName : dbNames) {
                ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
                if (db != null) {
                    dbs.add(db);
                }
            }
            return dbs;
        } else {
            return new HashSet<>(idToDb.values());
        }
    }

    @Override
    public boolean enableAutoAnalyze() {
        // By default, external catalog disables auto analyze, users could set catalog property to enable it:
        // "enable.auto.analyze" = "true"
        Map<String, String> properties = catalogProperty.getProperties();
        boolean ret = false;
        if (properties.containsKey(ENABLE_AUTO_ANALYZE)
                && properties.get(ENABLE_AUTO_ANALYZE).equalsIgnoreCase("true")) {
            ret = true;
        }
        return ret;
    }

    @Override
    public void truncateTable(TruncateTableStmt stmt) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new UnsupportedOperationException("Truncate table not supported in " + getName());
        }
        try {
            TableRef tableRef = stmt.getTblRef();
            TableName tableName = tableRef.getName();
            // delete all table data if null
            List<String> partitions = null;
            if (tableRef.getPartitionNames() != null) {
                partitions = tableRef.getPartitionNames().getPartitionNames();
            }
            metadataOps.truncateTable(tableName.getDb(), tableName.getTbl(), partitions);
        } catch (Exception e) {
            LOG.warn("Failed to drop a table", e);
            throw e;
        }
    }

    public String getQualifiedName(String dbName) {
        return String.join(".", name, dbName);
    }

    public void setAutoAnalyzePolicy(String dbName, String tableName, String policy) {
        Pair<String, String> key = Pair.of(dbName, tableName);
        if (policy == null) {
            tableAutoAnalyzePolicy.remove(key);
        } else {
            tableAutoAnalyzePolicy.put(key, policy);
        }
    }
}
