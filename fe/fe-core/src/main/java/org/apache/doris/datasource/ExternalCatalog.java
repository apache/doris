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

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
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
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.TableBranchOrTagInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.doris.transaction.TransactionManager;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * The abstract class for all types of external catalogs.
 */
public abstract class ExternalCatalog
        implements CatalogIf<ExternalDatabase<? extends ExternalTable>>, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalCatalog.class);

    public static final String ENABLE_AUTO_ANALYZE = "enable.auto.analyze";
    public static final String DORIS_VERSION = "doris.version";
    public static final String DORIS_VERSION_VALUE = Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH;
    public static final String USE_META_CACHE = "use_meta_cache";

    public static final String CREATE_TIME = "create_time";
    public static final boolean DEFAULT_USE_META_CACHE = true;

    public static final String FOUND_CONFLICTING = "Found conflicting";
    public static final String ONLY_TEST_LOWER_CASE_TABLE_NAMES = "only_test_lower_case_table_names";

    // https://help.aliyun.com/zh/emr/emr-on-ecs/user-guide/use-rootpolicy-to-access-oss-hdfs?spm=a2c4g.11186623.help-menu-search-28066.d_0
    public static final String OOS_ROOT_POLICY = "oss.root_policy";
    public static final String SCHEMA_CACHE_TTL_SECOND = "schema.cache.ttl-second";
    // -1 means cache with no ttl
    public static final int CACHE_NO_TTL = -1;
    // 0 means cache is disabled; >0 means cache with ttl;
    public static final int CACHE_TTL_DISABLE_CACHE = 0;

    // Properties that should not be shown in the `show create catalog` result
    public static final Set<String> HIDDEN_PROPERTIES = Sets.newHashSet(
            CREATE_TIME,
            USE_META_CACHE);

    protected static final int ICEBERG_CATALOG_EXECUTOR_THREAD_NUM = Runtime.getRuntime().availableProcessors();

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
    @SerializedName(value = "comment")
    private String comment;

    // Save the error info if initialization fails.
    // can be seen in `show catalogs` result.
    // no need to persist this field.
    private String errorMsg = "";

    // db name does not contains "default_cluster"
    protected Map<String, Long> dbNameToId = Maps.newConcurrentMap();
    private boolean objectCreated = false;
    protected ExternalMetadataOps metadataOps;
    protected TransactionManager transactionManager;
    protected Optional<Boolean> useMetaCache = Optional.empty();
    protected MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache;
    protected ExecutionAuthenticator executionAuthenticator;
    protected ThreadPoolExecutor threadPoolWithPreAuth;

    private volatile Configuration cachedConf = null;
    private byte[] confLock = new byte[0];

    private volatile boolean isInitializing = false;

    public ExternalCatalog() {
    }

    public ExternalCatalog(long catalogId, String name, InitCatalogLog.Type logType, String comment) {
        this.id = catalogId;
        this.name = name;
        this.logType = logType;
        this.comment = Strings.nullToEmpty(comment);
    }

    /**
     * Initializes the PreExecutionAuthenticator instance.
     * This method ensures that the authenticator is created only once in a thread-safe manner.
     * If additional authentication logic is required, it should be extended and implemented in subclasses.
     */
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = new ExecutionAuthenticator(){};
        }
    }

    public Configuration getConfiguration() {
        // build configuration is costly, so we cache it.
        if (cachedConf != null) {
            return cachedConf;
        }
        synchronized (confLock) {
            if (cachedConf != null) {
                return cachedConf;
            }
            cachedConf = buildConf();
            return cachedConf;
        }
    }

    private Configuration buildConf() {
        Configuration conf = DFSFileSystem.getHdfsConf(ifNotSetFallbackToSimpleAuth());
        Map<String, String> catalogProperties = catalogProperty.getHadoopProperties();
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    /**
     * Lists all database names in this catalog.
     *
     * @return list of database names in this catalog
     */
    protected List<String> listDatabaseNames() {
        if (metadataOps == null) {
            throw new UnsupportedOperationException("List databases is not supported for catalog: " + getName());
        } else {
            return metadataOps.listDatabaseNames();
        }
    }

    public ExternalMetadataOps getMetadataOps() {
        makeSureInitialized();
        return metadataOps;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("start to init catalog {}:{}", name, id);
        }
        if (isInitializing) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("catalog {}:{} is initializing, skip make sure initialized.", name, id, new Exception());
            }
            return;
        }
        try {
            initLocalObjects();
            if (!initialized) {
                if (useMetaCache.get()) {
                    buildMetaCache();
                    setLastUpdateTime(System.currentTimeMillis());
                } else {
                    if (!Env.getCurrentEnv().isMaster()) {
                        // Forward to master and wait the journal to replay.
                        int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeoutS();
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
                this.errorMsg = "";
            }
        } catch (Exception e) {
            LOG.warn("failed to init catalog {}:{}", name, id, e);
            this.errorMsg = ExceptionUtils.getRootCauseMessage(e);
            throw new RuntimeException("Failed to init catalog: " + name + ", error: " + this.errorMsg, e);
        } finally {
            isInitializing = false;
        }
    }

    protected final void initLocalObjects() {
        if (!objectCreated) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("start to init local objects of catalog {}:{}", getName(), id, new Exception());
            }
            initLocalObjectsImpl();
            objectCreated = true;
        }
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    private void buildMetaCache() {
        if (metaCache == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("buildMetaCache for catalog: {}:{}", this.name, this.id, new Exception());
            }
            metaCache = Env.getCurrentEnv().getExtMetaCacheMgr().buildMetaCache(
                    name,
                    OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                    OptionalLong.of(Config.external_cache_refresh_time_minutes * 60L),
                    Config.max_meta_object_cache_num,
                    ignored -> getFilteredDatabaseNames(),
                    localDbName -> Optional.ofNullable(
                            buildDbForInit(null, localDbName, Util.genIdByName(name, localDbName), logType,
                                    true)),
                    (key, value, cause) -> value.ifPresent(v -> v.resetToUninitialized()));
        }
    }

    // check if all required properties are set when creating catalog
    public void checkProperties() throws DdlException {
        // check refresh parameter of catalog
        Map<String, String> properties = catalogProperty.getProperties();
        if (properties.containsKey(CatalogMgr.METADATA_REFRESH_INTERVAL_SEC)) {
            try {
                int metadataRefreshIntervalSec = Integer.parseInt(
                        properties.get(CatalogMgr.METADATA_REFRESH_INTERVAL_SEC));
                if (metadataRefreshIntervalSec < 0) {
                    throw new DdlException("Invalid properties: " + CatalogMgr.METADATA_REFRESH_INTERVAL_SEC);
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid properties: " + CatalogMgr.METADATA_REFRESH_INTERVAL_SEC);
            }
        }

        // check schema.cache.ttl-second parameter
        String schemaCacheTtlSecond = catalogProperty.getOrDefault(SCHEMA_CACHE_TTL_SECOND, null);
        if (java.util.Objects.nonNull(schemaCacheTtlSecond) && NumberUtils.toInt(schemaCacheTtlSecond, CACHE_NO_TTL)
                < CACHE_TTL_DISABLE_CACHE) {
            throw new DdlException(
                    "The parameter " + SCHEMA_CACHE_TTL_SECOND + " is wrong, value is " + schemaCacheTtlSecond);
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
        Map<String, String> properties = catalogProperty.getProperties();
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
        List<Pair<String, String>> remoteToLocalPairs = getFilteredDatabaseNames();
        for (Pair<String, String> pair : remoteToLocalPairs) {
            String remoteDbName = pair.key();
            String localDbName = pair.value();
            long dbId;
            if (dbNameToId != null && dbNameToId.containsKey(localDbName)) {
                dbId = dbNameToId.get(localDbName);
                tmpDbNameToId.put(localDbName, dbId);
                ExternalDatabase<? extends ExternalTable> db = idToDb.get(dbId);
                // If the remote name is missing during upgrade, all databases in the Map will be reinitialized.
                if (Strings.isNullOrEmpty(db.getRemoteName())) {
                    db.setRemoteName(remoteDbName);
                }
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addRefreshDb(dbId, remoteDbName);
            } else {
                dbId = Env.getCurrentEnv().getNextId();
                tmpDbNameToId.put(localDbName, dbId);
                ExternalDatabase<? extends ExternalTable> db =
                        buildDbForInit(remoteDbName, localDbName, dbId, logType, false);
                tmpIdToDb.put(dbId, db);
                initCatalogLog.addCreateDb(dbId, localDbName, remoteDbName);
            }
        }

        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        lastUpdateTime = System.currentTimeMillis();
        initCatalogLog.setLastUpdateTime(lastUpdateTime);
        Env.getCurrentEnv().getEditLog().logInitCatalog(initCatalogLog);
    }

    /**
     * Retrieves a filtered list of database names and their corresponding local database names.
     * The method applies to include and exclude filters based on the database properties, ensuring
     * only the relevant databases are included for further operations.
     * <p>
     * The method also handles conflicts in database names under case-insensitive conditions
     * and throws an exception if such conflicts are detected.
     * <p>
     * Steps:
     * 1. Fetch all database names from the remote source.
     * 2. Apply to include and exclude database filters:
     * - Exclude filters take precedence over include filters.
     * - If a database is in the exclude list, it is ignored.
     * - If a database is not in the include list and the include list is not empty, it is ignored.
     * 3. Map the filtered remote database names to local database names.
     * 4. Handle conflicts when `lower_case_meta_names` is enabled:
     * - Detect cases where multiple remote database names map to the same lower-cased local name.
     * - Throw an exception if conflicts are found.
     *
     * @return A list of pairs where each pair contains the remote database name and local database name.
     * @throws RuntimeException if there are conflicting database names under case-insensitive conditions.
     */
    @NotNull
    private List<Pair<String, String>> getFilteredDatabaseNames() {
        List<String> allDatabases = Lists.newArrayList(listDatabaseNames());
        allDatabases.remove(InfoSchemaDb.DATABASE_NAME);
        allDatabases.add(InfoSchemaDb.DATABASE_NAME);
        allDatabases.remove(MysqlDb.DATABASE_NAME);
        allDatabases.add(MysqlDb.DATABASE_NAME);

        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();

        List<Pair<String, String>> remoteToLocalPairs = Lists.newArrayList();

        allDatabases = allDatabases.stream().filter(dbName -> {
            if (!dbName.equals(InfoSchemaDb.DATABASE_NAME) && !dbName.equals(MysqlDb.DATABASE_NAME)) {
                // Exclude database map take effect with higher priority over include database map
                if (!excludeDatabaseMap.isEmpty() && excludeDatabaseMap.containsKey(dbName)) {
                    return false;
                }
                if (!includeDatabaseMap.isEmpty() && !includeDatabaseMap.containsKey(dbName)) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());

        for (String remoteDbName : allDatabases) {
            String localDbName = fromRemoteDatabaseName(remoteDbName);
            remoteToLocalPairs.add(Pair.of(remoteDbName, localDbName));
        }

        // Check for conflicts when lower_case_meta_names = true
        if (Boolean.parseBoolean(getLowerCaseMetaNames())) {
            // Map to track lowercase local names and their corresponding remote names
            Map<String, List<String>> lowerCaseToRemoteNames = Maps.newHashMap();

            // Collect lowercased local names and their remote counterparts
            for (Pair<String, String> pair : remoteToLocalPairs) {
                String lowerCaseLocalName = pair.second.toLowerCase();
                lowerCaseToRemoteNames.computeIfAbsent(lowerCaseLocalName, k -> Lists.newArrayList()).add(pair.first);
            }

            // Identify conflicts: multiple remote names mapping to the same lowercase local name
            List<String> conflicts = lowerCaseToRemoteNames.values().stream()
                    .filter(remoteNames -> remoteNames.size() > 1) // Conflict: more than one remote name
                    .flatMap(List::stream) // Collect all conflicting remote names
                    .collect(Collectors.toList());

            // Throw exception if conflicts are found
            if (!conflicts.isEmpty()) {
                throw new RuntimeException(String.format(
                        FOUND_CONFLICTING + " database names under case-insensitive conditions. "
                                + "Conflicting remote database names: %s in catalog %s. "
                                + "Please use meta_names_mapping to handle name mapping.",
                        String.join(", ", conflicts), name));
            }
        }

        return remoteToLocalPairs;
    }

    /**
     * Resets the Catalog state to uninitialized, releases resources held by {@code initLocalObjectsImpl()}
     * <p>
     * This method is typically invoked during operations such as {@code CREATE CATALOG}
     * and {@code MODIFY CATALOG}. It marks the object as uninitialized, clears cached
     * configurations, and ensures that resources allocated during {@link #initLocalObjectsImpl()}
     * are properly released via {@link #onClose()}
     * </p>
     * <p>
     * The {@code onClose()} method is responsible for cleaning up resources that were initialized
     * in {@code initLocalObjectsImpl()}, preventing potential resource leaks.
     * </p>
     *
     * @param invalidCache if {@code true}, the catalog cache will be invalidated
     *                     and reloaded during the refresh process.
     */
    public synchronized void resetToUninitialized(boolean invalidCache) {
        this.objectCreated = false;
        this.initialized = false;
        synchronized (this.confLock) {
            this.cachedConf = null;
        }
        onClose();

        refreshOnlyCatalogCache(invalidCache);
    }

    // Only for hms event handling.
    public void onRefreshCache() {
        refreshOnlyCatalogCache(true);
    }

    private void refreshOnlyCatalogCache(boolean invalidCache) {
        if (useMetaCache.isPresent()) {
            if (useMetaCache.get() && metaCache != null) {
                metaCache.invalidateAll();
            } else if (!useMetaCache.get()) {
                this.initialized = false;
                for (ExternalDatabase<? extends ExternalTable> db : idToDb.values()) {
                    db.resetToUninitialized();
                }
            }
        }
        if (invalidCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalogCache(id);
        }
    }

    public final Optional<SchemaCacheValue> getSchema(SchemaCacheKey key) {
        makeSureInitialized();
        Optional<ExternalDatabase<? extends ExternalTable>> db = getDb(key.getNameMapping().getLocalDbName());
        if (db.isPresent()) {
            Optional<? extends ExternalTable> table = db.get().getTable(key.getNameMapping().getLocalTblName());
            if (table.isPresent()) {
                return table.get().initSchemaAndUpdateTime(key);
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

    @Override
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Different from 'listDatabases()', this method will return dbnames from cache.
     * while 'listDatabases()' will return dbnames from remote datasource.
     *
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
            return metaCache.getMetaObj(realDbName, Util.genIdByName(name, realDbName)).orElse(null);
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
        return catalogProperty.getProperties();
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
    public void onClose() {
        removeAccessController();
        if (threadPoolWithPreAuth != null) {
            ThreadPoolManager.shutdownExecutorService(threadPoolWithPreAuth);
        }
        if (null != executionAuthenticator) {
            executionAuthenticator = null;
        }
        if (null != transactionManager) {
            transactionManager = null;
        }
    }

    private void removeAccessController() {
        Env.getCurrentEnv().getAccessManager().removeAccessController(name);
    }

    public synchronized void replayInitCatalog(InitCatalogLog log) {
        // If the remote name is missing during upgrade, or
        // the refresh db's remote name is empty,
        // all databases in the Map will be reinitialized.
        if ((log.getCreateCount() > 0 && (log.getRemoteDbNames() == null || log.getRemoteDbNames().isEmpty()))
                || (log.getRefreshCount() > 0
                && (log.getRefreshRemoteDbNames() == null || log.getRefreshRemoteDbNames().isEmpty()))) {
            dbNameToId = Maps.newConcurrentMap();
            idToDb = Maps.newConcurrentMap();
            lastUpdateTime = log.getLastUpdateTime();
            initialized = false;
            return;
        }

        Map<String, Long> tmpDbNameToId = Maps.newConcurrentMap();
        Map<Long, ExternalDatabase<? extends ExternalTable>> tmpIdToDb = Maps.newConcurrentMap();
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
            db.get().setRemoteName(log.getRefreshRemoteDbNames().get(i));
            Preconditions.checkNotNull(db.get());
            tmpDbNameToId.put(db.get().getFullName(), db.get().getId());
            tmpIdToDb.put(db.get().getId(), db.get());
            LOG.info("Synchronized database (refresh): [Name: {}, ID: {}]", db.get().getFullName(), db.get().getId());
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            ExternalDatabase<? extends ExternalTable> db =
                    buildDbForInit(log.getRemoteDbNames().get(i), log.getCreateDbNames().get(i),
                            log.getCreateDbIds().get(i), log.getType(), false);
            if (db != null) {
                tmpDbNameToId.put(db.getFullName(), db.getId());
                tmpIdToDb.put(db.getId(), db);
                LOG.info("Synchronized database (create): [Name: {}, ID: {}, Remote Name: {}]",
                        db.getFullName(), db.getId(), log.getRemoteDbNames().get(i));
            }
        }
        // Check whether the remoteName of db in tmpIdToDb is empty
        for (ExternalDatabase<? extends ExternalTable> db : tmpIdToDb.values()) {
            if (Strings.isNullOrEmpty(db.getRemoteName())) {
                LOG.info("Database [{}] remoteName is empty in catalog [{}], mark as uninitialized",
                        db.getFullName(), name);
                dbNameToId = Maps.newConcurrentMap();
                idToDb = Maps.newConcurrentMap();
                lastUpdateTime = log.getLastUpdateTime();
                initialized = false;
                return;
            }
        }
        dbNameToId = tmpDbNameToId;
        idToDb = tmpIdToDb;
        lastUpdateTime = log.getLastUpdateTime();
        initialized = true;
    }

    /**
     * This method will try getting db from cache only,
     * If there is no cache, it will return empty.
     * Different from "getDbNullable()", this method will not visit the remote catalog to get db when it does not exist
     * in cache.
     * This is used for replaying the metadata, to avoid exception when trying to get db from remote catalog.
     *
     * @param dbId
     * @return
     */
    public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(long dbId) {
        Preconditions.checkState(useMetaCache.isPresent(), name);
        if (useMetaCache.get()) {
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.getMetaObjById(dbId);
        } else {
            return Optional.ofNullable(idToDb.get(dbId));
        }
    }

    /**
     * Same as "getDbForReplay(long dbId)", use "tryGetMetaObj" to get db from cache only.
     *
     * @param dbName
     * @return
     */
    public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
        Preconditions.checkState(useMetaCache.isPresent(), name);
        if (useMetaCache.get()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getDbForReplay from metacache, db: {}.{}, catalog id: {}, is catalog init: {}",
                        this.name, dbName, this.id, isInitialized());
            }
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.tryGetMetaObj(dbName);
        } else if (dbNameToId.containsKey(dbName)) {
            return Optional.ofNullable(idToDb.get(dbNameToId.get(dbName)));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Build a database instance.
     * If checkExists is true, it will check if the database exists in the remote system.
     *
     * @param remoteDbName
     * @param dbId
     * @param logType
     * @param checkExists
     * @return
     */
    protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
            long dbId, InitCatalogLog.Type logType, boolean checkExists) {
        // Step 1: Map local database name if not already provided
        if (localDbName == null && remoteDbName != null) {
            localDbName = fromRemoteDatabaseName(remoteDbName);
        }

        // Step 2:
        // When running ut, disable this check to make ut pass.
        // Because in ut, the database is not created in remote system.
        if (checkExists && (!FeConstants.runningUnitTest || this instanceof TestExternalCatalog)) {
            try {
                List<String> dbNames = getDbNames();
                if (!dbNames.contains(localDbName)) {
                    dbNames = getFilteredDatabaseNames().stream()
                            .map(Pair::value)
                            .collect(Collectors.toList());
                    if (!dbNames.contains(localDbName)) {
                        LOG.warn("Database {} does not exist in the remote system. Skipping initialization.",
                                localDbName);
                        return null;
                    }
                }
            } catch (RuntimeException e) {
                // Handle "Found conflicting" exception explicitly
                if (e.getMessage().contains(FOUND_CONFLICTING)) {
                    LOG.error(e.getMessage());
                    throw e; // Rethrow to let the caller handle this critical issue
                } else {
                    // Any errors other than name conflicts, we default to not finding the database
                    LOG.warn("Failed to check db {} exist in remote system, ignore it.", localDbName, e);
                    return null;
                }
            } catch (Exception e) {
                // If connection failed, it will throw exception.
                // ignore it and treat it as not exist.
                LOG.warn("Failed to check db {} exist in remote system, ignore it.", localDbName, e);
                return null;
            }
        }

        // Step 3: Resolve remote database name if using meta cache
        if (remoteDbName == null && useMetaCache.orElse(false)) {
            if (Boolean.parseBoolean(getLowerCaseMetaNames()) || !Strings.isNullOrEmpty(getMetaNamesMapping())) {
                remoteDbName = metaCache.getRemoteName(localDbName);
                if (remoteDbName == null) {
                    LOG.warn("Could not resolve remote database name for local database: {}", localDbName);
                    return null;
                }
            } else {
                remoteDbName = localDbName;
            }
        }

        // Step 4: Instantiate the appropriate ExternalDatabase based on logType
        if (localDbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            return new ExternalInfoSchemaDatabase(this, dbId);
        }
        if (localDbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            return new ExternalMysqlDatabase(this, dbId);
        }
        switch (logType) {
            case HMS:
                return new HMSExternalDatabase(this, dbId, localDbName, remoteDbName);
            case ES:
                return new EsExternalDatabase(this, dbId, localDbName, remoteDbName);
            case JDBC:
                return new JdbcExternalDatabase(this, dbId, localDbName, remoteDbName);
            case ICEBERG:
                return new IcebergExternalDatabase(this, dbId, localDbName, remoteDbName);
            case MAX_COMPUTE:
                return new MaxComputeExternalDatabase(this, dbId, localDbName, remoteDbName);
            case LAKESOUL:
                return new LakeSoulExternalDatabase(this, dbId, localDbName, remoteDbName);
            case TEST:
                return new TestExternalDatabase(this, dbId, localDbName, remoteDbName);
            case PAIMON:
                return new PaimonExternalDatabase(this, dbId, localDbName, remoteDbName);
            case TRINO_CONNECTOR:
                return new TrinoConnectorExternalDatabase(this, dbId, localDbName, remoteDbName);
            default:
                break;
        }
        return null;
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
        this.confLock = new byte[0];
        this.initialized = false;
        setDefaultPropsIfMissing(true);
        if (tableAutoAnalyzePolicy == null) {
            tableAutoAnalyzePolicy = Maps.newHashMap();
        }
    }

    public void addDatabaseForTest(ExternalDatabase<? extends ExternalTable> db) {
        // 1. add for "use_meta_cache = false"
        idToDb.put(db.getId(), db);
        dbNameToId.put(ClusterNamespace.getNameFromFullName(db.getFullName()), db.getId());
        // 2. add for "use_meta_cache = true"
        buildMetaCache();
        metaCache.addObjForTest(db.getId(), db.getFullName(), db);
    }

    /**
     * Set the initialized status for testing purposes only.
     * This method should only be used in test cases.
     */
    public void setInitializedForTest(boolean initialized) {
        this.initialized = initialized;
        if (this.initialized) {
            buildMetaCache();
            this.useMetaCache = Optional.of(true);
        }
    }

    @Override
    public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Create database is not supported for catalog: " + getName());
        }
        try {
            boolean res = metadataOps.createDb(dbName, ifNotExists, properties);
            if (!res) {
                // we should get the db stored in Doris, and use local name in edit log.
                CreateDbInfo info = new CreateDbInfo(getName(), dbName, null);
                Env.getCurrentEnv().getEditLog().logCreateDb(info);
            }
        } catch (Exception e) {
            LOG.warn("Failed to create database {} in catalog {}.", dbName, getName(), e);
            throw e;
        }
    }

    public void replayCreateDb(String dbName) {
        if (metadataOps != null) {
            metadataOps.afterCreateDb();
        }
    }

    @Override
    public void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Drop database is not supported for catalog: " + getName());
        }
        try {
            metadataOps.dropDb(dbName, ifExists, force);
            DropDbInfo info = new DropDbInfo(getName(), dbName);
            Env.getCurrentEnv().getEditLog().logDropDb(info);
        } catch (Exception e) {
            LOG.warn("Failed to drop database {} in catalog {}", dbName, getName(), e);
            throw e;
        }
    }

    public void replayDropDb(String dbName) {
        if (metadataOps != null) {
            metadataOps.afterDropDb(dbName);
        }
    }

    @Override
    public boolean createTable(CreateTableCommand command) throws UserException {
        makeSureInitialized();
        org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo createTableInfo =
                command.getCreateTableInfo();
        if (metadataOps == null) {
            throw new DdlException("Create table is not supported for catalog: " + getName());
        }
        try {
            boolean res = metadataOps.createTable(createTableInfo);
            if (!res) {
                // res == false means the table does not exist before, and we create it.
                // we should get the table stored in Doris, and use local name in edit log.
                CreateTableInfo info = new CreateTableInfo(getName(), createTableInfo.getDbName(),
                        createTableInfo.getTableName());
                Env.getCurrentEnv().getEditLog().logCreateTable(info);
                LOG.info("finished to create table {}.{}.{}", getName(), createTableInfo.getDbName(),
                        createTableInfo.getTableName());
            }
            return res;
        } catch (Exception e) {
            LOG.warn("Failed to create a table.", e);
            throw e;
        }
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Create table is not supported for catalog: " + getName());
        }
        try {
            boolean res = metadataOps.createTable(stmt);
            if (!res) {
                // res == false means the table does not exist before, and we create it.
                // we should get the table stored in Doris, and use local name in edit log.
                CreateTableInfo info = new CreateTableInfo(getName(), stmt.getDbName(), stmt.getTableName());
                Env.getCurrentEnv().getEditLog().logCreateTable(info);
                LOG.info("finished to create table {}.{}.{}", getName(), stmt.getDbName(), stmt.getTableName());
            }
            return res;
        } catch (Exception e) {
            LOG.warn("Failed to create a table.", e);
            throw e;
        }
    }

    public void replayCreateTable(String dbName, String tblName) {
        if (metadataOps != null) {
            metadataOps.afterCreateTable(dbName, tblName);
        }
    }

    @Override
    public void renameTable(String dbName, String oldTableName, String newTableName) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Rename table is not supported for catalog: " + getName());
        }
        try {
            metadataOps.renameTable(dbName, oldTableName, newTableName);
            Env.getCurrentEnv().getEditLog()
                    .logRefreshExternalTable(
                            ExternalObjectLog.createForRenameTable(getId(), dbName, oldTableName, newTableName));
        } catch (Exception e) {
            LOG.warn("Failed to rename table {} in database {}.", oldTableName, dbName, e);
            throw e;
        }
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean ifExists,
            boolean mustTemporary, boolean force) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Drop table is not supported for catalog: " + getName());
        }
        // 1. get table in doris catalog first.
        ExternalDatabase db = getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());
        }
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable == null) {
            if (ifExists) {
                return;
            }
            throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
        }
        try {
            metadataOps.dropTable(dorisTable, ifExists);
            DropInfo info = new DropInfo(getName(), dbName, tableName);
            Env.getCurrentEnv().getEditLog().logDropTable(info);
        } catch (Exception e) {
            LOG.warn("Failed to drop a table", e);
            throw e;
        }
    }

    public void replayDropTable(String dbName, String tblName) {
        if (metadataOps != null) {
            metadataOps.afterDropTable(dbName, tblName);
        }
    }

    /**
     * Unregisters a database from the catalog.
     * Internally, remove the database meta from cache
     *
     * @param dbName
     */
    public void unregisterDatabase(String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("unregister database [{}]", dbName);
        }
        if (useMetaCache.get()) {
            if (isInitialized()) {
                metaCache.invalidate(dbName, Util.genIdByName(name, dbName));
            }
        } else {
            Long dbId = dbNameToId.remove(dbName);
            if (dbId == null) {
                LOG.warn("unregister database {}.{} failed, not find in map. ignore.", this.name, dbName);
            } else {
                idToDb.remove(dbId);
            }
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDbCache(getId(), dbName);
    }

    public void registerDatabase(long dbId, String dbName) {
        throw new NotImplementedException("registerDatabase not implemented");
    }

    protected Map<String, Boolean> getIncludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.INCLUDE_DATABASE_LIST);
    }

    protected Map<String, Boolean> getExcludeDatabaseMap() {
        return getSpecifiedDatabaseMap(Resource.EXCLUDE_DATABASE_LIST);
    }

    private Map<String, Boolean> getSpecifiedDatabaseMap(String catalogPropertyKey) {
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


    public String getLowerCaseMetaNames() {
        return catalogProperty.getOrDefault(Resource.LOWER_CASE_META_NAMES, "false");
    }

    public int getOnlyTestLowerCaseTableNames() {
        return Integer.parseInt(catalogProperty.getOrDefault(ONLY_TEST_LOWER_CASE_TABLE_NAMES, "0"));
    }

    public String getMetaNamesMapping() {
        return catalogProperty.getOrDefault(Resource.META_NAMES_MAPPING, "");
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
    public void truncateTable(String dbName, String tableName, PartitionNames partitionNames, boolean forceDrop,
            String rawTruncateSql) throws DdlException {
        makeSureInitialized();
        if (metadataOps == null) {
            throw new DdlException("Truncate table is not supported for catalog: " + getName());
        }
        try {
            // delete all table data if null
            List<String> partitions = null;
            if (partitionNames != null) {
                partitions = partitionNames.getPartitionNames();
            }
            ExternalTable dorisTable = getDbOrDdlException(dbName).getTableOrDdlException(tableName);
            metadataOps.truncateTable(dorisTable, partitions);
            TruncateTableInfo info = new TruncateTableInfo(getName(), dbName, tableName, partitions);
            Env.getCurrentEnv().getEditLog().logTruncateTable(info);
        } catch (Exception e) {
            LOG.warn("Failed to truncate table {}.{} in catalog {}", dbName, tableName, getName(), e);
            throw e;
        }
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        if (metadataOps != null) {
            metadataOps.afterTruncateTable(info.getDb(), info.getTable());
        }
    }

    public void setAutoAnalyzePolicy(String dbName, String tableName, String policy) {
        Pair<String, String> key = Pair.of(dbName, tableName);
        if (policy == null) {
            tableAutoAnalyzePolicy.remove(key);
        } else {
            tableAutoAnalyzePolicy.put(key, policy);
        }
    }

    public ExecutionAuthenticator getExecutionAuthenticator() {
        if (null == executionAuthenticator) {
            throw new RuntimeException("ExecutionAuthenticator is null, please confirm it is initialized.");
        }
        return executionAuthenticator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalCatalog)) {
            return false;
        }
        ExternalCatalog that = (ExternalCatalog) o;
        return Objects.equal(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        CatalogIf.super.notifyPropertiesUpdated(updatedProps);
        String schemaCacheTtl = updatedProps.getOrDefault(SCHEMA_CACHE_TTL_SECOND, null);
        if (java.util.Objects.nonNull(schemaCacheTtl)) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidSchemaCache(id);
        }
    }

    public CatalogProperty getCatalogProperty() {
        return catalogProperty;
    }

    public Optional<Boolean> getUseMetaCache() {
        return useMetaCache;
    }

    public Map<Pair<String, String>, String> getTableAutoAnalyzePolicy() {
        return tableAutoAnalyzePolicy;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public ThreadPoolExecutor getThreadPoolWithPreAuth() {
        return threadPoolWithPreAuth;
    }

    /**
     * Check if an external view exists.
     * @param dbName
     * @param viewName
     * @return
     */
    public boolean viewExists(String dbName, String viewName) {
        throw new UnsupportedOperationException("View is not supported.");
    }

    @Override
    public void createOrReplaceBranch(TableIf dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("branching operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.createOrReplaceBranch(externalTable, branchInfo);
            TableBranchOrTagInfo info = new TableBranchOrTagInfo(getName(), externalTable.getDbName(),
                    externalTable.getName());
            Env.getCurrentEnv().getEditLog().logBranchOrTag(info);
        } catch (Exception e) {
            LOG.warn("Failed to create or replace branch for table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void createOrReplaceTag(TableIf dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Tagging operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.createOrReplaceTag(externalTable, tagInfo);
            TableBranchOrTagInfo info = new TableBranchOrTagInfo(getName(), externalTable.getDbName(),
                    externalTable.getName());
            Env.getCurrentEnv().getEditLog().logBranchOrTag(info);
        } catch (Exception e) {
            LOG.warn("Failed to create or replace tag for table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void replayOperateOnBranchOrTag(String dbName, String tblName) {
        if (metadataOps != null) {
            metadataOps.afterOperateOnBranchOrTag(dbName, tblName);
        }
    }

    @Override
    public void dropBranch(TableIf dorisTable, DropBranchInfo branchInfo) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("DropBranch operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.dropBranch(externalTable, branchInfo);
            TableBranchOrTagInfo info = new TableBranchOrTagInfo(getName(), externalTable.getDbName(),
                    externalTable.getName());
            Env.getCurrentEnv().getEditLog().logBranchOrTag(info);
        } catch (Exception e) {
            LOG.warn("Failed to drop branch for table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void dropTag(TableIf dorisTable, DropTagInfo tagInfo) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("DropTag operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.dropTag(externalTable, tagInfo);
            TableBranchOrTagInfo info = new TableBranchOrTagInfo(getName(), externalTable.getDbName(),
                    externalTable.getName());
            Env.getCurrentEnv().getEditLog().logBranchOrTag(info);
        } catch (Exception e) {
            LOG.warn("Failed to drop tag for table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    /**
     * Resets the name list in meta cache.
     * Usually used after creating database in catalog, so that user can see newly created db immediately.
     */
    public void resetMetaCacheNames() {
        if (useMetaCache.isPresent() && useMetaCache.get() && metaCache != null) {
            metaCache.resetNames();
        } else {
            resetToUninitialized(true);
        }
    }

    // log the refresh external table operation
    private void logRefreshExternalTable(ExternalTable dorisTable) {
        Env.getCurrentEnv().getEditLog()
                .logRefreshExternalTable(
                        ExternalObjectLog.createForRefreshTable(dorisTable.getCatalog().getId(),
                                dorisTable.getDbName(), dorisTable.getName()));
    }

    @Override
    public void addColumn(TableIf dorisTable, Column column, ColumnPosition position) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Add column operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.addColumn(externalTable, column, position);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to add column {} to table {}.{} in catalog {}",
                    column.getName(), externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void addColumns(TableIf dorisTable, List<Column> columns) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Add columns operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.addColumns(externalTable, columns);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to add columns to table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void dropColumn(TableIf dorisTable, String columnName) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Drop column operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.dropColumn(externalTable, columnName);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to drop column {} from table {}.{} in catalog {}",
                    columnName, externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void renameColumn(TableIf dorisTable, String oldName, String newName) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Rename column operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.renameColumn(externalTable, oldName, newName);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to rename column {} to {} in table {}.{} in catalog {}",
                    oldName, newName, externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void modifyColumn(TableIf dorisTable, Column column, ColumnPosition columnPosition) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Modify column operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.modifyColumn(externalTable, column, columnPosition);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to modify column {} in table {}.{} in catalog {}",
                    column.getName(), externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }

    @Override
    public void reorderColumns(TableIf dorisTable, List<String> newOrder) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        ExternalTable externalTable = (ExternalTable) dorisTable;
        if (metadataOps == null) {
            throw new DdlException("Reorder columns operation is not supported for catalog: " + getName());
        }
        try {
            metadataOps.reorderColumns(externalTable, newOrder);
            logRefreshExternalTable(externalTable);
        } catch (Exception e) {
            LOG.warn("Failed to reorder columns in table {}.{} in catalog {}",
                    externalTable.getDbName(), externalTable.getName(), getName(), e);
            throw e;
        }
    }
}

