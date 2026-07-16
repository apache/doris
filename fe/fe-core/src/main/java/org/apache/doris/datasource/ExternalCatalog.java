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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.doris.RemoteDorisExternalDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.qe.GlobalVariable;
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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
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
    @Deprecated
    // use LOWER_CASE_TABLE_NAMES instead
    public static final String ONLY_TEST_LOWER_CASE_TABLE_NAMES = "only_test_lower_case_table_names";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String LOWER_CASE_DATABASE_NAMES = "lower_case_database_names";

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
            USE_META_CACHE,
            CatalogProperty.ENABLE_MAPPING_VARBINARY,
            CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ);

    protected static final int ICEBERG_CATALOG_EXECUTOR_THREAD_NUM = Runtime.getRuntime().availableProcessors();

    public static final String TEST_CONNECTION = "test_connection";

    public static final String INCLUDE_DATABASE_LIST = "include_database_list";
    public static final String EXCLUDE_DATABASE_LIST = "exclude_database_list";
    public static final String LOWER_CASE_META_NAMES = "lower_case_meta_names";
    public static final String META_NAMES_MAPPING = "meta_names_mapping";
    // db1.tbl1,db2.tbl2,...
    public static final String INCLUDE_TABLE_LIST = "include_table_list";

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
    protected TransactionManager transactionManager;
    protected MetaCache<ExternalDatabase<? extends ExternalTable>> metaCache;
    protected ExecutionAuthenticator executionAuthenticator;
    protected ThreadPoolExecutor threadPoolWithPreAuth;
    // Map lowercase database names to actual remote database names for case-insensitive lookup
    private Map<String, String> lowerCaseToDatabaseName = Maps.newConcurrentMap();

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

    /**
     * Returns Hadoop-related properties as a plain Map.
     * Connector plugins should use this instead of getConfiguration()
     * and build their own Configuration internally when needed.
     */
    public Map<String, String> getHadoopProperties() {
        Map<String, String> props = new java.util.HashMap<>(catalogProperty.getHadoopProperties());
        if (ifNotSetFallbackToSimpleAuth()) {
            props.putIfAbsent("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
        return props;
    }

    /**
     * @deprecated Use {@link #getHadoopProperties()} and build Configuration locally.
     *             This method will be removed when connector SPI extraction is complete.
     */
    @Deprecated
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

    /**
     * Builds a Hadoop Configuration from a properties map.
     * Use this when you need a Configuration object from catalog properties.
     */
    public static Configuration buildHadoopConfiguration(Map<String, String> properties) {
        Configuration conf = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    private Configuration buildConf() {
        Configuration conf = new HdfsConfiguration();
        if (ifNotSetFallbackToSimpleAuth()) {
            conf.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
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
        throw new UnsupportedOperationException("List databases is not supported for catalog: " + getName());
    }

    // Will be called when creating catalog(so when as replaying)
    // to add some default properties if missing.
    public void setDefaultPropsIfMissing(boolean isReplay) {
        // set default value to true, no matter is replaying or not.
        // After 4.0, all external catalogs will use meta cache by default.
        catalogProperty.addProperty(USE_META_CACHE, String.valueOf(DEFAULT_USE_META_CACHE));
        if (catalogProperty.getOrDefault(CatalogProperty.ENABLE_MAPPING_VARBINARY, "").isEmpty()) {
            catalogProperty.setEnableMappingVarbinary(false);
        }
        if (catalogProperty.getOrDefault(CatalogProperty.ENABLE_MAPPING_TIMESTAMP_TZ, "").isEmpty()) {
            catalogProperty.setEnableMappingTimestampTz(false);
        }
    }

    public boolean getEnableMappingVarbinary() {
        return catalogProperty.getEnableMappingVarbinary();
    }

    public boolean getEnableMappingTimestampTz() {
        return catalogProperty.getEnableMappingTimestampTz();
    }

    // we need check auth fallback for kerberos or simple
    public boolean ifNotSetFallbackToSimpleAuth() {
        return catalogProperty.getOrDefault("ipc.client.fallback-to-simple-auth-allowed", "").isEmpty();
    }

    // Will be called when creating catalog(not replaying).
    // Subclass can override this method to do some check when creating catalog.
    // Connectivity testing (test_connection=true) is connector-specific and therefore lives behind the
    // connector SPI: PluginDrivenExternalCatalog overrides this and delegates to Connector#testConnection.
    // The built-in catalogs that still inherit this method (type=doris, type=test) carry neither metastore
    // nor storage properties, so there is nothing generic left to check here.
    public void checkWhenCreating() throws DdlException {
    }

    /**
     * @param dbName
     * @return names of tables in specified database, filtered by include_table_list if configured
     */
    public final List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        Map<String, List<String>> includeTableMap = getIncludeTableMap();
        if (includeTableMap.containsKey(dbName) && !includeTableMap.get(dbName).isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("get table list from include map. catalog: {}, db: {}, tables: {}",
                        name, dbName, includeTableMap.get(dbName));
            }
            return includeTableMap.get(dbName);
        }
        return listTableNamesFromRemote(ctx, dbName);
    }

    /**
     * Subclasses implement this method to list table names from the remote data source.
     *
     * @param ctx session context
     * @param dbName database name
     * @return names of tables in the specified database from the remote source
     */
    protected abstract List<String> listTableNamesFromRemote(SessionContext ctx, String dbName);

    /**
     * Returns whether the shared table-name cache should be skipped for the current session.
     *
     * Catalogs whose remote list result depends on session credentials should bypass the cache so one user's
     * visible table set is not reused for another user.
     *
     * @param ctx session context for the current request
     * @return true if table names must be fetched from the remote source for this session
     */
    protected boolean shouldBypassTableNameCache(SessionContext ctx) {
        return false;
    }

    /**
     * Returns whether the shared database-name cache should be skipped for the current session (the db-level
     * analog of {@link #shouldBypassTableNameCache}).
     *
     * <p>Catalogs whose remote {@code listDatabaseNames} result depends on session credentials (Iceberg REST
     * {@code session=user}) must bypass the shared (catalog-wide, NOT user-keyed) db-name cache, so one user's
     * visible database set is never served to another. Default {@code false} — every other catalog keeps the
     * cache.</p>
     *
     * @param ctx session context for the current request
     * @return true if database names must be fetched live from the remote source for this session
     */
    protected boolean shouldBypassDbNameCache(SessionContext ctx) {
        return false;
    }

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
                buildMetaCache();
                setLastUpdateTime(System.currentTimeMillis());
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

    /**
     * Records the root-cause of a deferred metadata-load failure into {@code errorMsg} so it is
     * visible in {@code show catalogs}. Some connectors connect lazily on first metadata access
     * (their {@link #initLocalObjectsImpl()} only constructs the client), so the initial failure
     * happens inside the meta-cache loader — outside {@link #makeSureInitialized()}'s try/catch,
     * which is the only other place {@code errorMsg} is written. The message is cleared again by
     * {@link #makeSureInitialized()} on the next successful (re-)initialization, e.g. after
     * {@code alter catalog ... set properties} triggers {@link #resetToUninitialized(boolean)}.
     */
    protected void recordDeferredInitError(Throwable t) {
        this.errorMsg = ExceptionUtils.getRootCauseMessage(t);
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
            metaCache = Env.getCurrentEnv().getExtMetaCacheMgr().legacyMetaCacheFactory().build(
                    name,
                    OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                    OptionalLong.of(Config.external_cache_refresh_time_minutes * 60L),
                    Math.max(Config.max_meta_object_cache_num, 1),
                    ignored -> getFilteredDatabaseNames(),
                    localDbName -> Optional.ofNullable(
                            buildDbForInit(null, localDbName, Util.genIdByName(name, localDbName), logType,
                                    true)),
                    (key, value, cause) -> value.ifPresent(v -> v.resetMetaToUninitialized()));
        }
    }

    /**
     * Hook for plugin/SPI catalogs to overlay DERIVED meta-cache config (e.g. a connector-provided schema-cache
     * TTL) onto the EPHEMERAL property copy the engine uses to size the meta cache. Default no-op. MUST NOT
     * mutate persisted catalog properties — the caller ({@code ExternalMetaCacheMgr.findCatalogProperties})
     * passes a throwaway copy, so SHOW CREATE CATALOG is unaffected. Connector-agnostic: the base does nothing;
     * {@code PluginDrivenExternalCatalog} delegates to the connector SPI.
     */
    public void overlayMetaCacheConfig(Map<String, String> metaCacheProperties) {
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
        return getFilteredDatabaseNames(true);
    }

    /**
     * @param updateDbNameLookup when {@code false}, the shared {@code lowerCaseToDatabaseName} lookup is NOT
     *     mutated. The db-name cache-bypass path ({@link #shouldBypassDbNameCache}) passes {@code false} so a
     *     per-user database listing never overwrites the shared (catalog-wide) case-insensitive lookup with one
     *     user's visible set — mirrors {@code ExternalDatabase.loadTableNamePairs}'s {@code updateTableNameLookup}.
     */
    @NotNull
    private List<Pair<String, String>> getFilteredDatabaseNames(boolean updateDbNameLookup) {
        List<String> allDatabases = Lists.newArrayList(listDatabaseNames());
        allDatabases.remove(InfoSchemaDb.DATABASE_NAME);
        allDatabases.add(InfoSchemaDb.DATABASE_NAME);
        allDatabases.remove(MysqlDb.DATABASE_NAME);
        allDatabases.add(MysqlDb.DATABASE_NAME);

        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();

        if (updateDbNameLookup) {
            lowerCaseToDatabaseName.clear();
        }
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
            // Populate lowercase mapping for case-insensitive lookups
            if (updateDbNameLookup) {
                lowerCaseToDatabaseName.put(remoteDbName.toLowerCase(), remoteDbName);
            }
            // Apply lower_case_database_names mode to local name
            int dbNameMode = getLowerCaseDatabaseNames();
            if (dbNameMode == 1) {
                localDbName = localDbName.toLowerCase();
            } else if (dbNameMode == 2) {
                // Mode 2: preserve original remote case for display
                localDbName = remoteDbName;
            }
            remoteToLocalPairs.add(Pair.of(remoteDbName, localDbName));
        }

        // Check for conflicts when lower_case_meta_names = true or lower_case_database_names = 2
        if (Boolean.parseBoolean(getLowerCaseMetaNames()) || getLowerCaseDatabaseNames() == 2) {
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
    public void resetToUninitialized(boolean invalidCache) {
        synchronized (this) {
            this.objectCreated = false;
            this.initialized = false;
            synchronized (this.confLock) {
                this.cachedConf = null;
            }
            this.lowerCaseToDatabaseName.clear();
            onClose();
        }
        onRefreshCache(invalidCache);
    }

    /**
     * Refresh both meta cache and catalog cache.
     *
     * @param invalidCache
     */
    public void onRefreshCache(boolean invalidCache) {
        setLastUpdateTime(System.currentTimeMillis());
        refreshMetaCacheOnly();
        if (invalidCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalog(id);
        }
    }

    /**
     * Refresh meta cache only (database level cache), without invalidating catalog level cache.
     * This method is safe to call within synchronized block.
     */
    private void refreshMetaCacheOnly() {
        if (metaCache != null) {
            metaCache.invalidateAll();
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
        SessionContext sessionContext = SessionContext.current();
        if (shouldBypassDbNameCache(sessionContext)) {
            // Per-user listing: read live (the loader's listDatabaseNames already runs under the current
            // session) and DO NOT touch the shared lowerCaseToDatabaseName lookup (updateDbNameLookup=false).
            return getFilteredDatabaseNames(false).stream()
                    .map(Pair::value)
                    .collect(Collectors.toList());
        }
        return metaCache.listNames();
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
    public List<String> getTableNameByTableId(long tableId) {
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

        SessionContext sessionContext = SessionContext.current();
        if (shouldBypassDbNameCache(sessionContext)) {
            return getDbNullableWithoutCache(dbName);
        }

        // information_schema db name is case-insensitive.
        // So, we first convert it to standard database name.
        if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            dbName = InfoSchemaDb.DATABASE_NAME;
        } else if (dbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            dbName = MysqlDb.DATABASE_NAME;
        } else {
            // Apply case-insensitive lookup for non-system databases
            String localDbName = getLocalDatabaseName(dbName, false);
            if (localDbName != null) {
                dbName = localDbName;
            }
        }

        // must use full qualified name to generate id.
        // otherwise, if 2 catalogs have the same db name, the id will be the same.
        return metaCache.getMetaObj(dbName, Util.genIdByName(name, dbName)).orElse(null);
    }

    /**
     * Live (no-cache) counterpart of {@link #getDbNullable(String)} for the db-name cache-bypass path
     * ({@link #shouldBypassDbNameCache}). Resolves the requested db against the per-user live listing (never
     * populating the shared caches) and builds the {@link ExternalDatabase} object directly. Mirrors
     * {@code ExternalDatabase.getTableNullableWithoutCache} one level up.
     */
    private ExternalDatabase<? extends ExternalTable> getDbNullableWithoutCache(String dbName) {
        // Normalize the case-insensitive system db names, mirroring the cached getDbNullable path.
        String requestedDbName = dbName;
        if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            requestedDbName = InfoSchemaDb.DATABASE_NAME;
        } else if (dbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            requestedDbName = MysqlDb.DATABASE_NAME;
        }
        final String target = requestedDbName;
        Optional<Pair<String, String>> dbNamePair = getFilteredDatabaseNames(false).stream()
                .filter(pair -> matchesLocalDbName(pair.value(), target))
                .findFirst();
        if (!dbNamePair.isPresent()) {
            return null;
        }
        String remoteDbName = dbNamePair.get().key();
        String localDbName = dbNamePair.get().value();
        // checkExists=false: the pair came from the live listing, so re-listing (getDbNames) is both
        // unnecessary and would recurse back through this bypass — build the db object directly.
        return buildDbForInit(remoteDbName, localDbName, Util.genIdByName(name, localDbName), logType, false);
    }

    /** Matches a live listing's LOCAL db name against a requested name, honoring the db-name case modes. */
    private boolean matchesLocalDbName(String localDbName, String dbName) {
        // System dbs (already normalized to canonical form by the caller) match case-insensitively.
        if (dbName.equals(InfoSchemaDb.DATABASE_NAME) || dbName.equals(MysqlDb.DATABASE_NAME)) {
            return localDbName.equalsIgnoreCase(dbName);
        }
        int mode = getLowerCaseDatabaseNames();
        if (mode == 1) {
            return localDbName.equals(dbName.toLowerCase());
        }
        if (mode == 2 || Boolean.parseBoolean(getLowerCaseMetaNames())) {
            return localDbName.equalsIgnoreCase(dbName);
        }
        return localDbName.equals(dbName);
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

        return metaCache.getMetaObjById(dbId).orElse(null);
    }

    @Override
    public List<Long> getDbIds() {
        makeSureInitialized();
        return getAllDbs().stream().map(DatabaseIf::getId).collect(Collectors.toList());
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
        if (!isInitialized()) {
            return Optional.empty();
        }
        return metaCache.getMetaObjById(dbId);
    }

    /**
     * Same as "getDbForReplay(long dbId)", use "tryGetMetaObj" to get db from cache only.
     *
     * @param dbName
     * @return
     */
    public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getDbForReplay from metacache, db: {}.{}, catalog id: {}, is catalog init: {}",
                    this.name, dbName, this.id, isInitialized());
        }
        if (!isInitialized()) {
            return Optional.empty();
        }

        // Apply case-insensitive lookup with isReplay=true (no remote calls)
        String localDbName = getLocalDatabaseName(dbName, true);
        if (localDbName == null) {
            localDbName = dbName;  // Fallback to original name
        }

        return metaCache.tryGetMetaObj(localDbName);
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
        if (remoteDbName == null) {
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
                // Hive (hms) is flipped to the plugin path (PluginDrivenExternalCatalog); the HMSExternalDatabase
                // entity class is dead-for-hms (deleted with the legacy subsystem in the deletion phase). This
                // case only fires when replaying an old InitCatalogLog persisted with Type.HMS (a base
                // ExternalCatalog whose logType was serialized as HMS) — build the post-flip runtime type so the
                // db (and the PluginDrivenMvccExternalTable it builds) matches the GSON remap. Keep the case
                // label: deleting it would fall through to `return null` and break db init on replay. The
                // Type.HMS enum is retained for old-image deserialization.
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            case JDBC:
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            case ICEBERG:
                // Native iceberg is flipped to the plugin path (PluginDrivenExternalCatalog); the
                // IcebergExternalDatabase entity class is being removed. This case only fires when
                // replaying an old InitCatalogLog persisted with Type.ICEBERG (a base ExternalCatalog
                // whose logType was serialized as ICEBERG) — build the post-flip runtime type so the
                // db (and the PluginDrivenExternalTable it builds) matches the GSON remap. Keep the
                // case label: deleting it would fall through to `return null` and break db init on
                // replay. Type.ICEBERG enum is retained for old-image deserialization.
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            case LAKESOUL:
                // LakeSoul is deprecated and was never migrated to the plugin path; its entity classes are
                // removed. This case only fires when replaying an old InitCatalogLog persisted with
                // Type.LAKESOUL — build a PluginDrivenExternalDatabase so the db matches the GSON remap
                // (registerCompatibleSubtype -> PluginDrivenExternalDatabase). Keep the case label: deleting
                // it would fall through to `return null` and break db init on replay. The Type.LAKESOUL enum
                // is retained for old-image deserialization.
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            case TEST:
                return new TestExternalDatabase(this, dbId, localDbName, remoteDbName);
            case TRINO_CONNECTOR:
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            case REMOTE_DORIS:
                return new RemoteDorisExternalDatabase(this, dbId, localDbName, remoteDbName);
            case PLUGIN:
                return new PluginDrivenExternalDatabase(this, dbId, localDbName, remoteDbName);
            default:
                break;
        }
        return null;
    }

    @Override
    public void gsonPostProcess() throws IOException {
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
        if (this.lowerCaseToDatabaseName == null) {
            this.lowerCaseToDatabaseName = Maps.newConcurrentMap();
        }
    }

    public void addDatabaseForTest(ExternalDatabase<? extends ExternalTable> db) {
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
        }
    }

    @Override
    public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
        makeSureInitialized();
        throw new DdlException("Create database is not supported for catalog: " + getName());
    }

    public void replayCreateDb(String dbName) {
        // Invalidate the FE cache directly so follower FEs reflect the create on edit-log replay.
        resetMetaCacheNames();
    }

    @Override
    public void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        makeSureInitialized();
        throw new DdlException("Drop database is not supported for catalog: " + getName());
    }

    public void replayDropDb(String dbName) {
        // Drop the db from the cache on replay.
        unregisterDatabase(dbName);
    }

    @Override
    public boolean createTable(CreateTableInfo createTableInfo) throws UserException {
        makeSureInitialized();
        throw new DdlException("Create table is not supported for catalog: " + getName());
    }

    public void replayCreateTable(String dbName, String tblName) {
        // Refresh the db's table-name cache on replay.
        getDbForReplay(dbName).ifPresent(db -> db.resetMetaCacheNames());
    }

    @Override
    public void renameTable(String dbName, String oldTableName, String newTableName) throws DdlException {
        makeSureInitialized();
        throw new DdlException("Rename table is not supported for catalog: " + getName());
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean isStream,
                          boolean ifExists, boolean mustTemporary, boolean force) throws DdlException {
        makeSureInitialized();
        throw new DdlException("Drop table is not supported for catalog: " + getName());
    }

    public void replayDropTable(String dbName, String tblName) {
        // Remove the table from the cache on replay.
        getDbForReplay(dbName).ifPresent(db -> db.unregisterTable(tblName));
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
        if (isInitialized()) {
            metaCache.invalidate(dbName, Util.genIdByName(name, dbName));
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDb(getId(), dbName);
    }

    public void registerDatabase(long dbId, String dbName) {
        throw new NotImplementedException("registerDatabase not implemented");
    }

    protected Map<String, Boolean> getIncludeDatabaseMap() {
        return getSpecifiedDatabaseMap(ExternalCatalog.INCLUDE_DATABASE_LIST);
    }

    protected Map<String, Boolean> getExcludeDatabaseMap() {
        return getSpecifiedDatabaseMap(ExternalCatalog.EXCLUDE_DATABASE_LIST);
    }

    protected Map<String, List<String>> getIncludeTableMap() {
        Map<String, List<String>> includeTableMap = Maps.newHashMap();
        String tableList = catalogProperty.getOrDefault(ExternalCatalog.INCLUDE_TABLE_LIST, "");
        if (Strings.isNullOrEmpty(tableList)) {
            return includeTableMap;
        }
        String[] parts = tableList.split(",");
        for (String part : parts) {
            String dbTbl = part.trim();
            String[] splits = dbTbl.split("\\.");
            if (splits.length != 2) {
                LOG.warn("debug invalid include table list: {}, ignore", part);
                continue;
            }
            String db = splits[0];
            String tbl = splits[1];
            List<String> tbls = includeTableMap.get(db);
            if (tbls == null) {
                includeTableMap.put(db, Lists.newArrayList());
                tbls = includeTableMap.get(db);
            }
            tbls.add(tbl);
        }
        LOG.info("debug get include table map: {}", includeTableMap);
        return includeTableMap;
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
        return catalogProperty.getOrDefault(LOWER_CASE_META_NAMES, "false");
    }

    @Override
    public int getLowerCaseTableNames() {
        return Integer.parseInt(catalogProperty.getOrDefault(LOWER_CASE_TABLE_NAMES,
                catalogProperty.getOrDefault(ONLY_TEST_LOWER_CASE_TABLE_NAMES,
                        String.valueOf(GlobalVariable.lowerCaseTableNames))));
    }

    /**
     * Get the lower_case_database_names configuration value.
     * Returns the mode for database name case handling:
     * - 0: Case-sensitive (default)
     * - 1: Database names are stored as lowercase
     * - 2: Database name comparison is case-insensitive
     */
    @Override
    public int getLowerCaseDatabaseNames() {
        return Integer.parseInt(catalogProperty.getOrDefault(LOWER_CASE_DATABASE_NAMES, "0"));
    }

    public String getMetaNamesMapping() {
        return catalogProperty.getOrDefault(ExternalCatalog.META_NAMES_MAPPING, "");
    }

    /**
     * Get the local database name based on the lower_case_database_names mode.
     * Handles case-insensitive database lookup similar to ExternalDatabase.getLocalTableName().
     */
    @Nullable
    private String getLocalDatabaseName(String dbName, boolean isReplay) {
        String finalName = dbName;
        int mode = getLowerCaseDatabaseNames();

        if (mode == 1) {
            // Mode 1: Store as lowercase
            finalName = dbName.toLowerCase();
        } else if (mode == 2) {
            // Mode 2: Case-insensitive comparison
            finalName = lowerCaseToDatabaseName.get(dbName.toLowerCase());
            if (finalName == null && !isReplay) {
                // Refresh database list and try again
                try {
                    getFilteredDatabaseNames();
                    finalName = lowerCaseToDatabaseName.get(dbName.toLowerCase());
                } catch (Exception e) {
                    LOG.warn("Failed to refresh database list for catalog {}", getName(), e);
                }
            }
            if (finalName == null && LOG.isDebugEnabled()) {
                LOG.debug("Failed to get database name from: {}.{}, isReplay={}",
                        getName(), dbName, isReplay);
            }
        }

        return finalName;
    }

    public String bindBrokerName() {
        return catalogProperty.getProperties().get(BrokerProperties.BIND_BROKER_NAME_KEY);
    }

    // ATTN: this method only return all cached databases.
    // will not visit remote datasource's metadata
    @Override
    public Collection<DatabaseIf<? extends TableIf>> getAllDbs() {
        makeSureInitialized();
        Set<DatabaseIf<? extends TableIf>> dbs = Sets.newHashSet();
        List<String> dbNames = getDbNames();
        for (String dbName : dbNames) {
            ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
            if (db != null) {
                dbs.add(db);
            }
        }
        return dbs;
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
    public void truncateTable(String dbName, String tableName, PartitionNamesInfo partitionNamesInfo, boolean forceDrop,
                              String rawTruncateSql) throws DdlException {
        makeSureInitialized();
        throw new DdlException("Truncate table is not supported for catalog: " + getName());
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        // External truncate replay is a cache no-op; the table is re-read from remote on access.
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
        resetToUninitialized(false);
        String schemaCacheTtl = updatedProps.getOrDefault(SCHEMA_CACHE_TTL_SECOND, null);
        if (java.util.Objects.nonNull(schemaCacheTtl)) {
            ExternalMetaCacheMgr extMetaCacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
            extMetaCacheMgr.removeCatalog(id);
        }
    }

    public CatalogProperty getCatalogProperty() {
        return catalogProperty;
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
        throw new DdlException("branching operation is not supported for catalog: " + getName());
    }

    @Override
    public void createOrReplaceTag(TableIf dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Tagging operation is not supported for catalog: " + getName());
    }

    @Override
    public void replayOperateOnBranchOrTag(String dbName, String tblName) {
        // External branch/tag replay is a cache no-op; the table is re-read from remote on access.
    }

    @Override
    public void dropBranch(TableIf dorisTable, DropBranchInfo branchInfo) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("DropBranch operation is not supported for catalog: " + getName());
    }

    @Override
    public void dropTag(TableIf dorisTable, DropTagInfo tagInfo) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("DropTag operation is not supported for catalog: " + getName());
    }

    /**
     * Resets the name list in meta cache.
     * Usually used after creating database in catalog, so that user can see newly created db immediately.
     */
    public void resetMetaCacheNames() {
        if (metaCache != null) {
            metaCache.resetNames();
        }
    }

    @Override
    public void addColumn(TableIf dorisTable, Column column, ColumnPosition position) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Add column operation is not supported for catalog: " + getName());
    }

    @Override
    public void addColumns(TableIf dorisTable, List<Column> columns) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Add columns operation is not supported for catalog: " + getName());
    }

    @Override
    public void dropColumn(TableIf dorisTable, String columnName) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Drop column operation is not supported for catalog: " + getName());
    }

    @Override
    public void renameColumn(TableIf dorisTable, String oldName, String newName) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Rename column operation is not supported for catalog: " + getName());
    }

    @Override
    public void modifyColumn(TableIf dorisTable, Column column, ColumnPosition columnPosition) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Modify column operation is not supported for catalog: " + getName());
    }

    @Override
    public void reorderColumns(TableIf dorisTable, List<String> newOrder) throws UserException {
        makeSureInitialized();
        Preconditions.checkState(dorisTable instanceof ExternalTable, dorisTable.getName());
        throw new DdlException("Reorder columns operation is not supported for catalog: " + getName());
    }
}
