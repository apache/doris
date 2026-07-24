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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.NameCacheValue;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class of external database.
 *
 * @param <T> External table type is ExternalTable or its subclass.
 */
public abstract class ExternalDatabase<T extends ExternalTable>
        implements DatabaseIf<T>, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalDatabase.class);

    protected MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock(true);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "remoteName")
    protected String remoteName;
    @SerializedName(value = "dbProperties")
    protected DatabaseProperty dbProperties = new DatabaseProperty();
    @SerializedName(value = "initialized")
    protected boolean initialized = false;
    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;
    protected final InitDatabaseLog.Type dbLogType;
    protected ExternalCatalog extCatalog;

    private MetaCacheEntry<String, NameCacheValue> tableNames;
    private MetaCacheEntry<String, T> tables;
    private Map<Long, String> tableIdToName = Maps.newConcurrentMap();

    private volatile boolean isInitializing = false;

    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param id Database id.
     * @param name Database name.
     * @param remoteName Remote database name.
     */
    public ExternalDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName,
            InitDatabaseLog.Type dbLogType) {
        this.extCatalog = extCatalog;
        this.id = id;
        this.name = name;
        this.remoteName = remoteName;
        this.dbLogType = dbLogType;
    }

    public void setExtCatalog(ExternalCatalog extCatalog) {
        this.extCatalog = extCatalog;
    }

    public void setRemoteName(String remoteName) {
        this.remoteName = remoteName;
    }

    public void resetMetaToUninitialized() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("resetToUninitialized db name {}, id {}, isInitializing: {}, initialized: {}",
                    this.name, this.id, isInitializing, initialized, new Exception());
        }
        synchronized (this) {
            this.initialized = false;
            invalidateAllTableCache();
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDb(extCatalog.getId(), getFullName());
    }

    public boolean isInitialized() {
        return initialized;
    }

    public final void makeSureInitialized() {
        // Must call this method before any operation on the database to avoid deadlock of synchronized block
        extCatalog.makeSureInitialized();
        synchronized (this) {
            if (isInitializing) {
                return;
            }
            isInitializing = true;
            try {
                if (!initialized) {
                    buildMetaCache();
                    setLastUpdateTime(System.currentTimeMillis());
                    initialized = true;
                }
            } catch (Exception e) {
                LOG.warn("failed to init db {}, id {}, isInitializing: {}, initialized: {}",
                        this.name, this.id, isInitializing, initialized, e);
                initialized = false;
            } finally {
                isInitializing = false;
            }
        }
    }

    private void buildMetaCache() {
        if (tableNames != null && tables != null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("buildMetaCache for database: {}:{}", this.name, this.id, new Exception());
        }
        CacheSpec namesSpec = CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                1);
        // Build one immutable names snapshot so list and lower-case index share the same cache version.
        tableNames = new MetaCacheEntry<>(
                name + ".table_names",
                ignored -> NameCacheValue.of(listTableNames()),
                namesSpec,
                Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
                true,
                MetaCacheEntry.singleKeyStripeCount());

        CacheSpec objectSpec = CacheSpec.of(
                true,
                Config.external_cache_expire_time_seconds_after_access,
                Math.max(Config.max_meta_object_cache_num, 1));
        // Keep table object entries on the normal manual-miss/refresh path because they do not need
        // the database-level reset callback used by catalog object caches.
        tables = new MetaCacheEntry<>(
                name + ".tables",
                localTableName -> buildTableForInit(
                        null,
                        localTableName,
                        Util.genIdByName(extCatalog.getName(), name, localTableName),
                        extCatalog,
                        this,
                        true),
                objectSpec,
                Env.getCurrentEnv().getExtMetaCacheMgr().commonRefreshExecutor(),
                false,
                MetaCacheEntry.defaultObjectStripeCount());
    }

    private List<Pair<String, String>> listTableNames() {
        return listTableNames(SessionContext.empty());
    }

    // Session-aware callers can bypass shared names/object caches and enumerate remote metadata directly.
    private List<Pair<String, String>> listTableNames(SessionContext ctx) {
        List<Pair<String, String>> tableNames;
        if (name.equals(InfoSchemaDb.DATABASE_NAME)) {
            tableNames = ExternalInfoSchemaDatabase.listTableNames().stream()
                    .map(tableName -> Pair.of(tableName, tableName))
                    .collect(Collectors.toList());
        } else if (name.equals(MysqlDb.DATABASE_NAME)) {
            tableNames = ExternalMysqlDatabase.listTableNames().stream()
                    .map(tableName -> Pair.of(tableName, tableName))
                    .collect(Collectors.toList());
        } else {
            // Allow manual regression to isolate database-level table enumeration cost during collect.
            if (DebugPointUtil.isEnable("ExternalDatabase.listTableNames.sleep")) {
                long sleepMs = DebugPointUtil.getDebugParamOrDefault(
                        "ExternalDatabase.listTableNames.sleep", "sleepMs", 0L);
                if (sleepMs > 0) {
                    LOG.info("debug point ExternalDatabase.listTableNames.sleep hit for {}.{}, sleep {}ms",
                            extCatalog.getName(), remoteName, sleepMs);
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            tableNames = extCatalog.listTableNames(ctx, remoteName).stream()
                    .map(tableName -> Pair.of(tableName, canonicalLocalTableNameFromRemote(tableName)))
                    .collect(Collectors.toList());
        }
        // Check for conflicts when stored table names or meta names are case-insensitive
        if (Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
                || this.isStoredTableNamesLowerCase()
                || this.isTableNamesCaseInsensitive()) {
            // Map to track lowercased local names and their corresponding remote names
            Map<String, List<String>> lowerCaseToRemoteNames = Maps.newHashMap();

            // Collect lowercased local names and their remote counterparts
            for (Pair<String, String> pair : tableNames) {
                String lowerCaseLocalName = pair.value().toLowerCase(Locale.ROOT);
                lowerCaseToRemoteNames.computeIfAbsent(lowerCaseLocalName, k -> Lists.newArrayList()).add(pair.key());
            }

            // Identify conflicts: multiple remote names mapping to the same lowercased local name
            List<String> conflicts = lowerCaseToRemoteNames.values().stream()
                    .filter(remoteNames -> remoteNames.size() > 1) // Conflict: more than one remote name
                    .flatMap(List::stream) // Collect all conflicting remote names
                    .collect(Collectors.toList());

            // Throw exception if conflicts are found
            if (!conflicts.isEmpty()) {
                throw new RuntimeException(String.format(
                        ExternalCatalog.FOUND_CONFLICTING + " table names under case-insensitive conditions. "
                                + "Conflicting remote table names: %s in remote database '%s' under catalog '%s'. "
                                + "Please use meta_names_mapping to handle name mapping.",
                        String.join(", ", conflicts), remoteName, extCatalog.getName()));
            }
        }
        return tableNames;
    }

    public T buildTableForInit(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog, ExternalDatabase db, boolean checkExists) {

        // Step 1: Resolve local table name if not provided
        if (localTableName == null && remoteTableName != null) {
            localTableName = extCatalog.fromRemoteTableName(remoteName, remoteTableName);
        }

        // Step 2: Check if the table exists in the system, if the `checkExists` flag is enabled
        if (checkExists && (!FeConstants.runningUnitTest || this instanceof TestExternalDatabase)) {
            try {
                final String lookupLocalTableName = localTableName;
                // Reuse the shared names lookup helper so existence checks follow the same miss-refresh policy.
                Boolean exists = resolveTableNameFromSnapshot(lookupLocalTableName, false,
                        namesValue -> namesValue.containsLocalName(lookupLocalTableName) ? Boolean.TRUE : null);
                if (!Boolean.TRUE.equals(exists)) {
                    LOG.warn("Table {} does not exist in the remote database {}. Skipping initialization.",
                            localTableName, this.name);
                    return null;
                }
            } catch (RuntimeException e) {
                // Handle "Found conflicting" exception explicitly
                if (e.getMessage().contains(ExternalCatalog.FOUND_CONFLICTING)) {
                    LOG.error(e.getMessage());
                    throw e; // Rethrow to let the caller handle this critical issue
                } else {
                    // Any errors other than name conflicts, we default to not finding the table
                    LOG.warn("Failed to check existence of table {} in the remote system. Ignoring this table.",
                            localTableName, e);
                    return null;
                }
            } catch (Exception e) {
                // If connection fails, treat the table as non-existent
                LOG.warn("Failed to check existence of table {} in the remote system. Ignoring this table.",
                        localTableName, e);
                return null;
            }
        }

        // Step 3: Resolve remote table name when local/remote mapping is active.
        if (remoteTableName == null) {
            if (Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
                    || !Strings.isNullOrEmpty(extCatalog.getMetaNamesMapping())
                    || this.isStoredTableNamesLowerCase()) {
                remoteTableName = getRemoteTableName(localTableName, false);
                if (remoteTableName == null) {
                    LOG.warn("Could not resolve remote table name for local table: {}", localTableName);
                    return null;
                }
            } else {
                remoteTableName = localTableName;
            }
        }

        // Step 4: Build and return the table instance using the resolved names and other parameters
        return buildTableInternal(remoteTableName, localTableName, tblId, catalog, db);
    }

    protected abstract T buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog, ExternalDatabase db);

    /**
     * This method tries getting table from cache only.
     * If there is no cache, it returns empty.
     * Different from "getTableNullable()", this method does not perform synchronous load-through on a replay miss.
     * Cache hits may still schedule asynchronous refresh-after-write in the background, but the replay caller never
     * waits for remote metadata loading.
     * This is used for replaying metadata to avoid synchronous remote lookup failures on the replay thread.
     *
     * @param tableId
     * @return
     */
    public Optional<T> getTableForReplay(long tableId) {
        if (!isInitialized()) {
            return Optional.empty();
        }
        String tableName = tableIdToName.get(tableId);
        if (tableName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(tables.getIfPresent(tableName));
    }

    /**
     * Resolve the retained local table name for replay without loading table metadata.
     *
     * <p>The ID map intentionally outlives object-cache eviction so normal by-ID lookup and legacy ID-only edit logs
     * can still resolve the table identity. Replay can use this name to invalidate independent engine caches when the
     * table object itself is cold.
     */
    public Optional<String> getTableNameForReplay(long tableId) {
        if (!isInitialized()) {
            return Optional.empty();
        }
        return Optional.ofNullable(tableIdToName.get(tableId));
    }

    /**
     * Same as "getTableForReplay(long tableId)", but resolves the local name from the cached names snapshot first.
     * Replay misses still skip synchronous load-through. If the names entry is already hot, cache internals may
     * schedule asynchronous refresh-after-write, but this method never waits for remote metadata loading.
     *
     * @param tblName
     * @return
     */
    public Optional<T> getTableForReplay(String tblName) {
        if (!isInitialized()) {
            return Optional.empty();
        }
        // Preserve replay cache-only semantics even after names-only invalidation
        // by checking the exact object key first.
        T exact = tables.getIfPresent(tblName);
        if (exact != null) {
            return Optional.of(exact);
        }
        String localName = getLocalTableName(tblName, true);
        if (localName == null) {
            // Replay must remain cache-only. When the names snapshot is cold in mode 2, fall back to
            // a case-insensitive scan over the current hot object-cache keys instead of reloading names.
            if (isTableNamesCaseInsensitive() && getTableNamesValue(false) == null) {
                T fallback = tables.findIfPresent(key -> key.equalsIgnoreCase(tblName));
                if (fallback != null) {
                    return Optional.of(fallback);
                }
            }
            return Optional.empty();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getTableForReplay from metacache, db: {}.{}, is db init: {}",
                    this.extCatalog.getName(), this.name, true);
        }
        return Optional.ofNullable(tables.getIfPresent(localName));
    }

    @Override
    public void readLock() {
        this.rwLock.readLock().lock();
    }

    @Override
    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    @Override
    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    @Override
    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at external db[" + id + "]", e);
            return false;
        }
    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    @Override
    public boolean writeLockIfExist() {
        writeLock();
        return true;
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
    }

    @Override
    public void writeLockOrDdlException() throws DdlException {
        writeLock();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getFullName() {
        return name;
    }

    public String getRemoteName() {
        return Strings.isNullOrEmpty(remoteName) ? name : remoteName;
    }

    @Override
    public DatabaseProperty getDbProperties() {
        return dbProperties;
    }

    @Override
    public boolean isTableExist(String tableName) {
        SessionContext sessionContext = SessionContext.current();
        String remoteTblName = tableName;
        if (extCatalog.shouldBypassTableNameCache(sessionContext) && requiresRemoteTableNameResolution()) {
            Optional<Pair<String, String>> matched = findTableNamePairWithoutCache(sessionContext, tableName);
            remoteTblName = matched.map(Pair::key).orElse(null);
        } else if (this.isTableNamesCaseInsensitive()) {
            // Route mode-2 lookups through the shared helper so hot-snapshot misses respect the mutable config.
            remoteTblName = resolveTableNameFromSnapshot(tableName, false,
                    namesValue -> namesValue.remoteNameForCaseInsensitiveLookup(tableName));
        }
        if (remoteTblName == null) {
            return false;
        }
        return extCatalog.tableExist(sessionContext, remoteName, remoteTblName);
    }

    // ATTN: this method only returned cached tables.
    @Override
    public List<T> getTables() {
        makeSureInitialized();
        List<T> tables = Lists.newArrayList();
        Set<String> tblNames = getTableNamesWithLock();
        for (String tblName : tblNames) {
            try {
                T tbl = getTableNullable(tblName);
                if (tbl != null) {
                    tables.add(tbl);
                }
            } catch (Exception e) {
                LOG.warn("Failed to get external table {}.{}.{} in SHOW TABLES path, skip it.",
                        extCatalog.getName(), name, tblName, e);
            }
        }
        return tables;
    }

    @Override
    public List<T> getViews() {
        throw new NotImplementedException("getViews() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        throw new NotImplementedException("getTablesOnIdOrderIfExist() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        throw new NotImplementedException("getTablesOnIdOrderOrThrowException() is not implemented");
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        SessionContext sessionContext = SessionContext.current();
        if (extCatalog.shouldBypassTableNameCache(sessionContext)) {
            return Sets.newHashSet(listLocalTableNamesWithoutCache(sessionContext));
        }
        return Sets.newHashSet(listLocalTableNamesFromCache());
    }

    @Override
    public T getTableNullable(String tableName) {
        makeSureInitialized();
        SessionContext sessionContext = SessionContext.current();
        if (extCatalog.shouldBypassTableNameCache(sessionContext)) {
            return getTableNullableWithoutCache(sessionContext, tableName);
        }
        String finalName = getLocalTableName(tableName, false);
        if (finalName == null) {
            return null;
        }
        return tables.getAndRunIfCurrent(
                finalName,
                (localTableName, table) -> !localTableName.equals(tableIdToName.get(table.getId())),
                (localTableName, table) -> tableIdToName.put(table.getId(), localTableName));
    }

    // User-session paths resolve table names directly from the remote source and intentionally skip shared caches.
    @Nullable
    private T getTableNullableWithoutCache(SessionContext sessionContext, String tableName) {
        Optional<Pair<String, String>> matched = findTableNamePairWithoutCache(sessionContext, tableName);
        if (!matched.isPresent()) {
            return null;
        }
        String remoteTableName = matched.get().key();
        String localTableName = matched.get().value();
        return buildTableForInit(remoteTableName, localTableName,
                Util.genIdByName(extCatalog.getName(), name, localTableName), extCatalog, this, false);
    }

    /**
     * Get the local table name based on the given table name.
     *
     * @param tableName
     * @param isReplay, if true, replay misses only consult the local snapshot and skip synchronous load-through.
     *         A hot names entry may still submit asynchronous refresh-after-write in the background.
     * @return
     */
    @Nullable
    private String getLocalTableName(String tableName, boolean isReplay) {
        String finalName = tableName;
        if (this.isStoredTableNamesLowerCase()) {
            finalName = tableName.toLowerCase(Locale.ROOT);
        }
        if (this.isTableNamesCaseInsensitive()) {
            // Route mode-2 lookups through the shared helper so hot-snapshot misses respect the mutable config.
            finalName = resolveTableNameFromSnapshot(tableName, isReplay,
                    namesValue -> namesValue.remoteNameForCaseInsensitiveLookup(tableName));
            if (finalName == null) {
                if (isReplay) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("failed to get final table name from: {}.{}.{}, is replay = true",
                                getCatalog().getName(), getFullName(), tableName);
                    }
                    return null;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("failed to get final table name from: {}.{}.{}",
                            getCatalog().getName(), getFullName(), tableName);
                }
                return null;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get table {} from database: {}.{}, final name is: {}, catalog id: {}",
                    tableName, getCatalog().getName(), getFullName(), finalName, getCatalog().getId());
        }
        return finalName;
    }

    private NameCacheValue getTableNamesValue(boolean allowLoad) {
        if (tableNames == null) {
            return null;
        }
        return allowLoad ? tableNames.get("") : tableNames.getIfPresent("");
    }

    // Centralize names-negative-lookup handling so all table lookup paths share the same config-driven policy.
    @Nullable
    private <R> R resolveTableNameFromSnapshot(String lookupName, boolean isReplay,
            Function<NameCacheValue, R> resolver) {
        NameCacheValue cached = getTableNamesValue(false);
        if (cached == null) {
            if (isReplay) {
                return null;
            }
            NameCacheValue loaded = getTableNamesValue(true);
            return loaded == null ? null : resolver.apply(loaded);
        }
        R resolved = resolver.apply(cached);
        if (resolved != null || isReplay || !Config.enable_external_meta_cache_name_miss_refresh) {
            return resolved;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("refresh table names after hot-snapshot miss, catalog: {}, db: {}, lookup: {}",
                    getCatalog().getName(), getFullName(), lookupName);
        }
        resetMetaCacheNames();
        NameCacheValue refreshed = getTableNamesValue(true);
        return refreshed == null ? null : resolver.apply(refreshed);
    }

    private List<String> listLocalTableNamesFromCache() {
        NameCacheValue namesValue = java.util.Objects.requireNonNull(
                getTableNamesValue(true), "table names cache can not be null");
        return namesValue.localNames();
    }

    private List<String> listLocalTableNamesWithoutCache(SessionContext sessionContext) {
        return listTableNames(sessionContext).stream().map(Pair::value).collect(Collectors.toList());
    }

    @Nullable
    private String getRemoteTableName(String localTableName, boolean isReplay) {
        // Route local-to-remote resolution through the shared helper so miss reload stays consistent with lookups.
        return resolveTableNameFromSnapshot(localTableName, isReplay,
                namesValue -> namesValue.remoteNameOfLocalName(localTableName));
    }

    private Optional<Pair<String, String>> findTableNamePairWithoutCache(SessionContext sessionContext,
            String requestedTableName) {
        return listTableNames(sessionContext).stream()
                .filter(pair -> matchesLocalTableName(pair.value(), requestedTableName))
                .findFirst();
    }

    private boolean matchesLocalTableName(String localTableName, String requestedTableName) {
        if (isStoredTableNamesLowerCase()) {
            return localTableName.equals(requestedTableName.toLowerCase(Locale.ROOT));
        }
        if (isTableNamesCaseInsensitive()) {
            return localTableName.equalsIgnoreCase(requestedTableName);
        }
        return localTableName.equals(requestedTableName);
    }

    private String resolveTableNameForInvalidation(String tableName) {
        if (isStoredTableNamesLowerCase()) {
            return tableName.toLowerCase(Locale.ROOT);
        }
        if (!isTableNamesCaseInsensitive()) {
            return tableName;
        }

        String localTableName = getLocalTableName(tableName, true);
        if (localTableName != null) {
            return localTableName;
        }
        T cachedTable = tables.findIfPresent(key -> key.equalsIgnoreCase(tableName));
        if (cachedTable != null) {
            return cachedTable.getName();
        }
        return tableIdToName.values().stream()
                .filter(name -> name.equalsIgnoreCase(tableName))
                .findFirst()
                .orElse(tableName);
    }

    private void updateTableCache(T table, String remoteTableName, String localTableName) {
        updateTableCache(table, remoteTableName, localTableName, false);
    }

    protected void updateTableCache(T table, String remoteTableName, String localTableName,
            boolean forceUpdateCacheState) {
        buildMetaCache();
        // Runtime incremental events only maintain names and object entries that are already hot. The ID map is a
        // lightweight lookup index and must always track registered objects so normal by-ID lookup can load on demand.
        if (forceUpdateCacheState) {
            tableNames.compute("", (ignored, current) ->
                    (current == null ? NameCacheValue.empty() : current).withName(remoteTableName, localTableName));
        } else {
            // Keep a cold names entry cold, but still advance its generation so an in-flight pre-event load cannot
            // publish a stale snapshot after this incremental update.
            tableNames.compute("", (ignored, current) ->
                    current == null ? null : current.withName(remoteTableName, localTableName));
        }
        tables.computeAndRun(
                localTableName,
                (ignored, current) -> forceUpdateCacheState || current != null ? table : null,
                () -> tableIdToName.put(table.getId(), localTableName));
    }

    protected void invalidateTableCache(String localTableName) {
        if (tableNames != null) {
            // Keep a cold names entry cold, but fence any in-flight pre-drop load from publishing stale state.
            tableNames.compute("", (ignored, current) ->
                    current == null ? null : current.withoutLocalName(localTableName));
        }
        if (tables != null) {
            tables.invalidateKeyAndRun(
                    localTableName,
                    () -> tableIdToName.entrySet().removeIf(
                            entry -> entry.getValue().equals(localTableName)));
        } else {
            tableIdToName.entrySet().removeIf(entry -> entry.getValue().equals(localTableName));
        }
    }

    private void invalidateAllTableCache() {
        if (tableNames != null) {
            tableNames.invalidateAll();
        }
        if (tables != null) {
            tables.invalidateAll();
        }
        tableIdToName.clear();
    }

    @Override
    public T getTableNullable(long tableId) {
        makeSureInitialized();
        String tableName = tableIdToName.get(tableId);
        if (tableName == null) {
            return null;
        }
        return tables.get(tableName);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.initialized = false;
        rwLock = new MonitoredReentrantReadWriteLock(true);
        tableIdToName = Maps.newConcurrentMap();
    }

    @Override
    public void unregisterTable(String tableName) {
        makeSureInitialized();
        if (LOG.isDebugEnabled()) {
            LOG.debug("unregister table {}.{}", this.name, tableName);
        }
        setLastUpdateTime(System.currentTimeMillis());
        String localTableName = resolveTableNameForInvalidation(tableName);
        // Always clean local names/object/ID state, even when the object entry is cold or already evicted.
        if (isInitialized()) {
            invalidateTableCache(localTableName);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateTable(extCatalog.getId(), getFullName(), localTableName);
    }

    @Override
    public CatalogIf getCatalog() {
        return extCatalog;
    }

    // Only used for sync hive metastore event
    @Override
    public boolean registerTable(TableIf tableIf) {
        makeSureInitialized();
        T table = (T) tableIf;
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", table.getName());
        }
        if (isInitialized()) {
            updateTableCache(table, table.getRemoteName(), table.getName());
        }
        setLastUpdateTime(System.currentTimeMillis());
        return true;
    }

    private boolean isStoredTableNamesLowerCase() {
        return extCatalog.getLowerCaseTableNames() == 1;
    }

    private boolean isTableNamesCaseInsensitive() {
        return extCatalog.getLowerCaseTableNames() == 2;
    }

    private boolean requiresRemoteTableNameResolution() {
        return isStoredTableNamesLowerCase()
                || isTableNamesCaseInsensitive()
                || Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
                || !Strings.isNullOrEmpty(extCatalog.getMetaNamesMapping());
    }

    String canonicalLocalTableNameFromRemote(String remoteTableName) {
        String localTableName = extCatalog.fromRemoteTableName(remoteName, remoteTableName);
        if (isStoredTableNamesLowerCase()) {
            return localTableName.toLowerCase(Locale.ROOT);
        }
        if (isTableNamesCaseInsensitive()) {
            // Mode 2 preserves the original remote case for display and object-cache keys.
            return remoteTableName;
        }
        return localTableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalDatabase)) {
            return false;
        }
        ExternalDatabase<?> that = (ExternalDatabase<?>) o;
        return Objects.equal(name, that.name) && Objects.equal(extCatalog,
                that.extCatalog);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, extCatalog);
    }

    @VisibleForTesting
    public void addTableForTest(T tbl) {
        buildMetaCache();
        // Test helpers only seed object/id state and keep names cache cold unless the test fills it explicitly.
        tables.computeAndRun(
                tbl.getName(),
                (ignored, current) -> tbl,
                () -> tableIdToName.put(tbl.getId(), tbl.getName()));
    }

    /**
     * Set the initialized status for testing purposes only.
     * This method should only be used in test cases.
     */
    @VisibleForTesting
    public void setInitializedForTest(boolean initialized) {
        this.initialized = initialized;
        if (this.initialized) {
            buildMetaCache();
        }
    }

    @VisibleForTesting
    @Nullable
    public NameCacheValue getCachedTableNamesForTest() {
        return tableNames == null ? null : tableNames.getIfPresent("");
    }

    @VisibleForTesting
    @Nullable
    public T getCachedTableForTest(String localTableName) {
        return tables == null ? null : tables.getIfPresent(localTableName);
    }

    @VisibleForTesting
    public void evictTableObjectForTest(String localTableName) {
        tables.invalidateKey(localTableName);
    }

    @VisibleForTesting
    @Nullable
    public String getCachedTableNameByIdForTest(long tableId) {
        return tableIdToName.get(tableId);
    }

    public void resetMetaCacheNames() {
        if (tableNames != null) {
            tableNames.invalidateAll();
        }
    }
}
