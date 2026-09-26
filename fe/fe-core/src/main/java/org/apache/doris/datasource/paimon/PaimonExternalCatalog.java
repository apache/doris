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
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager;
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;
import org.apache.doris.foundation.security.JdbcDriverUrlSecurity;
import org.apache.doris.transaction.TransactionManagerFactory;

import com.google.common.util.concurrent.Striped;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

// The subclasses of this class are all deprecated, only for meta persistence compatibility.
public class PaimonExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(PaimonExternalCatalog.class);
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_FILESYSTEM = "filesystem";
    public static final String PAIMON_HMS = "hms";
    public static final String PAIMON_DLF = "dlf";
    public static final String PAIMON_REST = "rest";
    public static final String PAIMON_JDBC = "jdbc";
    public static final String PAIMON_TABLE_CACHE_ENABLE = "meta.cache.paimon.table.enable";
    public static final String PAIMON_TABLE_CACHE_TTL_SECOND = "meta.cache.paimon.table.ttl-second";
    public static final String PAIMON_TABLE_CACHE_CAPACITY = "meta.cache.paimon.table.capacity";
    // Paimon 1.4.2 keeps the catalog partition cache in a protected field without a public
    // accessor. Resolve it once so a scoped invalidation can clear all matched keys in one batch.
    private static final Field PAIMON_PARTITION_CACHE_FIELD = paimonPartitionCacheField();
    protected String catalogType;
    protected Catalog catalog;

    private AbstractPaimonProperties paimonProperties;
    private ReentrantReadWriteLock sdkCatalogCacheLock = new ReentrantReadWriteLock(true);
    private Striped<Lock> sdkTableLocks = Striped.lazyWeakLock(1024);

    public PaimonExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                 String comment) {
        super(catalogId, name, InitCatalogLog.Type.PAIMON, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        paimonProperties = (AbstractPaimonProperties) catalogProperty.getMetastoreProperties();
        catalogType = paimonProperties.getPaimonCatalogType();
        catalog = createCatalog();
        initPreExecutionAuthenticator();
        metadataOps = ExternalMetadataOperations.newPaimonMetaOps(this, catalog);
        transactionManager = TransactionManagerFactory.createPaimonTransactionManager((PaimonMetadataOps) metadataOps);
    }

    @Override
    protected synchronized void initPreExecutionAuthenticator() {
        if (executionAuthenticator == null) {
            executionAuthenticator = paimonProperties.getExecutionAuthenticator();
        }
    }

    public String getCatalogType() {
        makeSureInitialized();
        return catalogType;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return metadataOps.tableExist(dbName, tblName);
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        return metadataOps.listTableNames(dbName);
    }

    public List<Partition> getPaimonPartitions(NameMapping nameMapping) {
        Identifier identifier = Identifier.create(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
        try {
            return withSdkCatalogCacheReadLock(() -> withSdkTableLock(identifier,
                    () -> executionAuthenticator.execute(() -> {
                        List<Partition> partitions = new ArrayList<>();
                        try {
                            partitions = catalog.listPartitions(identifier);
                        } catch (Catalog.TableNotExistException e) {
                            LOG.warn("TableNotExistException", e);
                        }
                        return partitions;
                    })));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table partitions:" + getName() + "."
                    + nameMapping.getRemoteDbName() + "." + nameMapping.getRemoteTblName() + ", because "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    public Table getPaimonTable(NameMapping nameMapping) {
        return getPaimonTable(nameMapping, null, null);
    }

    public Table getPaimonTable(NameMapping nameMapping, String branch, String queryType) {
        Identifier identifier = tableIdentifier(nameMapping, branch, queryType);
        return loadPaimonTable(nameMapping, queryType, identifier);
    }

    public synchronized void invalidatePaimonTable(NameMapping nameMapping) throws Exception {
        // Property changes reset and close the SDK catalog before retiring Doris cache entries.
        // Do not recreate that catalog merely to invalidate an already retired generation.
        if (!isInitialized()) {
            return;
        }
        Identifier identifier = tableIdentifier(nameMapping, null, null);
        withSdkCatalogCacheWriteLock(() -> executionAuthenticator.execute(() -> {
            invalidatePaimonTableEntries(identifier);
            return null;
        }));
    }

    public synchronized void invalidatePaimonDatabase(String remoteDbName) throws Exception {
        if (!isInitialized()) {
            return;
        }
        withSdkCatalogCacheWriteLock(() -> executionAuthenticator.execute(() -> {
            boolean caseSensitive = catalog.caseSensitive();
            invalidateCachedPaimonTables(identifier -> identifierPartEquals(
                    identifier.getDatabaseName(), remoteDbName, caseSensitive));
            return null;
        }));
    }

    public synchronized void invalidatePaimonCatalog() throws Exception {
        if (!isInitialized()) {
            return;
        }
        withSdkCatalogCacheWriteLock(() -> executionAuthenticator.execute(() -> {
            invalidateCachedPaimonTables(ignored -> true);
            return null;
        }));
    }

    private List<Identifier> invalidateCachedPaimonTables(Predicate<Identifier> predicate) throws Exception {
        // A property ALTER closes the old SDK catalog before Doris retires its cache entries.
        // The new SDK catalog must remain lazily initialized in that callback.
        if (!isInitialized()) {
            return Collections.emptyList();
        }
        CachingCatalog cachingCatalog = findCachingCatalog();
        if (cachingCatalog == null) {
            return Collections.emptyList();
        }
        List<Identifier> cachedIdentifiers = new ArrayList<>(cachingCatalog.tableCache().asMap().keySet());
        List<Identifier> matchedIdentifiers = new ArrayList<>();
        for (Identifier identifier : cachedIdentifiers) {
            if (predicate.test(identifier)) {
                matchedIdentifiers.add(identifier);
            }
        }
        if (matchedIdentifiers.isEmpty()) {
            return matchedIdentifiers;
        }
        // Remove all matching branch/system table handles in one pass, then clear their partition
        // entries in a single batch. CachingCatalog.invalidateTable() rescans the whole remaining
        // table cache per call, so a database-scope refresh with D matched and U unrelated tables
        // would otherwise be O(D*U) key visits while holding the SDK write fence.
        cachingCatalog.tableCache().invalidateAll(matchedIdentifiers);
        invalidatePaimonPartitionCache(cachingCatalog, matchedIdentifiers);
        return matchedIdentifiers;
    }

    @SuppressWarnings("unchecked")
    private void invalidatePaimonPartitionCache(CachingCatalog cachingCatalog, Collection<Identifier> identifiers) {
        if (identifiers.isEmpty()) {
            return;
        }
        // Paimon 1.4.2 exposes no batch partition-cache accessor, so reach the protected field once
        // and drop every matched key together. If a future Paimon build renames or removes the
        // field, fall back to the public per-key API, which is slower but still correct.
        if (PAIMON_PARTITION_CACHE_FIELD != null) {
            try {
                Object partitionCache = PAIMON_PARTITION_CACHE_FIELD.get(cachingCatalog);
                // A null cache means partition caching is disabled; there is nothing to clear.
                if (partitionCache instanceof Cache) {
                    ((Cache<Identifier, ?>) partitionCache).invalidateAll(identifiers);
                }
                return;
            } catch (IllegalAccessException e) {
                LOG.warn("Failed to access Paimon partition cache of catalog {}: {}", getName(), e.getMessage());
            }
        }
        identifiers.forEach(cachingCatalog::invalidateTable);
    }

    private static Field paimonPartitionCacheField() {
        try {
            Field field = CachingCatalog.class.getDeclaredField("partitionCache");
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            LOG.warn("Paimon CachingCatalog no longer exposes 'partitionCache'; "
                    + "partition keys will be invalidated one by one", e);
            return null;
        }
    }

    /**
     * Invalidates Paimon's catalog-level table cache and reloads the table.
     *
     * <p>Doris and Paimon cache table handles for different purposes. Doris owns the external
     * metadata lifecycle (TTL, REFRESH and FE-wide invalidation), while Paimon's CachingCatalog
     * attaches lower-level manifest, snapshot and deletion-vector caches to a table handle. A miss
     * in the Doris cache must therefore invalidate Paimon's table entry before loading; otherwise a
     * Doris REFRESH can repopulate its cache with the same stale Paimon table handle.
     */
    public Table reloadPaimonTable(NameMapping nameMapping) {
        Identifier identifier = tableIdentifier(nameMapping, null, null);
        try {
            return withSdkCatalogCacheReadLock(() -> withSdkTableLock(identifier,
                    () -> executionAuthenticator.execute(() -> {
                        invalidatePaimonTableEntries(identifier);
                        return copyWithCatalogTableOptions(catalog.getTable(identifier));
                    })));
        } catch (Exception e) {
            throw tableLoadException(nameMapping, null, e);
        }
    }

    private Identifier tableIdentifier(NameMapping nameMapping, String branch, String queryType) {
        if (branch != null && queryType != null) {
            return new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(),
                    branch, queryType);
        } else if (branch != null) {
            return new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), branch);
        } else if (queryType != null) {
            return new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), "main", queryType);
        }
        return new Identifier(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
    }

    private Table loadPaimonTable(NameMapping nameMapping, String queryType, Identifier identifier) {
        try {
            return withSdkCatalogCacheReadLock(() -> withSdkTableLock(identifier,
                    () -> executionAuthenticator.execute(
                            () -> copyWithCatalogTableOptions(catalog.getTable(identifier)))));
        } catch (Exception e) {
            throw tableLoadException(nameMapping, queryType, e);
        }
    }

    private <T> T withSdkCatalogCacheReadLock(Callable<T> action) throws Exception {
        ReentrantReadWriteLock.ReadLock readLock;
        synchronized (this) {
            makeSureInitialized();
            readLock = sdkCatalogCacheLock.readLock();
            readLock.lock();
        }
        try {
            return action.call();
        } finally {
            readLock.unlock();
        }
    }

    private <T> T withSdkCatalogCacheWriteLock(Callable<T> action) throws Exception {
        ReentrantReadWriteLock.WriteLock writeLock = sdkCatalogCacheLock.writeLock();
        writeLock.lock();
        try {
            return action.call();
        } finally {
            writeLock.unlock();
        }
    }

    private <T> T withSdkTableLock(Identifier identifier, Callable<T> action) throws Exception {
        Lock tableLock = sdkTableLocks.get(tableLockKey(identifier));
        tableLock.lock();
        try {
            return action.call();
        } finally {
            tableLock.unlock();
        }
    }

    private String tableLockKey(Identifier identifier) {
        boolean caseSensitive = catalog.caseSensitive();
        String databaseName = normalizeIdentifierPart(identifier.getDatabaseName(), caseSensitive);
        String tableName = normalizeIdentifierPart(identifier.getTableName(), caseSensitive);
        return databaseName + '\0' + tableName;
    }

    private boolean tableMatches(Identifier left, Identifier right) {
        boolean caseSensitive = catalog.caseSensitive();
        return identifierPartEquals(left.getDatabaseName(), right.getDatabaseName(), caseSensitive)
                && identifierPartEquals(left.getTableName(), right.getTableName(), caseSensitive);
    }

    private boolean identifierPartEquals(String left, String right, boolean caseSensitive) {
        return caseSensitive ? left.equals(right) : left.equalsIgnoreCase(right);
    }

    private String normalizeIdentifierPart(String value, boolean caseSensitive) {
        return caseSensitive ? value : value.toLowerCase(Locale.ROOT);
    }

    private CachingCatalog findCachingCatalog() {
        Catalog current = catalog;
        while (current instanceof DelegateCatalog) {
            if (current instanceof CachingCatalog) {
                return (CachingCatalog) current;
            }
            current = ((DelegateCatalog) current).wrapped();
        }
        return current instanceof CachingCatalog ? (CachingCatalog) current : null;
    }

    private void invalidatePaimonTableEntries(Identifier identifier) throws Exception {
        CachingCatalog cachingCatalog = findCachingCatalog();
        if (cachingCatalog == null) {
            catalog.invalidateTable(identifier);
            return;
        }
        invalidateCachedPaimonTables(cachedIdentifier -> tableMatches(cachedIdentifier, identifier));
        // The exact identifier may exist only in Paimon's partition cache after its table handle
        // has expired. Clear it in one batch together with the matched handles.
        invalidatePaimonPartitionCache(cachingCatalog, Collections.singletonList(identifier));
    }

    List<DataField> getSdkTableFields(Identifier identifier) throws Exception {
        return withSdkCatalogCacheReadLock(() -> withSdkTableLock(identifier,
                () -> executionAuthenticator.execute(
                        () -> new ArrayList<>(catalog.getTable(identifier).rowType().getFields()))));
    }

    boolean sdkTableExists(Identifier identifier) throws Exception {
        return withSdkCatalogCacheReadLock(() -> withSdkTableLock(identifier,
                () -> executionAuthenticator.execute(() -> {
                    try {
                        catalog.getTable(identifier);
                        return true;
                    } catch (Catalog.TableNotExistException e) {
                        return false;
                    }
                })));
    }

    private Table copyWithCatalogTableOptions(Table table) {
        Map<String, String> tableOptions = paimonProperties.getTableOptionsForCopy();
        // This handle is relation-neutral. Runtime validation and CPU-local capping belong
        // to the final relation copy, where relation options can override physical values.
        return tableOptions.isEmpty() ? table : table.copy(tableOptions);
    }

    private RuntimeException tableLoadException(NameMapping nameMapping, String queryType, Exception e) {
        return new RuntimeException("Failed to get Paimon table:" + getName() + "."
                + nameMapping.getRemoteDbName() + "." + nameMapping.getRemoteTblName() + "$" + queryType
                + ", because " + ExceptionUtils.getRootCauseMessage(e), e);
    }

    protected Catalog createCatalog() {
        try {
            paimonProperties.setDisableSdkMetadataCacheByDefault(isMetaCacheWeightGoverned());
            return paimonProperties.initializeCatalog(getName(), new ArrayList<>(catalogProperty
                    .getOrderedStoragePropertiesList()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create catalog, catalog name: " + getName() + ", exception: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /** Whether any Doris meta cache weight bound (global, catalog or entry level) applies. */
    private boolean isMetaCacheWeightGoverned() {
        return ExternalMetaCacheBudgetManager.appliesWeightGovernance(catalogProperty.getProperties());
    }

    public Map<String, String> getPaimonOptionsMap() {
        makeSureInitialized();
        return paimonProperties.getCatalogOptionsMap();
    }

    @Override
    public void checkProperties() throws DdlException {
        checkProperties(catalogProperty, catalogProperty.getProperties());
    }

    @Override
    public boolean validatePropertiesBeforeUpdate(
            Map<String, String> currentProperties, Map<String, String> updatedProperties) throws DdlException {
        Map<String, String> candidateProperties = currentProperties == null
                ? new HashMap<>() : new HashMap<>(currentProperties);
        candidateProperties.putAll(updatedProperties);
        checkProperties(new CatalogProperty(null, candidateProperties), updatedProperties);
        return true;
    }

    private void checkProperties(CatalogProperty property, Map<String, String> strictlyValidatedProperties)
            throws DdlException {
        super.checkProperties(property);
        CacheSpec.checkBooleanProperty(property.getOrDefault(PAIMON_TABLE_CACHE_ENABLE, null),
                PAIMON_TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(property.getOrDefault(PAIMON_TABLE_CACHE_TTL_SECOND, null),
                -1L, PAIMON_TABLE_CACHE_TTL_SECOND);
        CacheSpec.checkLongProperty(property.getOrDefault(PAIMON_TABLE_CACHE_CAPACITY, null),
                0L, PAIMON_TABLE_CACHE_CAPACITY);
        // Validate only newly supplied dynamic options on ALTER. This lets an old image containing
        // a formerly accepted option survive an unrelated update while still rejecting new writes.
        PaimonReaderOptions.validateCatalogProperties(strictlyValidatedProperties);
        // Mandatory, non-configurable security rule for the driver jar the jdbc flavor loads into the
        // FE JVM (shared with the jdbc / iceberg-jdbc catalogs; see JdbcDriverUrlSecurity). Both this
        // catalog's CREATE hook (checkProperties()) and its detached ALTER hook
        // (validatePropertiesBeforeUpdate) funnel through here, and never the replay/rebuild path.
        // Checked BEFORE the metastore-properties build below, which for the jdbc flavor already
        // attempts to register the driver. Keys owned by PaimonJdbcMetaStoreProperties; only the jdbc
        // flavor loads a jar, on every other flavor they are dead config that must not fail a catalog.
        if ("jdbc".equalsIgnoreCase(property.getOrDefault(PAIMON_CATALOG_TYPE, ""))) {
            for (String key : new String[] {"paimon.jdbc.driver_url", "jdbc.driver_url"}) {
                try {
                    JdbcDriverUrlSecurity.check(property.getOrDefault(key, null));
                } catch (IllegalArgumentException e) {
                    throw new DdlException(e.getMessage(), e);
                }
            }
        }
        property.checkMetaStoreAndStorageProperties(AbstractPaimonProperties.class);
    }

    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        try {
            super.notifyPropertiesUpdated(updatedProps);
        } finally {
            // The committed ALTER already published the properties; retire the engine group even
            // when the generic reset cleanup throws.
            if (updatedProps.keySet().stream()
                    .anyMatch(key -> CacheSpec.isMetaCacheKeyForEngine(key, PaimonExternalMetaCache.ENGINE)
                            || AbstractPaimonProperties.isTableOptionProperty(key))) {
                Env.getCurrentEnv().getExtMetaCacheMgr()
                        .removeCatalogByEngine(getId(), PaimonExternalMetaCache.ENGINE);
            }
        }
    }

    @Override
    public synchronized void onClose() {
        ReentrantReadWriteLock.WriteLock writeLock = sdkCatalogCacheLock.writeLock();
        writeLock.lock();
        try {
            super.onClose();
            if (null != catalog) {
                try {
                    catalog.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close paimon catalog: {}", getName(), e);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        sdkCatalogCacheLock = new ReentrantReadWriteLock(true);
        sdkTableLocks = Striped.lazyWeakLock(1024);
    }
}
