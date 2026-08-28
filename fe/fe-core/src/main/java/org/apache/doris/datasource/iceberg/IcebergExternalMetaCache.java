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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Iceberg engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code table}: loaded Iceberg {@link Table} instances per Doris table mapping</li>
 *   <li>{@code snapshot}: immutable snapshot projections keyed by a stable metadata generation</li>
 *   <li>{@code view}: loaded Iceberg {@link View} instances</li>
 *   <li>{@code manifest}: parsed manifest payload ({@link ManifestCacheValue}) keyed by
 *   manifest path and content type</li>
 *   <li>{@code schema}: schema cache keyed by table identity + schema id</li>
 * </ul>
 *
 * <p>Manifest entry keys are path-based and intentionally not table-scoped. This allows
 * shared manifests to reuse one cache entry across tables in the same catalog.
 *
 * <p>Invalidation behavior:
 * <ul>
 *   <li>catalog invalidation clears all entries and drops Iceberg {@link ManifestFiles} IO cache</li>
 *   <li>db/table invalidation clears table/snapshot/view/schema entries, while keeping manifest entries</li>
 *   <li>partition-level invalidation falls back to table-level invalidation</li>
 * </ul>
 */
public class IcebergExternalMetaCache extends AbstractExternalMetaCache {
    private static final Logger LOG = LogManager.getLogger(IcebergExternalMetaCache.class);

    public static final String ENGINE = "iceberg";
    public static final String ENTRY_TABLE = "table";
    public static final String ENTRY_SNAPSHOT = "snapshot";
    public static final String ENTRY_VIEW = "view";
    public static final String ENTRY_MANIFEST = "manifest";
    public static final String ENTRY_SCHEMA = "schema";
    private static final long DEFAULT_MANIFEST_CACHE_CAPACITY = 100_000L;

    private final EntryHandle<NameMapping, IcebergTableCacheValue> tableEntry;
    private final EntryHandle<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshotEntry;
    private final EntryHandle<NameMapping, View> viewEntry;
    private final EntryHandle<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry;
    private final EntryHandle<IcebergSchemaCacheKey, SchemaCacheValue> schemaEntry;

    public IcebergExternalMetaCache(ExecutorService refreshExecutor) {
        this(refreshExecutor, new ExternalMetaCacheBudgetManager(java.util.OptionalLong.empty()));
    }

    public IcebergExternalMetaCache(ExecutorService refreshExecutor, ExternalMetaCacheBudgetManager budgetManager) {
        super(ENGINE, refreshExecutor, budgetManager);
        tableEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class,
                this::loadTableCacheValue, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping))
                .withSizeEstimator(this::prepareTableForCachePublication)
                .withReplacementListener(this::retireTableGeneration)
                .withUnpublishedValueRetirer(IcebergTableCacheValue::retire)
                .withRemovalListener(value -> value, (key, value) -> {
                    if (value != null) {
                        retireRemovedTableGeneration(key, value);
                    }
                })
                .withStrongValues());
        snapshotEntry = registerEntry(MetaCacheEntryDef.contextualOnly(ENTRY_SNAPSHOT,
                IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(IcebergSnapshotEntryKey::getNameMapping))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key)));
        viewEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_VIEW, NameMapping.class, View.class, this::loadView,
                defaultEntryCacheSpec(), MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
        manifestEntry = registerEntry(MetaCacheEntryDef.contextualOnly(ENTRY_MANIFEST, IcebergManifestEntryKey.class,
                ManifestCacheValue.class,
                CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, DEFAULT_MANIFEST_CACHE_CAPACITY))
                .withSizeEstimator((key, value) -> MetaCacheSizeEstimator.estimateSafely(
                        "iceberg_manifest_preparation_failed",
                        () -> IcebergCacheSizeEstimator.estimateManifestEntry(key, value))));
        // Schema values are keyed by an immutable (table uuid, schemaId) pair, so a timed
        // refresh can never observe new content; it would also invoke the default loader
        // outside the catalog execution authenticator scope. Misses load contextually.
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, IcebergSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(), false,
                MetaCacheEntryInvalidation.forNameMapping(IcebergSchemaCacheKey::getNameMapping)));
    }

    public Table getIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease != null) {
            return lease.getIcebergTable();
        }
        // Background callers have no deterministic statement boundary. Use a live catalog load
        // instead of returning a cache generation that can be evicted immediately after lookup.
        return getWritableIcebergTable(dorisTable);
    }

    ThreadPoolExecutor getIcebergTableExecutor(ExternalTable dorisTable) {
        IcebergTableCacheValue.Lease lease = statementLease(dorisTable.getOrBuildNameMapping());
        if (lease == null || lease.getPlanningExecutor() == null) {
            return dorisTable.getCatalog().getThreadPoolWithPreAuth();
        }
        return lease.getPlanningExecutor();
    }

    <T> T withIcebergTable(ExternalTable dorisTable, Function<Table, T> action) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease != null) {
            return action.apply(lease.getIcebergTable());
        }
        try (IcebergTableCacheValue.Lease operationLease = borrow(nameMapping)) {
            return action.apply(operationLease.getIcebergTable());
        }
    }

    public Table getWritableIcebergTable(ExternalTable dorisTable) {
        return getWritableIcebergTable(dorisTable, null);
    }

    /**
     * Acquire a writable table for a caller that retains the {@code IcebergMetadataOps} of the
     * generation its operation was dispatched on (a DDL method of that ops instance, a DML
     * transaction). The acquisition fails when the catalog has moved to a different generation,
     * so the caller's later updates - executed through its retained ops and authenticator -
     * can never operate a newer generation's handle.
     */
    public Table getWritableIcebergTable(ExternalTable dorisTable, @Nullable IcebergMetadataOps expectedOps) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException("Cannot find catalog " + nameMapping.getCtlId()
                    + " when loading a writable Iceberg table");
        }
        // DDL/actions must start from the live catalog generation. DML that was planned against a
        // retained read generation wraps this live table separately in IcebergTransaction. The
        // authenticator, ops and loaded handle must all come from that one generation, so the
        // acquisition is re-validated afterwards: a concurrent property/credential ALTER can reset
        // and reinitialize the catalog mid-flight without retiring this engine's cache group.
        ExecutionAuthenticator authenticator = requireExecutionAuthenticator(catalog);
        IcebergMetadataOps ops = resolveMetadataOps(catalog);
        if (expectedOps != null && ops != expectedOps) {
            throw catalogGenerationMoved(nameMapping);
        }
        Table table = execute(authenticator, () -> ops.loadTable(
                nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
        ensureCatalogGenerationStable(catalog, ops, authenticator, nameMapping);
        return table;
    }

    WritableTableLease acquireWritableIcebergTable(
            ExternalTable dorisTable, @Nullable IcebergMetadataOps expectedOps) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException("Cannot find catalog " + nameMapping.getCtlId()
                    + " when loading a writable Iceberg table");
        }
        if (catalog instanceof IcebergExternalCatalog) {
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            try (IcebergExternalCatalog.TableLoadContext context = icebergCatalog.beginTableLoad()) {
                IcebergMetadataOps ops = context.getOps();
                if (expectedOps != null && ops != expectedOps) {
                    throw catalogGenerationMoved(nameMapping);
                }
                Table table;
                try {
                    table = context.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
                } catch (Exception e) {
                    throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
                }
                try (TableResourceOwner owner = new TableResourceOwner(
                        tableCleanup(context.getCatalogType(), ops, table))) {
                    ensureCatalogGenerationStable(catalog, ops, context.getAuthenticator(), nameMapping,
                            context.isEnableMappingVarbinary(), context.isEnableMappingTimestampTz());
                    owner.add(context.promote()::close);
                    WritableTableLease lease = new WritableTableLease(
                            table, context.getAuthenticator(), owner.cleanup());
                    owner.transfer();
                    return lease;
                }
            }
        }
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
            try (HMSExternalCatalog.IcebergTableLoadContext context = hmsCatalog.beginIcebergTableLoad()) {
                IcebergMetadataOps ops = context.getOps();
                if (expectedOps != null && ops != expectedOps) {
                    throw catalogGenerationMoved(nameMapping);
                }
                boolean enableMappingVarbinary = context.isEnableMappingVarbinary();
                boolean enableMappingTimestampTz = context.isEnableMappingTimestampTz();
                Table table;
                try {
                    table = context.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
                } catch (Exception e) {
                    throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
                }
                try (TableResourceOwner owner = new TableResourceOwner(() -> { })) {
                    ensureCatalogGenerationStable(catalog, ops, context.getAuthenticator(), nameMapping,
                            enableMappingVarbinary, enableMappingTimestampTz);
                    owner.add(context.promote()::close);
                    WritableTableLease lease = new WritableTableLease(
                            table, context.getAuthenticator(), owner.cleanup());
                    owner.transfer();
                    return lease;
                }
            }
        }
        throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
    }

    Table getQueryScopedIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue tableValue = statementValue(nameMapping);
        return createQueryTable(nameMapping, tableValue);
    }

    /** Resolve the current table generation, exposing the handle and its captured context together. */
    IcebergTableCacheValue getTableCacheValue(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return statementValue(nameMapping);
    }

    /** Query-scoped view of an already-resolved generation; see {@link #getTableCacheValue}. */
    Table createQueryScopedTable(ExternalTable dorisTable, IcebergTableCacheValue tableValue) {
        return createQueryTable(dorisTable.getOrBuildNameMapping(), tableValue);
    }

    private Table createQueryTable(
            NameMapping nameMapping, IcebergTableCacheValue tableValue) {
        boolean isolateForQueries = tableValue.isQueryIsolationPrepared()
                || snapshotEntry.get(nameMapping.getCtlId()).isWeightAccounting();
        if (!isolateForQueries) {
            return tableValue.getIcebergTable();
        }
        Table queryTable = tableValue.newQueryScopedTable();
        IcebergSnapshotCacheValue.loadQueryMetadataForStatement(queryTable);
        return queryTable;
    }

    public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue tableValue = statementValue(nameMapping);
        Table retainedTable = tableValue.getRetainedIcebergTable();
        java.util.Optional<IcebergSnapshotEntryKey> optionalKey =
                IcebergSnapshotEntryKey.tryCreate(nameMapping, retainedTable);
        if (!optionalKey.isPresent()) {
            boolean isolateForQueries = tableValue.isQueryIsolationPrepared();
            return executeForGeneration(tableValue, nameMapping.getCtlId(),
                    authenticator -> loadSnapshotProjection(
                            dorisTable,
                            isolateForQueries ? tableValue.newQueryScopedTable()
                                    : tableValue.getIcebergTable(),
                            tableValue.getRetainedIcebergTable(),
                            tableValue.getRetainedCurrentSnapshotJson(), isolateForQueries,
                            authenticator, tableValue.isEnableMappingVarbinary(),
                            tableValue.isEnableMappingTimestampTz())
                            .bindCapturedAuthenticator(authenticator)
                            .bindSchemaMappingOptions(tableValue.isEnableMappingVarbinary(),
                                    tableValue.isEnableMappingTimestampTz()));
        }
        IcebergSnapshotEntryKey key = optionalKey.get();
        MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> entry =
                snapshotEntry.get(nameMapping.getCtlId());
        boolean isolateForQueries = tableValue.isQueryIsolationPrepared()
                || entry.isWeightAccounting();
        Function<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> projectionLoader =
                ignored -> executeForGeneration(tableValue, nameMapping.getCtlId(), authenticator -> {
                    Table projectionTable = isolateForQueries
                            ? tableValue.newQueryScopedTable() : tableValue.getIcebergTable();
                    IcebergSnapshotCacheValue value = loadSnapshotProjection(
                            dorisTable, projectionTable,
                            tableValue.getRetainedIcebergTable(),
                            tableValue.getRetainedCurrentSnapshotJson(), isolateForQueries,
                            authenticator, tableValue.isEnableMappingVarbinary(),
                            tableValue.isEnableMappingTimestampTz())
                            .bindCapturedAuthenticator(authenticator)
                            .bindSchemaMappingOptions(tableValue.isEnableMappingVarbinary(),
                                    tableValue.isEnableMappingTimestampTz());
                    if (entry.isWeightAccounting()) {
                        value.prepareForCachePublication(key);
                    }
                    return value;
                });
        IcebergSnapshotCacheValue snapshotValue = entry.get(key, projectionLoader);
        if (!sharesOperationalResources(tableValue, snapshotValue)) {
            // A hit frozen on a previous handle of the same metadata generation keeps that handle's
            // FileIO (vended credentials). Whether or not the base entry publishes handles, scans
            // bind to the projection, so rebuild it from the handle this lookup just obtained.
            entry.invalidateKeyIfSame(key, snapshotValue);
            snapshotValue = entry.get(key, projectionLoader);
        }
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables =
                tableEntry.getIfInitialized(nameMapping.getCtlId());
        if (tables == null) {
            // The catalog group was retired mid-lookup; the caller keeps its immutable value and
            // nothing published remains to revalidate against.
            entry.invalidateKeyIfSame(key, snapshotValue);
            return snapshotValue;
        }
        IcebergTableCacheValue currentTable = tables.peekIfPresent(nameMapping);
        if (tables.isEffectivelyEnabled()
                && (currentTable == null || !tableValue.isSameOperationalGeneration(currentTable))) {
            // A query may have captured the previous table immediately before refresh publication,
            // or loaded through a table handle that was never admitted (weight rejection). It can
            // use that immutable value, but must not republish a projection no later lookup can
            // reach or one frozen on superseded operational resources. (An ineffective table entry
            // never publishes; its physically keyed projections stay reusable and are revalidated
            // against the fresh handle above.)
            entry.invalidateKeyIfSame(key, snapshotValue);
        }
        return snapshotValue;
    }

    public List<Snapshot> getSnapshotList(ExternalTable dorisTable) {
        Table icebergTable = getQueryScopedIcebergTable(dorisTable);
        List<Snapshot> snapshots = com.google.common.collect.Lists.newArrayList();
        com.google.common.collect.Iterables.addAll(snapshots, icebergTable.snapshots());
        return snapshots;
    }

    public View getIcebergView(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return viewEntry.get(nameMapping.getCtlId()).get(nameMapping);
    }

    public IcebergSchemaCacheValue getIcebergSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        IcebergTableCacheValue tableValue = statementValue(nameMapping);
        return getIcebergSchemaCacheValue(nameMapping, schemaId, tableValue.getRetainedIcebergTable(),
                tableValue.getAuthenticator(), tableValue.isEnableMappingVarbinary(),
                tableValue.isEnableMappingTimestampTz());
    }

    IcebergSchemaCacheValue getIcebergSchemaCacheValue(
            NameMapping nameMapping, long schemaId, Table retainedTable) {
        CatalogIf<?> catalog = getCatalog(nameMapping.getCtlId());
        if (!(catalog instanceof ExternalCatalog)) {
            return getIcebergSchemaCacheValue(nameMapping, schemaId, retainedTable,
                    new ExecutionAuthenticator() { }, false, false);
        }
        ExecutionAuthenticator authenticator = requireExecutionAuthenticator(catalog);
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        return getIcebergSchemaCacheValue(nameMapping, schemaId, retainedTable, authenticator,
                externalCatalog.getEnableMappingVarbinary(), externalCatalog.getEnableMappingTimestampTz());
    }

    IcebergSchemaCacheValue getIcebergSchemaCacheValue(NameMapping nameMapping, long schemaId, Table retainedTable,
            ExecutionAuthenticator authenticator,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        Optional<IcebergSnapshotEntryKey> generation = IcebergSnapshotEntryKey.tryCreate(nameMapping, retainedTable);
        if (!generation.isPresent()) {
            return (IcebergSchemaCacheValue) loadSchemaCacheValue(
                    new IcebergSchemaCacheKey(nameMapping, "", schemaId,
                            retainedTable.spec().specId(), retainedTable.schema().schemaId(),
                            enableMappingVarbinary, enableMappingTimestampTz),
                    retainedTable, authenticator);
        }
        IcebergSchemaCacheKey key = new IcebergSchemaCacheKey(
                nameMapping, generation.get().getTableUuid(), schemaId, retainedTable.spec().specId(),
                retainedTable.schema().schemaId(),
                enableMappingVarbinary, enableMappingTimestampTz);
        MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> entry = schemaEntry.get(nameMapping.getCtlId());
        SchemaCacheValue schemaCacheValue = entry
                .get(key, ignored -> loadSchemaCacheValue(key, retainedTable, authenticator));
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables =
                tableEntry.getIfInitialized(nameMapping.getCtlId());
        if (tables == null) {
            entry.invalidateKeyIfSame(key, schemaCacheValue);
            return (IcebergSchemaCacheValue) schemaCacheValue;
        }
        IcebergTableCacheValue currentTable = tables.peekIfPresent(nameMapping);
        Optional<IcebergSnapshotEntryKey> currentGeneration = currentTable == null
                ? Optional.empty()
                : IcebergSnapshotEntryKey.tryCreate(nameMapping, currentTable.getRetainedIcebergTable());
        if (tables.isEffectivelyEnabled() && (!currentGeneration.isPresent()
                || !currentGeneration.get().getTableUuid().equals(generation.get().getTableUuid()))) {
            // No published base table (replaced, expired or rejected at admission) can vouch for
            // this projection; keep it out of the count-bounded schema cache.
            entry.invalidateKeyIfSame(key, schemaCacheValue);
        }
        return (IcebergSchemaCacheValue) schemaCacheValue;
    }

    public ManifestCacheValue getManifestCacheValue(ExternalTable dorisTable,
            org.apache.iceberg.ManifestFile manifest,
            Table icebergTable,
            Consumer<Boolean> cacheHitRecorder) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry =
                this.manifestEntry.get(nameMapping.getCtlId());
        IcebergManifestEntryKey key = IcebergManifestEntryKey.of(manifest);
        boolean hit = manifestEntry.peekIfPresent(key) != null;
        if (cacheHitRecorder != null) {
            cacheHitRecorder.accept(hit);
        }
        return manifestEntry.get(key,
                ignored -> loadManifestCacheValue(
                        manifest, icebergTable, key.getContent(), manifestEntry.isWeightAccounting()));
    }

    @Override
    public void invalidateCatalog(long catalogId) {
        // Collect while the entries are still enumerable, drop only after the entries are
        // detached: a load racing a pre-detach drop could repopulate the SDK content cache
        // for a FileIO this reset already cleaned.
        List<FileIO> retainedFileIos = collectManifestFileIos(catalogId);
        super.invalidateCatalog(catalogId);
        dropManifestFileIoCaches(retainedFileIos);
    }

    @Override
    public void invalidateCatalogEntries(long catalogId) {
        List<FileIO> retainedFileIos = collectManifestFileIos(catalogId);
        super.invalidateCatalogEntries(catalogId);
        dropManifestFileIoCaches(retainedFileIos);
    }

    private IcebergTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException(String.format("Cannot find catalog %d when loading table %s/%s.",
                    nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        if (catalog instanceof IcebergExternalCatalog) {
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            try (IcebergExternalCatalog.TableLoadContext context = icebergCatalog.beginTableLoad()) {
                IcebergMetadataOps ops = context.getOps();
                boolean enableMappingVarbinary = context.isEnableMappingVarbinary();
                boolean enableMappingTimestampTz = context.isEnableMappingTimestampTz();
                Table table;
                try {
                    table = context.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
                } catch (Exception e) {
                    throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
                }
                try (TableResourceOwner owner = new TableResourceOwner(
                        tableCleanup(context.getCatalogType(), ops, table))) {
                    ensureCatalogGenerationStable(catalog, ops, context.getAuthenticator(), nameMapping,
                            enableMappingVarbinary, enableMappingTimestampTz);
                    try (TableResourceOwner catalogOwner = new TableResourceOwner(context.promote()::close)) {
                        IcebergTableCacheValue value = execute(context.getAuthenticator(), () -> createLoadedTableValue(
                                nameMapping, table, ops.getThreadPoolWithPreAuth(), context.getAuthenticator(),
                                enableMappingVarbinary, enableMappingTimestampTz,
                                owner, catalogOwner.cleanup()));
                        owner.transfer();
                        catalogOwner.transfer();
                        return value;
                    }
                }
            }
        }
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
            try (HMSExternalCatalog.IcebergTableLoadContext context = hmsCatalog.beginIcebergTableLoad()) {
                boolean enableMappingVarbinary = context.isEnableMappingVarbinary();
                boolean enableMappingTimestampTz = context.isEnableMappingTimestampTz();
                Table table;
                try {
                    table = context.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
                } catch (Exception e) {
                    throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
                }
                try (TableResourceOwner owner = new TableResourceOwner(() -> { })) {
                    try (TableResourceOwner catalogOwner = new TableResourceOwner(context.promote()::close)) {
                        IcebergTableCacheValue value = execute(context.getAuthenticator(), () -> createLoadedTableValue(
                                nameMapping, table, context.getExecutor(), context.getAuthenticator(),
                                enableMappingVarbinary, enableMappingTimestampTz,
                                owner, catalogOwner.cleanup()));
                        owner.transfer();
                        catalogOwner.transfer();
                        return value;
                    }
                }
            }
        }

        throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
    }

    private IcebergTableCacheValue createLoadedTableValue(NameMapping nameMapping, Table table,
            ThreadPoolExecutor planningExecutor, ExecutionAuthenticator authenticator,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz,
            TableResourceOwner tableOwner, Runnable cleanup) {
        IcebergTableCacheValue loaded = new IcebergTableCacheValue(
                table, planningExecutor, () -> null, tableOwner.cleanup(), cleanup);
        try {
            loaded.bindAuthenticator(authenticator);
            loaded.bindSchemaMappingOptions(enableMappingVarbinary, enableMappingTimestampTz);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> currentEntry =
                    tableEntry.getIfInitialized(nameMapping.getCtlId());
            IcebergTableCacheValue currentValue = currentEntry == null
                    ? null : currentEntry.peekIfPresent(nameMapping);
            if (currentValue != null && currentValue.sharesFileIoIdentity(table)) {
                // Adopt the current generation's cleanup token before cache publication. If this
                // load is rejected, its reference is released without closing the shared FileIO;
                // if it replaces current, the last retired generation closes the FileIO once.
                if (!loaded.shareTableCleanupWith(currentValue)) {
                    // The exact shared FileIO was closed after peek but before ownership could be
                    // retained. Suppress the duplicate close and reject this unusable generation.
                    loaded.abandonTableCleanup();
                    tableOwner.transfer();
                    throw catalogGenerationMoved(nameMapping);
                }
                tableOwner.transfer();
            }
            if (currentEntry != null && currentEntry.isWeightAccounting()) {
                prepareTableForCachePublication(nameMapping, loaded);
            }
            return loaded;
        } catch (RuntimeException | Error e) {
            loaded.retire();
            throw e;
        }
    }

    private IcebergTableCacheValue statementValue(NameMapping nameMapping) {
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease != null) {
            return lease.getValue();
        }
        IcebergTableCacheValue value = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        value.releaseLoaderReference();
        return value;
    }

    @Nullable
    private IcebergTableCacheValue.Lease statementLease(NameMapping nameMapping) {
        ConnectContext connectContext = ConnectContext.get();
        StatementContext statementContext = connectContext == null ? null : connectContext.getStatementContext();
        if (statementContext == null) {
            return null;
        }
        String resourceKey = "iceberg-table:" + nameMapping.getCtlId() + "\u0000"
                + nameMapping.getRemoteDbName() + "\u0000" + nameMapping.getRemoteTblName();
        return statementContext.getOrRegisterStatementResource(resourceKey, () -> borrow(nameMapping));
    }

    private IcebergTableCacheValue.Lease borrow(NameMapping nameMapping) {
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> entry = tableEntry.get(nameMapping.getCtlId());
        while (true) {
            IcebergTableCacheValue value = entry.get(nameMapping);
            IcebergTableCacheValue.Lease lease = value.tryAcquire();
            if (entry.peekIfPresent(nameMapping) != value) {
                // Disabled, weight-rejected and invalidation-suppressed loads have no cache owner.
                // Retire that owner here; the just-acquired lease remains the sole use boundary.
                value.releaseCacheReference();
            }
            value.releaseLoaderReference();
            if (lease != null) {
                return lease;
            }
        }
    }

    private Runnable tableCleanup(String catalogType, IcebergMetadataOps ops, Table table) {
        FileIO catalogFileIO = IcebergExternalCatalog.ICEBERG_REST.equals(catalogType) ? catalogFileIO(ops) : null;
        if (!shouldCloseTableFileIO(catalogType, table.io(), catalogFileIO)) {
            return () -> { };
        }
        FileIO tableFileIO = table.io();
        return () -> {
            try {
                tableFileIO.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Iceberg table FileIO", e);
            }
        };
    }

    static boolean shouldCloseTableFileIO(String catalogType, FileIO tableFileIO, FileIO catalogFileIO) {
        if (IcebergExternalCatalog.ICEBERG_GLUE.equals(catalogType)
                || IcebergExternalCatalog.ICEBERG_S3_TABLES.equals(catalogType)) {
            return true;
        }
        return IcebergExternalCatalog.ICEBERG_REST.equals(catalogType)
                && catalogFileIO != null && tableFileIO != catalogFileIO;
    }

    @Nullable
    private FileIO catalogFileIO(IcebergMetadataOps ops) {
        Object catalog = ops.getCatalog();
        try {
            if (catalog instanceof org.apache.iceberg.rest.RESTCatalog) {
                Field sessionCatalogField = org.apache.iceberg.rest.RESTCatalog.class
                        .getDeclaredField("sessionCatalog");
                sessionCatalogField.setAccessible(true);
                catalog = sessionCatalogField.get(catalog);
            }
            if (catalog instanceof org.apache.iceberg.rest.RESTSessionCatalog) {
                Field ioField = org.apache.iceberg.rest.RESTSessionCatalog.class.getDeclaredField("io");
                ioField.setAccessible(true);
                return (FileIO) ioField.get(catalog);
            }
        } catch (Exception e) {
            LOG.warn("Failed to identify REST catalog FileIO; skip per-table close to protect shared IO", e);
        }
        return null;
    }

    private static ExecutionAuthenticator requireExecutionAuthenticator(CatalogIf<?> catalog) {
        if (!(catalog instanceof ExternalCatalog)) {
            throw new RuntimeException("Iceberg metadata cache requires an external catalog");
        }
        // A credential/storage ALTER can leave the catalog reset-to-uninitialized while queries,
        // actions or transactions still retain this external table. The authenticator exists
        // again only after lazy initialization, which the loads below used to trigger
        // implicitly, so initialize before capturing the generation's execution context.
        ((ExternalCatalog) catalog).makeSureInitialized();
        return ((ExternalCatalog) catalog).getExecutionAuthenticator();
    }

    private <T> T execute(ExecutionAuthenticator authenticator, Callable<T> task) {
        try {
            return authenticator.execute(task);
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * Validate that the catalog still serves the generation this acquisition started from. The
     * check runs after the external load and before the result is used or published, so a
     * concurrent reset turns into a clean retryable failure instead of a handle spliced with the
     * next generation's execution context.
     */
    private void ensureCatalogGenerationStable(CatalogIf<?> catalog, IcebergMetadataOps ops,
            ExecutionAuthenticator authenticator, NameMapping nameMapping) {
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        ensureCatalogGenerationStable(catalog, ops, authenticator, nameMapping,
                externalCatalog.getEnableMappingVarbinary(), externalCatalog.getEnableMappingTimestampTz());
    }

    private void ensureCatalogGenerationStable(CatalogIf<?> catalog, IcebergMetadataOps ops,
            ExecutionAuthenticator authenticator, NameMapping nameMapping,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        boolean stable;
        try {
            // Read without re-initializing: a catalog that was reset mid-flight must count as
            // unstable here, not get quietly reinitialized by the validation itself.
            stable = resolveMetadataOps(catalog) == ops
                    && ((ExternalCatalog) catalog).getExecutionAuthenticator() == authenticator
                    && ((ExternalCatalog) catalog).getEnableMappingVarbinary() == enableMappingVarbinary
                    && ((ExternalCatalog) catalog).getEnableMappingTimestampTz() == enableMappingTimestampTz;
        } catch (RuntimeException e) {
            stable = false;
        }
        if (!stable) {
            throw catalogGenerationMoved(nameMapping);
        }
    }

    private static RuntimeException catalogGenerationMoved(NameMapping nameMapping) {
        return new RuntimeException(String.format(
                "Catalog %d was reset while acquiring iceberg table %s.%s, please retry.",
                nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
    }

    MetaCacheSizeEstimate prepareTableForCachePublication(
            NameMapping nameMapping, IcebergTableCacheValue value) {
        return value.prepareForCachePublication(nameMapping);
    }

    private View loadView(NameMapping nameMapping) {
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (!(catalog instanceof IcebergExternalCatalog)) {
            return null;
        }
        IcebergMetadataOps ops = (IcebergMetadataOps) (((IcebergExternalCatalog) catalog).getMetadataOps());
        try {
            return ((ExternalCatalog) catalog).getExecutionAuthenticator().execute(
                    () -> ops.loadView(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private ManifestCacheValue loadManifestCacheValue(org.apache.iceberg.ManifestFile manifest, Table icebergTable,
            ManifestContent content, boolean accountRetainedSize) {
        if (manifest == null || icebergTable == null) {
            String manifestPath = manifest == null ? "null" : manifest.path();
            throw new CacheException("Manifest cache loader context is missing for %s",
                    null, manifestPath);
        }
        try {
            if (content == ManifestContent.DELETES) {
                return loadDeleteFiles(manifest, icebergTable, accountRetainedSize);
            }
            return loadDataFiles(manifest, icebergTable, accountRetainedSize);
        } catch (IOException e) {
            throw new CacheException("Failed to read manifest %s", e, manifest.path());
        }
    }

    private SchemaCacheValue loadSchemaCacheValue(IcebergSchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load iceberg schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
    }

    private SchemaCacheValue loadSchemaCacheValue(IcebergSchemaCacheKey key, Table retainedTable,
            ExecutionAuthenticator authenticator) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        dorisTable.setUpdateTime(System.currentTimeMillis());
        boolean isView = dorisTable instanceof IcebergExternalTable
                && ((IcebergExternalTable) dorisTable).isView();
        SchemaCacheValue value = (isView
                ? IcebergUtils.loadSchemaCacheValue(dorisTable, key.getSchemaId(), true, retainedTable)
                : Optional.of(IcebergUtils.buildTableSchemaCacheValue(
                        dorisTable, key.getSchemaId(), retainedTable, authenticator,
                        key.isEnableMappingVarbinary(), key.isEnableMappingTimestampTz())))
                .orElseThrow(() ->
                new CacheException("failed to load iceberg schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
        // Contextual miss loaders bypass the default-loader schema validator; ambiguous
        // case-insensitive column names must be rejected on this path too.
        value.validateSchema();
        return value;
    }

    private void retireTableGeneration(NameMapping nameMapping,
            @Nullable IcebergTableCacheValue previousValue, IcebergTableCacheValue currentValue) {
        if (previousValue == null) {
            return;
        }
        try {
            if (previousValue.isSameOperationalGeneration(currentValue)) {
                if (previousValue.sharesFileIoIdentity(currentValue.getRetainedIcebergTable())) {
                    return;
                }
            }
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots =
                    snapshotEntry.getIfInitialized(nameMapping.getCtlId());
            if (snapshots != null) {
                // Projections of another metadata generation are unreachable. Projections of the same
                // generation frozen on a previous handle keep that handle's FileIO (vended credentials)
                // and location provider; scans bind to them, so they must be rebuilt from the new handle.
                snapshots.invalidateIf((key, value) -> key.getNameMapping().equals(nameMapping)
                        && (!key.belongsTo(currentValue) || !sharesOperationalResources(currentValue, value)));
            }
            Optional<String> currentUuid = currentValue.getTableUuid();
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas =
                    schemaEntry.getIfInitialized(nameMapping.getCtlId());
            if (schemas != null) {
                schemas.invalidateIf(key -> key.getNameMapping().equals(nameMapping)
                        && (!key.getTableUuid().equals(currentUuid)
                                || key.isEnableMappingVarbinary() != currentValue.isEnableMappingVarbinary()
                                || key.isEnableMappingTimestampTz()
                                        != currentValue.isEnableMappingTimestampTz()));
            }
        } finally {
            // Caffeine REPLACED notifications intentionally do not run the removal listener because
            // the cache reservation transfers to the new generation. Resource ownership does not:
            // retire the old value here and let active statement/async leases delay physical close.
            previousValue.retire();
        }
    }

    private void retireRemovedTableGeneration(NameMapping nameMapping, IcebergTableCacheValue removedValue) {
        try {
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots =
                    snapshotEntry.getIfInitialized(nameMapping.getCtlId());
            if (snapshots != null) {
                snapshots.invalidateIf(key -> key.getNameMapping().equals(nameMapping));
            }
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas =
                    schemaEntry.getIfInitialized(nameMapping.getCtlId());
            if (schemas != null) {
                schemas.invalidateIf(key -> key.getNameMapping().equals(nameMapping));
            }
        } finally {
            removedValue.retire();
        }
    }

    private static boolean sharesOperationalResources(
            IcebergTableCacheValue currentValue, @Nullable IcebergSnapshotCacheValue projection) {
        if (projection == null) {
            return false;
        }
        Optional<Table> retainedTable = projection.getRetainedIcebergTable();
        // Count-mode projections do not retain a table handle; nothing to rebind (and nothing is
        // planned through a frozen generation, so a stale captured context is inert).
        if (!retainedTable.isPresent()) {
            return true;
        }
        // The captured execution context must match as well: after an auth-only ALTER the frozen
        // handle is operationally equivalent but unplannable under the new context, and serving
        // it would make every retried statement hit the same rejected projection until expiry.
        return currentValue.sharesFileIoIdentity(retainedTable.get())
                && currentValue.getAuthenticator() == projection.getCapturedAuthenticator()
                && currentValue.isEnableMappingVarbinary() == projection.isEnableMappingVarbinary()
                && currentValue.isEnableMappingTimestampTz() == projection.isEnableMappingTimestampTz();
    }

    static final class WritableTableLease implements AutoCloseable {
        private final Table table;
        private final ExecutionAuthenticator authenticator;
        private final Runnable cleanup;
        private final AtomicBoolean closed = new AtomicBoolean();

        private WritableTableLease(Table table, ExecutionAuthenticator authenticator, Runnable cleanup) {
            this.table = table;
            this.authenticator = authenticator;
            this.cleanup = cleanup;
        }

        Table getTable() {
            return table;
        }

        ExecutionAuthenticator getAuthenticator() {
            return authenticator;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                cleanup.run();
            }
        }
    }

    private static final class TableResourceOwner implements AutoCloseable {
        private final List<Runnable> cleanups = new ArrayList<>();
        private final AtomicBoolean released = new AtomicBoolean();
        private boolean transferred;

        private TableResourceOwner(Runnable cleanup) {
            cleanups.add(cleanup);
        }

        private void add(Runnable cleanup) {
            cleanups.add(cleanup);
        }

        private Runnable cleanup() {
            return this::release;
        }

        private void transfer() {
            transferred = true;
        }

        private void release() {
            if (!released.compareAndSet(false, true)) {
                return;
            }
            RuntimeException failure = null;
            for (Runnable cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void close() {
            if (!transferred) {
                release();
            }
        }
    }

    private IcebergSnapshotCacheValue loadSnapshotProjection(
            ExternalTable dorisTable, Table projectionTable, Table retainedTable,
            String retainedCurrentSnapshotJson, boolean isolateForQueries,
            ExecutionAuthenticator authenticator,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        if (!(dorisTable instanceof MTMVRelatedTableIf)) {
            throw new RuntimeException(String.format("Table %s.%s is not a valid MTMV related table.",
                    dorisTable.getDbName(), dorisTable.getName()));
        }
        try {
            MTMVRelatedTableIf table = (MTMVRelatedTableIf) dorisTable;
            IcebergSnapshot latestIcebergSnapshot = IcebergUtils.getLatestIcebergSnapshot(projectionTable);
            IcebergPartitionInfo icebergPartitionInfo;
            if (!table.isValidRelatedTable()) {
                icebergPartitionInfo = IcebergPartitionInfo.empty();
            } else {
                icebergPartitionInfo = IcebergUtils.loadPartitionInfo(dorisTable, projectionTable,
                        latestIcebergSnapshot.getSnapshotId(), latestIcebergSnapshot.getSchemaId(), authenticator,
                        enableMappingVarbinary, enableMappingTimestampTz);
            }
            Optional<Map<Integer, List<String>>> nameMapping =
                    IcebergUtils.getNameMapping(projectionTable);
            return isolateForQueries
                    ? new IcebergSnapshotCacheValue(
                            icebergPartitionInfo, latestIcebergSnapshot, nameMapping,
                            retainedTable, retainedCurrentSnapshotJson)
                    : new IcebergSnapshotCacheValue(
                            icebergPartitionInfo, latestIcebergSnapshot, nameMapping,
                            retainedTable);
        } catch (AnalysisException e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private IcebergMetadataOps resolveMetadataOps(CatalogIf catalog) {
        if (catalog instanceof HMSExternalCatalog) {
            return ((HMSExternalCatalog) catalog).getIcebergMetadataOps();
        } else if (catalog instanceof IcebergExternalCatalog) {
            return (IcebergMetadataOps) (((IcebergExternalCatalog) catalog).getMetadataOps());
        }
        throw new RuntimeException("Only support 'hms' and 'iceberg' type for iceberg table");
    }

    /**
     * Execute on the authenticated context captured with the table generation, falling back to
     * the catalog's current authenticator for values that predate the capture. A concurrent
     * property ALTER resets the catalog before retiring the group, so a lookup that already
     * owns the old generation must not resolve authentication from the resetting catalog.
     */
    private <T> T executeForGeneration(IcebergTableCacheValue tableValue, long catalogId,
            Function<ExecutionAuthenticator, T> task) {
        ExecutionAuthenticator authenticator = tableValue.getAuthenticator();
        if (authenticator == null) {
            CatalogIf<?> catalog = getCatalog(catalogId);
            if (!(catalog instanceof ExternalCatalog)) {
                throw new RuntimeException("Iceberg metadata cache requires an external catalog");
            }
            ((ExternalCatalog) catalog).makeSureInitialized();
            authenticator = ((ExternalCatalog) catalog).getExecutionAuthenticator();
        }
        ExecutionAuthenticator generationAuthenticator = authenticator;
        try {
            return generationAuthenticator.execute(() -> task.apply(generationAuthenticator));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private <T> T executeAuthenticated(long catalogId, Callable<T> task) {
        CatalogIf<?> catalog = getCatalog(catalogId);
        if (catalog == null) {
            throw new RuntimeException("Cannot find catalog " + catalogId + " when loading Iceberg metadata.");
        }
        return executeAuthenticated(catalog, task);
    }

    private <T> T executeAuthenticated(CatalogIf<?> catalog, Callable<T> task) {
        if (!(catalog instanceof ExternalCatalog)) {
            throw new RuntimeException("Iceberg metadata cache requires an external catalog");
        }
        try {
            return ((ExternalCatalog) catalog).getExecutionAuthenticator().execute(task);
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        Map<String, String> compatibility = new java.util.HashMap<>(
                singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA));
        compatibility.put("meta.cache.iceberg.table.enable", "meta.cache.iceberg.snapshot.enable");
        compatibility.put("meta.cache.iceberg.table.ttl-second", "meta.cache.iceberg.snapshot.ttl-second");
        compatibility.put("meta.cache.iceberg.table.capacity", "meta.cache.iceberg.snapshot.capacity");
        return compatibility;
    }

    private ManifestCacheValue loadDataFiles(
            org.apache.iceberg.ManifestFile manifest, Table table, boolean accountRetainedSize)
            throws IOException {
        ManifestCacheValue.Builder builder = ManifestCacheValue.dataFilesBuilder(accountRetainedSize);
        try (ManifestReader<org.apache.iceberg.DataFile> reader = ManifestFiles.read(manifest, table.io())) {
            for (org.apache.iceberg.DataFile dataFile : reader) {
                builder.addDataFile(dataFile.copy());
            }
        }
        return builder.build();
    }

    private ManifestCacheValue loadDeleteFiles(
            org.apache.iceberg.ManifestFile manifest, Table table, boolean accountRetainedSize)
            throws IOException {
        ManifestCacheValue.Builder builder = ManifestCacheValue.deleteFilesBuilder(accountRetainedSize);
        try (ManifestReader<org.apache.iceberg.DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest,
                table.io(), table.specs())) {
            for (org.apache.iceberg.DeleteFile deleteFile : reader) {
                builder.addDeleteFile(deleteFile.copy());
            }
        }
        return builder.build();
    }

    /**
     * Collect every FileIO a catalog's cached values still retain. A snapshot value can outlive
     * its weighted/expired/collected table entry while retaining the same frozen table graph, so
     * both entries are enumerated; identity de-duplication keeps the later drop pass bounded.
     */
    private List<FileIO> collectManifestFileIos(long catalogId) {
        IdentityHashMap<FileIO, Boolean> seen = new IdentityHashMap<>();
        List<FileIO> fileIos = new ArrayList<>();
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = tableEntry.getIfInitialized(catalogId);
        if (tables != null) {
            tables.forEach((key, value) -> collectManifestFileIo(seen, fileIos,
                    value == null ? null : value.getIcebergTable()));
        }
        MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots =
                snapshotEntry.getIfInitialized(catalogId);
        if (snapshots != null) {
            snapshots.forEach((key, value) -> collectManifestFileIo(seen, fileIos,
                    value == null ? null : value.getRetainedIcebergTable().orElse(null)));
        }
        return fileIos;
    }

    private void collectManifestFileIo(IdentityHashMap<FileIO, Boolean> seen, List<FileIO> fileIos,
            @Nullable Table table) {
        if (table == null) {
            return;
        }
        try {
            FileIO fileIo = table.io();
            if (fileIo != null && seen.put(fileIo, Boolean.TRUE) == null) {
                fileIos.add(fileIo);
            }
        } catch (Exception e) {
            LOG.warn("Failed to resolve iceberg table FileIO for manifest cache cleanup", e);
        }
    }

    private void dropManifestFileIoCaches(List<FileIO> fileIos) {
        for (FileIO fileIo : fileIos) {
            try {
                ManifestFiles.dropCache(fileIo);
            } catch (Exception e) {
                LOG.warn("Failed to drop iceberg manifest files cache", e);
            }
        }
    }

}
