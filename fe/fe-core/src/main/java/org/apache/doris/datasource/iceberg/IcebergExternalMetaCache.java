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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
                .withReplacementListener(this::retireTableGeneration));
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
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, IcebergSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(IcebergSchemaCacheKey::getNameMapping)));
    }

    public Table getIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getIcebergTable();
    }

    public Table getWritableIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException("Cannot find catalog " + nameMapping.getCtlId()
                    + " when loading a writable Iceberg table");
        }
        IcebergMetadataOps ops = resolveMetadataOps(catalog);
        // DDL/actions must start from the live catalog generation. DML that was planned against a
        // retained read generation wraps this live table separately in IcebergTransaction.
        return executeAuthenticated(catalog, () -> ops.loadTable(
                nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
    }

    Table getQueryScopedIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> entry =
                tableEntry.get(nameMapping.getCtlId());
        IcebergTableCacheValue tableValue =
                entry.get(nameMapping);
        return createQueryTable(nameMapping, tableValue);
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
        IcebergTableCacheValue tableValue =
                tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        Table retainedTable = tableValue.getRetainedIcebergTable();
        java.util.Optional<IcebergSnapshotEntryKey> optionalKey =
                IcebergSnapshotEntryKey.tryCreate(nameMapping, retainedTable);
        if (!optionalKey.isPresent()) {
            boolean isolateForQueries = tableValue.isQueryIsolationPrepared();
            return executeAuthenticated(nameMapping.getCtlId(),
                    () -> loadSnapshotProjection(
                            dorisTable,
                            isolateForQueries ? tableValue.newQueryScopedTable()
                                    : tableValue.getIcebergTable(),
                            tableValue.getRetainedIcebergTable(),
                            tableValue.getRetainedCurrentSnapshotJson(), isolateForQueries));
        }
        IcebergSnapshotEntryKey key = optionalKey.get();
        MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> entry =
                snapshotEntry.get(nameMapping.getCtlId());
        boolean isolateForQueries = tableValue.isQueryIsolationPrepared()
                || entry.isWeightAccounting();
        Function<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> projectionLoader =
                ignored -> executeAuthenticated(nameMapping.getCtlId(), () -> {
                    Table projectionTable = isolateForQueries
                            ? tableValue.newQueryScopedTable() : tableValue.getIcebergTable();
                    IcebergSnapshotCacheValue value = loadSnapshotProjection(
                            dorisTable, projectionTable,
                            tableValue.getRetainedIcebergTable(),
                            tableValue.getRetainedCurrentSnapshotJson(), isolateForQueries);
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
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = tableEntry.get(nameMapping.getCtlId());
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
        IcebergTableCacheValue tableValue = tableEntry.get(nameMapping.getCtlId()).get(nameMapping);
        return getIcebergSchemaCacheValue(nameMapping, schemaId, tableValue.getRetainedIcebergTable());
    }

    IcebergSchemaCacheValue getIcebergSchemaCacheValue(
            NameMapping nameMapping, long schemaId, Table retainedTable) {
        Optional<IcebergSnapshotEntryKey> generation = IcebergSnapshotEntryKey.tryCreate(nameMapping, retainedTable);
        if (!generation.isPresent()) {
            return (IcebergSchemaCacheValue) loadSchemaCacheValue(
                    new IcebergSchemaCacheKey(nameMapping, schemaId), retainedTable);
        }
        IcebergSchemaCacheKey key = new IcebergSchemaCacheKey(
                nameMapping, generation.get().getTableUuid(), schemaId);
        MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> entry = schemaEntry.get(nameMapping.getCtlId());
        SchemaCacheValue schemaCacheValue = entry
                .get(key, ignored -> loadSchemaCacheValue(key, retainedTable));
        MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = tableEntry.get(nameMapping.getCtlId());
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
        dropManifestFileIoCacheForCatalog(catalogId);
        super.invalidateCatalog(catalogId);
    }

    @Override
    public void invalidateCatalogEntries(long catalogId) {
        dropManifestFileIoCacheForCatalog(catalogId);
        super.invalidateCatalogEntries(catalogId);
    }

    private IcebergTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        CatalogIf catalog = getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException(String.format("Cannot find catalog %d when loading table %s/%s.",
                    nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        IcebergMetadataOps ops = resolveMetadataOps(catalog);
        return executeAuthenticated(catalog, () -> {
            Table table = ops.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
            IcebergTableCacheValue value = new IcebergTableCacheValue(table);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> currentEntry =
                    tableEntry.getIfInitialized(nameMapping.getCtlId());
            if (currentEntry != null && currentEntry.isWeightAccounting()) {
                prepareTableForCachePublication(nameMapping, value);
            }
            return value;
        });
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

    private SchemaCacheValue loadSchemaCacheValue(IcebergSchemaCacheKey key, Table retainedTable) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        dorisTable.setUpdateTime(System.currentTimeMillis());
        boolean isView = dorisTable instanceof IcebergExternalTable
                && ((IcebergExternalTable) dorisTable).isView();
        return IcebergUtils.loadSchemaCacheValue(
                dorisTable, key.getSchemaId(), isView, retainedTable).orElseThrow(() ->
                new CacheException("failed to load iceberg schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
    }

    private void retireTableGeneration(NameMapping nameMapping,
            @Nullable IcebergTableCacheValue previousValue, IcebergTableCacheValue currentValue) {
        if (previousValue != null && previousValue.isSameOperationalGeneration(currentValue)) {
            return;
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
                    && !key.getTableUuid().equals(currentUuid));
        }
    }

    private static boolean sharesOperationalResources(
            IcebergTableCacheValue currentValue, @Nullable IcebergSnapshotCacheValue projection) {
        if (projection == null) {
            return false;
        }
        if (projection.getRetainedIcebergTable().map(table -> table == currentValue.getRetainedIcebergTable())
                .orElse(false)) {
            return true;
        }
        Optional<Table> retainedTable = projection.getRetainedIcebergTable();
        // Count-mode projections do not retain a table handle; nothing to rebind.
        return !retainedTable.isPresent() || currentValue.sharesOperationalResources(retainedTable.get());
    }

    private IcebergSnapshotCacheValue loadSnapshotProjection(
            ExternalTable dorisTable, Table projectionTable, Table retainedTable,
            String retainedCurrentSnapshotJson, boolean isolateForQueries) {
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
                        latestIcebergSnapshot.getSnapshotId(), latestIcebergSnapshot.getSchemaId());
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

    private void dropManifestFileIoCacheForCatalog(long catalogId) {
        tableEntry.get(catalogId).forEach((key, value) -> dropManifestFileIoCache(value));
    }

    private void dropManifestFileIoCache(IcebergTableCacheValue tableCacheValue) {
        try {
            ManifestFiles.dropCache(tableCacheValue.getIcebergTable().io());
        } catch (Exception e) {
            LOG.warn("Failed to drop iceberg manifest files cache", e);
        }
    }

}
