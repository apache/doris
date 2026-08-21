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

import org.apache.doris.catalog.Env;
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
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Iceberg engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code table}: loaded Iceberg {@link Table} instances per Doris table mapping, each
 *   memoizing its latest snapshot runtime projection</li>
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
 *   <li>db/table invalidation clears table/view/schema entries, while keeping manifest entries</li>
 *   <li>partition-level invalidation falls back to table-level invalidation</li>
 * </ul>
 */
public class IcebergExternalMetaCache extends AbstractExternalMetaCache {
    private static final Logger LOG = LogManager.getLogger(IcebergExternalMetaCache.class);

    public static final String ENGINE = "iceberg";
    public static final String ENTRY_TABLE = "table";
    public static final String ENTRY_VIEW = "view";
    public static final String ENTRY_MANIFEST = "manifest";
    public static final String ENTRY_SCHEMA = "schema";
    private static final long DEFAULT_MANIFEST_CACHE_CAPACITY = 100_000L;

    private final EntryHandle<NameMapping, IcebergTableCacheValue> tableEntry;
    private final EntryHandle<NameMapping, View> viewEntry;
    private final EntryHandle<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry;
    private final EntryHandle<IcebergSchemaCacheKey, SchemaCacheValue> schemaEntry;

    public IcebergExternalMetaCache(ExecutorService refreshExecutor) {
        super(ENGINE, refreshExecutor);
        tableEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class,
                this::loadTableCacheValue, defaultEntryCacheSpec(),
                true, MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping),
                (key, value) -> value.retire()));
        viewEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_VIEW, NameMapping.class, View.class, this::loadView,
                defaultEntryCacheSpec(), MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
        manifestEntry = registerEntry(MetaCacheEntryDef.contextualOnly(ENTRY_MANIFEST, IcebergManifestEntryKey.class,
                ManifestCacheValue.class,
                CacheSpec.of(false, CacheSpec.CACHE_NO_TTL, DEFAULT_MANIFEST_CACHE_CAPACITY)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, IcebergSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(IcebergSchemaCacheKey::getNameMapping)));
    }

    public Table getIcebergTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease != null) {
            return lease.getIcebergTable();
        }
        // Background/bootstrap callers without a StatementContext have no deterministic release boundary.
        // Load directly instead of borrowing a cache generation that could be evicted while they use it.
        return loadTable(nameMapping);
    }

    /** Returns the executor owned by the exact table generation retained by this statement. */
    ThreadPoolExecutor getIcebergTableExecutor(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease == null || lease.getPlanningExecutor() == null) {
            return dorisTable.getCatalog().getThreadPoolWithPreAuth();
        }
        return lease.getPlanningExecutor();
    }

    /** Runs a bounded metadata operation while retaining the exact table generation it uses. */
    <T> T withIcebergTable(ExternalTable dorisTable, Function<Table, T> action) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease statementLease = statementLease(nameMapping);
        if (statementLease != null) {
            return action.apply(statementLease.getIcebergTable());
        }
        try (IcebergTableCacheValue.Lease operationLease = borrow(nameMapping)) {
            return action.apply(operationLease.getIcebergTable());
        }
    }

    public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        IcebergTableCacheValue.Lease lease = statementLease(nameMapping);
        if (lease != null) {
            return lease.getLatestSnapshotCacheValue();
        }
        Table table = loadTable(nameMapping);
        ExternalTable tableForProjection = findExternalTable(nameMapping, ENGINE);
        return loadSnapshotProjection(tableForProjection, table);
    }

    public List<Snapshot> getSnapshotList(ExternalTable dorisTable) {
        Table icebergTable = getIcebergTable(dorisTable);
        List<Snapshot> snapshots = com.google.common.collect.Lists.newArrayList();
        com.google.common.collect.Iterables.addAll(snapshots, icebergTable.snapshots());
        return snapshots;
    }

    public View getIcebergView(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return viewEntry.get(nameMapping.getCtlId()).get(nameMapping);
    }

    public IcebergSchemaCacheValue getIcebergSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        IcebergSchemaCacheKey key = new IcebergSchemaCacheKey(nameMapping, schemaId);
        SchemaCacheValue schemaCacheValue = schemaEntry.get(nameMapping.getCtlId()).get(key);
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
        boolean hit = manifestEntry.getIfPresent(key) != null;
        if (cacheHitRecorder != null) {
            cacheHitRecorder.accept(hit);
        }
        return manifestEntry.get(key, ignored -> loadManifestCacheValue(manifest, icebergTable, key.getContent()));
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
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(nameMapping.getCtlId());
        if (catalog instanceof IcebergExternalCatalog) {
            IcebergExternalCatalog icebergCatalog = (IcebergExternalCatalog) catalog;
            try (IcebergExternalCatalog.TableLoadContext loadContext = icebergCatalog.beginTableLoad()) {
                IcebergMetadataOps ops = loadContext.getOps();
                Table table;
                try {
                    table = loadContext.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
                } catch (Exception e) {
                    throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
                }
                ExternalTable dorisTable = findExternalTable(nameMapping, ENGINE);
                Runnable tableCleanup = tableCleanup(loadContext.getCatalogType(), ops, table);
                IcebergCatalogResourceTracker.ResourceLease catalogLease = loadContext.promote();
                return new IcebergTableCacheValue(table, ops.getThreadPoolWithPreAuth(),
                        () -> loadSnapshotProjection(dorisTable, table), () -> {
                    try {
                        tableCleanup.run();
                    } finally {
                        catalogLease.close();
                    }
                });
            }
        }
        Table table = loadTable(nameMapping);
        IcebergMetadataOps ops = resolveMetadataOps(catalog);
        ExternalTable dorisTable = findExternalTable(nameMapping, ENGINE);
        return new IcebergTableCacheValue(table, () -> loadSnapshotProjection(dorisTable, table),
                tableCleanup(catalog, ops, table));
    }

    private Table loadTable(NameMapping nameMapping) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(nameMapping.getCtlId());
        if (catalog == null) {
            throw new RuntimeException(String.format("Cannot find catalog %d when loading table %s/%s.",
                    nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        return loadTable(nameMapping, catalog, resolveMetadataOps(catalog));
    }

    private Table loadTable(NameMapping nameMapping, CatalogIf catalog, IcebergMetadataOps ops) {
        try {
            return ((ExternalCatalog) catalog).getExecutionAuthenticator()
                    .execute(() -> ops.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

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
            value.releaseLoaderReference();
            if (lease != null) {
                return lease;
            }
            // Eviction won between lookup and retain. Retry against the current exact generation.
        }
    }

    private Runnable tableCleanup(CatalogIf catalog, IcebergMetadataOps ops, Table table) {
        if (!(catalog instanceof IcebergExternalCatalog)) {
            return () -> { };
        }
        return tableCleanup(((IcebergExternalCatalog) catalog).getIcebergCatalogType(), ops, table);
    }

    private Runnable tableCleanup(String type, IcebergMetadataOps ops, Table table) {
        FileIO catalogFileIO = IcebergExternalCatalog.ICEBERG_REST.equals(type) ? catalogFileIO(ops) : null;
        boolean tableOwned = shouldCloseTableFileIO(type, table.io(), catalogFileIO);
        if (!tableOwned) {
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

    private View loadView(NameMapping nameMapping) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(nameMapping.getCtlId());
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
            ManifestContent content) {
        if (manifest == null || icebergTable == null) {
            String manifestPath = manifest == null ? "null" : manifest.path();
            throw new CacheException("Manifest cache loader context is missing for %s",
                    null, manifestPath);
        }
        try {
            if (content == ManifestContent.DELETES) {
                return ManifestCacheValue.forDeleteFiles(
                        loadDeleteFiles(manifest, icebergTable));
            }
            return ManifestCacheValue.forDataFiles(loadDataFiles(manifest, icebergTable));
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

    private IcebergSnapshotCacheValue loadSnapshotProjection(ExternalTable dorisTable, Table icebergTable) {
        if (!(dorisTable instanceof MTMVRelatedTableIf)) {
            throw new RuntimeException(String.format("Table %s.%s is not a valid MTMV related table.",
                    dorisTable.getDbName(), dorisTable.getName()));
        }
        try {
            // Freeze before deriving snapshot, partitions, and aliases; BaseTable accessors share
            // refreshable operations and otherwise could mix two concurrent metadata generations.
            Table retainedTable = IcebergSnapshotCacheValue.retainTableGeneration(icebergTable);
            MTMVRelatedTableIf table = (MTMVRelatedTableIf) dorisTable;
            IcebergSnapshot latestIcebergSnapshot = IcebergUtils.getLatestIcebergSnapshot(retainedTable);
            IcebergPartitionInfo icebergPartitionInfo;
            boolean validRelatedTable = dorisTable instanceof IcebergExternalTable
                    ? ((IcebergExternalTable) dorisTable).isValidRelatedTable(retainedTable)
                    : table.isValidRelatedTable();
            if (!validRelatedTable) {
                icebergPartitionInfo = IcebergPartitionInfo.empty();
            } else {
                icebergPartitionInfo = IcebergUtils.loadPartitionInfo(dorisTable, retainedTable,
                        latestIcebergSnapshot.getSnapshotId(), latestIcebergSnapshot.getSchemaId());
            }
            return new IcebergSnapshotCacheValue(
                    icebergPartitionInfo, latestIcebergSnapshot, IcebergUtils.getNameMapping(retainedTable),
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

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
    }

    private List<org.apache.iceberg.DataFile> loadDataFiles(org.apache.iceberg.ManifestFile manifest, Table table)
            throws IOException {
        List<org.apache.iceberg.DataFile> dataFiles = com.google.common.collect.Lists.newArrayList();
        try (ManifestReader<org.apache.iceberg.DataFile> reader = ManifestFiles.read(manifest, table.io())) {
            for (org.apache.iceberg.DataFile dataFile : reader) {
                dataFiles.add(dataFile.copy());
            }
        }
        return dataFiles;
    }

    private List<org.apache.iceberg.DeleteFile> loadDeleteFiles(org.apache.iceberg.ManifestFile manifest, Table table)
            throws IOException {
        List<org.apache.iceberg.DeleteFile> deleteFiles = com.google.common.collect.Lists.newArrayList();
        try (ManifestReader<org.apache.iceberg.DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest,
                table.io(), table.specs())) {
            for (org.apache.iceberg.DeleteFile deleteFile : reader) {
                deleteFiles.add(deleteFile.copy());
            }
        }
        return deleteFiles;
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
