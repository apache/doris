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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

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
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
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
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getIcebergTable();
    }

    public IcebergSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getLatestSnapshotCacheValue();
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
        if (catalog == null) {
            throw new RuntimeException(String.format("Cannot find catalog %d when loading table %s/%s.",
                    nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        IcebergMetadataOps ops = resolveMetadataOps(catalog);
        try {
            Table table = ((ExternalCatalog) catalog).getExecutionAuthenticator()
                    .execute(() -> ops.loadTable(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName()));
            ExternalTable dorisTable = findExternalTable(nameMapping, ENGINE);
            return new IcebergTableCacheValue(table, () -> loadSnapshotProjection(dorisTable, table));
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
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
            MTMVRelatedTableIf table = (MTMVRelatedTableIf) dorisTable;
            IcebergSnapshot latestIcebergSnapshot = IcebergUtils.getLatestIcebergSnapshot(icebergTable);
            IcebergPartitionInfo icebergPartitionInfo;
            if (!table.isValidRelatedTable()) {
                icebergPartitionInfo = IcebergPartitionInfo.empty();
            } else {
                icebergPartitionInfo = IcebergUtils.loadPartitionInfo(dorisTable, icebergTable,
                        latestIcebergSnapshot.getSnapshotId(), latestIcebergSnapshot.getSchemaId());
            }
            return new IcebergSnapshotCacheValue(icebergPartitionInfo, latestIcebergSnapshot);
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
