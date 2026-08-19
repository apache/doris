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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.ExternalMetaCacheBudgetManager;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.FileSystemDirectoryLister;
import org.apache.doris.fs.FileSystemIOException;
import org.apache.doris.fs.RemoteIterator;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.planner.ListPartitionPrunerV2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import lombok.Data;
import lombok.Getter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Hive engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>This cache consolidates schema metadata and Hive Metastore-derived runtime metadata
 * under one engine so callers can use a unified invalidation path.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code schema}: table schema cache keyed by {@link SchemaCacheKey}</li>
 *   <li>{@code partition_values}: partition value/index structures per table</li>
 *   <li>{@code partition}: single partition metadata keyed by partition values</li>
 *   <li>{@code file}: file listing cache for partition/table locations</li>
 * </ul>
 *
 * <p>Invalidation behavior:
 * <ul>
 *   <li>{@link #invalidateDb(long, String)} and {@link #invalidateTable(long, String, String)}
 *   clear all related entries with table/db granularity.</li>
 *   <li>{@link #invalidatePartitions(long, String, String, List)} supports partition-level
 *   invalidation when specific partition names are provided, and falls back to table-level
 *   invalidation for empty input or unresolved table metadata.</li>
 * </ul>
 */
public class HiveExternalMetaCache extends AbstractExternalMetaCache {
    private static final Logger LOG = LogManager.getLogger(HiveExternalMetaCache.class);
    private static final int PARTITION_EVENT_REPLACE_MAX_RETRIES = 8;

    public static final String ENGINE = "hive";
    public static final String ENTRY_SCHEMA = "schema";
    public static final String ENTRY_PARTITION_VALUES = "partition_values";
    public static final String ENTRY_PARTITION = "partition";
    public static final String ENTRY_FILE = "file";

    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    public static final String ERR_CACHE_INCONSISTENCY = "ERR_CACHE_INCONSISTENCY: ";

    private final ExecutorService fileListingExecutor;
    // Per-catalog counters that let statement-scoped Hive scan reuse observe global file-cache
    // version changes. ADVANCE when the global file cache is invalidated (invalidateCatalog /
    // invalidateDb / invalidateTable / invalidatePartitions) or replaced (loadFileCacheValue),
    // so a later alias in the same statement builds a different statement key and re-lists instead
    // of reusing a stale FileCacheValue. Do NOT advance in paths that only read or refresh
    // metadata without replacing file entries (e.g. schema-only reloads), or the statement cache
    // would be invalidated for no data change. Both maps are pruned on terminal catalog removal
    // (invalidateCatalog), because catalog ids are never reused.
    private final Map<Long, AtomicLong> fileCacheInvalidationGenerations = new ConcurrentHashMap<>();
    private final Map<Long, AtomicLong> fileCacheValueGenerations = new ConcurrentHashMap<>();

    private final EntryHandle<SchemaCacheKey, SchemaCacheValue> schemaEntry;
    private final EntryHandle<PartitionValueCacheKey, HivePartitionValues> partitionValuesEntry;
    private final EntryHandle<PartitionCacheKey, HivePartition> partitionEntry;
    private final EntryHandle<FileCacheKey, FileCacheValue> fileEntry;
    private final PartitionCacheCoordinator partitionCacheCoordinator = new PartitionCacheCoordinator();

    public HiveExternalMetaCache(ExecutorService refreshExecutor, ExecutorService fileListingExecutor) {
        this(refreshExecutor, fileListingExecutor, new ExternalMetaCacheBudgetManager(java.util.OptionalLong.empty()));
    }

    public HiveExternalMetaCache(ExecutorService refreshExecutor, ExecutorService fileListingExecutor,
            ExternalMetaCacheBudgetManager budgetManager) {
        super(ENGINE, refreshExecutor, budgetManager);
        this.fileListingExecutor = fileListingExecutor;

        schemaEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_SCHEMA,
                SchemaCacheKey.class,
                SchemaCacheValue.class,
                this::loadSchemaCacheValue,
                defaultSchemaCacheSpec()));
        partitionValuesEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_PARTITION_VALUES,
                PartitionValueCacheKey.class,
                HivePartitionValues.class,
                this::loadPartitionValuesCacheValue,
                CacheSpec.of(
                        true,
                        Config.external_cache_expire_time_seconds_after_access,
                        Config.max_hive_partition_table_cache_num))
                .withSizeEstimator((key, value) -> value.prepareForCachePublication(key)));
        partitionEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_PARTITION,
                PartitionCacheKey.class,
                HivePartition.class,
                this::loadPartitionCacheValue,
                CacheSpec.of(
                        true,
                        Config.external_cache_expire_time_seconds_after_access,
                        Config.max_hive_partition_cache_num)));
        fileEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_FILE,
                FileCacheKey.class,
                FileCacheValue.class,
                this::loadFileCacheValue,
                CacheSpec.of(
                        true,
                        Config.external_cache_expire_time_seconds_after_access,
                        Config.max_external_file_cache_num)));
    }

    @Override
    public Collection<String> aliases() {
        return Collections.singleton("hms");
    }

    public void refreshCatalog(long catalogId) {
        invalidateCatalog(catalogId);
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        Map<String, String> catalogProperties = catalog == null || catalog.getProperties() == null
                ? Maps.newHashMap()
                : Maps.newHashMap(catalog.getProperties());
        initCatalog(catalogId, catalogProperties);
    }

    @Override
    public void invalidateCatalog(long catalogId) {
        super.invalidateCatalog(catalogId);
        advanceFileCacheInvalidationGeneration(catalogId);
        // The catalog is being removed (drop or property-driven full rebuild); its id is never
        // reused, so drop the generation counters instead of letting them accumulate forever.
        // The advance above already invalidated any in-flight statement keyed on the old
        // generation; a later get() rebuilds them from zero for a catalog that no longer exists.
        fileCacheInvalidationGenerations.remove(catalogId);
        fileCacheValueGenerations.remove(catalogId);
    }

    @Override
    public void invalidateCatalogEntries(long catalogId) {
        super.invalidateCatalogEntries(catalogId);
        advanceFileCacheInvalidationGeneration(catalogId);
    }

    @Override
    public void invalidateDb(long catalogId, String dbName) {
        schemaEntry.get(catalogId).invalidateIf(key -> matchDb(key.getNameMapping(), dbName));
        partitionValuesEntry.get(catalogId).invalidateIf(key -> matchDb(key.getNameMapping(), dbName));
        partitionEntry.get(catalogId).invalidateIf(key -> matchDb(key.getNameMapping(), dbName));
        fileEntry.get(catalogId).invalidateAll();
        advanceFileCacheInvalidationGeneration(catalogId);
    }

    @Override
    public void invalidateTable(long catalogId, String dbName, String tableName) {
        schemaEntry.get(catalogId).invalidateIf(key -> matchTable(key.getNameMapping(), dbName, tableName));
        partitionValuesEntry.get(catalogId).invalidateIf(key -> matchTable(key.getNameMapping(), dbName, tableName));
        partitionEntry.get(catalogId).invalidateIf(key -> matchTable(key.getNameMapping(), dbName, tableName));
        long tableId = Util.genIdByName(dbName, tableName);
        fileEntry.get(catalogId).invalidateIf(key -> key.isSameTable(tableId));
        advanceFileCacheInvalidationGeneration(catalogId);
    }

    public long getFileCacheInvalidationGeneration(long catalogId) {
        return fileCacheInvalidationGenerations
                .computeIfAbsent(catalogId, ignored -> new AtomicLong())
                .get();
    }

    private void advanceFileCacheInvalidationGeneration(long catalogId) {
        fileCacheInvalidationGenerations
                .computeIfAbsent(catalogId, ignored -> new AtomicLong())
                .incrementAndGet();
    }

    @Override
    public void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            invalidateTable(catalogId, dbName, tableName);
            return;
        }

        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof HMSExternalCatalog)) {
            return;
        }

        HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
        if (hmsCatalog.getDbNullable(dbName) == null
                || !(hmsCatalog.getDbNullable(dbName).getTableNullable(tableName) instanceof HMSExternalTable)) {
            invalidateTable(catalogId, dbName, tableName);
            return;
        }
        HMSExternalTable hmsTable = (HMSExternalTable) hmsCatalog.getDbNullable(dbName).getTableNullable(tableName);

        for (String partition : partitions) {
            invalidatePartitionCache(hmsTable, partition);
        }
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        Map<String, String> compatibilityMap = Maps.newHashMap();
        compatibilityMap.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, metaCacheTtlKey(ENTRY_SCHEMA));
        compatibilityMap.put(HMSExternalCatalog.PARTITION_CACHE_TTL_SECOND, metaCacheTtlKey(ENTRY_PARTITION_VALUES));
        compatibilityMap.put(HMSExternalCatalog.FILE_META_CACHE_TTL_SECOND, metaCacheTtlKey(ENTRY_FILE));
        return compatibilityMap;
    }

    private MetaCacheEntry<SchemaCacheKey, SchemaCacheValue> schemaEntryIfInitialized(long catalogId) {
        return schemaEntry.getIfInitialized(catalogId);
    }

    private MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> partitionValuesEntryIfInitialized(
            long catalogId) {
        return partitionValuesEntry.getIfInitialized(catalogId);
    }

    private MetaCacheEntry<PartitionCacheKey, HivePartition> partitionEntryIfInitialized(long catalogId) {
        return partitionEntry.getIfInitialized(catalogId);
    }

    private MetaCacheEntry<FileCacheKey, FileCacheValue> fileEntryIfInitialized(long catalogId) {
        return fileEntry.getIfInitialized(catalogId);
    }

    private SchemaCacheValue loadSchemaCacheValue(SchemaCacheKey key) {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(key.getNameMapping().getCtlId());
        if (!(catalog instanceof ExternalCatalog)) {
            throw new CacheException("catalog %s is not external when loading hive schema cache",
                    null, key.getNameMapping().getCtlId());
        }
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        return externalCatalog.getSchema(key).orElseThrow(() -> new CacheException(
                "failed to load hive schema cache value for: %s.%s.%s",
                null, key.getNameMapping().getCtlId(),
                key.getNameMapping().getLocalDbName(),
                key.getNameMapping().getLocalTblName()));
    }

    private HivePartitionValues loadPartitionValuesCacheValue(PartitionValueCacheKey key) {
        return loadPartitionValues(key);
    }

    private HivePartition loadPartitionCacheValue(PartitionCacheKey key) {
        return loadPartition(key);
    }

    private FileCacheValue loadFileCacheValue(FileCacheKey key) {
        FileCacheValue value = loadFiles(key, new FileSystemDirectoryLister(), null);
        value.setCacheGeneration(fileCacheValueGenerations
                .computeIfAbsent(key.catalogId, ignored -> new AtomicLong())
                .incrementAndGet());
        return value;
    }

    private HMSExternalCatalog hmsCatalog(long catalogId) {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof HMSExternalCatalog)) {
            throw new CacheException("catalog %s is not hms when loading hive metastore cache", null, catalogId);
        }
        return (HMSExternalCatalog) catalog;
    }

    private HivePartitionValues loadPartitionValues(PartitionValueCacheKey key) {
        NameMapping nameMapping = key.nameMapping;
        HMSExternalCatalog catalog = hmsCatalog(nameMapping.getCtlId());
        List<String> partitionNames = catalog.getClient()
                .listPartitionNames(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
        if (LOG.isDebugEnabled()) {
            LOG.debug("load #{} partitions for {} in catalog {}", partitionNames.size(), key, catalog.getName());
        }

        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionNames.size());
        BiMap<String, Long> partitionNameToIdMap = HashBiMap.create(partitionNames.size());
        long partitionNamePayloadBytes = 0L;
        String localDbName = nameMapping.getLocalDbName();
        String localTblName = nameMapping.getLocalTblName();
        for (String partitionName : partitionNames) {
            partitionNamePayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    partitionNamePayloadBytes,
                    MetaCacheWeightUtils.estimatedStringPayloadBytes(partitionName));
            long partitionId = Util.genIdByName(catalog.getName(), localDbName, localTblName, partitionName);
            ListPartitionItem listPartitionItem = toListPartitionItem(partitionName, key.types, catalog.getName());
            idToPartitionItem.put(partitionId, listPartitionItem);
            partitionNameToIdMap.put(partitionName, partitionId);
        }

        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        HivePartitionValues partitionValues =
                new HivePartitionValues(idToPartitionItem, partitionNameToIdMap, partitionValuesMap,
                        partitionNamePayloadBytes, key.types == null ? 0 : key.types.size());
        preparePartitionValuesForPublication(partitionValues);
        return partitionValues;
    }

    private void preparePartitionValuesForPublication(HivePartitionValues partitionValues) {
        partitionValues.rebuildSortedPartitionRangesForPublication();
    }

    private ListPartitionItem toListPartitionItem(String partitionName, List<Type> types, String catalogName) {
        List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
        Preconditions.checkState(types != null,
                ERR_CACHE_INCONSISTENCY + "partition types is null for partition " + partitionName);
        Preconditions.checkState(partitionValues.size() == types.size(),
                ERR_CACHE_INCONSISTENCY + partitionName + " vs. " + types);

        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            values.add(new PartitionValue(partitionValue, HIVE_DEFAULT_PARTITION.equals(partitionValue)));
        }
        try {
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
            return new ListPartitionItem(Lists.newArrayList(partitionKey));
        } catch (AnalysisException e) {
            throw new CacheException("failed to convert hive partition %s to list partition in catalog %s",
                    e, partitionName, catalogName);
        }
    }

    private HivePartition loadPartition(PartitionCacheKey key) {
        NameMapping nameMapping = key.nameMapping;
        HMSExternalCatalog catalog = hmsCatalog(nameMapping.getCtlId());
        Partition partition = catalog.getClient()
                .getPartition(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), key.values);
        StorageDescriptor sd = partition.getSd();
        if (LOG.isDebugEnabled()) {
            LOG.debug("load partition format: {}, location: {} for {} in catalog {}",
                    sd.getInputFormat(), sd.getLocation(), key, catalog.getName());
        }
        return new HivePartition(nameMapping, false, sd.getInputFormat(), sd.getLocation(), key.values,
                partition.getParameters());
    }

    private Map<PartitionCacheKey, HivePartition> loadPartitions(Iterable<? extends PartitionCacheKey> keys) {
        Map<PartitionCacheKey, HivePartition> result = new HashMap<>();
        if (keys == null) {
            return result;
        }

        List<PartitionCacheKey> keyList = Streams.stream(keys).collect(Collectors.toList());
        if (keyList.isEmpty()) {
            return result;
        }

        PartitionCacheKey oneKey = keyList.get(0);
        NameMapping nameMapping = oneKey.nameMapping;
        HMSExternalCatalog catalog = hmsCatalog(nameMapping.getCtlId());

        String localDbName = nameMapping.getLocalDbName();
        String localTblName = nameMapping.getLocalTblName();
        List<Column> partitionColumns = ((HMSExternalTable) catalog.getDbNullable(localDbName)
                .getTableNullable(localTblName)).getPartitionColumns();

        List<String> partitionNames = keyList.stream().map(key -> {
            StringBuilder sb = new StringBuilder();
            Preconditions.checkState(key.getValues().size() == partitionColumns.size());
            for (int i = 0; i < partitionColumns.size(); i++) {
                sb.append(FileUtils.escapePathName(partitionColumns.get(i).getName()));
                sb.append("=");
                sb.append(FileUtils.escapePathName(key.getValues().get(i)));
                sb.append("/");
            }
            sb.delete(sb.length() - 1, sb.length());
            return sb.toString();
        }).collect(Collectors.toList());

        List<Partition> partitions = catalog.getClient().getPartitions(
                nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitionNames);
        for (Partition partition : partitions) {
            StorageDescriptor sd = partition.getSd();
            result.put(new PartitionCacheKey(nameMapping, partition.getValues()),
                    new HivePartition(nameMapping, false,
                            sd.getInputFormat(), sd.getLocation(), partition.getValues(),
                            partition.getParameters()));
        }
        return result;
    }

    private FileCacheValue getFileCache(HMSExternalCatalog catalog,
            LocationPath path,
            String inputFormat,
            List<String> partitionValues,
            DirectoryLister directoryLister,
            TableIf table) throws UserException {
        FileCacheValue result = new FileCacheValue();

        FileSystemCache.FileSystemCacheKey fileSystemCacheKey = new FileSystemCache.FileSystemCacheKey(
                path.getFsIdentifier(), path.getStorageProperties());
        RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache()
                .getRemoteFileSystem(fileSystemCacheKey);
        result.setSplittable(HiveUtil.isSplittable(fs, inputFormat, path.getNormalizedLocation()));

        boolean isRecursiveDirectories = Boolean.valueOf(
                catalog.getProperties().getOrDefault("hive.recursive_directories", "true"));
        try {
            RemoteIterator<RemoteFile> iterator = directoryLister.listFiles(fs, isRecursiveDirectories,
                    table, path.getNormalizedLocation());
            while (iterator.hasNext()) {
                RemoteFile remoteFile = iterator.next();
                String srcPath = remoteFile.getPath().toString();
                LocationPath locationPath = LocationPath.of(srcPath, path.getStorageProperties());
                result.addFile(remoteFile, locationPath);
            }
        } catch (FileSystemIOException e) {
            if (e.getErrorCode().isPresent() && e.getErrorCode().get().equals(ErrCode.NOT_FOUND)) {
                LOG.warn("File {} not exist.", path.getNormalizedLocation());
                if (!Boolean.valueOf(catalog.getProperties()
                        .getOrDefault("hive.ignore_absent_partitions", "true"))) {
                    throw new UserException("Partition location does not exist: " + path.getNormalizedLocation());
                }
            } else {
                throw new RuntimeException(e);
            }
        }

        result.setPartitionValues(Lists.newArrayList(partitionValues));
        return result;
    }

    private FileCacheValue loadFiles(FileCacheKey key, DirectoryLister directoryLister, TableIf table) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        HMSExternalCatalog catalog = hmsCatalog(key.catalogId);
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            LocationPath finalLocation = LocationPath.of(
                    key.getLocation(), catalog.getCatalogProperty().getStoragePropertiesMap());
            try {
                FileCacheValue result = getFileCache(catalog, finalLocation, key.inputFormat,
                        key.getPartitionValues(), directoryLister, table);
                // Replace default hive partition with null to distinguish it from a literal "\N".
                for (int i = 0; i < result.getValuesSize(); i++) {
                    if (HIVE_DEFAULT_PARTITION.equals(result.getPartitionValues().get(i))) {
                        result.getPartitionValues().set(i, null);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("load #{} splits for {} in catalog {}",
                            result.getFiles().size(), key, catalog.getName());
                }
                return result;
            } catch (Exception e) {
                throw new CacheException("failed to get input splits for %s in catalog %s",
                        e, key, catalog.getName());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public HivePartitionValues getPartitionValues(ExternalTable dorisTable, List<Type> types) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dorisTable.getOrBuildNameMapping(), types);
        return getPartitionValues(key);
    }

    @VisibleForTesting
    public HivePartitionValues getPartitionValues(PartitionValueCacheKey key) {
        return partitionValuesEntry.get(key.nameMapping.getCtlId()).get(key);
    }

    public List<FileCacheValue> getFilesByPartitions(List<HivePartition> partitions,
            boolean withCache,
            boolean concurrent,
            DirectoryLister directoryLister,
            TableIf table) {
        long start = System.currentTimeMillis();
        if (partitions.isEmpty()) {
            return Lists.newArrayList();
        }

        HivePartition firstPartition = partitions.get(0);
        long catalogId = firstPartition.getNameMapping().getCtlId();
        long fileId = Util.genIdByName(firstPartition.getNameMapping().getLocalDbName(),
                firstPartition.getNameMapping().getLocalTblName());
        List<FileCacheKey> keys = partitions.stream().map(p -> p.isDummyPartition()
                ? FileCacheKey.createDummyCacheKey(catalogId, fileId, p.getPath(), p.getInputFormat())
                : new FileCacheKey(catalogId, fileId, p.getPath(), p.getInputFormat(), p.getPartitionValues()))
                .collect(Collectors.toList());

        List<FileCacheValue> fileLists;
        try {
            if (withCache) {
                MetaCacheEntry<FileCacheKey, FileCacheValue> fileEntry = this.fileEntry.get(catalogId);
                fileLists = keys.stream().map(fileEntry::get).collect(Collectors.toList());
            } else if (concurrent) {
                List<Future<FileCacheValue>> futures = keys.stream().map(
                                key -> fileListingExecutor.submit(() -> loadFiles(key, directoryLister, table)))
                        .collect(Collectors.toList());
                fileLists = Lists.newArrayListWithExpectedSize(keys.size());
                for (Future<FileCacheValue> future : futures) {
                    fileLists.add(future.get());
                }
            } else {
                fileLists = keys.stream()
                        .map(key -> loadFiles(key, directoryLister, table))
                        .collect(Collectors.toList());
            }
        } catch (ExecutionException e) {
            throw new CacheException("failed to get files from partitions in catalog %s",
                    e, hmsCatalog(catalogId).getName());
        } catch (InterruptedException e) {
            throw new CacheException("failed to get files from partitions in catalog %s with interrupted exception",
                    e, hmsCatalog(catalogId).getName());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} files from #{} partitions in catalog {} cost: {} ms",
                    fileLists.stream().mapToInt(l -> l.getFiles() == null ? 0 : l.getFiles().size()).sum(),
                    partitions.size(), hmsCatalog(catalogId).getName(), (System.currentTimeMillis() - start));
        }
        return fileLists;
    }

    public HivePartition getHivePartition(ExternalTable dorisTable, List<String> partitionValues) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return partitionEntry.get(nameMapping.getCtlId()).get(new PartitionCacheKey(nameMapping, partitionValues));
    }

    public List<HivePartition> getAllPartitionsWithCache(ExternalTable dorisTable,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dorisTable, partitionValuesList, true);
    }

    public List<HivePartition> getAllPartitionsWithoutCache(ExternalTable dorisTable,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dorisTable, partitionValuesList, false);
    }

    private List<HivePartition> getAllPartitions(ExternalTable dorisTable,
            List<List<String>> partitionValuesList,
            boolean withCache) {
        long start = System.currentTimeMillis();
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        long catalogId = nameMapping.getCtlId();
        List<PartitionCacheKey> keys = partitionValuesList.stream()
                .map(p -> new PartitionCacheKey(nameMapping, p))
                .collect(Collectors.toList());

        List<HivePartition> partitions;
        if (withCache) {
            MetaCacheEntry<PartitionCacheKey, HivePartition> partitionEntry = this.partitionEntry.get(catalogId);
            partitions = keys.stream().map(partitionEntry::get).collect(Collectors.toList());
        } else {
            partitions = new ArrayList<>(loadPartitions(keys).values());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} partitions in catalog {} cost: {} ms", partitions.size(),
                    hmsCatalog(catalogId).getName(), (System.currentTimeMillis() - start));
        }
        return partitions;
    }

    public void invalidateTableCache(NameMapping nameMapping) {
        long catalogId = nameMapping.getCtlId();

        MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> partitionValuesEntry =
                partitionValuesEntryIfInitialized(catalogId);
        if (partitionValuesEntry != null) {
            partitionValuesEntry.invalidateKey(new PartitionValueCacheKey(nameMapping, null));
        }

        MetaCacheEntry<PartitionCacheKey, HivePartition> partitionEntry = partitionEntryIfInitialized(catalogId);
        if (partitionEntry != null) {
            partitionEntry.invalidateIf(k -> k.isSameTable(
                    nameMapping.getLocalDbName(), nameMapping.getLocalTblName()));
        }

        MetaCacheEntry<FileCacheKey, FileCacheValue> fileEntry = fileEntryIfInitialized(catalogId);
        if (fileEntry != null) {
            long tableId = Util.genIdByName(nameMapping.getLocalDbName(), nameMapping.getLocalTblName());
            fileEntry.invalidateIf(k -> k.isSameTable(tableId));
            advanceFileCacheInvalidationGeneration(catalogId);
        }
    }

    public void invalidatePartitionCache(ExternalTable dorisTable, String partitionName) {
        invalidatePartitionCache(dorisTable.getOrBuildNameMapping(), partitionName);
    }

    public void invalidatePartitionCache(NameMapping nameMapping, String partitionName) {
        partitionCacheCoordinator.invalidatePartitionCache(nameMapping, partitionName);
    }

    /**
     * Selectively refreshes cache for affected partitions based on update information from BE.
     */
    public void refreshAffectedPartitions(HMSExternalTable table,
            List<org.apache.doris.thrift.THivePartitionUpdate> partitionUpdates,
            List<String> modifiedPartNames,
            List<String> newPartNames) {
        partitionCacheCoordinator.refreshAffectedPartitions(table, partitionUpdates, modifiedPartNames, newPartNames);
    }

    public void refreshAffectedPartitionsCache(HMSExternalTable table,
            List<String> modifiedPartNames,
            List<String> newPartNames) {
        partitionCacheCoordinator.refreshAffectedPartitionsCache(table, modifiedPartNames, newPartNames);
    }

    public void addPartitionsCache(NameMapping nameMapping,
            List<String> partitionNames,
            List<Type> partitionColumnTypes) {
        partitionCacheCoordinator.addPartitionsCache(nameMapping, partitionNames, partitionColumnTypes);
    }

    public void dropPartitionsCache(ExternalTable dorisTable,
            List<String> partitionNames,
            boolean invalidPartitionCache) {
        partitionCacheCoordinator.dropPartitionsCache(dorisTable, partitionNames, invalidPartitionCache);
    }

    private final class PartitionCacheCoordinator {
        private void invalidatePartitionCache(NameMapping nameMapping, String partitionName) {
            long catalogId = nameMapping.getCtlId();

            MetaCacheEntry<PartitionCacheKey, HivePartition> partitionEntry = partitionEntryIfInitialized(catalogId);
            MetaCacheEntry<FileCacheKey, FileCacheValue> fileEntry = fileEntryIfInitialized(catalogId);
            if (partitionEntry == null || fileEntry == null) {
                return;
            }

            long tableId = Util.genIdByName(nameMapping.getLocalDbName(), nameMapping.getLocalTblName());
            // Derive the partition values directly from the partition name (the partition-values cache is
            // populated the same way, via HiveUtil.toPartitionValues) instead of reading them from that
            // cache, so file cache invalidation still runs when the table's partition-values cache entry has
            // been evicted while its file listings are still cached.
            List<String> values = HiveUtil.toPartitionValues(partitionName);

            PartitionCacheKey partKey = new PartitionCacheKey(nameMapping, values);
            HivePartition partition = partitionEntry.peekIfPresent(partKey);
            if (partition == null) {
                // Partition metadata cache miss: the exact FileCacheKey cannot be rebuilt here because it
                // needs the partition path and input format carried by HivePartition. Invalidate this
                // table's cached file listings for the partition by (table id + partition values). Scoping
                // by table id is intentional: matching partition values alone would also drop other tables'
                // listings that merely share the same partition value names (e.g. dt=...) at a different
                // location, forcing needless re-listing. The exact-key path below (taken when the partition
                // is cached) already clears a listing regardless of which table id populated it.
                fileEntry.invalidateIf(k -> k.isSameTable(tableId) && Objects.equals(k.getPartitionValues(), values));
                advanceFileCacheInvalidationGeneration(catalogId);
                partitionEntry.invalidateKey(partKey);
                return;
            }

            fileEntry.invalidateKey(new FileCacheKey(nameMapping.getCtlId(), tableId, partition.getPath(),
                    null, partition.getPartitionValues()));
            advanceFileCacheInvalidationGeneration(catalogId);
            partitionEntry.invalidateKey(partKey);
        }

        private void refreshAffectedPartitions(HMSExternalTable table,
                List<org.apache.doris.thrift.THivePartitionUpdate> partitionUpdates,
                List<String> modifiedPartNames,
                List<String> newPartNames) {
            if (partitionUpdates == null || partitionUpdates.isEmpty()) {
                return;
            }

            for (org.apache.doris.thrift.THivePartitionUpdate update : partitionUpdates) {
                String partitionName = update.getName();
                if (Strings.isNullOrEmpty(partitionName)) {
                    continue;
                }

                switch (update.getUpdateMode()) {
                    case APPEND:
                    case OVERWRITE:
                        modifiedPartNames.add(partitionName);
                        break;
                    case NEW:
                        newPartNames.add(partitionName);
                        break;
                    default:
                        LOG.warn("Unknown update mode {} for partition {}",
                                update.getUpdateMode(), partitionName);
                        break;
                }
            }

            refreshAffectedPartitionsCache(table, modifiedPartNames, newPartNames);
        }

        private void refreshAffectedPartitionsCache(HMSExternalTable table,
                List<String> modifiedPartNames,
                List<String> newPartNames) {
            for (String partitionName : modifiedPartNames) {
                invalidatePartitionCache(table.getOrBuildNameMapping(), partitionName);
            }

            List<String> mergedPartNames = Lists.newArrayList(modifiedPartNames);
            mergedPartNames.addAll(newPartNames);
            if (!mergedPartNames.isEmpty()) {
                addPartitionsCache(table.getOrBuildNameMapping(), mergedPartNames,
                        table.getPartitionColumnTypes(java.util.Optional.empty()));
            }

            LOG.info("Refreshed cache for table {}: {} modified partitions, {} new partitions",
                    table.getName(), modifiedPartNames.size(), newPartNames.size());
        }

        private void addPartitionsCache(NameMapping nameMapping,
                List<String> partitionNames,
                List<Type> partitionColumnTypes) {
            long catalogId = nameMapping.getCtlId();
            MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> partitionValuesEntry =
                    partitionValuesEntryIfInitialized(catalogId);
            if (partitionValuesEntry == null) {
                return;
            }

            PartitionValueCacheKey key = new PartitionValueCacheKey(nameMapping, partitionColumnTypes);
            HMSExternalCatalog catalog = hmsCatalog(catalogId);
            String localDbName = nameMapping.getLocalDbName();
            String localTblName = nameMapping.getLocalTblName();
            for (int attempt = 0; attempt < PARTITION_EVENT_REPLACE_MAX_RETRIES; attempt++) {
                HivePartitionValues current = partitionValuesEntry.peekIfPresent(key);
                if (current == null) {
                    // Fence a concurrent miss load that may have read HMS before this event.
                    partitionValuesEntry.invalidateKey(key);
                    return;
                }

                HivePartitionValues copy = current.mutableCopy();
                Map<Long, PartitionItem> allItems = copy.getIdToPartitionItem();
                Map<String, Long> allNames = copy.getPartitionNameToIdMap();
                Map<Long, PartitionItem> addedItems = new HashMap<>();
                for (String partitionName : partitionNames) {
                    if (allNames.containsKey(partitionName)) {
                        if (attempt == 0) {
                            LOG.info("addPartitionsCache partitionName:[{}] has exist in table:[{}]",
                                    partitionName, localTblName);
                        }
                        continue;
                    }
                    long partitionId = Util.genIdByName(
                            catalog.getName(), localDbName, localTblName, partitionName);
                    ListPartitionItem item = toListPartitionItem(partitionName, key.types, catalog.getName());
                    allItems.put(partitionId, item);
                    addedItems.put(partitionId, item);
                    allNames.put(partitionName, partitionId);
                    copy.addPartitionNamePayload(partitionName);
                }
                if (addedItems.isEmpty()) {
                    // Even a replay/no-op event must fence a refresh that started before the event.
                    // Otherwise that refresh could replace this already-correct graph with stale HMS data.
                    if (partitionValuesEntry.fenceInFlightLoadIfSame(key, current)) {
                        return;
                    }
                    continue;
                }
                copy.getPartitionValuesMap().putAll(
                        ListPartitionPrunerV2.getPartitionValuesMap(addedItems));
                preparePartitionValuesForPublication(copy);

                MetaCacheEntry.ReplaceResult result = partitionValuesEntry.tryReplace(key, current, copy);
                if (result == MetaCacheEntry.ReplaceResult.REPLACED
                        || result == MetaCacheEntry.ReplaceResult.DISABLED) {
                    return;
                }
                if (result == MetaCacheEntry.ReplaceResult.REJECTED
                        && partitionValuesEntry.invalidateKeyIfSame(key, current)) {
                    LOG.warn("Invalidated stale partition-values cache after add event was rejected: {}", key);
                    return;
                }
            }
            // Repeated conflicts mean we cannot prove the cached graph contains this event. Force
            // the next reader to rebuild it from HMS rather than retaining a possibly stale value.
            partitionValuesEntry.invalidateKey(key);
            LOG.warn("Invalidated partition-values cache after repeated add-event conflicts: {}", key);
        }

        private void dropPartitionsCache(ExternalTable dorisTable,
                List<String> partitionNames,
                boolean invalidPartitionCache) {
            NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
            long catalogId = nameMapping.getCtlId();

            MetaCacheEntry<PartitionValueCacheKey, HivePartitionValues> partitionValuesEntry =
                    partitionValuesEntryIfInitialized(catalogId);
            if (partitionValuesEntry == null) {
                return;
            }

            PartitionValueCacheKey key = new PartitionValueCacheKey(nameMapping, null);
            if (invalidPartitionCache) {
                for (String partitionName : partitionNames) {
                    invalidatePartitionCache(nameMapping, partitionName);
                }
            }

            for (int attempt = 0; attempt < PARTITION_EVENT_REPLACE_MAX_RETRIES; attempt++) {
                HivePartitionValues current = partitionValuesEntry.peekIfPresent(key);
                if (current == null) {
                    // Fence a concurrent miss load that may have read HMS before this event.
                    partitionValuesEntry.invalidateKey(key);
                    return;
                }
                HivePartitionValues copy = current.mutableCopy();
                Map<String, Long> allNames = copy.getPartitionNameToIdMap();
                Map<Long, PartitionItem> allItems = copy.getIdToPartitionItem();
                Map<Long, List<String>> allValues = copy.getPartitionValuesMap();
                boolean changed = false;
                for (String partitionName : partitionNames) {
                    Long partitionId = allNames.remove(partitionName);
                    if (partitionId == null) {
                        LOG.info("dropPartitionsCache partitionName:[{}] not exist in table:[{}]",
                                partitionName, nameMapping.getFullLocalName());
                        continue;
                    }
                    allItems.remove(partitionId);
                    allValues.remove(partitionId);
                    copy.removePartitionNamePayload(partitionName);
                    changed = true;
                }
                if (!changed) {
                    // See the add-event no-op path: event ordering still has to win over an older refresh.
                    if (partitionValuesEntry.fenceInFlightLoadIfSame(key, current)) {
                        return;
                    }
                    continue;
                }
                preparePartitionValuesForPublication(copy);
                MetaCacheEntry.ReplaceResult result = partitionValuesEntry.tryReplace(key, current, copy);
                if (result == MetaCacheEntry.ReplaceResult.REPLACED
                        || result == MetaCacheEntry.ReplaceResult.DISABLED) {
                    return;
                }
                if (result == MetaCacheEntry.ReplaceResult.REJECTED
                        && partitionValuesEntry.invalidateKeyIfSame(key, current)) {
                    LOG.warn("Invalidated stale partition-values cache after drop event was rejected: {}", key);
                    return;
                }
            }
            partitionValuesEntry.invalidateKey(key);
            LOG.warn("Invalidated partition-values cache after repeated drop-event conflicts: {}", key);
        }
    }

    @VisibleForTesting
    public void putPartitionValuesCacheForTest(PartitionValueCacheKey key, HivePartitionValues values) {
        preparePartitionValuesForPublication(values);
        partitionValuesEntry.get(key.getNameMapping().getCtlId()).put(key, values);
    }

    public List<FileCacheValue> getFilesByTransaction(List<HivePartition> partitions,
            Map<String, String> txnValidIds,
            boolean isFullAcid,
            String bindBrokerName) {
        List<FileCacheValue> fileCacheValues = Lists.newArrayList();
        try {
            if (partitions.isEmpty()) {
                return fileCacheValues;
            }
            for (HivePartition partition : partitions) {
                HMSExternalCatalog catalog = hmsCatalog(partition.getNameMapping().getCtlId());
                LocationPath locationPath = LocationPath.of(partition.getPath(),
                        catalog.getCatalogProperty().getStoragePropertiesMap());
                RemoteFileSystem fileSystem = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache()
                        .getRemoteFileSystem(new FileSystemCache.FileSystemCacheKey(
                                locationPath.getNormalizedLocation(),
                                locationPath.getStorageProperties()));
                AuthenticationConfig authenticationConfig = AuthenticationConfig
                        .getKerberosConfig(locationPath.getStorageProperties().getBackendConfigProperties());
                HadoopAuthenticator hadoopAuthenticator =
                        HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);

                fileCacheValues.add(
                        hadoopAuthenticator.doAs(() -> AcidUtil.getAcidState(
                                fileSystem,
                                partition,
                                txnValidIds,
                                catalog.getCatalogProperty().getStoragePropertiesMap(),
                                isFullAcid)));
            }
        } catch (Exception e) {
            throw new CacheException("Failed to get input splits %s", e, txnValidIds.toString());
        }
        return fileCacheValues;
    }

    /**
     * The key of hive partition values cache.
     */
    @Getter
    public static class PartitionValueCacheKey {
        private final NameMapping nameMapping;
        // Not part of cache identity.
        private final List<Type> types;

        public PartitionValueCacheKey(NameMapping nameMapping, List<Type> types) {
            this.nameMapping = nameMapping;
            this.types = types == null ? null : ImmutableList.copyOf(types);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PartitionValueCacheKey)) {
                return false;
            }
            return nameMapping.equals(((PartitionValueCacheKey) obj).nameMapping);
        }

        @Override
        public int hashCode() {
            return nameMapping.hashCode();
        }

        @Override
        public String toString() {
            return "PartitionValueCacheKey{" + "dbName='" + nameMapping.getLocalDbName() + '\''
                    + ", tblName='" + nameMapping.getLocalTblName() + '\'' + '}';
        }
    }

    @Data
    public static class PartitionCacheKey {
        private NameMapping nameMapping;
        private List<String> values;

        public PartitionCacheKey(NameMapping nameMapping, List<String> values) {
            this.nameMapping = nameMapping;
            this.values = values;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PartitionCacheKey)) {
                return false;
            }
            return nameMapping.equals(((PartitionCacheKey) obj).nameMapping)
                    && Objects.equals(values, ((PartitionCacheKey) obj).values);
        }

        boolean isSameTable(String dbName, String tblName) {
            return this.nameMapping.getLocalDbName().equals(dbName)
                    && this.nameMapping.getLocalTblName().equals(tblName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nameMapping, values);
        }

        @Override
        public String toString() {
            return "PartitionCacheKey{" + "dbName='" + nameMapping.getLocalDbName() + '\''
                    + ", tblName='" + nameMapping.getLocalTblName() + '\'' + ", values=" + values + '}';
        }
    }

    @Data
    public static class FileCacheKey {
        private long dummyKey = 0;
        private long catalogId;
        private String location;
        // Not part of cache identity.
        private String inputFormat;
        // The values of partitions.
        protected List<String> partitionValues;
        private long id;

        public FileCacheKey(long catalogId, long id, String location, String inputFormat,
                List<String> partitionValues) {
            this.catalogId = catalogId;
            this.location = location;
            this.inputFormat = inputFormat;
            this.partitionValues = partitionValues == null ? Lists.newArrayList() : partitionValues;
            this.id = id;
        }

        public static FileCacheKey createDummyCacheKey(long catalogId, long id, String location,
                String inputFormat) {
            FileCacheKey fileCacheKey = new FileCacheKey(catalogId, id, location, inputFormat, null);
            fileCacheKey.dummyKey = Objects.hash(catalogId, id);
            return fileCacheKey;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileCacheKey)) {
                return false;
            }
            if (dummyKey != 0) {
                return dummyKey == ((FileCacheKey) obj).dummyKey;
            }
            return catalogId == ((FileCacheKey) obj).catalogId
                    && location.equals(((FileCacheKey) obj).location)
                    && Objects.equals(partitionValues, ((FileCacheKey) obj).partitionValues);
        }

        boolean isSameTable(long id) {
            return this.id == id;
        }

        @Override
        public int hashCode() {
            if (dummyKey != 0) {
                return Objects.hash(dummyKey);
            }
            return Objects.hash(catalogId, location, partitionValues);
        }

        @Override
        public String toString() {
            return "FileCacheKey{" + "catalogId=" + catalogId + ", location='" + location + '\''
                    + ", inputFormat='" + inputFormat + '\'' + '}';
        }
    }

    @Data
    public static class FileCacheValue {
        private final List<HiveFileStatus> files = Lists.newArrayList();
        // Monotonic per-catalog load counter stamped by loadFileCacheValue. Statement scan-reuse
        // keys include this value so an automatic refresh, TTL expiry reload, or capacity-eviction
        // reload of the global entry produces a different key and cannot be reused by an earlier
        // alias in the same statement.
        private long cacheGeneration;
        private boolean isSplittable;
        protected List<String> partitionValues;
        private AcidInfo acidInfo;

        public void addFile(RemoteFile file, LocationPath locationPath) {
            if (isFileVisible(file.getPath())) {
                HiveFileStatus status = new HiveFileStatus();
                status.setBlockLocations(file.getBlockLocations());
                status.setPath(locationPath);
                status.length = file.getSize();
                status.blockSize = file.getBlockSize();
                status.modificationTime = file.getModificationTime();
                files.add(status);
            }
        }

        public int getValuesSize() {
            return partitionValues == null ? 0 : partitionValues.size();
        }

        @VisibleForTesting
        public static boolean isFileVisible(Path path) {
            if (path == null) {
                return false;
            }
            String pathStr = path.toUri().toString();
            if (containsHiddenPath(pathStr)) {
                return false;
            }
            return true;
        }

        private static boolean containsHiddenPath(String path) {
            if (path.startsWith(".") || path.startsWith("_")) {
                return true;
            }
            for (int i = 0; i < path.length() - 1; i++) {
                if (path.charAt(i) == '/' && (path.charAt(i + 1) == '.' || path.charAt(i + 1) == '_')) {
                    return true;
                }
            }
            return false;
        }
    }

    @Data
    public static class HiveFileStatus {
        BlockLocation[] blockLocations;
        LocationPath path;
        long length;
        long blockSize;
        long modificationTime;
        boolean splittable;
        List<String> partitionValues;
        AcidInfo acidInfo;
    }

    @Getter
    public static class HivePartitionValues {
        private BiMap<String, Long> partitionNameToIdMap;
        private Map<Long, PartitionItem> idToPartitionItem;
        private Map<Long, List<String>> partitionValuesMap;

        // Sorted partition ranges for binary search filtering.
        private SortedPartitionRanges<String> sortedPartitionRanges;
        // Prepared once after construction/update; the cache weigher only reads this value.
        private transient volatile MetaCacheSizeEstimate sizeEstimate;
        // Maintained while the metadata is already being loaded or updated. Admission only reads it.
        private long partitionNamePayloadBytes;
        private int partitionColumnCount;
        private transient boolean sortedPartitionRangesPrepared;

        public HivePartitionValues() {
        }

        public HivePartitionValues(Map<Long, PartitionItem> idToPartitionItem,
                BiMap<String, Long> partitionNameToIdMap,
                Map<Long, List<String>> partitionValuesMap) {
            this(idToPartitionItem, partitionNameToIdMap, partitionValuesMap,
                    countPartitionNamePayloadBytes(partitionNameToIdMap),
                    inferPartitionColumnCount(partitionValuesMap));
        }

        HivePartitionValues(Map<Long, PartitionItem> idToPartitionItem,
                BiMap<String, Long> partitionNameToIdMap,
                Map<Long, List<String>> partitionValuesMap,
                long partitionNamePayloadBytes,
                int partitionColumnCount) {
            this.idToPartitionItem = idToPartitionItem;
            this.partitionNameToIdMap = partitionNameToIdMap;
            this.partitionValuesMap = partitionValuesMap;
            this.partitionNamePayloadBytes = partitionNamePayloadBytes;
            this.partitionColumnCount = partitionColumnCount;
        }

        HivePartitionValues mutableCopy() {
            HivePartitionValues copy = new HivePartitionValues();
            copy.partitionNameToIdMap = partitionNameToIdMap == null ? null : HashBiMap.create(partitionNameToIdMap);
            copy.idToPartitionItem = idToPartitionItem == null ? null : Maps.newHashMap(idToPartitionItem);
            copy.partitionValuesMap = partitionValuesMap == null ? null : Maps.newHashMap(partitionValuesMap);
            copy.partitionNamePayloadBytes = partitionNamePayloadBytes;
            copy.partitionColumnCount = partitionColumnCount;
            return copy;
        }

        /** Compatibility hook for tests and benchmarks; publication uses copy-on-write updates. */
        void sealForPublication() {
            if (!sortedPartitionRangesPrepared) {
                rebuildSortedPartitionRangesForPublication();
            }
        }

        void rebuildSortedPartitionRangesForPublication() {
            sortedPartitionRanges = buildSortedPartitionRanges();
            sortedPartitionRangesPrepared = true;
        }

        MetaCacheSizeEstimate prepareForCachePublication(PartitionValueCacheKey key) {
            if (sizeEstimate == null) {
                sizeEstimate = MetaCacheSizeEstimator.estimateSafely(
                        "hive_partition_values_preparation_failed", () -> {
                            prepareSizeEstimate(key);
                            return getSizeEstimate();
                        });
            }
            return sizeEstimate;
        }

        public MetaCacheSizeEstimate getSizeEstimate() {
            MetaCacheSizeEstimate result = sizeEstimate;
            return result == null ? MetaCacheSizeEstimate.incomplete("estimate_not_prepared") : result;
        }

        void prepareSizeEstimate(PartitionValueCacheKey key) {
            sizeEstimate = HiveCacheSizeEstimator.estimatePartitionValuesEntry(key, this);
        }

        long getPartitionNamePayloadBytes() {
            return partitionNamePayloadBytes;
        }

        int getPartitionColumnCount() {
            return partitionColumnCount;
        }

        private void addPartitionNamePayload(String partitionName) {
            partitionNamePayloadBytes = MetaCacheWeightUtils.saturatedAdd(
                    partitionNamePayloadBytes,
                    MetaCacheWeightUtils.estimatedStringPayloadBytes(partitionName));
        }

        private void removePartitionNamePayload(String partitionName) {
            partitionNamePayloadBytes = Math.max(0L, partitionNamePayloadBytes
                    - MetaCacheWeightUtils.estimatedStringPayloadBytes(partitionName));
        }

        private static long countPartitionNamePayloadBytes(BiMap<String, Long> names) {
            long payloadBytes = 0L;
            if (names != null) {
                for (String name : names.keySet()) {
                    payloadBytes = MetaCacheWeightUtils.saturatedAdd(
                            payloadBytes, MetaCacheWeightUtils.estimatedStringPayloadBytes(name));
                }
            }
            return payloadBytes;
        }

        private static int inferPartitionColumnCount(Map<Long, List<String>> values) {
            if (values == null || values.isEmpty()) {
                return 0;
            }
            List<String> first = values.values().iterator().next();
            return first == null ? 0 : first.size();
        }

        public java.util.Optional<SortedPartitionRanges<String>> getSortedPartitionRanges() {
            return java.util.Optional.ofNullable(sortedPartitionRanges);
        }

        private SortedPartitionRanges<String> buildSortedPartitionRanges() {
            if (partitionNameToIdMap == null || partitionNameToIdMap.isEmpty()
                    || idToPartitionItem == null || idToPartitionItem.isEmpty()) {
                return null;
            }

            BiMap<Long, String> idToName = partitionNameToIdMap.inverse();
            Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMapWithExpectedSize(idToPartitionItem.size());
            for (Map.Entry<Long, PartitionItem> entry : idToPartitionItem.entrySet()) {
                String partitionName = idToName.get(entry.getKey());
                if (partitionName != null) {
                    nameToPartitionItem.put(partitionName, entry.getValue());
                }
            }

            return SortedPartitionRanges.build(nameToPartitionItem);
        }
    }
}
