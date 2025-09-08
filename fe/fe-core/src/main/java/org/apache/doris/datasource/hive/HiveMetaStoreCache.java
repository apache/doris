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
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.util.CacheBulkLoader;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.FileSystemDirectoryLister;
import org.apache.doris.fs.FileSystemIOException;
import org.apache.doris.fs.RemoteIterator;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPrunerV2Base.UniqueId;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeRangeMap;
import lombok.Data;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

// The cache of a hms catalog. 3 kind of caches:
// 1. partitionValuesCache: cache the partition values of a table, for partition prune.
// 2. partitionCache: cache the partition info(location, input format, etc.) of a table.
// 3. fileCache: cache the files of a location.
public class HiveMetaStoreCache {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreCache.class);
    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private final HMSExternalCatalog catalog;
    private JobConf jobConf;
    private final ExecutorService refreshExecutor;
    private final ExecutorService fileListingExecutor;

    // cache from <dbname-tblname> -> <values of partitions>
    private LoadingCache<PartitionValueCacheKey, HivePartitionValues> partitionValuesCache;
    // cache from <dbname-tblname-partition_values> -> <partition info>
    private LoadingCache<PartitionCacheKey, HivePartition> partitionCache;
    // the ref of cache from <location> -> <file list>
    // Other thread may reset this cache, so use AtomicReference to wrap it.
    private volatile AtomicReference<LoadingCache<FileCacheKey, FileCacheValue>> fileCacheRef
            = new AtomicReference<>();

    public HiveMetaStoreCache(HMSExternalCatalog catalog,
            ExecutorService refreshExecutor, ExecutorService fileListingExecutor) {
        this.catalog = catalog;
        this.refreshExecutor = refreshExecutor;
        this.fileListingExecutor = fileListingExecutor;
        init();
        initMetrics();
    }

    /**
     * Because the partitionValuesCache|partitionCache|fileCache use the same executor for batch loading,
     * we need to be very careful and try to avoid the circular dependency of these tasks
     * which will bring out thread deadlock.
     **/
    public void init() {
        long partitionCacheTtlSecond = NumberUtils.toLong(
                (catalog.getProperties().get(HMSExternalCatalog.PARTITION_CACHE_TTL_SECOND)),
                ExternalCatalog.CACHE_NO_TTL);

        CacheFactory partitionValuesCacheFactory = new CacheFactory(
                OptionalLong.of(partitionCacheTtlSecond >= ExternalCatalog.CACHE_TTL_DISABLE_CACHE
                        ? partitionCacheTtlSecond : Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60L),
                Config.max_hive_partition_table_cache_num,
                true,
                null);
        partitionValuesCache = partitionValuesCacheFactory.buildCache(this::loadPartitionValues, null,
                refreshExecutor);

        CacheFactory partitionCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.empty(),
                Config.max_hive_partition_cache_num,
                true,
                null);
        partitionCache = partitionCacheFactory.buildCache(new CacheLoader<PartitionCacheKey, HivePartition>() {
            @Override
            public HivePartition load(PartitionCacheKey key) {
                return loadPartition(key);
            }

            @Override
            public Map<PartitionCacheKey, HivePartition> loadAll(Iterable<? extends PartitionCacheKey> keys) {
                return loadPartitions(keys);
            }
        }, null, refreshExecutor);

        setNewFileCache();
    }

    /***
     * generate a filecache and set to fileCacheRef
     */
    private void setNewFileCache() {
        // if the file.meta.cache.ttl-second is equal or greater than 0, the cache expired will be set to that value
        int fileMetaCacheTtlSecond = NumberUtils.toInt(
                (catalog.getProperties().get(HMSExternalCatalog.FILE_META_CACHE_TTL_SECOND)),
                ExternalCatalog.CACHE_NO_TTL);

        CacheFactory fileCacheFactory = new CacheFactory(
                OptionalLong.of(fileMetaCacheTtlSecond >= ExternalCatalog.CACHE_TTL_DISABLE_CACHE
                        ? fileMetaCacheTtlSecond : Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60L),
                Config.max_external_file_cache_num,
                true,
                null);

        CacheLoader<FileCacheKey, FileCacheValue> loader = new CacheBulkLoader<FileCacheKey, FileCacheValue>() {
            @Override
            protected ExecutorService getExecutor() {
                return HiveMetaStoreCache.this.fileListingExecutor;
            }

            @Override
            public FileCacheValue load(FileCacheKey key) {
                return loadFiles(key, new FileSystemDirectoryLister(), null);
            }
        };

        LoadingCache<FileCacheKey, FileCacheValue> oldFileCache = fileCacheRef.get();

        fileCacheRef.set(fileCacheFactory.buildCache(loader, null, this.refreshExecutor));
        if (Objects.nonNull(oldFileCache)) {
            oldFileCache.invalidateAll();
        }
    }

    private void initMetrics() {
        // partition value
        GaugeMetric<Long> valueCacheGauge = new GaugeMetric<Long>("hive_meta_cache",
                Metric.MetricUnit.NOUNIT, "hive partition value cache number") {
            @Override
            public Long getValue() {
                return partitionValuesCache.estimatedSize();
            }
        };
        valueCacheGauge.addLabel(new MetricLabel("type", "partition_value"));
        valueCacheGauge.addLabel(new MetricLabel("catalog", catalog.getName()));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(valueCacheGauge);
        // partition
        GaugeMetric<Long> partitionCacheGauge = new GaugeMetric<Long>("hive_meta_cache",
                Metric.MetricUnit.NOUNIT, "hive partition cache number") {
            @Override
            public Long getValue() {
                return partitionCache.estimatedSize();
            }
        };
        partitionCacheGauge.addLabel(new MetricLabel("type", "partition"));
        partitionCacheGauge.addLabel(new MetricLabel("catalog", catalog.getName()));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(partitionCacheGauge);
        // file
        GaugeMetric<Long> fileCacheGauge = new GaugeMetric<Long>("hive_meta_cache",
                Metric.MetricUnit.NOUNIT, "hive file cache number") {
            @Override
            public Long getValue() {
                return fileCacheRef.get().estimatedSize();
            }
        };
        fileCacheGauge.addLabel(new MetricLabel("type", "file"));
        fileCacheGauge.addLabel(new MetricLabel("catalog", catalog.getName()));
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(fileCacheGauge);
    }

    private HivePartitionValues loadPartitionValues(PartitionValueCacheKey key) {
        // partition name format: nation=cn/city=beijing,`listPartitionNames` returned string is the encoded string.
        NameMapping nameMapping = key.nameMapping;
        List<String> partitionNames = catalog.getClient()
                .listPartitionNames(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName());
        if (LOG.isDebugEnabled()) {
            LOG.debug("load #{} partitions for {} in catalog {}", partitionNames.size(), key, catalog.getName());
        }
        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionNames.size());
        BiMap<String, Long> partitionNameToIdMap = HashBiMap.create(partitionNames.size());
        Map<Long, List<UniqueId>> idToUniqueIdsMap = Maps.newHashMapWithExpectedSize(partitionNames.size());
        for (String partitionName : partitionNames) {
            long partitionId = Util.genIdByName(catalog.getName(), nameMapping.getLocalDbName(),
                    nameMapping.getLocalTblName(), partitionName);
            ListPartitionItem listPartitionItem = toListPartitionItem(partitionName, key.types);
            idToPartitionItem.put(partitionId, listPartitionItem);
            partitionNameToIdMap.put(partitionName, partitionId);
        }

        Map<UniqueId, Range<PartitionKey>> uidToPartitionRange = null;
        Map<Range<PartitionKey>, UniqueId> rangeToId = null;
        RangeMap<ColumnBound, UniqueId> singleColumnRangeMap = null;
        Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap = null;
        if (key.types.size() > 1) {
            // uidToPartitionRange and rangeToId are only used for multi-column partition
            uidToPartitionRange = ListPartitionPrunerV2.genUidToPartitionRange(idToPartitionItem, idToUniqueIdsMap);
            rangeToId = ListPartitionPrunerV2.genRangeToId(uidToPartitionRange);
        } else {
            Preconditions.checkState(key.types.size() == 1, key.types);
            // singleColumnRangeMap is only used for single-column partition
            singleColumnRangeMap = ListPartitionPrunerV2.genSingleColumnRangeMap(idToPartitionItem, idToUniqueIdsMap);
            singleUidToColumnRangeMap = ListPartitionPrunerV2.genSingleUidToColumnRange(singleColumnRangeMap);
        }
        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        return new HivePartitionValues(idToPartitionItem, uidToPartitionRange, rangeToId, singleColumnRangeMap,
                partitionNameToIdMap, idToUniqueIdsMap, singleUidToColumnRangeMap, partitionValuesMap);
    }

    public ListPartitionItem toListPartitionItem(String partitionName, List<Type> types) {
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            values.add(new PartitionValue(partitionValue, HIVE_DEFAULT_PARTITION.equals(partitionValue)));
        }
        try {
            PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
            ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
            return listPartitionItem;
        } catch (AnalysisException e) {
            throw new CacheException("failed to convert hive partition %s to list partition in catalog %s",
                    e, partitionName, catalog.getName());
        }
    }

    private HivePartition loadPartition(PartitionCacheKey key) {
        NameMapping nameMapping = key.nameMapping;
        Partition partition = catalog.getClient()
                .getPartition(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), key.values);
        StorageDescriptor sd = partition.getSd();
        if (LOG.isDebugEnabled()) {
            LOG.debug("load partition format: {}, location: {} for {} in catalog {}",
                    sd.getInputFormat(), sd.getLocation(), key, catalog.getName());
        }
        // TODO: more info?
        return new HivePartition(nameMapping, false, sd.getInputFormat(), sd.getLocation(), key.values,
                partition.getParameters());
    }

    private Map<PartitionCacheKey, HivePartition> loadPartitions(Iterable<? extends PartitionCacheKey> keys) {
        Map<PartitionCacheKey, HivePartition> ret = new HashMap<>();
        if (keys == null || !keys.iterator().hasNext()) {
            return ret;
        }
        // The name mapping is same for all keys, so we can just get one key to get the name mapping.
        PartitionCacheKey oneKey = Iterables.get(keys, 0);
        NameMapping nameMapping = oneKey.nameMapping;
        String localDbName = nameMapping.getLocalDbName();
        String localTblName = nameMapping.getLocalTblName();
        List<Column> partitionColumns = ((HMSExternalTable)
                (catalog.getDbNullable(localDbName).getTableNullable(localTblName))).getPartitionColumns();
        // A partitionName is like "country=China/city=Beijing" or "date=2023-02-01"
        List<String> partitionNames = Streams.stream(keys).map(key -> {
            StringBuilder sb = new StringBuilder();
            Preconditions.checkState(key.getValues().size() == partitionColumns.size());
            for (int i = 0; i < partitionColumns.size(); i++) {
                // Partition name and value  may contain special character, like / and so on. Need to encode.
                sb.append(FileUtils.escapePathName(partitionColumns.get(i).getName()));
                sb.append("=");
                sb.append(FileUtils.escapePathName(key.getValues().get(i)));
                sb.append("/");
            }
            sb.delete(sb.length() - 1, sb.length());
            return sb.toString();
        }).collect(Collectors.toList());


        List<Partition> partitions = catalog.getClient()
                .getPartitions(nameMapping.getRemoteDbName(), nameMapping.getRemoteTblName(), partitionNames);
        // Compose the return result map.
        for (Partition partition : partitions) {
            StorageDescriptor sd = partition.getSd();
            ret.put(new PartitionCacheKey(nameMapping, partition.getValues()),
                    new HivePartition(nameMapping, false,
                            sd.getInputFormat(), sd.getLocation(), partition.getValues(), partition.getParameters()));
        }
        return ret;
    }

    // Get File Status by using FileSystem API.
    private FileCacheValue getFileCache(LocationPath path, String inputFormat,
                                        List<String> partitionValues,
                                        DirectoryLister directoryLister,
                                        TableIf table) throws UserException {
        FileCacheValue result = new FileCacheValue();

        FileSystemCache.FileSystemCacheKey fileSystemCacheKey = new FileSystemCache.FileSystemCacheKey(
                path.getFsIdentifier(), path.getStorageProperties()
        );
        RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache()
                .getRemoteFileSystem(fileSystemCacheKey);
        result.setSplittable(HiveUtil.isSplittable(fs, inputFormat, path.getNormalizedLocation()));
        // For Tez engine, it may generate subdirectoies for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /user/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to recursively list data location.
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        boolean isRecursiveDirectories = Boolean.valueOf(
                catalog.getProperties().getOrDefault("hive.recursive_directories", "true"));
        try {
            RemoteIterator<RemoteFile> iterator = directoryLister.listFiles(fs, isRecursiveDirectories,
                    table, path.getNormalizedLocation());
            while (iterator.hasNext()) {
                RemoteFile remoteFile = iterator.next();
                String srcPath = remoteFile.getPath().toString();
                LocationPath locationPath = LocationPath.of(srcPath, catalog.getCatalogProperty()
                        .getStoragePropertiesMap());
                result.addFile(remoteFile, locationPath);
            }
        } catch (FileSystemIOException e) {
            if (e.getErrorCode().isPresent() && e.getErrorCode().get().equals(ErrCode.NOT_FOUND)) {
                // User may manually remove partition under HDFS, in this case,
                // Hive doesn't aware that the removed partition is missing.
                // Here is to support this case without throw an exception.
                LOG.warn(String.format("File %s not exist.", path.getNormalizedLocation()));
                if (!Boolean.valueOf(catalog.getProperties()
                        .getOrDefault("hive.ignore_absent_partitions", "true"))) {
                    throw new UserException("Partition location does not exist: " + path.getNormalizedLocation());
                }
            } else {
                throw new RuntimeException(e);
            }
        }
        // Must copy the partitionValues to avoid concurrent modification of key and value
        result.setPartitionValues(Lists.newArrayList(partitionValues));
        return result;
    }

    private FileCacheValue loadFiles(FileCacheKey key, DirectoryLister directoryLister, TableIf table) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            LocationPath finalLocation = LocationPath.of(key.getLocation(), catalog.getCatalogProperty()
                    .getStoragePropertiesMap());
            // NOTICE: the setInputPaths has 2 overloads, the 2nd arg should be Path not String
            try {
                FileCacheValue result = getFileCache(finalLocation, key.inputFormat,
                        key.getPartitionValues(), directoryLister, table);
                // Replace default hive partition with a null_string.
                for (int i = 0; i < result.getValuesSize(); i++) {
                    if (HIVE_DEFAULT_PARTITION.equals(result.getPartitionValues().get(i))) {
                        result.getPartitionValues().set(i, FeConstants.null_string);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("load #{} splits for {} in catalog {}", result.getFiles().size(), key, catalog.getName());
                }
                return result;
            } catch (Exception e) {
                throw new CacheException("failed to get input splits for %s in catalog %s", e, key, catalog.getName());
            }
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public HivePartitionValues getPartitionValues(ExternalTable dorisTable, List<Type> types) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dorisTable.getOrBuildNameMapping(), types);
        return getPartitionValues(key);
    }

    public HivePartitionValues getPartitionValues(PartitionValueCacheKey key) {
        return partitionValuesCache.get(key);
    }

    public List<FileCacheValue> getFilesByPartitionsWithCache(List<HivePartition> partitions,
                                                              DirectoryLister directoryLister,
                                                              TableIf table) {
        return getFilesByPartitions(partitions, true, true, directoryLister, table);
    }

    public List<FileCacheValue> getFilesByPartitionsWithoutCache(List<HivePartition> partitions,
                                                                 DirectoryLister directoryLister,
                                                                 TableIf table) {
        return getFilesByPartitions(partitions, false, true, directoryLister, table);
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
        long fileId = Util.genIdByName(firstPartition.getNameMapping().getLocalDbName(),
                firstPartition.getNameMapping().getLocalTblName());
        List<FileCacheKey> keys = partitions.stream().map(p -> p.isDummyPartition()
                        ? FileCacheKey.createDummyCacheKey(
                        fileId, p.getPath(), p.getInputFormat())
                        : new FileCacheKey(fileId, p.getPath(),
                        p.getInputFormat(), p.getPartitionValues()))
                .collect(Collectors.toList());

        List<FileCacheValue> fileLists;
        try {
            if (withCache) {
                fileLists = new ArrayList<>(fileCacheRef.get().getAll(keys).values());
            } else {
                if (concurrent) {
                    List<Future<FileCacheValue>> pList = keys.stream().map(
                                    key -> fileListingExecutor.submit(() -> loadFiles(key, directoryLister, table)))
                            .collect(Collectors.toList());
                    fileLists = Lists.newArrayListWithExpectedSize(keys.size());
                    for (Future<FileCacheValue> p : pList) {
                        fileLists.add(p.get());
                    }
                } else {
                    fileLists = keys.stream().map((key) -> loadFiles(key, directoryLister, table))
                            .collect(Collectors.toList());
                }
            }
        } catch (ExecutionException e) {
            throw new CacheException("failed to get files from partitions in catalog %s",
                    e, catalog.getName());
        } catch (InterruptedException e) {
            throw new CacheException("failed to get files from partitions in catalog %s with interrupted exception",
                    e, catalog.getName());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} files from #{} partitions in catalog {} cost: {} ms",
                    fileLists.stream().mapToInt(l -> l.getFiles() == null ? 0 : l.getFiles().size()).sum(),
                    partitions.size(), catalog.getName(), (System.currentTimeMillis() - start));
        }
        return fileLists;
    }

    public HivePartition getHivePartition(ExternalTable dorisTable, List<String> partitionValues) {
        return partitionCache.get(new PartitionCacheKey(dorisTable.getOrBuildNameMapping(), partitionValues));
    }

    public List<HivePartition> getAllPartitionsWithCache(ExternalTable dorisTable,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dorisTable, partitionValuesList, true);
    }

    public List<HivePartition> getAllPartitionsWithoutCache(ExternalTable dorisTable,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dorisTable, partitionValuesList, false);
    }

    private List<HivePartition> getAllPartitions(ExternalTable dorisTable, List<List<String>> partitionValuesList,
            boolean withCache) {
        long start = System.currentTimeMillis();
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        List<PartitionCacheKey> keys = partitionValuesList.stream()
                .map(p -> new PartitionCacheKey(nameMapping, p))
                .collect(Collectors.toList());

        List<HivePartition> partitions;
        if (withCache) {
            partitions = partitionCache.getAll(keys).values().stream().collect(Collectors.toList());
        } else {
            Map<PartitionCacheKey, HivePartition> map = loadPartitions(keys);
            partitions = map.values().stream().collect(Collectors.toList());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("get #{} partitions in catalog {} cost: {} ms", partitions.size(), catalog.getName(),
                    (System.currentTimeMillis() - start));
        }
        return partitions;
    }

    public void invalidateTableCache(NameMapping nameMapping) {
        partitionValuesCache.invalidate(new PartitionValueCacheKey(nameMapping, null));
        partitionCache.asMap().keySet().forEach(k -> {
            if (k.isSameTable(nameMapping.getLocalDbName(), nameMapping.getLocalTblName())) {
                partitionCache.invalidate(k);
            }
        });
        long id = Util.genIdByName(nameMapping.getLocalDbName(), nameMapping.getLocalTblName());
        LoadingCache<FileCacheKey, FileCacheValue> fileCache = fileCacheRef.get();
        fileCache.asMap().keySet().forEach(k -> {
            if (k.isSameTable(id)) {
                fileCache.invalidate(k);
            }
        });
    }

    public void invalidatePartitionCache(ExternalTable dorisTable, String partitionName) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        long id = Util.genIdByName(nameMapping.getLocalDbName(), nameMapping.getLocalTblName());
        PartitionValueCacheKey key = new PartitionValueCacheKey(nameMapping, null);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues != null) {
            Long partitionId = partitionValues.partitionNameToIdMap.get(partitionName);
            List<String> values = partitionValues.partitionValuesMap.get(partitionId);
            PartitionCacheKey partKey = new PartitionCacheKey(nameMapping, values);
            HivePartition partition = partitionCache.getIfPresent(partKey);
            if (partition != null) {
                fileCacheRef.get().invalidate(new FileCacheKey(id, partition.getPath(),
                        null, partition.getPartitionValues()));
                partitionCache.invalidate(partKey);
            }
        }
    }

    public void invalidateDbCache(String dbName) {
        long start = System.currentTimeMillis();
        Set<PartitionValueCacheKey> keys = partitionValuesCache.asMap().keySet();
        for (PartitionValueCacheKey key : keys) {
            if (key.nameMapping.getLocalDbName().equals(dbName)) {
                invalidateTableCache(key.nameMapping);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid db cache for {} in catalog {}, cache num: {}, cost: {} ms", dbName, catalog.getName(),
                    keys.size(), (System.currentTimeMillis() - start));
        }
    }

    public void invalidateAll() {
        partitionValuesCache.invalidateAll();
        partitionCache.invalidateAll();
        fileCacheRef.get().invalidateAll();
        if (LOG.isDebugEnabled()) {
            LOG.debug("invalid all meta cache in catalog {}", catalog.getName());
        }
    }

    // partition name format: nation=cn/city=beijing
    public void addPartitionsCache(NameMapping nameMapping, List<String> partitionNames,
            List<Type> partitionColumnTypes) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(nameMapping, partitionColumnTypes);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues == null) {
            return;
        }
        HivePartitionValues copy = partitionValues.copy();
        Map<Long, PartitionItem> idToPartitionItemBefore = copy.getIdToPartitionItem();
        Map<String, Long> partitionNameToIdMapBefore = copy.getPartitionNameToIdMap();
        Map<Long, List<UniqueId>> idToUniqueIdsMap = copy.getIdToUniqueIdsMap();
        Map<Long, PartitionItem> idToPartitionItem = new HashMap<>();
        String localDbName = nameMapping.getLocalDbName();
        String localTblName = nameMapping.getLocalTblName();
        for (String partitionName : partitionNames) {
            if (partitionNameToIdMapBefore.containsKey(partitionName)) {
                LOG.info("addPartitionsCache partitionName:[{}] has exist in table:[{}]", partitionName, localTblName);
                continue;
            }
            long partitionId = Util.genIdByName(catalog.getName(), localDbName, localTblName, partitionName);
            ListPartitionItem listPartitionItem = toListPartitionItem(partitionName, key.types);
            idToPartitionItemBefore.put(partitionId, listPartitionItem);
            idToPartitionItem.put(partitionId, listPartitionItem);
            partitionNameToIdMapBefore.put(partitionName, partitionId);
        }
        Map<Long, List<String>> partitionValuesMapBefore = copy.getPartitionValuesMap();
        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        partitionValuesMapBefore.putAll(partitionValuesMap);
        if (key.types.size() > 1) {
            Map<UniqueId, Range<PartitionKey>> uidToPartitionRangeBefore = copy.getUidToPartitionRange();
            // uidToPartitionRange and rangeToId are only used for multi-column partition
            Map<UniqueId, Range<PartitionKey>> uidToPartitionRange = ListPartitionPrunerV2
                    .genUidToPartitionRange(idToPartitionItem, idToUniqueIdsMap);
            uidToPartitionRangeBefore.putAll(uidToPartitionRange);
            Map<Range<PartitionKey>, UniqueId> rangeToIdBefore = copy.getRangeToId();
            Map<Range<PartitionKey>, UniqueId> rangeToId = ListPartitionPrunerV2.genRangeToId(uidToPartitionRange);
            rangeToIdBefore.putAll(rangeToId);
        } else {
            Preconditions.checkState(key.types.size() == 1, key.types);
            // singleColumnRangeMap is only used for single-column partition
            RangeMap<ColumnBound, UniqueId> singleColumnRangeMapBefore = copy.getSingleColumnRangeMap();
            RangeMap<ColumnBound, UniqueId> singleColumnRangeMap = ListPartitionPrunerV2
                    .genSingleColumnRangeMap(idToPartitionItem, idToUniqueIdsMap);
            singleColumnRangeMapBefore.putAll(singleColumnRangeMap);
            Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMapBefore = copy
                    .getSingleUidToColumnRangeMap();
            Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap = ListPartitionPrunerV2
                    .genSingleUidToColumnRange(singleColumnRangeMap);
            singleUidToColumnRangeMapBefore.putAll(singleUidToColumnRangeMap);
        }
        HivePartitionValues partitionValuesCur = partitionValuesCache.getIfPresent(key);
        if (partitionValuesCur == partitionValues) {
            partitionValuesCache.put(key, copy);
        }
    }

    public void dropPartitionsCache(ExternalTable dorisTable, List<String> partitionNames,
                                    boolean invalidPartitionCache) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        PartitionValueCacheKey key = new PartitionValueCacheKey(nameMapping, null);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues == null) {
            return;
        }
        HivePartitionValues copy = partitionValues.copy();
        Map<String, Long> partitionNameToIdMapBefore = copy.getPartitionNameToIdMap();
        Map<Long, PartitionItem> idToPartitionItemBefore = copy.getIdToPartitionItem();
        Map<Long, List<UniqueId>> idToUniqueIdsMapBefore = copy.getIdToUniqueIdsMap();
        Map<UniqueId, Range<PartitionKey>> uidToPartitionRangeBefore = copy.getUidToPartitionRange();
        Map<Range<PartitionKey>, UniqueId> rangeToIdBefore = copy.getRangeToId();
        RangeMap<ColumnBound, UniqueId> singleColumnRangeMapBefore = copy.getSingleColumnRangeMap();
        Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMapBefore = copy.getSingleUidToColumnRangeMap();
        Map<Long, List<String>> partitionValuesMap = copy.getPartitionValuesMap();
        for (String partitionName : partitionNames) {
            if (!partitionNameToIdMapBefore.containsKey(partitionName)) {
                LOG.info("dropPartitionsCache partitionName:[{}] not exist in table:[{}]",
                        partitionName, nameMapping.getFullLocalName());
                continue;
            }
            Long partitionId = partitionNameToIdMapBefore.remove(partitionName);
            idToPartitionItemBefore.remove(partitionId);
            partitionValuesMap.remove(partitionId);
            List<UniqueId> uniqueIds = idToUniqueIdsMapBefore.remove(partitionId);
            for (UniqueId uniqueId : uniqueIds) {
                if (uidToPartitionRangeBefore != null) {
                    Range<PartitionKey> range = uidToPartitionRangeBefore.remove(uniqueId);
                    if (range != null) {
                        rangeToIdBefore.remove(range);
                    }
                }

                if (singleUidToColumnRangeMapBefore != null) {
                    Range<ColumnBound> range = singleUidToColumnRangeMapBefore.remove(uniqueId);
                    if (range != null) {
                        singleColumnRangeMapBefore.remove(range);
                    }
                }
            }

            if (invalidPartitionCache) {
                invalidatePartitionCache(dorisTable, partitionName);
            }
        }
        HivePartitionValues partitionValuesCur = partitionValuesCache.getIfPresent(key);
        if (partitionValuesCur == partitionValues) {
            partitionValuesCache.put(key, copy);
        }
    }

    public void putPartitionValuesCacheForTest(PartitionValueCacheKey key, HivePartitionValues values) {
        partitionValuesCache.put(key, values);
    }

    /***
     * get fileCache ref
     * @return
     */
    @VisibleForTesting
    public AtomicReference<LoadingCache<FileCacheKey, FileCacheValue>> getFileCacheRef() {
        return fileCacheRef;
    }

    @VisibleForTesting
    public LoadingCache<PartitionValueCacheKey, HivePartitionValues> getPartitionValuesCache() {
        return partitionValuesCache;
    }

    @VisibleForTesting
    public LoadingCache<PartitionCacheKey, HivePartition> getPartitionCache() {
        return partitionCache;
    }

    public List<FileCacheValue> getFilesByTransaction(List<HivePartition> partitions, Map<String, String> txnValidIds,
                                                      boolean isFullAcid, String bindBrokerName) {
        List<FileCacheValue> fileCacheValues = Lists.newArrayList();
        try {
            if (partitions.isEmpty()) {
                return fileCacheValues;
            }
            for (HivePartition partition : partitions) {
                //Get filesystem multiple times, Reason: https://github.com/apache/doris/pull/23409.
                LocationPath locationPath = LocationPath.of(partition.getPath(),
                        catalog.getCatalogProperty().getStoragePropertiesMap());
                // Use the bind broker name to get the file system, so that the file system can be shared
                RemoteFileSystem fileSystem = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache().getRemoteFileSystem(
                        new FileSystemCache.FileSystemCacheKey(
                                locationPath.getNormalizedLocation(),
                                locationPath.getStorageProperties()));
                // consider other methods to get the authenticator
                AuthenticationConfig authenticationConfig = AuthenticationConfig.getKerberosConfig(locationPath
                        .getStorageProperties().getBackendConfigProperties());
                HadoopAuthenticator hadoopAuthenticator =
                        HadoopAuthenticator.getHadoopAuthenticator(authenticationConfig);

                fileCacheValues.add(
                        hadoopAuthenticator.doAs(() -> AcidUtil.getAcidState(
                                fileSystem, partition, txnValidIds, catalog.getCatalogProperty()
                                        .getStoragePropertiesMap(), isFullAcid))
                );
            }
        } catch (Exception e) {
            throw new CacheException("Failed to get input splits %s", e, txnValidIds.toString());
        }
        return fileCacheValues;
    }

    /**
     * The Key of hive partition value cache
     */
    @Data
    public static class PartitionValueCacheKey {
        private NameMapping nameMapping;
        // not in key
        private List<Type> types;

        public PartitionValueCacheKey(NameMapping nameMapping, List<Type> types) {
            this.nameMapping = nameMapping;
            this.types = types;
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
            return "PartitionValueCacheKey{" + "dbName='" + nameMapping.getLocalDbName() + '\'' + ", tblName='"
                    + nameMapping.getLocalTblName() + '\'' + '}';
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
                    + ", tblName='" + nameMapping.getLocalTblName() + '\'' + ", values="
                    + values + '}';
        }
    }

    @Data
    public static class FileCacheKey {
        private long dummyKey = 0;
        private String location;
        // not in key
        private String inputFormat;
        // The values of partitions.
        // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
        // partitionValues would be ["part1", "part2"]
        protected List<String> partitionValues;
        private long id;

        public FileCacheKey(long id, String location, String inputFormat,
                            List<String> partitionValues) {
            this.location = location;
            this.inputFormat = inputFormat;
            this.partitionValues = partitionValues == null ? Lists.newArrayList() : partitionValues;
            this.id = id;
        }

        public static FileCacheKey createDummyCacheKey(long id, String location,
                                                       String inputFormat) {
            FileCacheKey fileCacheKey = new FileCacheKey(id, location, inputFormat, null);
            fileCacheKey.dummyKey = id;
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
            return location.equals(((FileCacheKey) obj).location)
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
            return Objects.hash(location, partitionValues);
        }

        @Override
        public String toString() {
            return "FileCacheKey{" + "location='" + location + '\'' + ", inputFormat='" + inputFormat + '\'' + '}';
        }
    }

    @Data
    public static class FileCacheValue {
        // File Cache for self splitter.
        private final List<HiveFileStatus> files = Lists.newArrayList();
        private boolean isSplittable;
        // The values of partitions.
        // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
        // partitionValues would be ["part1", "part2"]
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

        public AcidInfo getAcidInfo() {
            return acidInfo;
        }

        public void setAcidInfo(AcidInfo acidInfo) {
            this.acidInfo = acidInfo;
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
            // Hive ignores files starting with _ and .
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

    @Data
    public static class HivePartitionValues {
        private BiMap<String, Long> partitionNameToIdMap;
        private Map<Long, List<UniqueId>> idToUniqueIdsMap;
        private Map<Long, PartitionItem> idToPartitionItem;
        private Map<Long, List<String>> partitionValuesMap;
        //multi pair
        private Map<UniqueId, Range<PartitionKey>> uidToPartitionRange;
        private Map<Range<PartitionKey>, UniqueId> rangeToId;
        //single pair
        private RangeMap<ColumnBound, UniqueId> singleColumnRangeMap;
        private Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap;

        public HivePartitionValues() {
        }

        public HivePartitionValues(Map<Long, PartitionItem> idToPartitionItem,
                Map<UniqueId, Range<PartitionKey>> uidToPartitionRange,
                Map<Range<PartitionKey>, UniqueId> rangeToId,
                RangeMap<ColumnBound, UniqueId> singleColumnRangeMap,
                BiMap<String, Long> partitionNameToIdMap,
                Map<Long, List<UniqueId>> idToUniqueIdsMap,
                Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap,
                Map<Long, List<String>> partitionValuesMap) {
            this.idToPartitionItem = idToPartitionItem;
            this.uidToPartitionRange = uidToPartitionRange;
            this.rangeToId = rangeToId;
            this.singleColumnRangeMap = singleColumnRangeMap;
            this.partitionNameToIdMap = partitionNameToIdMap;
            this.idToUniqueIdsMap = idToUniqueIdsMap;
            this.singleUidToColumnRangeMap = singleUidToColumnRangeMap;
            this.partitionValuesMap = partitionValuesMap;
        }

        public HivePartitionValues copy() {
            HivePartitionValues copy = new HivePartitionValues();
            copy.setPartitionNameToIdMap(partitionNameToIdMap == null ? null : HashBiMap.create(partitionNameToIdMap));
            copy.setIdToUniqueIdsMap(idToUniqueIdsMap == null ? null : Maps.newHashMap(idToUniqueIdsMap));
            copy.setIdToPartitionItem(idToPartitionItem == null ? null : Maps.newHashMap(idToPartitionItem));
            copy.setPartitionValuesMap(partitionValuesMap == null ? null : Maps.newHashMap(partitionValuesMap));
            copy.setUidToPartitionRange(uidToPartitionRange == null ? null : Maps.newHashMap(uidToPartitionRange));
            copy.setRangeToId(rangeToId == null ? null : Maps.newHashMap(rangeToId));
            copy.setSingleUidToColumnRangeMap(
                    singleUidToColumnRangeMap == null ? null : Maps.newHashMap(singleUidToColumnRangeMap));
            if (singleColumnRangeMap != null) {
                RangeMap<ColumnBound, UniqueId> copySingleColumnRangeMap = TreeRangeMap.create();
                copySingleColumnRangeMap.putAll(singleColumnRangeMap);
                copy.setSingleColumnRangeMap(copySingleColumnRangeMap);
            }
            return copy;
        }
    }

    /**
     * get cache stats
     * @return <cache name -> <metric name -> metric value>>
     */
    public Map<String, Map<String, String>> getStats() {
        Map<String, Map<String, String>> res = Maps.newHashMap();
        res.put("hive_partition_values_cache", ExternalMetaCacheMgr.getCacheStats(partitionValuesCache.stats(),
                partitionCache.estimatedSize()));
        res.put("hive_partition_cache",
                ExternalMetaCacheMgr.getCacheStats(partitionCache.stats(), partitionCache.estimatedSize()));
        res.put("hive_file_cache",
                ExternalMetaCacheMgr.getCacheStats(fileCacheRef.get().stats(), fileCacheRef.get().estimatedSize()));
        return res;
    }
}
