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
import org.apache.doris.backup.Status;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.util.CacheBulkLoader;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
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
import com.google.common.base.Strings;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
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
    // After hive 3, transactional table's will have file '_orc_acid_version' with value >= '2'.
    public static final String HIVE_ORC_ACID_VERSION_FILE = "_orc_acid_version";

    private static final String HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX = "bucket_";

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
    private void init() {
        CacheFactory partitionValuesCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60L),
                Config.max_hive_partition_table_cache_num,
                true,
                null);
        partitionValuesCache = partitionValuesCacheFactory.buildCache(key -> loadPartitionValues(key), null,
                refreshExecutor);

        CacheFactory partitionCacheFactory = new CacheFactory(
                OptionalLong.of(28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60L),
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
    public void setNewFileCache() {
        // init or refresh job conf
        setJobConf();
        // if the file.meta.cache.ttl-second is equal or greater than 0, the cache expired will be set to that value
        int fileMetaCacheTtlSecond = NumberUtils.toInt(
                (catalog.getProperties().get(HMSExternalCatalog.FILE_META_CACHE_TTL_SECOND)),
                HMSExternalCatalog.FILE_META_CACHE_NO_TTL);

        CacheFactory fileCacheFactory = new CacheFactory(
                OptionalLong.of(fileMetaCacheTtlSecond >= HMSExternalCatalog.FILE_META_CACHE_TTL_DISABLE_CACHE
                        ? fileMetaCacheTtlSecond : 28800L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60L),
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
                return loadFiles(key);
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
        // partition name format: nation=cn/city=beijing
        List<String> partitionNames = catalog.getClient().listPartitionNames(key.dbName, key.tblName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("load #{} partitions for {} in catalog {}", partitionNames.size(), key, catalog.getName());
        }
        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionNames.size());
        BiMap<String, Long> partitionNameToIdMap = HashBiMap.create(partitionNames.size());
        Map<Long, List<UniqueId>> idToUniqueIdsMap = Maps.newHashMapWithExpectedSize(partitionNames.size());
        for (String partitionName : partitionNames) {
            long partitionId = Util.genIdByName(catalog.getName(), key.dbName, key.tblName, partitionName);
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
        String[] parts = partitionName.split("/");
        Preconditions.checkState(parts.length == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String part : parts) {
            String partitionValue = HiveUtil.getHivePartitionValue(part);
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
        Partition partition = catalog.getClient().getPartition(key.dbName, key.tblName, key.values);
        StorageDescriptor sd = partition.getSd();
        if (LOG.isDebugEnabled()) {
            LOG.debug("load partition format: {}, location: {} for {} in catalog {}",
                    sd.getInputFormat(), sd.getLocation(), key, catalog.getName());
        }
        // TODO: more info?
        return new HivePartition(key.dbName, key.tblName, false, sd.getInputFormat(), sd.getLocation(), key.values,
                partition.getParameters());
    }

    private Map<PartitionCacheKey, HivePartition> loadPartitions(Iterable<? extends PartitionCacheKey> keys) {
        Map<PartitionCacheKey, HivePartition> ret = new HashMap<>();
        if (keys == null || !keys.iterator().hasNext()) {
            return ret;
        }
        PartitionCacheKey oneKey = Iterables.get(keys, 0);
        String dbName = oneKey.getDbName();
        String tblName = oneKey.getTblName();
        List<Column> partitionColumns = ((HMSExternalTable)
                (catalog.getDbNullable(dbName).getTableNullable(tblName))).getPartitionColumns();
        // A partitionName is like "country=China/city=Beijing" or "date=2023-02-01"
        List<String> partitionNames = Streams.stream(keys).map(key -> {
            StringBuilder sb = new StringBuilder();
            Preconditions.checkState(key.getValues().size() == partitionColumns.size());
            for (int i = 0; i < partitionColumns.size(); i++) {
                sb.append(partitionColumns.get(i).getName());
                sb.append("=");
                // Partition value may contain special character, like / and so on. Need to encode.
                sb.append(FileUtils.escapePathName(key.getValues().get(i)));
                sb.append("/");
            }
            sb.delete(sb.length() - 1, sb.length());
            return sb.toString();
        }).collect(Collectors.toList());
        List<Partition> partitions = catalog.getClient().getPartitions(dbName, tblName, partitionNames);
        // Compose the return result map.
        for (Partition partition : partitions) {
            StorageDescriptor sd = partition.getSd();
            ret.put(new PartitionCacheKey(dbName, tblName, partition.getValues()),
                    new HivePartition(dbName, tblName, false,
                            sd.getInputFormat(), sd.getLocation(), partition.getValues(), partition.getParameters()));
        }
        return ret;
    }

    // Get File Status by using FileSystem API.
    private FileCacheValue getFileCache(String location, String inputFormat,
            JobConf jobConf,
            List<String> partitionValues,
            String bindBrokerName) throws UserException {
        FileCacheValue result = new FileCacheValue();
        RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache().getRemoteFileSystem(
                new FileSystemCache.FileSystemCacheKey(LocationPath.getFSIdentity(
                        location, bindBrokerName),
                        catalog.getCatalogProperty().getProperties(),
                        bindBrokerName, jobConf));
        result.setSplittable(HiveUtil.isSplittable(fs, inputFormat, location));
        // For Tez engine, it may generate subdirectoies for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /user/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to recursively list data location.
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        List<RemoteFile> remoteFiles = new ArrayList<>();
        boolean isRecursiveDirectories = Boolean.valueOf(
                catalog.getProperties().getOrDefault("hive.recursive_directories", "false"));
        Status status = fs.listFiles(location, isRecursiveDirectories, remoteFiles);
        if (status.ok()) {
            for (RemoteFile remoteFile : remoteFiles) {
                String srcPath = remoteFile.getPath().toString();
                LocationPath locationPath = new LocationPath(srcPath, catalog.getProperties());
                result.addFile(remoteFile, locationPath);
            }
        } else if (status.getErrCode().equals(ErrCode.NOT_FOUND)) {
            // User may manually remove partition under HDFS, in this case,
            // Hive doesn't aware that the removed partition is missing.
            // Here is to support this case without throw an exception.
            LOG.warn(String.format("File %s not exist.", location));
            if (!Boolean.valueOf(catalog.getProperties()
                    .getOrDefault("hive.ignore_absent_partitions", "true"))) {
                throw new UserException("Partition location does not exist: " + location);
            }
        } else {
            throw new RuntimeException(status.getErrMsg());
        }
        // Must copy the partitionValues to avoid concurrent modification of key and value
        result.setPartitionValues(Lists.newArrayList(partitionValues));
        return result;
    }

    private FileCacheValue loadFiles(FileCacheKey key) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            Map<String, String> props = catalog.getCatalogProperty().getProperties();
            LocationPath finalLocation = new LocationPath(key.location, props);
            // disable the fs cache in FileSystem, or it will always from new FileSystem
            // and save it in cache when calling FileInputFormat.setInputPaths().
            try {
                URI uri = finalLocation.getPath().toUri();
                if (uri.getScheme() != null) {
                    String scheme = uri.getScheme();
                    updateJobConf("fs." + scheme + ".impl.disable.cache", "true");
                    if (jobConf.get("fs." + scheme + ".impl") == null) {
                        if (!scheme.equals("hdfs") && !scheme.equals("viewfs")) {
                            updateJobConf("fs." + scheme + ".impl", PropertyConverter.getHadoopFSImplByScheme(scheme));
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("unknown scheme in path: " + finalLocation, e);
            }
            FileInputFormat.setInputPaths(jobConf, finalLocation.get());
            try {
                FileCacheValue result = getFileCache(finalLocation.get(), key.inputFormat, jobConf,
                        key.getPartitionValues(), key.bindBrokerName);
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

    private synchronized void setJobConf() {
        Configuration configuration = DFSFileSystem.getHdfsConf(catalog.ifNotSetFallbackToSimpleAuth());
        for (Map.Entry<String, String> entry : catalog.getCatalogProperty().getHadoopProperties().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        jobConf = new JobConf(configuration);
        // For Tez engine, it may generate subdirectories for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /us£er/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to set this config to support visit dir recursively.
        // Otherwise, getSplits() may throw exception: "Not a file xxx"
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        // disable FileSystem's cache
        jobConf.set("fs.hdfs.impl.disable.cache", "true");
        jobConf.set("fs.file.impl.disable.cache", "true");
    }

    private synchronized void updateJobConf(String key, String value) {
        jobConf.set(key, value);
    }

    public HivePartitionValues getPartitionValues(String dbName, String tblName, List<Type> types) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, types);
        return getPartitionValues(key);
    }

    public HivePartitionValues getPartitionValues(PartitionValueCacheKey key) {
        return partitionValuesCache.get(key);
    }

    public List<FileCacheValue> getFilesByPartitionsWithCache(List<HivePartition> partitions,
                                                              String bindBrokerName) {
        return getFilesByPartitions(partitions, true, true, bindBrokerName);
    }

    public List<FileCacheValue> getFilesByPartitionsWithoutCache(List<HivePartition> partitions,
                                                                 String bindBrokerName) {
        return getFilesByPartitions(partitions, false, true, bindBrokerName);
    }

    public List<FileCacheValue> getFilesByPartitions(List<HivePartition> partitions,
                                                     boolean withCache,
                                                     boolean concurrent,
                                                     String bindBrokerName) {
        long start = System.currentTimeMillis();
        List<FileCacheKey> keys = partitions.stream().map(p -> p.isDummyPartition()
                ? FileCacheKey.createDummyCacheKey(
                        p.getDbName(), p.getTblName(), p.getPath(), p.getInputFormat(), bindBrokerName)
                : new FileCacheKey(p.getPath(), p.getInputFormat(), p.getPartitionValues(), bindBrokerName))
                .collect(Collectors.toList());

        List<FileCacheValue> fileLists;
        try {
            if (withCache) {
                fileLists = new ArrayList<>(fileCacheRef.get().getAll(keys).values());
            } else {
                if (concurrent) {
                    List<Future<FileCacheValue>> pList = keys.stream().map(
                            key -> fileListingExecutor.submit(() -> loadFiles(key))).collect(Collectors.toList());
                    fileLists = Lists.newArrayListWithExpectedSize(keys.size());
                    for (Future<FileCacheValue> p : pList) {
                        fileLists.add(p.get());
                    }
                } else {
                    fileLists = keys.stream().map(this::loadFiles).collect(Collectors.toList());
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

    public HivePartition getHivePartition(String dbName, String name, List<String> partitionValues) {
        return partitionCache.get(new PartitionCacheKey(dbName, name, partitionValues));
    }

    public List<HivePartition> getAllPartitionsWithCache(String dbName, String name,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dbName, name, partitionValuesList, true);
    }

    public List<HivePartition> getAllPartitionsWithoutCache(String dbName, String name,
            List<List<String>> partitionValuesList) {
        return getAllPartitions(dbName, name, partitionValuesList, false);
    }

    private List<HivePartition> getAllPartitions(String dbName, String name, List<List<String>> partitionValuesList,
            boolean withCache) {
        long start = System.currentTimeMillis();
        List<PartitionCacheKey> keys = partitionValuesList.stream()
                .map(p -> new PartitionCacheKey(dbName, name, p))
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

    public void invalidateTableCache(String dbName, String tblName) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, null);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues != null) {
            long start = System.currentTimeMillis();
            for (List<String> values : partitionValues.partitionValuesMap.values()) {
                PartitionCacheKey partKey = new PartitionCacheKey(dbName, tblName, values);
                HivePartition partition = partitionCache.getIfPresent(partKey);
                if (partition != null) {
                    fileCacheRef.get().invalidate(new FileCacheKey(partition.getPath(),
                            null, partition.getPartitionValues(), null));
                    partitionCache.invalidate(partKey);
                }
            }
            partitionValuesCache.invalidate(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug("invalid table cache for {}.{} in catalog {}, cache num: {}, cost: {} ms",
                        dbName, tblName, catalog.getName(), partitionValues.partitionValuesMap.size(),
                        (System.currentTimeMillis() - start));
            }
        } else {
            /**
             * A file cache entry can be created reference to
             * {@link org.apache.doris.planner.external.HiveSplitter#getSplits},
             * so we need to invalidate it if this is a non-partitioned table.
             * We use {@link org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheKey#createDummyCacheKey}
             * to avoid invocation by Hms Client, because this method may be invoked when salve FE replay journal logs,
             * and FE will exit if some network problems occur.
             * */
            FileCacheKey fileCacheKey = FileCacheKey.createDummyCacheKey(
                    dbName, tblName, null, null, null);
            fileCacheRef.get().invalidate(fileCacheKey);
        }
    }

    public void invalidatePartitionCache(String dbName, String tblName, String partitionName) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, null);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues != null) {
            Long partitionId = partitionValues.partitionNameToIdMap.get(partitionName);
            List<String> values = partitionValues.partitionValuesMap.get(partitionId);
            PartitionCacheKey partKey = new PartitionCacheKey(dbName, tblName, values);
            HivePartition partition = partitionCache.getIfPresent(partKey);
            if (partition != null) {
                fileCacheRef.get().invalidate(new FileCacheKey(partition.getPath(),
                        null, partition.getPartitionValues(), null));
                partitionCache.invalidate(partKey);
            }
        }
    }

    public void invalidateDbCache(String dbName) {
        long start = System.currentTimeMillis();
        Set<PartitionValueCacheKey> keys = partitionValuesCache.asMap().keySet();
        for (PartitionValueCacheKey key : keys) {
            if (key.dbName.equals(dbName)) {
                invalidateTableCache(dbName, key.tblName);
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
    public void addPartitionsCache(String dbName, String tblName, List<String> partitionNames,
            List<Type> partitionColumnTypes) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, partitionColumnTypes);
        HivePartitionValues partitionValues = partitionValuesCache.getIfPresent(key);
        if (partitionValues == null) {
            return;
        }
        HivePartitionValues copy = partitionValues.copy();
        Map<Long, PartitionItem> idToPartitionItemBefore = copy.getIdToPartitionItem();
        Map<String, Long> partitionNameToIdMapBefore = copy.getPartitionNameToIdMap();
        Map<Long, List<UniqueId>> idToUniqueIdsMap = copy.getIdToUniqueIdsMap();
        Map<Long, PartitionItem> idToPartitionItem = new HashMap<>();
        for (String partitionName : partitionNames) {
            if (partitionNameToIdMapBefore.containsKey(partitionName)) {
                LOG.info("addPartitionsCache partitionName:[{}] has exist in table:[{}]", partitionName, tblName);
                continue;
            }
            long partitionId = Util.genIdByName(catalog.getName(), dbName, tblName, partitionName);
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

    public void dropPartitionsCache(String dbName, String tblName, List<String> partitionNames,
                                    boolean invalidPartitionCache) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, null);
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
                LOG.info("dropPartitionsCache partitionName:[{}] not exist in table:[{}]", partitionName, tblName);
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
                invalidatePartitionCache(dbName, tblName, partitionName);
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
    public AtomicReference<LoadingCache<FileCacheKey, FileCacheValue>> getFileCacheRef() {
        return fileCacheRef;
    }

    public List<FileCacheValue> getFilesByTransaction(List<HivePartition> partitions, ValidWriteIdList validWriteIds,
            boolean isFullAcid, long tableId, String bindBrokerName) {
        List<FileCacheValue> fileCacheValues = Lists.newArrayList();
        String remoteUser = jobConf.get(AuthenticationConfig.HADOOP_USER_NAME);
        try {
            for (HivePartition partition : partitions) {
                FileCacheValue fileCacheValue = new FileCacheValue();
                AcidUtils.Directory directory;
                if (!Strings.isNullOrEmpty(remoteUser)) {
                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(remoteUser);
                    directory = ugi.doAs((PrivilegedExceptionAction<AcidUtils.Directory>) () -> AcidUtils.getAcidState(
                            new Path(partition.getPath()), jobConf, validWriteIds, false, true));
                } else {
                    directory = AcidUtils.getAcidState(new Path(partition.getPath()), jobConf, validWriteIds, false,
                            true);
                }
                if (directory == null) {
                    return Collections.emptyList();
                }
                if (!directory.getOriginalFiles().isEmpty()) {
                    throw new Exception("Original non-ACID files in transactional tables are not supported");
                }

                if (isFullAcid) {
                    int acidVersion = 2;
                    /**
                     * From Hive version >= 3.0, delta/base files will always have file '_orc_acid_version'
                     * with value >= '2'.
                     */
                    Path baseOrDeltaPath = directory.getBaseDirectory() != null ? directory.getBaseDirectory() :
                            !directory.getCurrentDirectories().isEmpty() ? directory.getCurrentDirectories().get(0)
                                    .getPath() : null;
                    if (baseOrDeltaPath == null) {
                        return Collections.emptyList();
                    }
                    String acidVersionPath = new Path(baseOrDeltaPath, "_orc_acid_version").toUri().toString();
                    RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache().getRemoteFileSystem(
                            new FileSystemCache.FileSystemCacheKey(
                                    LocationPath.getFSIdentity(baseOrDeltaPath.toUri().toString(),
                                            bindBrokerName),
                                            catalog.getCatalogProperty().getProperties(),
                                            bindBrokerName, jobConf));
                    Status status = fs.exists(acidVersionPath);
                    if (status != Status.OK) {
                        if (status.getErrCode() == ErrCode.NOT_FOUND) {
                            acidVersion = 0;
                        } else {
                            throw new Exception(String.format("Failed to check remote path {} exists.",
                                    acidVersionPath));
                        }
                    }
                    if (acidVersion == 0 && !directory.getCurrentDirectories().isEmpty()) {
                        throw new Exception(
                                "Hive 2.x versioned full-acid tables need to run major compaction.");
                    }
                }

                // delta directories
                List<DeleteDeltaInfo> deleteDeltas = new ArrayList<>();
                for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
                    String location = delta.getPath().toString();
                    RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache().getRemoteFileSystem(
                            new FileSystemCache.FileSystemCacheKey(
                                    LocationPath.getFSIdentity(location, bindBrokerName),
                                            catalog.getCatalogProperty().getProperties(), bindBrokerName, jobConf));
                    List<RemoteFile> remoteFiles = new ArrayList<>();
                    Status status = fs.listFiles(location, false, remoteFiles);
                    if (status.ok()) {
                        if (delta.isDeleteDelta()) {
                            List<String> deleteDeltaFileNames = remoteFiles.stream().map(f -> f.getName()).filter(
                                            name -> name.startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX))
                                    .collect(Collectors.toList());
                            deleteDeltas.add(new DeleteDeltaInfo(location, deleteDeltaFileNames));
                            continue;
                        }
                        remoteFiles.stream().filter(
                                f -> f.getName().startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX)).forEach(file -> {
                                    LocationPath path = new LocationPath(file.getPath().toString(),
                                            catalog.getProperties());
                                    fileCacheValue.addFile(file, path);
                                });
                    } else {
                        throw new RuntimeException(status.getErrMsg());
                    }
                }

                // base
                if (directory.getBaseDirectory() != null) {
                    String location = directory.getBaseDirectory().toString();
                    RemoteFileSystem fs = Env.getCurrentEnv().getExtMetaCacheMgr().getFsCache().getRemoteFileSystem(
                            new FileSystemCache.FileSystemCacheKey(
                                    LocationPath.getFSIdentity(location, bindBrokerName),
                                            catalog.getCatalogProperty().getProperties(), bindBrokerName, jobConf));
                    List<RemoteFile> remoteFiles = new ArrayList<>();
                    Status status = fs.listFiles(location, false, remoteFiles);
                    if (status.ok()) {
                        remoteFiles.stream().filter(
                                        f -> f.getName().startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX))
                                .forEach(file -> {
                                    LocationPath path = new LocationPath(file.getPath().toString(),
                                            catalog.getProperties());
                                    fileCacheValue.addFile(file, path);
                                });
                    } else {
                        throw new RuntimeException(status.getErrMsg());
                    }
                }
                fileCacheValue.setAcidInfo(new AcidInfo(partition.getPath(), deleteDeltas));
                fileCacheValues.add(fileCacheValue);
            }
        } catch (Exception e) {
            throw new CacheException("failed to get input splits for write ids %s in catalog %s", e,
                    validWriteIds.toString(), catalog.getName());
        }
        return fileCacheValues;
    }

    /**
     * The Key of hive partition value cache
     */
    @Data
    public static class PartitionValueCacheKey {
        private String dbName;
        private String tblName;
        // not in key
        private List<Type> types;

        public PartitionValueCacheKey(String dbName, String tblName, List<Type> types) {
            this.dbName = dbName;
            this.tblName = tblName;
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
            return dbName.equals(((PartitionValueCacheKey) obj).dbName)
                    && tblName.equals(((PartitionValueCacheKey) obj).tblName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tblName);
        }

        @Override
        public String toString() {
            return "PartitionValueCacheKey{" + "dbName='" + dbName + '\'' + ", tblName='" + tblName + '\'' + '}';
        }
    }

    @Data
    public static class PartitionCacheKey {
        private String dbName;
        private String tblName;
        private List<String> values;

        public PartitionCacheKey(String dbName, String tblName, List<String> values) {
            this.dbName = dbName;
            this.tblName = tblName;
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
            return dbName.equals(((PartitionCacheKey) obj).dbName)
                    && tblName.equals(((PartitionCacheKey) obj).tblName)
                    && Objects.equals(values, ((PartitionCacheKey) obj).values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tblName, values);
        }

        @Override
        public String toString() {
            return "PartitionCacheKey{" + "dbName='" + dbName + '\'' + ", tblName='" + tblName + '\'' + ", values="
                    + values + '}';
        }
    }

    @Data
    public static class FileCacheKey {
        private String dummyKey;
        private String location;
        // not in key
        private String inputFormat;
        // Broker name for file split and file scan.
        private String bindBrokerName;
        // The values of partitions.
        // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
        // partitionValues would be ["part1", "part2"]
        protected List<String> partitionValues;

        public FileCacheKey(String location, String inputFormat, List<String> partitionValues, String bindBrokerName) {
            this.location = location;
            this.inputFormat = inputFormat;
            this.partitionValues = partitionValues == null ? Lists.newArrayList() : partitionValues;
            this.bindBrokerName = bindBrokerName;
        }

        public static FileCacheKey createDummyCacheKey(String dbName, String tblName, String location,
                                                       String inputFormat,
                                                       String bindBrokerName) {
            FileCacheKey fileCacheKey = new FileCacheKey(location, inputFormat, null, bindBrokerName);
            fileCacheKey.dummyKey = dbName + "." + tblName;
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
            if (dummyKey != null) {
                return dummyKey.equals(((FileCacheKey) obj).dummyKey);
            }
            return location.equals(((FileCacheKey) obj).location)
                && Objects.equals(partitionValues, ((FileCacheKey) obj).partitionValues);
        }

        @Override
        public int hashCode() {
            if (dummyKey != null) {
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
            if (containsHiddenPath(pathStr) || path.getName().startsWith("_")) {
                return false;
            }
            for (String name : pathStr.split("/")) {
                if (isGeneratedPath(name)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean containsHiddenPath(String path) {
            if (path.startsWith(".")) {
                return true;
            }
            for (int i = 0; i < path.length() - 1; i++) {
                if (path.charAt(i) == '/' && path.charAt(i + 1) == '.') {
                    return true;
                }
            }
            return false;
        }

        private static boolean isGeneratedPath(String name) {
            return "_temporary".equals(name) // generated by spark
                    || "_imapala_insert_staging".equals(name) // generated by impala
                    || name.startsWith("."); // generated by hive or hidden file
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
