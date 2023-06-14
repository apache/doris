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
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.RemoteFiles;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPrunerV2Base.UniqueId;
import org.apache.doris.planner.external.FileSplit;
import org.apache.doris.spi.Split;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Data;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// The cache of a hms catalog. 3 kind of caches:
// 1. partitionValuesCache: cache the partition values of a table, for partition prune.
// 2. partitionCache: cache the partition info(location, input format, etc.) of a table.
// 3. fileCache: cache the files of a location.
public class HiveMetaStoreCache {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreCache.class);
    private static final int MIN_BATCH_FETCH_PARTITION_NUM = 50;
    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    // After hive 3, transactional table's will have file '_orc_acid_version' with value >= '2'.
    public static final String HIVE_ORC_ACID_VERSION_FILE = "_orc_acid_version";

    private static final String HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX = "bucket_";

    private HMSExternalCatalog catalog;

    private Executor executor;

    // cache from <dbname-tblname> -> <values of partitions>
    private LoadingCache<PartitionValueCacheKey, HivePartitionValues> partitionValuesCache;
    // cache from <dbname-tblname-partition_values> -> <partition info>
    private LoadingCache<PartitionCacheKey, HivePartition> partitionCache;
    // the ref of cache from <location> -> <file list>
    private volatile AtomicReference<LoadingCache<FileCacheKey, FileCacheValue>> fileCacheRef
            = new AtomicReference<>();

    public HiveMetaStoreCache(HMSExternalCatalog catalog, Executor executor) {
        this.catalog = catalog;
        this.executor = executor;
        init();
        initMetrics();
    }

    private void init() {
        partitionValuesCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_table_catch_num)
                .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
                .build(CacheLoader.asyncReloading(
                        new CacheLoader<PartitionValueCacheKey, HivePartitionValues>() {
                            @Override
                            public HivePartitionValues load(PartitionValueCacheKey key) throws Exception {
                                return loadPartitionValues(key);
                            }
                        }, executor));

        partitionCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_partition_cache_num)
                .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
                .build(CacheLoader.asyncReloading(new CacheLoader<PartitionCacheKey, HivePartition>() {
                    @Override
                    public HivePartition load(PartitionCacheKey key) throws Exception {
                        return loadPartitions(key);
                    }
                }, executor));

        setNewFileCache();
    }

    /***
     * generate a filecache and set to fileCacheRef
     */
    public void setNewFileCache() {
        // if the file.meta.cache.ttl-second is equal or greater than 0, the cache expired will be set to that value
        int fileMetaCacheTtlSecond = NumberUtils.toInt(
                (catalog.getProperties().get(HMSExternalCatalog.FILE_META_CACHE_TTL_SECOND)),
                HMSExternalCatalog.FILE_META_CACHE_NO_TTL);

        CacheBuilder<Object, Object> fileCacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(Config.max_external_file_cache_num)
                .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES);

        if (fileMetaCacheTtlSecond >= HMSExternalCatalog.FILE_META_CACHE_TTL_DISABLE_CACHE) {
            fileCacheBuilder.expireAfterWrite(fileMetaCacheTtlSecond, TimeUnit.SECONDS);
        }
        // if the file.meta.cache.ttl-second is equal 0, use the synchronous loader
        // if the file.meta.cache.ttl-second greater than 0, use the asynchronous loader
        CacheLoader<FileCacheKey, FileCacheValue> loader = getGuavaCacheLoader(executor,
                fileMetaCacheTtlSecond);

        LoadingCache<FileCacheKey, FileCacheValue> preFileCache = fileCacheRef.get();

        fileCacheRef.set(fileCacheBuilder.build(loader));
        if (Objects.nonNull(preFileCache)) {
            preFileCache.invalidateAll();
        }
    }

    private void initMetrics() {
        // partition value
        GaugeMetric<Long> valueCacheGauge = new GaugeMetric<Long>("hive_meta_cache",
                Metric.MetricUnit.NOUNIT, "hive partition value cache number") {
            @Override
            public Long getValue() {
                return partitionValuesCache.size();
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
                return partitionCache.size();
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
                return fileCacheRef.get().size();
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
        Map<String, Long> partitionNameToIdMap = Maps.newHashMapWithExpectedSize(partitionNames.size());
        Map<Long, List<UniqueId>> idToUniqueIdsMap = Maps.newHashMapWithExpectedSize(partitionNames.size());
        long idx = 0;
        for (String partitionName : partitionNames) {
            long partitionId = idx++;
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
        return new HivePartitionValues(idToPartitionItem, uidToPartitionRange, rangeToId, singleColumnRangeMap, idx,
                partitionNameToIdMap, idToUniqueIdsMap, singleUidToColumnRangeMap, partitionValuesMap);
    }

    public ListPartitionItem toListPartitionItem(String partitionName, List<Type> types) {
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        String[] parts = partitionName.split("/");
        Preconditions.checkState(parts.length == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String part : parts) {
            String[] kv = part.split("=");
            Preconditions.checkState(kv.length == 2, partitionName);
            String partitionValue;
            try {
                // hive partition value maybe contains special characters like '=' and '/'
                partitionValue = URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                // It should not be here
                throw new RuntimeException(e);
            }
            values.add(new PartitionValue(partitionValue, HIVE_DEFAULT_PARTITION.equals(partitionValue)));
        }
        try {
            PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types);
            return new ListPartitionItem(Lists.newArrayList(key));
        } catch (AnalysisException e) {
            throw new CacheException("failed to convert hive partition %s to list partition in catalog %s",
                    e, partitionName, catalog.getName());
        }
    }

    private HivePartition loadPartitions(PartitionCacheKey key) {
        Partition partition = catalog.getClient().getPartition(key.dbName, key.tblName, key.values);
        StorageDescriptor sd = partition.getSd();
        if (LOG.isDebugEnabled()) {
            LOG.debug("load partition format: {}, location: {} for {} in catalog {}",
                    sd.getInputFormat(), sd.getLocation(), key, catalog.getName());
        }
        // TODO: more info?
        return new HivePartition(key.dbName, key.tblName, false, sd.getInputFormat(), sd.getLocation(), key.values);
    }

    // Get File Status by using FileSystem API.
    private FileCacheValue getFileCache(String location, InputFormat<?, ?> inputFormat,
                                                                 JobConf jobConf,
                                                                 List<String> partitionValues) throws UserException {
        FileCacheValue result = new FileCacheValue();
        result.setSplittable(HiveUtil.isSplittable(inputFormat, new Path(location), jobConf));
        RemoteFileSystem fs = FileSystemFactory.getByLocation(location, jobConf);
        try {
            // For Tez engine, it may generate subdirectoies for "union" query.
            // So there may be files and directories in the table directory at the same time. eg:
            //      /user/hive/warehouse/region_tmp_union_all2/000000_0
            //      /user/hive/warehouse/region_tmp_union_all2/1
            //      /user/hive/warehouse/region_tmp_union_all2/2
            // So we need to recursively list data location.
            // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
            RemoteFiles locatedFiles = fs.listLocatedFiles(location, true, true);
            locatedFiles.files().forEach(result::addFile);
        } catch (Exception e) {
            // User may manually remove partition under HDFS, in this case,
            // Hive doesn't aware that the removed partition is missing.
            // Here is to support this case without throw an exception.
            if (e.getCause() instanceof FileNotFoundException) {
                LOG.warn(String.format("File %s not exist.", location));
            } else {
                throw e;
            }
        }
        result.setPartitionValues(partitionValues);
        return result;
    }

    private FileCacheValue loadFiles(FileCacheKey key) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
            String finalLocation = S3Util.convertToS3IfNecessary(key.location);
            JobConf jobConf = getJobConf();
            // For Tez engine, it may generate subdirectories for "union" query.
            // So there may be files and directories in the table directory at the same time. eg:
            //      /us£er/hive/warehouse/region_tmp_union_all2/000000_0
            //      /user/hive/warehouse/region_tmp_union_all2/1
            //      /user/hive/warehouse/region_tmp_union_all2/2
            // So we need to set this config to support visit dir recursively.
            // Otherwise, getSplits() may throw exception: "Not a file xxx"
            // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
            jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
            FileInputFormat.setInputPaths(jobConf, finalLocation);
            try {
                FileCacheValue result;
                InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(jobConf, key.inputFormat, false);
                // TODO: This is a temp config, will remove it after the HiveSplitter is stable.
                if (key.useSelfSplitter) {
                    result = getFileCache(finalLocation, inputFormat, jobConf, key.getPartitionValues());
                } else {
                    InputSplit[] splits;
                    String remoteUser = jobConf.get(HdfsResource.HADOOP_USER_NAME);
                    if (!Strings.isNullOrEmpty(remoteUser)) {
                        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(remoteUser);
                        splits = ugi.doAs(
                            (PrivilegedExceptionAction<InputSplit[]>) () -> inputFormat.getSplits(jobConf, 0));
                    } else {
                        splits = inputFormat.getSplits(jobConf, 0 /* use hdfs block size as default */);
                    }
                    result = new FileCacheValue();
                    // Convert the hadoop split to Doris Split.
                    for (int i = 0; i < splits.length; i++) {
                        org.apache.hadoop.mapred.FileSplit fs = ((org.apache.hadoop.mapred.FileSplit) splits[i]);
                        // todo: get modification time
                        result.addSplit(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), -1, null, null));
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

    private JobConf getJobConf() {
        Configuration configuration = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : catalog.getCatalogProperty().getHadoopProperties().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        return new JobConf(configuration);
    }

    public HivePartitionValues getPartitionValues(String dbName, String tblName, List<Type> types) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, types);
        return getPartitionValues(key);
    }

    public HivePartitionValues getPartitionValues(PartitionValueCacheKey key) {
        try {
            return partitionValuesCache.get(key);
        } catch (ExecutionException e) {
            throw new CacheException("failed to get partition values for %s in catalog %s", e, key, catalog.getName());
        }
    }

    public List<FileCacheValue> getFilesByPartitions(List<HivePartition> partitions, boolean useSelfSplitter) {
        long start = System.currentTimeMillis();
        List<FileCacheKey> keys = Lists.newArrayListWithExpectedSize(partitions.size());
        partitions.stream().forEach(p -> {
            FileCacheKey fileCacheKey = p.isDummyPartition()
                    ? FileCacheKey.createDummyCacheKey(p.getDbName(), p.getTblName(), p.getPath(),
                    p.getInputFormat(), useSelfSplitter)
                    : new FileCacheKey(p.getPath(), p.getInputFormat(), p.getPartitionValues());
            fileCacheKey.setUseSelfSplitter(useSelfSplitter);
            keys.add(fileCacheKey);
        });

        Stream<FileCacheKey> stream;
        if (partitions.size() < MIN_BATCH_FETCH_PARTITION_NUM) {
            stream = keys.stream();
        } else {
            stream = keys.parallelStream();
        }
        List<FileCacheValue> fileLists = stream.map(k -> {
            try {
                FileCacheValue fileCacheValue = fileCacheRef.get().get(k);
                // Replace default hive partition with a null_string.
                for (int i = 0; i < fileCacheValue.getValuesSize(); i++) {
                    if (HIVE_DEFAULT_PARTITION.equals(fileCacheValue.getPartitionValues().get(i))) {
                        fileCacheValue.getPartitionValues().set(i, FeConstants.null_string);
                    }
                }
                return fileCacheValue;
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        LOG.debug("get #{} files from #{} partitions in catalog {} cost: {} ms",
                fileLists.stream().mapToInt(l -> l.getFiles() == null
                    ? (l.getSplits() == null ? 0 : l.getSplits().size()) : l.getFiles().size()).sum(),
                partitions.size(), catalog.getName(), (System.currentTimeMillis() - start));
        return fileLists;
    }

    public List<HivePartition> getAllPartitions(String dbName, String name, List<List<String>> partitionValuesList) {
        long start = System.currentTimeMillis();
        List<PartitionCacheKey> keys = Lists.newArrayListWithExpectedSize(partitionValuesList.size());
        partitionValuesList.stream().forEach(p -> keys.add(new PartitionCacheKey(dbName, name, p)));

        Stream<PartitionCacheKey> stream;
        if (partitionValuesList.size() < MIN_BATCH_FETCH_PARTITION_NUM) {
            stream = keys.stream();
        } else {
            stream = keys.parallelStream();
        }
        List<HivePartition> partitions = stream.map(k -> {
            try {
                return partitionCache.get(k);
            } catch (ExecutionException e) {
                throw new CacheException("failed to get partition for %s in catalog %s", e, k, catalog.getName());
            }
        }).collect(Collectors.toList());
        LOG.debug("get #{} partitions in catalog {} cost: {} ms", partitions.size(), catalog.getName(),
                (System.currentTimeMillis() - start));
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
                            null, partition.getPartitionValues()));
                    partitionCache.invalidate(partKey);
                }
            }
            partitionValuesCache.invalidate(key);
            LOG.debug("invalid table cache for {}.{} in catalog {}, cache num: {}, cost: {} ms",
                    dbName, tblName, catalog.getName(), partitionValues.partitionValuesMap.size(),
                    (System.currentTimeMillis() - start));
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
                    dbName, tblName, null, null, false);
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
                        null, partition.getPartitionValues()));
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
        LOG.debug("invalid db cache for {} in catalog {}, cache num: {}, cost: {} ms", dbName, catalog.getName(),
                keys.size(), (System.currentTimeMillis() - start));
    }

    public void invalidateAll() {
        partitionValuesCache.invalidateAll();
        partitionCache.invalidateAll();
        fileCacheRef.get().invalidateAll();
        LOG.debug("invalid all meta cache in catalog {}", catalog.getName());
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
        long idx = copy.getNextPartitionId();
        for (String partitionName : partitionNames) {
            if (partitionNameToIdMapBefore.containsKey(partitionName)) {
                LOG.info("addPartitionsCache partitionName:[{}] has exist in table:[{}]", partitionName, tblName);
                continue;
            }
            long partitionId = idx++;
            ListPartitionItem listPartitionItem = toListPartitionItem(partitionName, key.types);
            idToPartitionItemBefore.put(partitionId, listPartitionItem);
            idToPartitionItem.put(partitionId, listPartitionItem);
            partitionNameToIdMapBefore.put(partitionName, partitionId);
        }
        Map<Long, List<String>> partitionValuesMapBefore = copy.getPartitionValuesMap();
        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        partitionValuesMapBefore.putAll(partitionValuesMap);
        copy.setNextPartitionId(idx);
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
     * get the guava CacheLoader
     * if the fileMetaCacheTtlSecond equal 0 , the synchronous loader is used
     * if the fileMetaCacheTtlSecond greater than 0 , the asynchronous loader is used
     * @param executor
     * @param fileMetaCacheTtlSecond
     * @return
     */
    private CacheLoader<FileCacheKey, FileCacheValue> getGuavaCacheLoader(Executor executor,
            int fileMetaCacheTtlSecond) {
        CacheLoader<FileCacheKey, FileCacheValue> loader =
                new CacheLoader<FileCacheKey, FileCacheValue>() {
                    @Override
                    public FileCacheValue load(FileCacheKey key) throws Exception {
                        return loadFiles(key);
                    }
                };
        if (fileMetaCacheTtlSecond == HMSExternalCatalog.FILE_META_CACHE_TTL_DISABLE_CACHE) {
            return loader;
        } else {
            return CacheLoader.asyncReloading(loader, executor);
        }
    }

    /***
     * get fileCache ref
     * @return
     */
    public AtomicReference<LoadingCache<FileCacheKey, FileCacheValue>> getFileCacheRef() {
        return fileCacheRef;
    }

    public List<FileCacheValue> getFilesByTransaction(List<HivePartition> partitions, ValidWriteIdList validWriteIds,
            boolean isFullAcid) {
        List<FileCacheValue> fileCacheValues = Lists.newArrayList();
        JobConf jobConf = getJobConf();
        String remoteUser = jobConf.get(HdfsResource.HADOOP_USER_NAME);
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
                    String acidVersionPath = new Path(baseOrDeltaPath, "_orc_acid_version").toUri().toString();
                    RemoteFileSystem fs = FileSystemFactory.getByLocation(baseOrDeltaPath.toUri().toString(), jobConf);
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
                    RemoteFileSystem fs = FileSystemFactory.getByLocation(location, jobConf);
                    RemoteFiles locatedFiles = fs.listLocatedFiles(location, true, false);
                    if (delta.isDeleteDelta()) {
                        List<String> deleteDeltaFileNames = locatedFiles.files().stream().map(f -> f.getName()).filter(
                                        name -> name.startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX))
                                        .collect(Collectors.toList());
                        deleteDeltas.add(new DeleteDeltaInfo(location, deleteDeltaFileNames));
                        continue;
                    }
                    locatedFiles.files().stream().filter(
                            f -> f.getName().startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX))
                            .forEach(fileCacheValue::addFile);
                }

                // base
                if (directory.getBaseDirectory() != null) {
                    String location = directory.getBaseDirectory().toString();
                    RemoteFileSystem fs = FileSystemFactory.getByLocation(location, jobConf);
                    RemoteFiles locatedFiles = fs.listLocatedFiles(location, true, false);
                    locatedFiles.files().stream().filter(
                            f -> f.getName().startsWith(HIVE_TRANSACTIONAL_ORC_BUCKET_PREFIX))
                            .forEach(fileCacheValue::addFile);
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
        // Temp variable, use self file splitter or use InputFormat.getSplits.
        // Will remove after self splitter is stable.
        private boolean useSelfSplitter;
        // The values of partitions.
        // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
        // partitionValues would be ["part1", "part2"]
        protected List<String> partitionValues;

        public FileCacheKey(String location, String inputFormat, List<String> partitionValues) {
            this.location = location;
            this.inputFormat = inputFormat;
            this.partitionValues = partitionValues == null ? Lists.newArrayList() : partitionValues;
            this.useSelfSplitter = true;
        }

        public static FileCacheKey createDummyCacheKey(String dbName, String tblName, String location,
                                                       String inputFormat, boolean useSelfSplitter) {
            FileCacheKey fileCacheKey = new FileCacheKey(location, inputFormat, null);
            fileCacheKey.dummyKey = dbName + "." + tblName;
            fileCacheKey.useSelfSplitter = useSelfSplitter;
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
                && partitionValues.equals(((FileCacheKey) obj).partitionValues);
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
        private List<HiveFileStatus> files;
        // File split cache for old splitter. This is a temp variable.
        private List<Split> splits;
        private boolean isSplittable;
        // The values of partitions.
        // e.g for file : hdfs://path/to/table/part1=a/part2=b/datafile
        // partitionValues would be ["part1", "part2"]
        protected List<String> partitionValues;

        private AcidInfo acidInfo;

        public void addFile(RemoteFile file) {
            if (files == null) {
                files = Lists.newArrayList();
            }
            HiveFileStatus status = new HiveFileStatus();
            status.setBlockLocations(file.getBlockLocations());
            status.setPath(file.getPath());
            status.length = file.getSize();
            status.blockSize = file.getBlockSize();
            status.modificationTime = file.getModificationTime();
            files.add(status);
        }

        public void addSplit(Split split) {
            if (splits == null) {
                splits = Lists.newArrayList();
            }
            splits.add(split);
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
    }

    @Data
    public static class HiveFileStatus {
        BlockLocation[] blockLocations;
        Path path;
        long length;
        long blockSize;
        long modificationTime;
    }

    @Data
    public static class HivePartitionValues {
        private long nextPartitionId;
        private Map<String, Long> partitionNameToIdMap;
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
                long nextPartitionId,
                Map<String, Long> partitionNameToIdMap,
                Map<Long, List<UniqueId>> idToUniqueIdsMap,
                Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap,
                Map<Long, List<String>> partitionValuesMap) {
            this.idToPartitionItem = idToPartitionItem;
            this.uidToPartitionRange = uidToPartitionRange;
            this.rangeToId = rangeToId;
            this.singleColumnRangeMap = singleColumnRangeMap;
            this.nextPartitionId = nextPartitionId;
            this.partitionNameToIdMap = partitionNameToIdMap;
            this.idToUniqueIdsMap = idToUniqueIdsMap;
            this.singleUidToColumnRangeMap = singleUidToColumnRangeMap;
            this.partitionValuesMap = partitionValuesMap;
        }

        public HivePartitionValues copy() {
            HivePartitionValues copy = new HivePartitionValues();
            copy.setNextPartitionId(nextPartitionId);
            copy.setPartitionNameToIdMap(partitionNameToIdMap == null ? null : Maps.newHashMap(partitionNameToIdMap));
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
}

