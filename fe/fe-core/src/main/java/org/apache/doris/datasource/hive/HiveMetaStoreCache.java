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
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.metric.GaugeMetric;
import org.apache.doris.metric.Metric;
import org.apache.doris.metric.MetricLabel;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPrunerV2Base.UniqueId;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// The cache of a hms catalog. 3 kind of caches:
// 1. partitionValuesCache: cache the partition values of a table, for partition prune.
// 2. partitionCache: cache the partition info(location, input format, etc.) of a table.
// 3. fileCache: cache the files of a location.
public class HiveMetaStoreCache {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreCache.class);
    private static final int MIN_BATCH_FETCH_PARTITION_NUM = 50;

    private HMSExternalCatalog catalog;

    // cache from <dbname-tblname> -> <values of partitions>
    private LoadingCache<PartitionValueCacheKey, HivePartitionValues> partitionValuesCache;
    // cache from <dbname-tblname-partition_values> -> <partition info>
    private LoadingCache<PartitionCacheKey, HivePartition> partitionCache;
    // cache from <location> -> <file list>
    private LoadingCache<FileCacheKey, ImmutableList<InputSplit>> fileCache;

    public HiveMetaStoreCache(HMSExternalCatalog catalog, Executor executor) {
        this.catalog = catalog;
        init(executor);
        initMetrics();
    }

    private void init(Executor executor) {
        partitionValuesCache = CacheBuilder.newBuilder().maximumSize(Config.max_hive_partition_cache_num)
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

        fileCache = CacheBuilder.newBuilder().maximumSize(Config.max_external_file_cache_num)
                .expireAfterAccess(Config.external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
                .build(CacheLoader.asyncReloading(new CacheLoader<FileCacheKey, ImmutableList<InputSplit>>() {
                    @Override
                    public ImmutableList<InputSplit> load(FileCacheKey key) throws Exception {
                        return loadFiles(key);
                    }
                }, executor));
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
                return fileCache.size();
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
        long idx = 0;
        for (String partitionName : partitionNames) {
            idToPartitionItem.put(idx++, toListPartitionItem(partitionName, key.types));
        }

        Map<UniqueId, Range<PartitionKey>> uidToPartitionRange = null;
        Map<Range<PartitionKey>, UniqueId> rangeToId = null;
        RangeMap<ColumnBound, UniqueId> singleColumnRangeMap = null;
        if (key.types.size() > 1) {
            // uidToPartitionRange and rangeToId are only used for multi-column partition
            uidToPartitionRange = ListPartitionPrunerV2.genUidToPartitionRange(idToPartitionItem);
            rangeToId = ListPartitionPrunerV2.genRangeToId(uidToPartitionRange);
        } else {
            Preconditions.checkState(key.types.size() == 1, key.types);
            // singleColumnRangeMap is only used for single-column partition
            singleColumnRangeMap = ListPartitionPrunerV2.genSingleColumnRangeMap(idToPartitionItem);
        }
        return new HivePartitionValues(idToPartitionItem, uidToPartitionRange, rangeToId, singleColumnRangeMap);
    }

    private ListPartitionItem toListPartitionItem(String partitionName, List<Type> types) {
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        String[] parts = partitionName.split("/");
        Preconditions.checkState(parts.length == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String part : parts) {
            String[] kv = part.split("=");
            Preconditions.checkState(kv.length == 2, partitionName);
            values.add(new PartitionValue(kv[1]));
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
        return new HivePartition(sd.getInputFormat(), sd.getLocation(), key.values);
    }

    private ImmutableList<InputSplit> loadFiles(FileCacheKey key) {
        String finalLocation = convertToS3IfNecessary(key.location);
        Configuration conf = getConfiguration();
        JobConf jobConf = new JobConf(conf);
        // For Tez engine, it may generate subdirectories for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /user/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to set this config to support visit dir recursively.
        // Otherwise, getSplits() may throw exception: "Not a file xxx"
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        FileInputFormat.setInputPaths(jobConf, finalLocation);
        try {
            InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(conf, key.inputFormat, false);
            InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
            if (LOG.isDebugEnabled()) {
                LOG.debug("load #{} files for {} in catalog {}", splits.length, key, catalog.getName());
            }
            return ImmutableList.copyOf(splits);
        } catch (Exception e) {
            throw new CacheException("failed to get input splits for %s in catalog %s", e, key, catalog.getName());
        }
    }

    // convert oss:// to s3://
    private String convertToS3IfNecessary(String location) {
        LOG.debug("try convert location to s3 prefix: " + location);
        if (location.startsWith(FeConstants.FS_PREFIX_COS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_BOS)
                || location.startsWith(FeConstants.FS_PREFIX_OSS)
                || location.startsWith(FeConstants.FS_PREFIX_S3A)
                || location.startsWith(FeConstants.FS_PREFIX_S3N)) {
            int pos = location.indexOf("://");
            if (pos == -1) {
                throw new RuntimeException("No '://' found in location: " + location);
            }
            return "s3" + location.substring(pos);
        }
        return location;
    }

    private Configuration getConfiguration() {
        Configuration configuration = new HdfsConfiguration();
        for (Map.Entry<String, String> entry : catalog.getCatalogProperty().getProperties().entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = catalog.getCatalogProperty().getS3Properties();
        for (Map.Entry<String, String> entry : s3Properties.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
        return configuration;
    }

    public HivePartitionValues getPartitionValues(String dbName, String tblName, List<Type> types) {
        PartitionValueCacheKey key = new PartitionValueCacheKey(dbName, tblName, types);
        try {
            return partitionValuesCache.get(key);
        } catch (ExecutionException e) {
            throw new CacheException("failed to get partition values for %s in catalog %s", e, key, catalog.getName());
        }
    }

    public List<InputSplit> getFilesByPartitions(List<HivePartition> partitions) {
        long start = System.currentTimeMillis();
        List<FileCacheKey> keys = Lists.newArrayListWithExpectedSize(partitions.size());
        partitions.stream().forEach(p -> keys.add(new FileCacheKey(p.getPath(), p.getInputFormat())));

        Stream<FileCacheKey> stream;
        if (partitions.size() < MIN_BATCH_FETCH_PARTITION_NUM) {
            stream = keys.stream();
        } else {
            stream = keys.parallelStream();
        }
        List<ImmutableList<InputSplit>> fileLists = stream.map(k -> {
            try {
                return fileCache.get(k);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        List<InputSplit> retFiles = Lists.newArrayListWithExpectedSize(
                fileLists.stream().mapToInt(l -> l.size()).sum());
        fileLists.stream().forEach(l -> retFiles.addAll(l));
        LOG.debug("get #{} files from #{} partitions in catalog {} cost: {} ms",
                retFiles.size(), partitions.size(), catalog.getName(), (System.currentTimeMillis() - start));
        return retFiles;
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
                    fileCache.invalidate(new FileCacheKey(partition.getPath(), null));
                    partitionCache.invalidate(partKey);
                }
            }
            partitionValuesCache.invalidate(key);
            LOG.debug("invalid table cache for {}.{} in catalog {}, cache num: {}, cost: {} ms",
                    dbName, tblName, catalog.getName(), partitionValues.partitionValuesMap.size(),
                    (System.currentTimeMillis() - start));
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
        fileCache.invalidateAll();
        LOG.debug("invalid all meta cache in catalog {}", catalog.getName());
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
        private String location;
        // not in key
        private String inputFormat;

        public FileCacheKey(String location, String inputFormat) {
            this.location = location;
            this.inputFormat = inputFormat;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileCacheKey)) {
                return false;
            }
            return location.equals(((FileCacheKey) obj).location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }

        @Override
        public String toString() {
            return "FileCacheKey{" + "location='" + location + '\'' + ", inputFormat='" + inputFormat + '\'' + '}';
        }
    }

    @Data
    public static class HivePartitionValues {
        private Map<Long, PartitionItem> idToPartitionItem;
        private Map<Long, List<String>> partitionValuesMap = Maps.newHashMap();
        private Map<UniqueId, Range<PartitionKey>> uidToPartitionRange;
        private Map<Range<PartitionKey>, UniqueId> rangeToId;
        private RangeMap<ColumnBound, UniqueId> singleColumnRangeMap;

        public HivePartitionValues(Map<Long, PartitionItem> idToPartitionItem,
                Map<UniqueId, Range<PartitionKey>> uidToPartitionRange,
                Map<Range<PartitionKey>, UniqueId> rangeToId,
                RangeMap<ColumnBound, UniqueId> singleColumnRangeMap) {
            this.idToPartitionItem = idToPartitionItem;
            for (Map.Entry<Long, PartitionItem> entry : this.idToPartitionItem.entrySet()) {
                partitionValuesMap.put(entry.getKey(),
                        ((ListPartitionItem) entry.getValue()).getItems().get(0).getPartitionValuesAsStringList());
            }
            this.uidToPartitionRange = uidToPartitionRange;
            this.rangeToId = rangeToId;
            this.singleColumnRangeMap = singleColumnRangeMap;
        }
    }
}
