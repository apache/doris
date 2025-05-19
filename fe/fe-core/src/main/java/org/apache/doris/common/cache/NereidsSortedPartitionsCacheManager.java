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

package org.apache.doris.common.cache;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.rules.expression.rules.MultiColumnBound;
import org.apache.doris.nereids.rules.expression.rules.PartitionItemToRange;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndId;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndRange;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Range;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

/**
 * This cache is used to sort the table partitions by range, so we can do binary search to skip
 * filter the huge numbers of partitions, for example, the table partition column is `dt date`
 * and one date for one partition, range from '2017-01-01' to '2025-01-01', for partition predicate
 * `where dt = '2024-12-24'`, we can fast jump to '2024-12-24' within few partition range comparison,
 * and the QPS can be improved
 */
public class NereidsSortedPartitionsCacheManager {
    private static final Logger LOG = LogManager.getLogger(NereidsSortedPartitionsCacheManager.class);
    private volatile Cache<TableIdentifier, PartitionCacheContext> partitionCaches;

    public NereidsSortedPartitionsCacheManager() {
        partitionCaches = buildCaches(
                Config.cache_partition_meta_table_manage_num,
                Config.expire_cache_partition_meta_table_in_fe_second
        );
    }

    public Optional<SortedPartitionRanges<?>> get(
            SupportBinarySearchFilteringPartitions table, CatalogRelation scan) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && !connectContext.getSessionVariable().enableBinarySearchFilteringPartitions) {
            return Optional.empty();
        }

        DatabaseIf<?> database = table.getDatabase();
        if (database == null) {
            return Optional.empty();
        }
        CatalogIf<?> catalog = database.getCatalog();
        if (catalog == null) {
            return Optional.empty();
        }
        TableIdentifier key = new TableIdentifier(
                catalog.getName(), database.getFullName(), table.getName());
        PartitionCacheContext partitionCacheContext = partitionCaches.getIfPresent(key);

        try {
            if (partitionCacheContext == null) {
                return Optional.ofNullable(loadCache(key, table, scan));
            }
            if (table.getId() != partitionCacheContext.tableId
                    || !Objects.equals(table.getPartitionMetaVersion(scan),
                    partitionCacheContext.partitionMetaVersion)) {
                partitionCaches.invalidate(key);
                return Optional.ofNullable(loadCache(key, table, scan));
            }
        } catch (Throwable t) {
            LOG.warn("Failed to load cache for table {}, key {}.", table.getName(), key, t);
            partitionCaches.invalidate(key);
            return Optional.empty();
        }
        return Optional.of(partitionCacheContext.sortedPartitionRanges);
    }

    private SortedPartitionRanges<?> loadCache(
            TableIdentifier key, SupportBinarySearchFilteringPartitions table, CatalogRelation scan)
            throws RpcException {
        long now = System.currentTimeMillis();
        long partitionMetaLoadTime = table.getPartitionMetaLoadTimeMillis(scan);

        // if insert too frequently, we will skip sort partitions
        if (now <= partitionMetaLoadTime || (now - partitionMetaLoadTime) <= (10 * 1000)) {
            return null;
        }

        Map<?, PartitionItem> unsortedMap = table.getOriginPartitions(scan);
        List<Entry<?, PartitionItem>> unsortedList = Lists.newArrayList(unsortedMap.entrySet());
        List<PartitionItemAndRange<?>> sortedRanges = Lists.newArrayListWithCapacity(unsortedMap.size());
        List<PartitionItemAndId<?>> defaultPartitions = Lists.newArrayList();
        for (Entry<?, PartitionItem> entry : unsortedList) {
            PartitionItem partitionItem = entry.getValue();
            Object id = entry.getKey();
            if (!partitionItem.isDefaultPartition()) {
                List<Range<MultiColumnBound>> ranges = PartitionItemToRange.toRanges(partitionItem);
                for (Range<MultiColumnBound> range : ranges) {
                    sortedRanges.add(new PartitionItemAndRange<>(id, partitionItem, range));
                }
            } else {
                defaultPartitions.add(new PartitionItemAndId<>(id, partitionItem));
            }
        }

        sortedRanges.sort((o1, o2) -> {
            Range<MultiColumnBound> span1 = o1.range;
            Range<MultiColumnBound> span2 = o2.range;
            int result = span1.lowerEndpoint().compareTo(span2.lowerEndpoint());
            if (result != 0) {
                return result;
            }
            result = span1.upperEndpoint().compareTo(span2.upperEndpoint());
            return result;
        });
        SortedPartitionRanges<?> sortedPartitionRanges = new SortedPartitionRanges(
                sortedRanges, defaultPartitions
        );
        PartitionCacheContext context = new PartitionCacheContext(
                table.getId(), table.getPartitionMetaVersion(scan), sortedPartitionRanges);
        partitionCaches.put(key, context);
        return sortedPartitionRanges;
    }

    private static Cache<TableIdentifier, PartitionCacheContext> buildCaches(
            int sortedPartitionTableManageNum, int expireSortedPartitionTableInFeSecond) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                // auto evict cache when jvm memory too low
                .softValues();
        if (sortedPartitionTableManageNum > 0) {
            cacheBuilder = cacheBuilder.maximumSize(sortedPartitionTableManageNum);
        }
        if (expireSortedPartitionTableInFeSecond > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireSortedPartitionTableInFeSecond));
        }

        return cacheBuilder.build();
    }

    public static synchronized void updateConfig() {
        Env currentEnv = Env.getCurrentEnv();
        if (currentEnv == null) {
            return;
        }
        NereidsSortedPartitionsCacheManager cacheManager = currentEnv.getSortedPartitionsCacheManager();
        if (cacheManager == null) {
            return;
        }

        Cache<TableIdentifier, PartitionCacheContext> caches = buildCaches(
                Config.cache_partition_meta_table_manage_num,
                Config.expire_cache_partition_meta_table_in_fe_second
        );
        caches.putAll(cacheManager.partitionCaches.asMap());
        cacheManager.partitionCaches = caches;
    }

    @Data
    @AllArgsConstructor
    private static class TableIdentifier {
        public final String catalog;
        public final String db;
        public final String table;
    }

    private static class PartitionCacheContext {
        private final long tableId;
        private final Object partitionMetaVersion;
        private final SortedPartitionRanges sortedPartitionRanges;

        public PartitionCacheContext(
                long tableId, Object partitionMetaVersion, SortedPartitionRanges sortedPartitionRanges) {
            this.tableId = tableId;
            this.partitionMetaVersion
                    = Objects.requireNonNull(partitionMetaVersion, "partitionMetaVersion cannot be null");
            this.sortedPartitionRanges = sortedPartitionRanges;
        }

        @Override
        public String toString() {
            return "PartitionCacheContext(tableId="
                    + tableId + ", tableVersion=" + partitionMetaVersion
                    + ", partitionNum=" + sortedPartitionRanges.sortedPartitions.size() + ")";
        }
    }

    // NOTE: used in Config.cache_partition_meta_table_manage_num.callbackClassString and
    //       Config.expire_cache_partition_meta_table_in_fe_second.callbackClassString,
    //       don't remove it!
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            NereidsSortedPartitionsCacheManager.updateConfig();
        }
    }
}
