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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.TablePartitionValues.TablePartitionKey;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class HudiCachedPartitionProcessor extends HudiPartitionProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(HudiCachedPartitionProcessor.class);
    private final long catalogId;
    private final Executor executor;
    private final LoadingCache<TablePartitionKey, TablePartitionValues> partitionCache;

    public HudiCachedPartitionProcessor(long catalogId, ExecutorService executor) {
        this.catalogId = catalogId;
        this.executor = executor;
        CacheFactory partitionCacheFactory = new CacheFactory(
                OptionalLong.of(86400L),
                OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60),
                Config.max_hive_table_cache_num,
                false,
                null);
        this.partitionCache = partitionCacheFactory.buildCache(key -> new TablePartitionValues(), null, executor);
    }

    @Override
    public void cleanUp() {
        partitionCache.cleanUp();
    }

    @Override
    public void cleanDatabasePartitions(String dbName) {
        partitionCache.asMap().keySet().stream().filter(k -> k.getDbName().equals(dbName)).collect(Collectors.toList())
                .forEach(partitionCache::invalidate);
    }

    @Override
    public void cleanTablePartitions(String dbName, String tblName) {
        partitionCache.asMap().keySet().stream()
                .filter(k -> k.getDbName().equals(dbName) && k.getTblName().equals(tblName))
                .collect(Collectors.toList())
                .forEach(partitionCache::invalidate);
    }

    public TablePartitionValues getSnapshotPartitionValues(HMSExternalTable table,
            HoodieTableMetaClient tableMetaClient, String timestamp, boolean useHiveSyncPartition) {
        Preconditions.checkState(catalogId == table.getCatalog().getId());
        Option<String[]> partitionColumns = tableMetaClient.getTableConfig().getPartitionFields();
        if (!partitionColumns.isPresent()) {
            return null;
        }
        HoodieTimeline timeline = tableMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> lastInstant = timeline.lastInstant();
        if (!lastInstant.isPresent()) {
            return null;
        }
        long lastTimestamp = Long.parseLong(lastInstant.get().getTimestamp());
        if (Long.parseLong(timestamp) == lastTimestamp) {
            return getPartitionValues(table, tableMetaClient, useHiveSyncPartition);
        }
        List<String> partitionNameAndValues = getPartitionNamesBeforeOrEquals(timeline, timestamp);
        List<String> partitionNames = Arrays.asList(partitionColumns.get());
        TablePartitionValues partitionValues = new TablePartitionValues();
        partitionValues.addPartitions(partitionNameAndValues,
                partitionNameAndValues.stream().map(p -> parsePartitionValues(partitionNames, p))
                        .collect(Collectors.toList()), table.getPartitionColumnTypes());
        return partitionValues;
    }

    public TablePartitionValues getPartitionValues(HMSExternalTable table, HoodieTableMetaClient tableMetaClient,
                                                   boolean useHiveSyncPartition)
            throws CacheException {
        Preconditions.checkState(catalogId == table.getCatalog().getId());
        Option<String[]> partitionColumns = tableMetaClient.getTableConfig().getPartitionFields();
        if (!partitionColumns.isPresent()) {
            return null;
        }
        HoodieTimeline timeline = tableMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> lastInstant = timeline.lastInstant();
        if (!lastInstant.isPresent()) {
            return null;
        }
        try {
            long lastTimestamp = Long.parseLong(lastInstant.get().getTimestamp());
            TablePartitionValues partitionValues = partitionCache.get(
                    new TablePartitionKey(table.getDbName(), table.getName(), table.getPartitionColumnTypes()));
            partitionValues.readLock().lock();
            try {
                long lastUpdateTimestamp = partitionValues.getLastUpdateTimestamp();
                if (lastTimestamp <= lastUpdateTimestamp) {
                    return partitionValues;
                }
            } finally {
                partitionValues.readLock().unlock();
            }

            partitionValues.writeLock().lock();
            try {
                long lastUpdateTimestamp = partitionValues.getLastUpdateTimestamp();
                if (lastTimestamp <= lastUpdateTimestamp) {
                    return partitionValues;
                }
                HMSExternalCatalog catalog = (HMSExternalCatalog) table.getCatalog();
                List<String> partitionNames;
                if (useHiveSyncPartition) {
                    // When a Hudi table is synchronized to HMS, the partition information is also synchronized,
                    // so even if the metastore is not enabled in the Hudi table
                    //     (for example, if the Metastore is false for a Hudi table created with Flink),
                    // we can still obtain the partition information through the HMS API.
                    partitionNames = catalog.getClient().listPartitionNames(table.getDbName(), table.getName());
                    if (partitionNames.size() == 0) {
                        LOG.warn("Failed to get partitions from hms api, switch it from hudi api.");
                        partitionNames = getAllPartitionNames(tableMetaClient);
                    }
                } else {
                    partitionNames = getAllPartitionNames(tableMetaClient);
                }
                List<String> partitionColumnsList = Arrays.asList(partitionColumns.get());
                partitionValues.cleanPartitions();
                partitionValues.addPartitions(partitionNames,
                        partitionNames.stream().map(p -> parsePartitionValues(partitionColumnsList, p))
                                .collect(Collectors.toList()), table.getPartitionColumnTypes());
                partitionValues.setLastUpdateTimestamp(lastTimestamp);
                return partitionValues;
            } finally {
                partitionValues.writeLock().unlock();
            }
        } catch (Exception e) {
            LOG.warn("Failed to get hudi partitions", e);
            throw new CacheException("Failed to get hudi partitions: " + Util.getRootCauseMessage(e), e);
        }
    }
}
