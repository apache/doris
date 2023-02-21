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

package org.apache.doris.nereids;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.PartitionRange;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Context used in cache.
 */
public class CacheContext {
    private OlapTable lastOlapTable;

    private Partition lastPartition;

    private String cacheKey;

    private PartitionRange range;

    private Cache.HitRange hitRange;

    private InternalService.PFetchCacheResult cacheResult;

    private int countNewTable;

    public CacheContext() {
        countNewTable = 0;
        hitRange = Cache.HitRange.None;
    }

    public OlapTable getLastOlapTable() {
        return lastOlapTable;
    }

    public Partition getLastPartition() {
        return lastPartition;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public PartitionRange getRange() {
        return range;
    }

    public Cache.HitRange getHitRange() {
        return hitRange;
    }

    public InternalService.PFetchCacheResult getCacheResult() {
        return cacheResult;
    }

    public void setLastOlapTable(OlapTable lastOlapTable) {
        this.lastOlapTable = lastOlapTable;
    }

    public void setLastPartition(Partition lastPartition) {
        this.lastPartition = lastPartition;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public void setRange(PartitionRange range) {
        this.range = range;
    }

    public void setHitRange(Cache.HitRange hitRange) {
        this.hitRange = hitRange;
    }

    public void setCacheResult(InternalService.PFetchCacheResult cacheResult) {
        this.cacheResult = cacheResult;
    }

    /**
     * Check the last version time of olapScans.
     */
    public boolean checkOlapScans(Set<OlapScan> olapScans) {
        long now = System.currentTimeMillis();
        long interval = Config.cache_last_version_interval_second * 1000L;
        countNewTable = 0;
        for (OlapScan olapScan : olapScans) {
            OlapTable olapTable = olapScan.getTable();
            Optional<Partition> partition = olapTable.getAllPartitions().stream().max(
                    Comparator.comparing(i -> i.getVisibleVersionTime())
            );
            if (!partition.isPresent()) {
                continue;
            }
            if ((now - partition.get().getVisibleVersionTime()) < interval) {
                ++countNewTable;
            }
            if (lastPartition == null
                    || partition.get().getVisibleVersionTime() > lastPartition.getVisibleVersionTime()) {
                lastOlapTable = olapTable;
                lastPartition = partition.get();
            }
        }
        return isEnableCache();
    }

    /**
     * Check if agg expr contain partition column.
     */
    public boolean checkAggs(Set<LogicalAggregate> aggs) {
        String columnName = getPartColumn().getName();
        return !aggs.isEmpty() && aggs.stream().allMatch(agg -> {
            boolean noneMatchColumn = agg.getGroupByExpressions().stream().noneMatch(expr ->
                    expr instanceof SlotReference
                            && ((SlotReference) expr).getName().equals(columnName));
            if (noneMatchColumn) {
                return false;
            }

            Set<OlapScan> olapScans = (Set<OlapScan>) agg.collect(OlapScan.class::isInstance);
            return olapScans.stream().allMatch(olapScan ->
                    olapScan.getSelectedPartitionIds().size() <= 1);
        });
    }

    public boolean isEnableSqlCache() {
        return isEnableCache() && countNewTable == 0;
    }

    public boolean isEnablePartitionCache() {
        return isEnableCache() && countNewTable < 2;
    }

    public boolean isEnableCache() {
        return lastOlapTable != null
            && lastPartition != null
            && lastOlapTable.getPartitionInfo().getPartitionColumns().size() == 1;
    }

    public boolean isCacheSuccess() {
        return cacheResult != null && cacheResult.getStatus() == InternalService.PCacheStatus.CACHE_OK;
    }

    public Column getPartColumn() {
        List<Column> columnList = lastOlapTable.getPartitionInfo().getPartitionColumns();
        Preconditions.checkArgument(columnList.size() == 1, "empty column list");
        return columnList.get(0);
    }

    public org.apache.doris.catalog.RangePartitionInfo getRangePartitionInfo() {
        return range == null ? null : range.getRangePartitionInfo();
    }

    public boolean isSqlCacheSuccess() {
        return isEnableSqlCache() && isCacheSuccess();
    }

    public boolean isPartitionCacheSuccess() {
        return isEnablePartitionCache() && isCacheSuccess();
    }
}
