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
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.PartitionRange;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Context used in cache.
 */
public class CacheContext {
    private OlapTable lastOlapTable;

    private Partition lastPartition;

    private String cacheKey;

    private Optional<Column> partColumn;

    private PartitionRange range;

    private Cache.HitRange hitRange = Cache.HitRange.None;

    private InternalService.PFetchCacheResult cacheResult;

    private int countNewTable = 0;

    private Set<Expression> partitionConjuncts = new HashSet<>();

    private Set<Expression> otherConjuncts = new HashSet<>();

    public CacheContext() {}

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

    public Set<Expression> getMissConjuncts() {
        List<PartitionRange.PartitionSingle> missedRange = new ArrayList<>();
        hitRange = range.buildDiskPartitionRange(missedRange);
        Set<Expression> missedConjuncts = range.rewriteConjuncts(partitionConjuncts, missedRange);
        missedConjuncts.addAll(otherConjuncts);
        return missedConjuncts;
    }

    public Set<Expression> getHitConjuncts() {
        List<PartitionRange.PartitionSingle> hitRange = range.getHitPartitionRange();
        Set<Expression> hitConjuncts = range.rewriteConjuncts(partitionConjuncts, hitRange);
        hitConjuncts.addAll(otherConjuncts);
        return hitConjuncts;
    }

    /**
     * Check the last version time of olapScans.
     */
    public boolean checkOlapScans(Set<Scan> olapScans) {
        long now = System.currentTimeMillis();
        long interval = Config.cache_last_version_interval_second * 1000L;
        for (Scan scan : olapScans) {
            if (!(scan instanceof OlapScan)) {
                return false;
            }
            OlapTable olapTable = ((OlapScan) scan).getTable();
            Optional<Long> partitionId = ((OlapScan) scan).getSelectedPartitionIds().stream().max(
                    Comparator.comparing(i -> olapTable.getPartition(i).getVisibleVersionTime())
            );
            if (!partitionId.isPresent()) {
                continue;
            }
            Partition partition = olapTable.getPartition(partitionId.get());
            if ((now - partition.getVisibleVersionTime()) < interval) {
                ++countNewTable;
            }
            if (lastPartition == null
                    || partition.getVisibleVersionTime() > lastPartition.getVisibleVersionTime()) {
                lastOlapTable = olapTable;
                lastPartition = partition;
            }
        }
        if (lastOlapTable != null && lastPartition != null) {
            List<Column> columnList = lastOlapTable.getPartitionInfo().getPartitionColumns();
            if (columnList.size() == 1) {
                partColumn = Optional.ofNullable(columnList.get(0));
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if agg expr contain partition column.
     */
    public boolean checkAggs(Set<LogicalAggregate> aggs) {
        return partColumn.isPresent()
                && !aggs.isEmpty()
                && aggs.stream().allMatch(agg -> checkAggColumn(agg) && checkAggScan(agg));
    }

    private boolean checkAggColumn(LogicalAggregate agg) {
        return agg.getInputSlots().stream().anyMatch(slot -> slot instanceof SlotReference
                && ((SlotReference) slot).getColumn().equals(partColumn));
    }

    private boolean checkAggScan(LogicalAggregate agg) {
        Set<OlapScan> olapScans = (Set<OlapScan>) agg.collect(OlapScan.class::isInstance);
        return !olapScans.isEmpty() && olapScans.stream().allMatch(olapScan ->
            olapScan.getSelectedPartitionIds().size() <= 1);
    }

    /**
     * Check if predicate expr contain partition column.
     */
    public boolean checkPredicates(Set<LogicalFilter> predicates) {
        return partColumn.isPresent() && predicates.stream()
                .filter(p -> checkConjunct(p.getConjuncts()))
                .count() == 1;
    }

    private boolean checkConjunct(Set<Expression> conjuncts) {
        Map<Boolean, List<Expression>> splitConjunts = conjuncts.stream().collect(
                Collectors.partitioningBy(this::checkCloumn)
        );
        Set<Expression> relatedConjuncts = new HashSet<>(splitConjunts.get(true));
        Set<Expression> unrelatedConjuncts = new HashSet<>(splitConjunts.get(false));
        // 2 bounds: upper and lower bounds
        if (relatedConjuncts.size() == 2) {
            this.partitionConjuncts.addAll(relatedConjuncts);
            this.otherConjuncts.addAll(unrelatedConjuncts);
            return true;
        } else {
            this.otherConjuncts.addAll(unrelatedConjuncts);
            return false;
        }
    }

    private boolean checkCloumn(Expression expr) {
        if (expr instanceof LessThan
                || expr instanceof LessThanEqual
                || expr instanceof GreaterThan
                || expr instanceof GreaterThanEqual) {

            List<Slot> slots = ((ImmutableSet) expr.getInputSlots()).asList();
            if (slots.size() == 1 && slots.get(0) instanceof SlotReference) {
                return ((SlotReference) slots.get(0)).getColumn().equals(partColumn);
            }
        }
        return false;
    }

    /**
     *  generate key and range for partition cache
     */
    public void initPartitionCache() {
        Preconditions.checkArgument(isEnablePartitionCache(), "partition cache is not enabled");
        cacheKey = Utils.toSqlString("",
                "table", lastOlapTable,
                "partitionColumn", partColumn.get().getName(),
                "otherConjucts", otherConjuncts
        );
        range = new PartitionRange(
                partitionConjuncts,
                lastOlapTable,
                (RangePartitionInfo) lastOlapTable.getPartitionInfo());
    }

    public boolean isEnableSqlCache() {
        return lastOlapTable != null && lastPartition != null && countNewTable == 0;
    }

    public boolean isEnablePartitionCache() {
        return lastOlapTable != null && lastPartition != null && partColumn.isPresent()
                && lastOlapTable.getPartitionInfo().getType() == PartitionType.RANGE
                && countNewTable < 2 && partitionConjuncts.size() == 2;
    }

    public Optional<Column> getPartColumn() {
        return partColumn;
    }

    public org.apache.doris.catalog.RangePartitionInfo getRangePartitionInfo() {
        return range == null ? null : range.getRangePartitionInfo();
    }

    public boolean isCacheSuccess() {
        return cacheResult != null
                && cacheResult.getStatus() == InternalService.PCacheStatus.CACHE_OK;
    }
}
