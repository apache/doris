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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to prune partition of olap scan, should execute after SwapProjectAndFilter, MergeConsecutiveFilters,
 * MergeConsecutiveProjects and all predicate push down related rules.
 */
public class PruneOlapScanPartition implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalOlapScan()
                    .when(scan -> !scan.isPartitionPruned()
                            && !scan.getManuallySpecifiedTabletIds().isEmpty()
                            && scan.getTable().isPartitionedTable()
                    )
                    .thenApply(ctx -> {
                        // Case1: sql without filter condition, e.g. SELECT * FROM tbl (${tabletID})
                        LogicalOlapScan scan = ctx.root;
                        OlapTable table = scan.getTable();
                        return prunePartition(scan, table, null, ctx);
                    }).toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE),
                logicalFilter(logicalOlapScan()
                    .whenNot(LogicalOlapScan::isPartitionPruned))
                    .thenApply(ctx -> {
                        // Case2: sql with filter condition, e.g. SELECT * FROM tbl (${tabletID}) WHERE part_column='x'
                        LogicalFilter<LogicalOlapScan> filter = ctx.root;
                        LogicalOlapScan scan = filter.child();
                        OlapTable table = scan.getTable();
                        LogicalRelation rewrittenLogicalRelation = prunePartition(scan, table, filter, ctx);
                        if (rewrittenLogicalRelation == null) {
                            return null;
                        }
                        if (rewrittenLogicalRelation instanceof LogicalEmptyRelation) {
                            return rewrittenLogicalRelation;
                        } else {
                            LogicalOlapScan rewrittenScan = (LogicalOlapScan) rewrittenLogicalRelation;
                            return filter.withChildren(ImmutableList.of(rewrittenScan));
                        }
                    }).toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE)
        );
    }

    private LogicalRelation prunePartition(LogicalOlapScan scan,
                                      OlapTable table,
                                      LogicalFilter filter,
                                      MatchingContext ctx) {
        List<Long> prunedPartitionsByFilters = prunePartitionByFilters(scan, table, filter, ctx);
        List<Long> prunedPartitions = prunePartitionByTabletIds(scan, table, prunedPartitionsByFilters);
        if (prunedPartitions == null) {
            return null;
        }
        if (prunedPartitions.isEmpty()) {
            return new LogicalEmptyRelation(
                ConnectContext.get().getStatementContext().getNextRelationId(),
                ctx.root.getOutput());
        }
        return scan.withSelectedPartitionIds(prunedPartitions);
    }

    private List<Long> prunePartitionByFilters(LogicalOlapScan scan,
                                               OlapTable table,
                                               LogicalFilter filter,
                                               MatchingContext ctx) {
        Set<String> partitionColumnNameSet = Utils.execWithReturnVal(table::getPartitionColumnNames);
        if (partitionColumnNameSet.isEmpty()) {
            return null;
        }
        List<Slot> output = scan.getOutput();
        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        List<Slot> partitionSlots = new ArrayList<>(partitionColumns.size());
        for (Column column : partitionColumns) {
            Slot partitionSlot = null;
            // loop search is faster than build a map
            for (Slot slot : output) {
                if (slot.getName().equalsIgnoreCase(column.getName())) {
                    partitionSlot = slot;
                    break;
                }
            }
            if (partitionSlot == null) {
                return null;
            } else {
                partitionSlots.add(partitionSlot);
            }
        }
        NereidsSortedPartitionsCacheManager sortedPartitionsCacheManager = Env.getCurrentEnv()
                .getSortedPartitionsCacheManager();
        List<Long> manuallySpecifiedPartitions = scan.getManuallySpecifiedPartitions();
        Map<Long, PartitionItem> idToPartitions;
        Optional<SortedPartitionRanges<Long>> sortedPartitionRanges = Optional.empty();
        if (manuallySpecifiedPartitions.isEmpty()) {
            Optional<SortedPartitionRanges<?>> sortedPartitionRangesOpt
                    = sortedPartitionsCacheManager.get(table, scan);
            if (sortedPartitionRangesOpt.isPresent()) {
                sortedPartitionRanges = (Optional) sortedPartitionRangesOpt;
            }
            idToPartitions = partitionInfo.getIdToItem(false);
        } else {
            Map<Long, PartitionItem> allPartitions = partitionInfo.getAllPartitions();
            idToPartitions = allPartitions.keySet().stream()
                    .filter(manuallySpecifiedPartitions::contains)
                    .collect(Collectors.toMap(Function.identity(), allPartitions::get));
        }
        if (filter != null) {
            List<Long> prunedPartitions = PartitionPruner.prune(
                    partitionSlots, filter.getPredicate(), idToPartitions, ctx.cascadesContext,
                    PartitionTableType.OLAP, sortedPartitionRanges);
            return prunedPartitions;
        } else if (!manuallySpecifiedPartitions.isEmpty()) {
            return Utils.fastToImmutableList(idToPartitions.keySet());
        } else {
            return null;
        }
    }

    private List<Long> prunePartitionByTabletIds(LogicalOlapScan scan,
                                                 OlapTable table,
                                                 List<Long> prunedPartitionsByFilters) {
        if (scan.getManuallySpecifiedTabletIds().size() == 0
                || (prunedPartitionsByFilters != null && prunedPartitionsByFilters.isEmpty())) {
            // `prunedPartitionsByFilters is not null and is empty` means empty partitions after pruner
            return prunedPartitionsByFilters;
        }

        Set<Long> selectedPartitions = new LinkedHashSet<>();
        if (prunedPartitionsByFilters != null) {
            selectedPartitions.addAll(prunedPartitionsByFilters);
        }

        Set<Long> manuallySpecifiedTabletIds = ImmutableSet.copyOf(scan.getManuallySpecifiedTabletIds());
        List<Long> selectPartitionIds = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            if (!selectedPartitions.isEmpty() && !selectedPartitions.contains(partition.getId())) {
                continue;
            }
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Tablet tablet : baseIndex.getTablets()) {
                if (manuallySpecifiedTabletIds.contains(tablet.getId())) {
                    selectPartitionIds.add(partition.getId());
                    break;
                }
            }
        }
        return selectPartitionIds;
    }
}
