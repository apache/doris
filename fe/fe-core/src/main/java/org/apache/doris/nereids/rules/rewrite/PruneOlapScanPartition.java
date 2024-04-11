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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                logicalFilter(logicalOlapScan())
                        .when(p -> !p.child().isPartitionPruned())
                        .thenApply(ctx -> prunePartitions(ctx.cascadesContext, ctx.root.child(), ctx.root))
                        .toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE),

                logicalFilter(logicalProject(logicalOlapScan()))
                        .when(p -> !p.child().child().isPartitionPruned())
                        .when(p -> p.child().hasPushedDownToProjectionFunctions())
                        .thenApply(ctx -> prunePartitions(ctx.cascadesContext, ctx.root.child().child(), ctx.root))
                        .toRule(RuleType.OLAP_SCAN_WITH_PROJECT_PARTITION_PRUNE)
        );
    }

    private <T extends Plan> Plan prunePartitions(CascadesContext ctx,
                    LogicalOlapScan scan, LogicalFilter<T> originalFilter) {
        OlapTable table = scan.getTable();
        Set<String> partitionColumnNameSet = Utils.execWithReturnVal(table::getPartitionColumnNames);
        if (partitionColumnNameSet.isEmpty()) {
            return originalFilter;
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
                return originalFilter;
            } else {
                partitionSlots.add(partitionSlot);
            }
        }

        List<Long> manuallySpecifiedPartitions = scan.getManuallySpecifiedPartitions();

        Map<Long, PartitionItem> idToPartitions;
        if (manuallySpecifiedPartitions.isEmpty()) {
            idToPartitions = partitionInfo.getIdToItem(false);
        } else {
            Map<Long, PartitionItem> allPartitions = partitionInfo.getAllPartitions();
            idToPartitions = allPartitions.keySet().stream()
                    .filter(id -> manuallySpecifiedPartitions.contains(id))
                    .collect(Collectors.toMap(Function.identity(), id -> allPartitions.get(id)));
        }
        List<Long> prunedPartitions = PartitionPruner.prune(
                partitionSlots, originalFilter.getPredicate(), idToPartitions, ctx,
                PartitionTableType.OLAP);
        if (prunedPartitions.isEmpty()) {
            return new LogicalEmptyRelation(
                    ConnectContext.get().getStatementContext().getNextRelationId(),
                    originalFilter.getOutput());
        }

        LogicalOlapScan rewrittenScan = scan.withSelectedPartitionIds(prunedPartitions);
        if (originalFilter.child() instanceof LogicalProject) {
            LogicalProject<LogicalOlapScan> rewrittenProject
                        = (LogicalProject<LogicalOlapScan>) originalFilter.child()
                                    .withChildren(ImmutableList.of(rewrittenScan));
            return new LogicalFilter<>(originalFilter.getConjuncts(), rewrittenProject);
        }
        return originalFilter.withChildren(ImmutableList.of(rewrittenScan));
    }
}
