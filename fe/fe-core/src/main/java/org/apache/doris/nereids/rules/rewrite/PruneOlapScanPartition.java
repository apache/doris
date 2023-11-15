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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

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
public class PruneOlapScanPartition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan()).when(p -> !p.child().isPartitionPruned()).thenApply(ctx -> {
            LogicalFilter<LogicalOlapScan> filter = ctx.root;
            LogicalOlapScan scan = filter.child();
            OlapTable table = scan.getTable();
            Set<String> partitionColumnNameSet = Utils.execWithReturnVal(table::getPartitionColumnNames);
            if (partitionColumnNameSet.isEmpty()) {
                return filter;
            }

            Map<String, Slot> scanOutput = scan.getOutput()
                    .stream()
                    .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));

            PartitionInfo partitionInfo = table.getPartitionInfo();
            List<Slot> partitionSlots = partitionInfo.getPartitionColumns()
                    .stream()
                    .map(column -> scanOutput.get(column.getName().toLowerCase()))
                    .collect(Collectors.toList());

            List<Long> prunedPartitions = new ArrayList<>(PartitionPruner.prune(
                    partitionSlots, filter.getPredicate(), partitionInfo, ctx.cascadesContext,
                    PartitionTableType.OLAP));

            List<Long> manuallySpecifiedPartitions = scan.getManuallySpecifiedPartitions();
            if (!CollectionUtils.isEmpty(manuallySpecifiedPartitions)) {
                prunedPartitions.retainAll(manuallySpecifiedPartitions);
            }
            if (prunedPartitions.isEmpty()) {
                return new LogicalEmptyRelation(
                        ConnectContext.get().getStatementContext().getNextRelationId(),
                        filter.getOutput());
            }
            LogicalOlapScan rewrittenScan = scan.withSelectedPartitionIds(ImmutableList.copyOf(prunedPartitions));
            return new LogicalFilter<>(filter.getConjuncts(), rewrittenScan);
        }).toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE);
    }
}
