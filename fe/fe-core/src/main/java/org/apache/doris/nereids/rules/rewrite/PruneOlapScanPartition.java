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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.OneRangePartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.rules.expression.rules.PartitionSlotInput;
import org.apache.doris.nereids.rules.expression.rules.TryEliminateUninterestedPredicates;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
            LogicalOlapScan rewrittenScan = scan.withSelectedPartitionIds(ImmutableList.copyOf(prunedPartitions));
            Set<PartitionItem> prunedPartitionItems = new HashSet<>();
            for (Long partitionId : prunedPartitions) {
                PartitionItem item = partitionInfo.getItem(partitionId);
                prunedPartitionItems.add(item);
            }
            boolean canPrunedFilterConjuncts = false;
            if (partitionInfo.getPartitionColumns().size() == 1) {
                // NOT support multi-partition key cases
                canPrunedFilterConjuncts = canPruneFilterConjuncts(filter, prunedPartitions,
                        partitionInfo.getIdToItem(false),
                        partitionSlots, ctx.cascadesContext, prunedPartitionItems);
            }
            if (canPrunedFilterConjuncts) {
                return rewrittenScan;
            } else {
                return new LogicalFilter<>(filter.getConjuncts(), rewrittenScan);
            }
        }).toRule(RuleType.OLAP_SCAN_PARTITION_PRUNE);
    }

    private boolean canPruneFilterConjuncts(LogicalFilter<LogicalOlapScan> filter, List<Long> prunedPartitions,
            Map<Long, PartitionItem> idToPartitions,
            List<Slot> partitionSlots, CascadesContext cascadesContext,
            Set<PartitionItem> prunedPartitionItems) {
        if (prunedPartitionItems.isEmpty() || filter.getConjuncts().isEmpty()) {
            return true;
        } else {
            boolean isRangePartItem = prunedPartitionItems.iterator().next() instanceof RangePartitionItem;
            if (isRangePartItem) {
                Expression prunedExpression = TryEliminateUninterestedPredicates.rewrite(
                        filter.getPredicate(), ImmutableSet.copyOf(partitionSlots), cascadesContext);
                // prunedConjuncts = ExpressionUtils.extractConjunctionToSet(prunedExpression);
                Set<RangePartitionItem> itemSet = idToPartitions.entrySet()
                        .stream()
                        .filter(f -> prunedPartitions.contains(f.getKey()))
                        .map(kv -> (RangePartitionItem) kv.getValue())
                        .collect(ImmutableSet.toImmutableSet());
                RangePartitionItem mergedPartitionItem = mergePartitionItem(itemSet,
                        ((SlotReference) partitionSlots.get(0)).getColumn().get());
                if (mergedPartitionItem == null) {
                    return false;
                } else {
                    OneRangePartitionEvaluator evaluator = (OneRangePartitionEvaluator) PartitionPruner
                            .toPartitionEvaluator(-1, mergedPartitionItem, partitionSlots, cascadesContext,
                                    PartitionTableType.OLAP, true);

                    Map<Slot, PartitionSlotInput> onePartitionInput = evaluator.getOnePartitionInputs().get(0);

                    return evaluator.checkEqualRange(prunedExpression, onePartitionInput);
                }
            } else {
                // TODO: support list partition
                return false;
            }
        }
    }

    private RangePartitionItem mergePartitionItem(Set<RangePartitionItem> partitionItemSet, Column partitionColumn) {
        List<RangePartitionItem> newPartitionItemList = new ArrayList<>(partitionItemSet);
        List<Boolean> visited = new ArrayList<>();
        for (int i = 0; i < newPartitionItemList.size(); i++) {
            visited.add(false);
        }
        RangePartitionItem mergedPartitionItem = newPartitionItemList.get(0);
        boolean matched = true;
        for (int i = 1; i < newPartitionItemList.size(); i++) {
            RangePartitionItem tempPartitionItem = null;
            int matchIndex = -1;
            for (int j = 1; j < newPartitionItemList.size(); j++) {
                if (!visited.get(j)) {
                    RangePartitionItem otherItm = newPartitionItemList.get(j);
                    tempPartitionItem = mergeTwoPartitionItem(mergedPartitionItem, otherItm, partitionColumn);
                    if (tempPartitionItem != null) {
                        matchIndex = j;
                        break;
                    }
                }
            }
            if (tempPartitionItem != null) {
                visited.set(matchIndex, true);
                mergedPartitionItem = tempPartitionItem;
            } else {
                matched = false;
                break;
            }
        }
        if (matched) {
            return mergedPartitionItem;
        } else {
            return null;
        }
    }

    private RangePartitionItem mergeTwoPartitionItem(RangePartitionItem item, RangePartitionItem other,
            Column partitionColumn) {
        String itemLowerBorder = item.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
        String itemUpperBorder = item.getItems().upperEndpoint().getKeys().get(0).getStringValue();
        String otherLowerBorder = other.getItems().lowerEndpoint().getKeys().get(0).getStringValue();
        String otherUpperBorder = other.getItems().upperEndpoint().getKeys().get(0).getStringValue();

        RangePartitionItem newPartitionItem;
        PartitionValue newLowerPartValue;
        PartitionValue newUpperPartValue;
        Range<PartitionKey> newPartitionKeyRange;

        if (itemUpperBorder.equals(otherLowerBorder)) {
            newLowerPartValue = new PartitionValue(itemLowerBorder);
            newUpperPartValue = new PartitionValue(otherUpperBorder);
        } else if (otherUpperBorder.equals(itemLowerBorder)) {
            newLowerPartValue = new PartitionValue(otherLowerBorder);
            newUpperPartValue = new PartitionValue(itemUpperBorder);
        } else {
            return null;
        }

        try {
            PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(newLowerPartValue),
                    Collections.singletonList(partitionColumn));
            PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(newUpperPartValue),
                    Collections.singletonList(partitionColumn));
            newPartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
            newPartitionItem = new RangePartitionItem(newPartitionKeyRange);
        } catch (AnalysisException | IllegalArgumentException e) {
            newPartitionItem = null;
        }

        return newPartitionItem;
    }
}
