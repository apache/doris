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
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionColumnFilterConverter;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.HashDistributionPruner;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * prune bucket
 */
public class PruneOlapScanTablet extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan().when(scan -> scan.getSelectedTabletIds().isEmpty())).thenApply(ctx -> {
            LogicalFilter<LogicalOlapScan> filter = ctx.root;

            LogicalOlapScan olapScan = filter.child();
            OlapTable table = olapScan.getTable();
            Builder<Long> selectedTabletIdsBuilder = ImmutableList.builder();

            Map<String, PartitionColumnFilter> filterMap = new CaseInsensitiveMap();

            for (Expression conjunct : filter.getConjuncts()) {
                if (conjunct instanceof EqualTo && conjunct.child(0).isSlot()) {
                    Slot slot = (Slot) conjunct.child(0);
                    if (slot instanceof SlotReference && !((SlotReference) slot).isVisible()) {
                        continue;
                    }
                }
                Optional<Expression> conjunctOpt = ExpressionUtils.checkAndMaybeCommute(conjunct);
                if (conjunctOpt.isPresent()) {
                    new ExpressionColumnFilterConverter(filterMap).convert(conjunctOpt.get());
                }
            }

            if (olapScan.getManuallySpecifiedTabletIds().isEmpty()) {
                for (Long id : olapScan.getSelectedPartitionIds()) {
                    Partition partition = table.getPartition(id);
                    MaterializedIndex index = partition.getIndex(olapScan.getSelectedIndexId());
                    boolean isBaseIndexSelected = olapScan.getSelectedIndexId() == olapScan.getTable().getBaseIndexId();
                    Collection<Long> prunedTabletIds = getSelectedTabletIds(
                            olapScan.getTable().getSchemaByIndexId(olapScan.getSelectedIndexId()),
                            filterMap, index, isBaseIndexSelected, partition.getDistributionInfo());
                    selectedTabletIdsBuilder.addAll(prunedTabletIds);
                }
            } else {
                selectedTabletIdsBuilder.addAll(olapScan.getManuallySpecifiedTabletIds());
            }
            List<Long> selectedTabletIds = selectedTabletIdsBuilder.build();
            if ((selectedTabletIds.isEmpty() && olapScan.getManuallySpecifiedTabletIds().isEmpty())) {
                return null;
            } else if (!selectedTabletIds.isEmpty() && !olapScan.getManuallySpecifiedTabletIds().isEmpty()
                    && new HashSet<>(selectedTabletIds).equals(
                            new HashSet<>(olapScan.getManuallySpecifiedTabletIds()))) {
                return null;
            }
            return filter.withChildren(olapScan.withSelectedTabletIds(selectedTabletIds));
        }).toRule(RuleType.OLAP_SCAN_TABLET_PRUNE);
    }

    private Collection<Long> getSelectedTabletIds(List<Column> schema, Map<String, PartitionColumnFilter> filterMap,
            MaterializedIndex index, boolean isBaseIndexSelected, DistributionInfo info) {
        if (info.getType() != DistributionInfoType.HASH) {
            return index.getTabletIdsInOrder();
        }
        HashDistributionInfo hashInfo = (HashDistributionInfo) info;
        return new HashDistributionPruner(schema, index.getTabletIdsInOrder(),
                hashInfo.getDistributionColumns(),
                filterMap,
                hashInfo.getBucketNum(),
                isBaseIndexSelected).prune();
    }
}
