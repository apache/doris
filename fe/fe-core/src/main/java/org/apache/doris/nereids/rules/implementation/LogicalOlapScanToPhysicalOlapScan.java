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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.properties.DistributionSpecStorageGather;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;

/**
 * Implementation rule that convert logical OlapScan to physical OlapScan.
 */
public class LogicalOlapScanToPhysicalOlapScan extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapScan().thenApplyMulti(ctx -> {
            LogicalOlapScan olapScan = ctx.root;
            DistributionSpec distribution = convertDistribution(olapScan);
            if (SessionVariable.canUseNereidsDistributePlanner(ctx.connectContext)
                    && canUseSingleTabletGather(olapScan, ctx.connectContext.getSessionVariable(),
                    ctx.connectContext.getSessionVariable().resolveCloudClusterName(ctx.connectContext))) {
                // Keep the natural distribution candidate for colocate/bucket-shuffle plans.
                return ImmutableList.of(implement(olapScan, distribution),
                        implement(olapScan, DistributionSpecStorageGather.INSTANCE));
            }
            return ImmutableList.of(implement(olapScan, distribution));
        }).toRule(RuleType.LOGICAL_OLAP_SCAN_TO_PHYSICAL_OLAP_SCAN_RULE);
    }

    private PhysicalOlapScan implement(LogicalOlapScan olapScan, DistributionSpec distribution) {
        return new PhysicalOlapScan(
                olapScan.getRelationId(),
                olapScan.getTable(),
                olapScan.getQualifier(),
                olapScan.getSelectedIndexId(),
                olapScan.getSelectedTabletIds(),
                olapScan.getSelectedPartitionIds(),
                olapScan.hasPartitionPredicate(),
                distribution,
                olapScan.getPreAggStatus(),
                olapScan.getOutputByIndex(olapScan.getTable().getBaseIndexId()),
                Optional.empty(),
                olapScan.getLogicalProperties(),
                null,
                null,
                olapScan.getTableSample(),
                olapScan.getOperativeSlots(),
                olapScan.getVirtualColumns(),
                olapScan.getScoreOrderKeys(),
                olapScan.getScoreLimit(),
                olapScan.getScoreRangeInfo(),
                olapScan.getAnnOrderKeys(),
                olapScan.getAnnLimit(),
                olapScan.getTableAlias(),
                olapScan.getPartitionPrunablePredicates(),
                olapScan.getScanParams());
    }

    /** A storage gather must execute the entire selected input in one instance. */
    static boolean canUseSingleTabletGather(LogicalOlapScan scan, SessionVariable session, String clusterName) {
        if (!session.enableSingleTabletGather || !session.isEnableNereidsDistributePlanner()
                || session.getParallelExecInstanceNum(clusterName) != 1
                || session.getEnableQueryCache() || scan.getTableSample().isPresent()
                || scan.getScanParams().isPresent()) {
            return false;
        }
        if (!scan.getSelectedTabletIds().isEmpty()) {
            return scan.getSelectedTabletIds().size() == 1;
        }
        // An empty pruned-tablet list means no tablet restriction, not an empty scan.
        int tablets = 0;
        for (long partitionId : scan.getSelectedPartitionIds()) {
            Partition partition = scan.getTable().getPartition(partitionId);
            tablets += scan.getTable().getPartitionIndex(partition, scan.getSelectedIndexId()).getTablets().size();
            if (tablets > 1) {
                return false;
            }
        }
        return tablets == 1;
    }

    /** convertDistribution */
    public static DistributionSpec convertDistribution(LogicalOlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
        // When there are multiple partitions, olapScan tasks of different buckets are dispatched in
        // rounded robin algorithm. Therefore, the hashDistributedSpec can be broken except they are in
        // the same stable colocateGroup(CG)
        boolean isBelongStableCG = Utils.isBelongStableCG(olapTable);
        boolean isSelectUnpartition = Utils.isSelectUnpartition(olapTable, olapScan.getSelectedPartitionIds());
        // TODO: find a better way to handle both tablet num == 1 and colocate table together in future
        if (distributionInfo instanceof HashDistributionInfo && (isBelongStableCG || isSelectUnpartition)) {
            if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Slot> output = olapScan.getOutput();
                List<Slot> baseOutput = olapScan.getOutputByIndex(olapScan.getTable().getBaseIndexId());
                List<ExprId> hashColumns = Lists.newArrayList();
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    for (Slot slot : output) {
                        Column originalColumn = ((SlotReference) slot).getOriginalColumn().get();
                        String origName = originalColumn.tryGetBaseColumnName();
                        if (origName.equals(column.getName())) {
                            hashColumns.add(slot.getExprId());
                        }
                    }
                }
                if (hashColumns.size() != hashDistributionInfo.getDistributionColumns().size()) {
                    for (Column column : hashDistributionInfo.getDistributionColumns()) {
                        for (Slot slot : baseOutput) {
                            // If the length of the column in the bucket key changes after DDL, the length cannot be
                            // determined. As a result, some bucket fields are lost in the query execution plan.
                            // So here we use the column name to avoid this problem
                            if (((SlotReference) slot).getOriginalColumn().get().getName()
                                    .equalsIgnoreCase(column.getName())) {
                                hashColumns.add(slot.getExprId());
                            }
                        }
                    }
                }
                return new DistributionSpecHash(hashColumns, ShuffleType.NATURAL, olapScan.getTable().getId(),
                        olapScan.getSelectedIndexId(), Sets.newLinkedHashSet(olapScan.getSelectedPartitionIds()));
            } else {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Slot> output = olapScan.getOutput();
                List<ExprId> hashColumns = Lists.newArrayList();
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    for (Slot slot : output) {
                        // If the length of the column in the bucket key changes after DDL, the length cannot be
                        // determined. As a result, some bucket fields are lost in the query execution plan.
                        // So here we use the column name to avoid this problem
                        if (((SlotReference) slot).getOriginalColumn().isPresent()
                                && ((SlotReference) slot).getOriginalColumn().get().getName()
                                .equalsIgnoreCase(column.getName())) {
                            hashColumns.add(slot.getExprId());
                        }
                    }
                }
                return new DistributionSpecHash(hashColumns, ShuffleType.NATURAL, olapScan.getTable().getId(),
                        olapScan.getSelectedIndexId(), Sets.newLinkedHashSet(olapScan.getSelectedPartitionIds()));
            }
        } else {
            // RandomDistributionInfo
            return DistributionSpecStorageAny.INSTANCE;
        }
    }
}
