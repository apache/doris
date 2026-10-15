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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.nereids.properties.DistributionMapping;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation rule that convert logical OlapScan to physical OlapScan.
 */
public class LogicalOlapScanToPhysicalOlapScan extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapScan().then(olapScan ->
                new PhysicalOlapScan(
                        olapScan.getRelationId(),
                        olapScan.getTable(),
                        olapScan.getQualifier(),
                        olapScan.getSelectedIndexId(),
                        olapScan.getSelectedTabletIds(),
                        olapScan.getSelectedPartitionIds(),
                        olapScan.hasPartitionPredicate(),
                        convertDistribution(olapScan),
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
                        olapScan.getScanParams())
        ).toRule(RuleType.LOGICAL_OLAP_SCAN_TO_PHYSICAL_OLAP_SCAN_RULE);
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
                return createNaturalHashSpec(olapScan, hashDistributionInfo, hashColumns, output);
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
                return createNaturalHashSpec(olapScan, hashDistributionInfo, hashColumns, output);
            }
        } else {
            if (!(distributionInfo instanceof HashDistributionInfo)) {
                validateDistributionMappingsForPlanning(olapTable);
            }
            return DistributionSpecStorageAny.INSTANCE;
        }
    }

    private static void validateDistributionMappingsForPlanning(OlapTable table) {
        ConnectContext context = ConnectContext.get();
        if (context != null && context.getSessionVariable().isEnableColocateMappingConstraint()) {
            Env.getCurrentEnv().getConstraintManager().getDistributionMappingConstraintsForPlanning(table);
        }
    }

    private static DistributionSpecHash createNaturalHashSpec(LogicalOlapScan olapScan,
            HashDistributionInfo hashDistributionInfo, List<ExprId> hashColumns, List<Slot> output) {
        return new DistributionSpecHash(hashColumns, ShuffleType.NATURAL, olapScan.getTable().getId(),
                olapScan.getSelectedIndexId(), Sets.newLinkedHashSet(olapScan.getSelectedPartitionIds()),
                buildDistributionMappings(olapScan.getTable(), hashDistributionInfo, output));
    }

    static List<DistributionMapping> buildDistributionMappings(OlapTable table,
            HashDistributionInfo hashDistributionInfo, List<Slot> output) {
        ConnectContext context = ConnectContext.get();
        if (context == null || !context.getSessionVariable().isEnableColocateMappingConstraint()) {
            return ImmutableList.of();
        }
        List<DistributionMapping> mappings = buildDistributionMappings(
                hashDistributionInfo,
                output,
                Env.getCurrentEnv().getConstraintManager()
                        .getDistributionMappingConstraintsForPlanning(table));
        if (!mappings.isEmpty()) {
            context.getStatementContext().getSqlCacheContext()
                    .ifPresent(sqlCacheContext -> sqlCacheContext.setHasUnsupportedTables(true));
        }
        return mappings;
    }

    static List<DistributionMapping> buildDistributionMappings(HashDistributionInfo hashDistributionInfo,
            List<Slot> output, List<DistributionMappingConstraint> constraints) {
        Map<String, ExprId> columnExprIds = new HashMap<>();
        for (Slot slot : output) {
            SlotReference slotReference = (SlotReference) slot;
            slotReference.getOriginalColumn().ifPresent(
                    column -> columnExprIds.put(
                            column.tryGetBaseColumnName().toLowerCase(Locale.ROOT), slot.getExprId()));
        }
        Map<String, Integer> distributionIndices = new HashMap<>();
        List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
        for (int i = 0; i < distributionColumns.size(); i++) {
            distributionIndices.put(distributionColumns.get(i).getName().toLowerCase(Locale.ROOT), i);
        }

        ImmutableList.Builder<DistributionMapping> mappings = ImmutableList.builder();
        for (DistributionMappingConstraint constraint : constraints) {
            ImmutableList.Builder<ExprId> determinants = ImmutableList.builder();
            boolean allDeterminantsAvailable = true;
            for (String column : constraint.getDeterminantColumnNames()) {
                ExprId exprId = columnExprIds.get(column.toLowerCase(Locale.ROOT));
                if (exprId == null) {
                    allDeterminantsAvailable = false;
                    break;
                }
                determinants.add(exprId);
            }
            if (!allDeterminantsAvailable) {
                continue;
            }
            ImmutableList.Builder<Integer> targetIndices = ImmutableList.builder();
            boolean allTargetsAvailable = true;
            for (String column : constraint.getDistributionColumnNames()) {
                Integer targetIndex = distributionIndices.get(column.toLowerCase(Locale.ROOT));
                if (targetIndex == null) {
                    allTargetsAvailable = false;
                    break;
                }
                targetIndices.add(targetIndex);
            }
            if (!allTargetsAvailable) {
                continue;
            }
            mappings.add(new DistributionMapping(
                    constraint.getMappingId(), determinants.build(), targetIndices.build()));
        }
        return mappings.build();
    }
}
