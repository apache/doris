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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Generates target-scoped partition and bucket pruning metadata with a Nereids runtime filter. */
final class RuntimeFilterPruneClassifier {
    private RuntimeFilterPruneClassifier() {
    }

    static Classification classify(RuntimeFilter filter, SessionVariable sessionVariable) {
        BucketClassification bucketClassification = sessionVariable.isEnableRuntimeFilterBucketPrune()
                ? classifyBucketPruning(filter)
                : BucketClassification.unsupported("runtime-filter bucket pruning is disabled");
        PartitionClassification partitionClassification = sessionVariable.isEnableRuntimeFilterPartitionPrune()
                ? classifyPartitionPruning(filter)
                : PartitionClassification.unsupported("runtime-filter partition pruning is disabled");
        return new Classification(bucketClassification, partitionClassification);
    }

    private static BucketClassification classifyBucketPruning(RuntimeFilter filter) {
        if (filter.getType() != TRuntimeFilterType.IN
                && filter.getType() != TRuntimeFilterType.IN_OR_BLOOM) {
            return BucketClassification.unsupported("runtime filter is not IN or IN_OR_BLOOM");
        }
        if (!(filter.getTargetScan() instanceof PhysicalOlapScan)) {
            return BucketClassification.unsupported("target scan is not a PhysicalOlapScan");
        }
        if (!isFinalTargetDirectSlot(filter)) {
            return BucketClassification.unsupported("target expression is not a direct slot");
        }

        Column targetColumn = targetColumn(filter.getTargetSlot());
        if (targetColumn == null) {
            return BucketClassification.unsupported("target slot has no column");
        }

        PhysicalOlapScan scan = (PhysicalOlapScan) filter.getTargetScan();
        OlapTable table = scan.getTable();
        if (table == null || scan.getSelectedPartitionIds().isEmpty()) {
            return BucketClassification.unsupported("target scan has no selected partitions");
        }
        Column targetBaseColumn = directTargetBaseColumn(scan, targetColumn);
        if (targetBaseColumn == null) {
            return BucketClassification.unsupported(
                    "target has no direct base-column definition");
        }

        Column distributionColumn = null;
        for (Long partitionId : scan.getSelectedPartitionIds()) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                return BucketClassification.unsupported("selected partition does not exist");
            }
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            if (!(distributionInfo instanceof HashDistributionInfo)) {
                return BucketClassification.unsupported("distribution type is not HASH");
            }
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            if (hashDistributionInfo.getDistributionColumns().size() != 1) {
                return BucketClassification.unsupported("HASH distribution is not single-column");
            }
            Column currentDistributionColumn = hashDistributionInfo.getDistributionColumns().get(0);
            if (!sameBucketColumn(targetColumn, currentDistributionColumn)) {
                return BucketClassification.unsupported("target slot is not the HASH distribution column");
            }
            if (distributionColumn != null
                    && !sameBucketColumn(distributionColumn, currentDistributionColumn)) {
                return BucketClassification.unsupported(
                        "selected partitions use different distribution columns");
            }
            distributionColumn = currentDistributionColumn;
        }
        return BucketClassification.supported();
    }

    private static PartitionClassification classifyPartitionPruning(RuntimeFilter filter) {
        if (!(filter.getTargetScan() instanceof PhysicalOlapScan)) {
            return PartitionClassification.unsupported("target scan is not a PhysicalOlapScan");
        }
        if (requiresTargetTypeAdjustment(filter)
                && !filter.getTargetSlot().equals(filter.getTargetExpression())) {
            return PartitionClassification.unsupported(
                    "non-identity target expression requires a type adjustment");
        }

        PhysicalOlapScan scan = (PhysicalOlapScan) filter.getTargetScan();
        OlapTable table = scan.getTable();
        if (table == null) {
            return PartitionClassification.unsupported("target scan has no OlapTable");
        }

        PartitionInfo partitionInfo = table.getPartitionInfo();
        PartitionType partitionType = partitionInfo.getType();
        if (partitionType != PartitionType.RANGE && partitionType != PartitionType.LIST) {
            return PartitionClassification.unsupported("partition type is not RANGE or LIST");
        }
        if (filter.getType() == TRuntimeFilterType.BLOOM && partitionType == PartitionType.RANGE) {
            return PartitionClassification.unsupported(
                    "BLOOM runtime filter does not support RANGE partition pruning");
        }
        if (hasUnsupportedAutomaticPartitionExpression(partitionInfo)) {
            return PartitionClassification.unsupported(
                    "automatic partition expression boundary is not modeled");
        }

        Expression targetExpression = filter.getTargetExpression();
        if (targetExpression.containsType(NoneMovableFunction.class)) {
            return PartitionClassification.unsupported(
                    "target expression contains non-movable function");
        }
        Column targetColumn = targetColumn(filter.getTargetSlot());
        Column targetBaseColumn = directTargetBaseColumn(scan, targetColumn);
        if (targetBaseColumn == null) {
            return PartitionClassification.unsupported(
                    "target column has no direct base-column definition");
        }
        if (!isPartitionColumn(targetColumn, targetBaseColumn, partitionInfo.getPartitionColumns())) {
            return PartitionClassification.unsupported(
                    "target expression is not rooted on one partition column");
        }
        if (!hasSerializedBoundary(targetColumn, targetBaseColumn, partitionInfo, partitionType)) {
            return PartitionClassification.unsupported(
                    "target expression has no serialized partition boundary");
        }

        if (isFinalTargetDirectSlot(filter)) {
            return supportedIncreasingPartitions(scan, partitionInfo,
                    "target slot has no prunable selected partitions");
        }
        if (partitionType == PartitionType.LIST) {
            if (targetExpression.containsNondeterministic()) {
                return PartitionClassification.unsupported(
                        "target expression contains non-deterministic function");
            }
            return supportedIncreasingPartitions(scan, partitionInfo,
                    "target expression has no prunable selected partitions");
        }

        Map<Long, TTargetExprMonotonicity> partitionMonotonicity =
                classifyLocalMonotonicity(targetExpression, scan, partitionInfo, targetColumn);
        if (partitionMonotonicity.isEmpty()) {
            return PartitionClassification.unsupported(
                    "target expression is not monotonic on selected partitions");
        }
        return PartitionClassification.supported(partitionMonotonicity);
    }

    private static PartitionClassification supportedIncreasingPartitions(
            PhysicalOlapScan scan, PartitionInfo partitionInfo, String emptyReason) {
        Map<Long, TTargetExprMonotonicity> partitionMonotonicity =
                allSelectedPartitionsIncreasing(scan, partitionInfo);
        return partitionMonotonicity.isEmpty()
                ? PartitionClassification.unsupported(emptyReason)
                : PartitionClassification.supported(partitionMonotonicity);
    }

    private static boolean isFinalTargetDirectSlot(RuntimeFilter filter) {
        return filter.getTargetSlot().equals(filter.getTargetExpression())
                && !requiresTargetTypeAdjustment(filter);
    }

    private static boolean requiresTargetTypeAdjustment(RuntimeFilter filter) {
        return !filter.getSrcExpr().getDataType().toCatalogDataType().equals(
                filter.getTargetExpression().getDataType().toCatalogDataType());
    }

    private static Column targetColumn(Slot slot) {
        if (!(slot instanceof SlotReference)) {
            return null;
        }
        return ((SlotReference) slot).getOriginalColumn().orElse(null);
    }

    private static Column directTargetBaseColumn(PhysicalOlapScan scan, Column targetColumn) {
        if (targetColumn == null) {
            return null;
        }
        if (scan.getSelectedIndexId() != scan.getTable().getBaseIndexId()
                && targetColumn.getDefineExpr() == null) {
            return null;
        }
        return directBaseColumn(targetColumn);
    }

    private static boolean sameBucketColumn(Column targetColumn, Column distributionColumn) {
        Column targetBaseColumn = directBaseColumn(targetColumn);
        Column distributionBaseColumn = directBaseColumn(distributionColumn);
        if (targetBaseColumn == null || distributionBaseColumn == null) {
            return false;
        }
        return targetBaseColumn.getName().equalsIgnoreCase(distributionBaseColumn.getName())
                && targetColumn.getType().equals(distributionColumn.getType());
    }

    private static Column directBaseColumn(Column column) {
        Expr defineExpr = column.getDefineExpr();
        if (defineExpr == null) {
            return column;
        }
        if (!(defineExpr instanceof SlotRef)) {
            return null;
        }
        Column baseColumn = ((SlotRef) defineExpr).getColumn();
        if (baseColumn == null || baseColumn.isMaterializedViewColumn()) {
            return null;
        }
        return baseColumn;
    }

    private static boolean hasUnsupportedAutomaticPartitionExpression(PartitionInfo partitionInfo) {
        if (!partitionInfo.enableAutomaticPartition()) {
            return false;
        }
        for (Expr partitionExpr : partitionInfo.getPartitionExprs()) {
            if (containsFunctionCall(partitionExpr)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsFunctionCall(Expr expression) {
        if (expression instanceof FunctionCallExpr) {
            return true;
        }
        for (Expr child : expression.getChildren()) {
            if (containsFunctionCall(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasSerializedBoundary(
            Column targetColumn, Column targetBaseColumn,
            PartitionInfo partitionInfo, PartitionType partitionType) {
        if (partitionType != PartitionType.RANGE) {
            return true;
        }
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        return !partitionColumns.isEmpty()
                && samePartitionColumn(targetColumn, targetBaseColumn, partitionColumns.get(0));
    }

    private static boolean isPartitionColumn(
            Column targetColumn, Column targetBaseColumn, List<Column> partitionColumns) {
        for (Column partitionColumn : partitionColumns) {
            if (samePartitionColumn(targetColumn, targetBaseColumn, partitionColumn)) {
                return true;
            }
        }
        return false;
    }

    private static boolean samePartitionColumn(
            Column targetColumn, Column targetBaseColumn, Column partitionColumn) {
        return targetColumn.getName().equalsIgnoreCase(partitionColumn.getName())
                && targetColumn.getType().equals(partitionColumn.getType())
                && targetBaseColumn.getName().equalsIgnoreCase(partitionColumn.getName())
                && targetBaseColumn.getType().equals(partitionColumn.getType());
    }

    private static Map<Long, TTargetExprMonotonicity> classifyLocalMonotonicity(
            Expression targetExpression, PhysicalOlapScan scan,
            PartitionInfo partitionInfo, Column partitionColumn) {
        Map<Long, TTargetExprMonotonicity> result = new HashMap<>();
        if (!(targetExpression instanceof Monotonic)) {
            return result;
        }

        Monotonic monotonic = (Monotonic) targetExpression;
        int childIndex = monotonic.getMonotonicFunctionChildIndex();
        if (childIndex < 0 || childIndex >= targetExpression.arity()
                || !(targetExpression.child(childIndex) instanceof Slot)
                || !hasInputSlotOnlyInMonotonicChild(targetExpression, childIndex)) {
            return result;
        }

        for (Long partitionId : scan.getSelectedPartitionIds()) {
            PartitionItem item = partitionInfo.getItem(partitionId);
            if (!(item instanceof RangePartitionItem)) {
                continue;
            }
            Range<PartitionKey> range = ((RangePartitionItem) item).getItems();
            Literal lower = null;
            Literal upper = null;
            if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                lower = toNereidsLiteral(range.lowerEndpoint().getKeys().get(0), partitionColumn);
                if (lower == null) {
                    continue;
                }
            }
            if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                upper = toNereidsLiteral(range.upperEndpoint().getKeys().get(0), partitionColumn);
                if (upper == null) {
                    continue;
                }
            }
            if (monotonic.isMonotonic(lower, upper)) {
                result.put(partitionId, monotonic.isPositive()
                        ? TTargetExprMonotonicity.MONOTONIC_INCREASING
                        : TTargetExprMonotonicity.MONOTONIC_DECREASING);
            }
        }
        return result;
    }

    static boolean hasInputSlotOnlyInMonotonicChild(Expression expression, int monotonicChildIndex) {
        for (int i = 0; i < expression.arity(); i++) {
            if (i != monotonicChildIndex && !expression.child(i).getInputSlots().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static Map<Long, TTargetExprMonotonicity> allSelectedPartitionsIncreasing(
            PhysicalOlapScan scan, PartitionInfo partitionInfo) {
        Map<Long, TTargetExprMonotonicity> result = new HashMap<>();
        for (Long partitionId : scan.getSelectedPartitionIds()) {
            PartitionItem item = partitionInfo.getItem(partitionId);
            if (item == null || (item instanceof ListPartitionItem
                    && ((ListPartitionItem) item).isDefaultPartition())) {
                continue;
            }
            result.put(partitionId, TTargetExprMonotonicity.MONOTONIC_INCREASING);
        }
        return result;
    }

    private static Literal toNereidsLiteral(LiteralExpr literalExpr, Column column) {
        try {
            return Literal.fromLegacyLiteral(literalExpr, column.getType());
        } catch (AnalysisException e) {
            return null;
        }
    }

    static final class Classification {
        private final BucketClassification bucketClassification;
        private final PartitionClassification partitionClassification;

        private Classification(BucketClassification bucketClassification,
                PartitionClassification partitionClassification) {
            this.bucketClassification = bucketClassification;
            this.partitionClassification = partitionClassification;
        }

        boolean canPruneBuckets() {
            return bucketClassification.canPruneBuckets;
        }

        boolean canPrunePartitions() {
            return !partitionClassification.partitionMonotonicity.isEmpty();
        }

        Map<Long, TTargetExprMonotonicity> getPartitionMonotonicity() {
            return partitionClassification.partitionMonotonicity;
        }

        String getBucketUnsupportedReason() {
            return bucketClassification.unsupportedReason;
        }

        String getPartitionUnsupportedReason() {
            return partitionClassification.unsupportedReason;
        }
    }

    private static final class BucketClassification {
        private final boolean canPruneBuckets;
        private final String unsupportedReason;

        private BucketClassification(boolean canPruneBuckets, String unsupportedReason) {
            this.canPruneBuckets = canPruneBuckets;
            this.unsupportedReason = unsupportedReason;
        }

        private static BucketClassification supported() {
            return new BucketClassification(true, "");
        }

        private static BucketClassification unsupported(String reason) {
            return new BucketClassification(false, reason);
        }
    }

    private static final class PartitionClassification {
        private final Map<Long, TTargetExprMonotonicity> partitionMonotonicity;
        private final String unsupportedReason;

        private PartitionClassification(
                Map<Long, TTargetExprMonotonicity> partitionMonotonicity, String unsupportedReason) {
            this.partitionMonotonicity = partitionMonotonicity;
            this.unsupportedReason = unsupportedReason;
        }

        private static PartitionClassification supported(
                Map<Long, TTargetExprMonotonicity> partitionMonotonicity) {
            return new PartitionClassification(ImmutableMap.copyOf(partitionMonotonicity), "");
        }

        private static PartitionClassification unsupported(String reason) {
            return new PartitionClassification(ImmutableMap.of(), reason);
        }
    }
}
