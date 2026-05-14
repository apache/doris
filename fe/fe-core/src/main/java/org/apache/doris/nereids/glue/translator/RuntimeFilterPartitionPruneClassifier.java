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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import com.google.common.collect.Range;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Classifies whether one runtime-filter target can safely drive BE-side partition pruning.
 *
 * <p>This is the single FE gate for RF partition pruning. It intentionally reasons about the
 * final legacy target expression that will be sent to BE, so late-added casts and partition
 * expression domains cannot bypass the safety checks.
 */
final class RuntimeFilterPartitionPruneClassifier {
    private RuntimeFilterPartitionPruneClassifier() {
    }

    static Classification classify(Expr targetExpr, Expression nereidsTargetExpr, PlanNode scanNode) {
        if (!(scanNode instanceof OlapScanNode)) {
            return Classification.unsupported("target scan is not an OlapScanNode");
        }

        OlapScanNode olapScanNode = (OlapScanNode) scanNode;
        OlapTable table = olapScanNode.getOlapTable();
        if (table == null) {
            return Classification.unsupported("target scan has no OlapTable");
        }

        PartitionInfo partitionInfo = table.getPartitionInfo();
        PartitionType partType = partitionInfo.getType();
        if (partType != PartitionType.RANGE && partType != PartitionType.LIST) {
            return Classification.unsupported("partition type is not RANGE or LIST");
        }
        if (hasUnsupportedAutomaticPartitionExpression(partitionInfo)) {
            return Classification.unsupported("automatic partition expression boundary is not modeled");
        }

        if (targetExpr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) targetExpr;
            if (!isPartitionColumnSlot(slotRef, partitionInfo.getPartitionColumns())) {
                return Classification.unsupported("target SlotRef is not a partition column");
            }
            return Classification.supportedIdentity(slotRef);
        }

        SlotRef leafSlot = findUniqueSlotRef(targetExpr);
        if (leafSlot == null || !isPartitionColumnSlot(leafSlot, partitionInfo.getPartitionColumns())) {
            return Classification.unsupported("target expression is not rooted on one partition column");
        }
        if (partType != PartitionType.RANGE) {
            return Classification.unsupported("local monotonicity is only supported for RANGE partition");
        }

        Map<Long, TTargetExprMonotonicity> partitionMonotonicity =
                classifyLocalMonotonicity(nereidsTargetExpr, olapScanNode, partitionInfo, leafSlot);
        if (partitionMonotonicity.isEmpty()) {
            return Classification.unsupported("target expression is not monotonic on selected partitions");
        }
        return Classification.supportedPartitions(leafSlot, partitionMonotonicity);
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

    private static boolean containsFunctionCall(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (containsFunctionCall(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPartitionColumnSlot(SlotRef slotRef, List<Column> partitionColumns) {
        Column targetColumn = slotRef.getColumn();
        if (targetColumn == null) {
            return false;
        }
        for (Column partitionColumn : partitionColumns) {
            if (sameColumn(targetColumn, partitionColumn)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameColumn(Column targetColumn, Column partitionColumn) {
        if (targetColumn == partitionColumn) {
            return true;
        }
        int targetUniqueId = targetColumn.getUniqueId();
        int partitionUniqueId = partitionColumn.getUniqueId();
        if (targetUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && partitionUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && targetUniqueId == partitionUniqueId) {
            return true;
        }
        return targetColumn.equals(partitionColumn);
    }

    private static SlotRef findUniqueSlotRef(Expr expr) {
        if (expr instanceof SlotRef) {
            return (SlotRef) expr;
        }
        SlotRef result = null;
        for (Expr child : expr.getChildren()) {
            SlotRef childSlot = findUniqueSlotRef(child);
            if (childSlot == null) {
                continue;
            }
            if (result != null && result.getSlotId().asInt() != childSlot.getSlotId().asInt()) {
                return null;
            }
            result = childSlot;
        }
        return result;
    }

    private static Map<Long, TTargetExprMonotonicity> classifyLocalMonotonicity(
            Expression nereidsTargetExpr, OlapScanNode scanNode, PartitionInfo partitionInfo, SlotRef leafSlot) {
        Map<Long, TTargetExprMonotonicity> result = new HashMap<>();
        if (!(nereidsTargetExpr instanceof Monotonic) || nereidsTargetExpr.getInputSlots().size() != 1) {
            return result;
        }

        Monotonic monotonic = (Monotonic) nereidsTargetExpr;
        int childIndex = monotonic.getMonotonicFunctionChildIndex();
        if (childIndex < 0 || childIndex >= nereidsTargetExpr.arity()
                || !(nereidsTargetExpr.child(childIndex) instanceof Slot)) {
            return result;
        }

        Column partitionColumn = leafSlot.getColumn();
        for (Long partitionId : scanNode.getSelectedPartitionIds()) {
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

    private static Literal toNereidsLiteral(LiteralExpr literalExpr, Column column) {
        try {
            return Literal.fromLegacyLiteral(literalExpr, column.getType());
        } catch (AnalysisException e) {
            return null;
        }
    }

    static final class Classification {
        private final boolean canPrunePartitions;
        private final boolean emitTargetMonotonicity;
        private final SlotRef partitionSlot;
        private final TTargetExprMonotonicity monotonicity;
        private final Map<Long, TTargetExprMonotonicity> partitionMonotonicity;
        private final String unsupportedReason;

        private Classification(boolean canPrunePartitions, boolean emitTargetMonotonicity,
                SlotRef partitionSlot, TTargetExprMonotonicity monotonicity,
                Map<Long, TTargetExprMonotonicity> partitionMonotonicity, String unsupportedReason) {
            this.canPrunePartitions = canPrunePartitions;
            this.emitTargetMonotonicity = emitTargetMonotonicity;
            this.partitionSlot = partitionSlot;
            this.monotonicity = monotonicity;
            this.partitionMonotonicity = partitionMonotonicity;
            this.unsupportedReason = unsupportedReason;
        }

        static Classification supportedIdentity(SlotRef partitionSlot) {
            return new Classification(true, true, partitionSlot,
                    TTargetExprMonotonicity.MONOTONIC_INCREASING, new HashMap<>(), "");
        }

        static Classification supportedPartitions(SlotRef partitionSlot,
                Map<Long, TTargetExprMonotonicity> partitionMonotonicity) {
            return new Classification(true, false, partitionSlot, TTargetExprMonotonicity.NON_MONOTONIC,
                    partitionMonotonicity, "");
        }

        static Classification unsupported(String reason) {
            return new Classification(false, false, null, TTargetExprMonotonicity.NON_MONOTONIC,
                    new HashMap<>(), reason);
        }

        boolean canPrunePartitions() {
            return canPrunePartitions;
        }

        boolean emitTargetMonotonicity() {
            return emitTargetMonotonicity;
        }

        SlotRef getPartitionSlot() {
            return partitionSlot;
        }

        TTargetExprMonotonicity getMonotonicity() {
            return monotonicity;
        }

        Map<Long, TTargetExprMonotonicity> getPartitionMonotonicity() {
            return partitionMonotonicity;
        }

        String getUnsupportedReason() {
            return unsupportedReason;
        }
    }
}
