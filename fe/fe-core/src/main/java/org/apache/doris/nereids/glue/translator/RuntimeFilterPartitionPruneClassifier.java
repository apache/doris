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
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import java.util.List;

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

    static Classification classify(Expr targetExpr, PlanNode scanNode) {
        if (!(scanNode instanceof OlapScanNode)) {
            return Classification.unsupported("target scan is not an OlapScanNode");
        }
        if (!(targetExpr instanceof SlotRef)) {
            return Classification.unsupported("target expression is not an identity SlotRef");
        }

        OlapTable table = ((OlapScanNode) scanNode).getOlapTable();
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

        SlotRef slotRef = (SlotRef) targetExpr;
        if (!isPartitionColumnSlot(slotRef, partitionInfo.getPartitionColumns())) {
            return Classification.unsupported("target SlotRef is not a partition column");
        }
        return Classification.supported(slotRef, TTargetExprMonotonicity.MONOTONIC_INCREASING);
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

    static final class Classification {
        private final SlotRef partitionSlot;
        private final TTargetExprMonotonicity monotonicity;
        private final String unsupportedReason;

        private Classification(SlotRef partitionSlot, TTargetExprMonotonicity monotonicity,
                String unsupportedReason) {
            this.partitionSlot = partitionSlot;
            this.monotonicity = monotonicity;
            this.unsupportedReason = unsupportedReason;
        }

        static Classification supported(SlotRef partitionSlot, TTargetExprMonotonicity monotonicity) {
            return new Classification(partitionSlot, monotonicity, "");
        }

        static Classification unsupported(String reason) {
            return new Classification(null, TTargetExprMonotonicity.NON_MONOTONIC, reason);
        }

        SlotRef getPartitionSlot() {
            return partitionSlot;
        }

        TTargetExprMonotonicity getMonotonicity() {
            return monotonicity;
        }

        String getUnsupportedReason() {
            return unsupportedReason;
        }
    }
}
