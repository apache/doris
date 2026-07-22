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
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.List;

/**
 * Classifies whether one runtime-filter target can safely drive BE-side tablet pruning.
 *
 * <p>BE prunes a tablet when none of the filter values hashes into the tablet's bucket
 * index (hash(value) % bucket_num). This requires the filter values to be enumerable
 * and hash-compatible with the raw distribution column values, so this gate is much
 * stricter than the partition-pruning one:
 *
 * <ul>
 *   <li>Only IN filters qualify: BLOOM filters are bitmaps (values cannot be
 *       enumerated) and MIN_MAX filters only carry a range.</li>
 *   <li>The target must be a direct SlotRef on the table's single hash distribution
 *       column. A cast-wrapped or computed target would be hashed by BE on a value
 *       whose type/encoding differs from what the load-time distribution hash used.</li>
 *   <li>Auto-bucket tables are excluded: their bucket count is adjusted dynamically
 *       and the value seen at planning time may be stale at execution time.</li>
 * </ul>
 *
 * <p>Note this classifier cannot check per-partition bucket counts (partitions are
 * selected after RF translation). OlapScanNode.toThrift re-validates that all selected
 * partitions share one bucket count before serializing tablet bucket info.
 */
final class RuntimeFilterTabletPruneClassifier {
    private RuntimeFilterTabletPruneClassifier() {
    }

    static Classification classify(TRuntimeFilterType filterType, Expr targetExpr, PlanNode scanNode) {
        if (filterType != TRuntimeFilterType.IN) {
            return Classification.unsupported("only IN runtime filter can drive tablet pruning");
        }
        if (!(scanNode instanceof OlapScanNode)) {
            return Classification.unsupported("target scan is not an OlapScanNode");
        }
        OlapScanNode olapScanNode = (OlapScanNode) scanNode;
        OlapTable table = olapScanNode.getOlapTable();
        if (table == null) {
            return Classification.unsupported("target scan has no OlapTable");
        }

        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        if (!(distributionInfo instanceof HashDistributionInfo)) {
            return Classification.unsupported("table is not hash distributed");
        }
        HashDistributionInfo hashDistribution = (HashDistributionInfo) distributionInfo;
        if (hashDistribution.getAutoBucket()) {
            return Classification.unsupported("auto bucket table has unstable bucket num");
        }
        List<Column> distributionColumns = hashDistribution.getDistributionColumns();
        if (distributionColumns.size() != 1) {
            return Classification.unsupported("only single-column distribution key is supported");
        }

        if (!(targetExpr instanceof SlotRef)) {
            return Classification.unsupported("target is not a direct SlotRef on the distribution column");
        }
        SlotRef slotRef = (SlotRef) targetExpr;
        if (slotRef.getColumn() == null) {
            return Classification.unsupported("target SlotRef has no resolved column");
        }
        if (!sameColumn(slotRef.getColumn(), distributionColumns.get(0))) {
            return Classification.unsupported("target SlotRef is not the distribution column");
        }
        return Classification.supported();
    }

    private static boolean sameColumn(Column targetColumn, Column distributionColumn) {
        if (targetColumn == distributionColumn) {
            return true;
        }
        int targetUniqueId = targetColumn.getUniqueId();
        int distributionUniqueId = distributionColumn.getUniqueId();
        if (targetUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && distributionUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && targetUniqueId == distributionUniqueId) {
            return true;
        }
        return targetColumn.equals(distributionColumn);
    }

    static final class Classification {
        private final boolean canPruneTablets;
        private final String unsupportedReason;

        private Classification(boolean canPruneTablets, String unsupportedReason) {
            this.canPruneTablets = canPruneTablets;
            this.unsupportedReason = unsupportedReason;
        }

        static Classification supported() {
            return new Classification(true, "");
        }

        static Classification unsupported(String reason) {
            return new Classification(false, reason);
        }

        boolean canPruneTablets() {
            return canPruneTablets;
        }

        String getUnsupportedReason() {
            return unsupportedReason;
        }
    }
}
