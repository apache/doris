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
import org.apache.doris.catalog.Partition;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TRuntimeFilterType;

/** Classifies direct single-column HASH targets for BE-side runtime-filter bucket pruning. */
final class RuntimeFilterBucketPruneClassifier {
    private RuntimeFilterBucketPruneClassifier() {
    }

    static Classification classify(TRuntimeFilterType filterType, Expr targetExpr, PlanNode scanNode) {
        if (filterType != TRuntimeFilterType.IN && filterType != TRuntimeFilterType.IN_OR_BLOOM) {
            return Classification.unsupported("runtime filter is not IN or IN_OR_BLOOM");
        }
        if (!(scanNode instanceof OlapScanNode)) {
            return Classification.unsupported("target scan is not an OlapScanNode");
        }
        if (!(targetExpr instanceof SlotRef)) {
            return Classification.unsupported("target expression is not a direct SlotRef");
        }

        Column targetColumn = ((SlotRef) targetExpr).getColumn();
        if (targetColumn == null) {
            return Classification.unsupported("target SlotRef has no column");
        }

        OlapScanNode olapScanNode = (OlapScanNode) scanNode;
        OlapTable table = olapScanNode.getOlapTable();
        if (table == null || olapScanNode.getSelectedPartitionIds().isEmpty()) {
            return Classification.unsupported("target scan has no selected partitions");
        }

        Column distributionColumn = null;
        for (Long partitionId : olapScanNode.getSelectedPartitionIds()) {
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                return Classification.unsupported("selected partition does not exist");
            }
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            if (!(distributionInfo instanceof HashDistributionInfo)) {
                return Classification.unsupported("distribution type is not HASH");
            }
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            if (hashDistributionInfo.getDistributionColumns().size() != 1) {
                return Classification.unsupported("HASH distribution is not single-column");
            }
            Column currentDistributionColumn = hashDistributionInfo.getDistributionColumns().get(0);
            if (!sameColumn(targetColumn, currentDistributionColumn)) {
                return Classification.unsupported("target SlotRef is not the HASH distribution column");
            }
            if (distributionColumn != null && !sameColumn(distributionColumn, currentDistributionColumn)) {
                return Classification.unsupported("selected partitions use different distribution columns");
            }
            distributionColumn = currentDistributionColumn;
        }
        return Classification.supported();
    }

    private static boolean sameColumn(Column targetColumn, Column distributionColumn) {
        if (targetColumn == distributionColumn || targetColumn.equals(distributionColumn)) {
            return true;
        }
        int targetUniqueId = targetColumn.getUniqueId();
        int distributionUniqueId = distributionColumn.getUniqueId();
        if (targetUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && distributionUniqueId != Column.COLUMN_UNIQUE_ID_INIT_VALUE
                && targetUniqueId == distributionUniqueId) {
            return true;
        }
        return targetColumn.tryGetBaseColumnName().equalsIgnoreCase(distributionColumn.getName());
    }

    static final class Classification {
        private final boolean canPruneBuckets;
        private final String unsupportedReason;

        private Classification(boolean canPruneBuckets, String unsupportedReason) {
            this.canPruneBuckets = canPruneBuckets;
            this.unsupportedReason = unsupportedReason;
        }

        static Classification supported() {
            return new Classification(true, "");
        }

        static Classification unsupported(String reason) {
            return new Classification(false, reason);
        }

        boolean canPruneBuckets() {
            return canPruneBuckets;
        }

        String getUnsupportedReason() {
            return unsupportedReason;
        }
    }
}
