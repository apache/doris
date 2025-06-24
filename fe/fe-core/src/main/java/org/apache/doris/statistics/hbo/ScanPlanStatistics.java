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

package org.apache.doris.statistics.hbo;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScanPlanStatistics extends PlanStatistics {
    private final PhysicalOlapScan scan;
    private final ImmutableList<Long> selectedPartitionIds;
    private final Set<Expression> partitionColumnPredicates = new HashSet<>();
    private final Set<Expression> otherPredicate = new HashSet<>();
    private final Set<Expression> tableFilterSet;
    private final PartitionInfo partitionInfo;
    private final boolean isPartitionedTable;

    public ScanPlanStatistics(int nodeId, long inputRows, long outputRows, long commonFilteredRows,
            long commonFilterInputRows, long runtimeFilteredRows, long runtimeFilterInputRows, long joinBuilderRows,
            long joinProbeRows, int joinBuilderSkewRatio, int joinProbeSkewRatio, int instanceNum,
            PhysicalOlapScan scan, Set<Expression> tableFilterSet, boolean isPartitionedTable,
            PartitionInfo partitionInfo, List<Long> selectedPartitionIds) {
        super(nodeId, inputRows, outputRows, commonFilteredRows, commonFilterInputRows, runtimeFilteredRows,
                runtimeFilterInputRows, joinBuilderRows, joinProbeRows, joinBuilderSkewRatio, joinProbeSkewRatio,
                instanceNum);
        this.scan = scan;
        this.tableFilterSet = tableFilterSet;
        this.isPartitionedTable = isPartitionedTable;
        this.partitionInfo = partitionInfo;
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        buildPartitionColumnPredicatesAndOthers(tableFilterSet, partitionInfo);
    }

    public ScanPlanStatistics(PlanStatistics other, PhysicalOlapScan scan, Set<Expression> tableFilterSet,
            boolean isPartitionedTable, PartitionInfo partitionInfo, List<Long> selectedPartitionIds) {
        super(other.nodeId, other.inputRows, other.outputRows, other.commonFilteredRows, other.commonFilterInputRows,
                other.runtimeFilteredRows, other.runtimeFilterInputRows, other.joinBuilderRows, other.joinProbeRows,
                other.joinBuilderSkewRatio, other.joinProbeSkewRatio, other.instanceNum);
        this.scan = scan;
        this.tableFilterSet = tableFilterSet;
        this.isPartitionedTable = isPartitionedTable;
        this.partitionInfo = partitionInfo;
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        buildPartitionColumnPredicatesAndOthers(tableFilterSet, partitionInfo);
    }

    public void buildPartitionColumnPredicatesAndOthers(Set<Expression> tableFilterSet, PartitionInfo partitionInfo) {
        partitionColumnPredicates.clear();
        otherPredicate.clear();
        if (tableFilterSet != null) {
            for (Expression expr : tableFilterSet) {
                Set<Slot> inputSlot = expr.getInputSlots();
                if (inputSlot.size() == 1 && inputSlot.iterator().next() instanceof SlotReference
                        && ((SlotReference) inputSlot.iterator().next()).getOriginalColumn().isPresent()) {
                    Column filterColumn = ((SlotReference) inputSlot.iterator().next()).getOriginalColumn().get();
                    if (partitionInfo.getPartitionColumns().contains(filterColumn)) {
                        partitionColumnPredicates.add(expr);
                    } else {
                        otherPredicate.add(expr);
                    }
                } else {
                    otherPredicate.add(expr);
                }
            }
        }
    }

    public boolean hasSameOtherPredicates(ScanPlanStatistics other) {
        return this.otherPredicate.containsAll(other.otherPredicate)
                && other.otherPredicate.containsAll(this.otherPredicate);
    }

    public boolean hasSamePartitionColumnPredicates(ScanPlanStatistics other) {
        return this.partitionColumnPredicates.containsAll(other.partitionColumnPredicates)
                && other.partitionColumnPredicates.containsAll(this.partitionColumnPredicates);
    }

    public boolean hasSamePartitionId(ScanPlanStatistics other) {
        return this.selectedPartitionIds.equals(other.selectedPartitionIds);
    }

    public boolean isPartitionedTable() {
        return this.isPartitionedTable;
    }

    public PhysicalOlapScan getScan() {
        return scan;
    }
}
