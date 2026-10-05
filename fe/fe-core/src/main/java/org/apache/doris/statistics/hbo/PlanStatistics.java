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

import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.thrift.TPlanNodeRuntimeStatsItem;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanStatistics {
    protected final int nodeId;
    protected final long inputRows;
    protected final long outputRows;
    protected final long commonFilteredRows;
    protected final long commonFilterInputRows;
    protected final long runtimeFilteredRows;
    protected final long runtimeFilterInputRows;
    protected final long joinBuilderRows;
    protected final long joinProbeRows;
    protected final int joinBuilderSkewRatio;
    protected final int joinProbeSkewRatio;

    protected final int instanceNum;

    public static final PlanStatistics EMPTY = new PlanStatistics(
            -1, -1, -1, -1, -1,
            -1, -1, -1, -1,
            -1, -1, -1);

    public PlanStatistics(int nodeId, long inputRows, long outputRows,
                        long commonFilteredRows, long commonFilterInputRows,
                        long runtimeFilteredRows, long runtimeFilterInputRows,
                        long joinBuilderRows, long joinProbeRows,
                        int joinBuilderSkewRatio, int joinProbeSkewRatio, int instanceNum) {
        this.nodeId = nodeId;
        this.inputRows = inputRows;
        this.outputRows = outputRows;
        this.commonFilteredRows = commonFilteredRows;
        this.commonFilterInputRows = commonFilterInputRows;
        this.runtimeFilteredRows = runtimeFilteredRows;
        this.runtimeFilterInputRows = runtimeFilterInputRows;
        this.joinBuilderRows = joinBuilderRows;
        this.joinProbeRows = joinProbeRows;
        this.joinBuilderSkewRatio = joinBuilderSkewRatio;
        this.joinProbeSkewRatio = joinProbeSkewRatio;
        this.instanceNum = instanceNum;
    }

    public static PlanStatistics buildFromStatsItem(TPlanNodeRuntimeStatsItem item,
            PhysicalPlan planNode, Map<RelationId, Set<Expression>> scanToFilterMap) {
        if (planNode instanceof PhysicalOlapScan) {
            boolean isPartitionedTable = ((PhysicalOlapScan) planNode).getTable().isPartitionedTable();
            PartitionInfo partitionInfo = ((PhysicalOlapScan) planNode).getTable().getPartitionInfo();
            List<Long> selectedPartitionIds = ((PhysicalOlapScan) planNode).getSelectedPartitionIds();
            RelationId relationId = ((PhysicalOlapScan) planNode).getRelationId();
            Set<Expression> scanToFilterSet = scanToFilterMap.getOrDefault(relationId, new HashSet<>());

            return new ScanPlanStatistics(item.getNodeId(), item.getInputRows(), item.getOutputRows(),
                    item.getCommonFilterRows(), item.getCommonFilterInputRows(),
                    item.getRuntimeFilterRows(), item.getRuntimeFilterInputRows(),
                    item.getJoinBuilderRows(), item.getJoinProbeRows(),
                    item.getJoinBuilderSkewRatio(), item.getJoinProberSkewRatio(), item.getInstanceNum(),
                    (PhysicalOlapScan) planNode, scanToFilterSet,
                    isPartitionedTable, partitionInfo, selectedPartitionIds);
        } else {
            return new PlanStatistics(item.getNodeId(), item.getInputRows(), item.getOutputRows(),
                    item.getCommonFilterRows(), item.getCommonFilterInputRows(),
                    item.getRuntimeFilterRows(), item.getRuntimeFilterInputRows(),
                    item.getJoinBuilderRows(), item.getJoinProbeRows(),
                    item.getJoinBuilderSkewRatio(), item.getJoinProberSkewRatio(), item.getInstanceNum());
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getInputRows() {
        return inputRows;
    }

    public long getOutputRows() {
        return outputRows;
    }

    public long getCommonFilteredRows() {
        return commonFilteredRows;
    }

    public long getRuntimeFilteredRows() {
        return runtimeFilteredRows;
    }

    public long getCommonFilterInputRows() {
        return commonFilterInputRows;
    }

    public long getRuntimeFilterInputRows() {
        return runtimeFilterInputRows;
    }

    public long getJoinBuilderRows() {
        return joinBuilderRows;
    }

    public long getJoinProbeRows() {
        return joinProbeRows;
    }

    public int getJoinBuilderSkewRatio() {
        return joinBuilderSkewRatio;
    }

    public int getJoinProbeSkewRatio() {
        return joinProbeSkewRatio;
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public boolean isRuntimeFilterSafeNode(double rfSafeThreshold) {
        // no need to check runtimeFilterInputRows if runtimeFilteredRows is 0 or threshold <= 0
        if (rfSafeThreshold <= 0) {
            return true;
        } else if (runtimeFilteredRows == 0 /*&& runtimeFilterInputRows == 0*/) {
            return true;
        } else if (runtimeFilteredRows > 0 && runtimeFilterInputRows > 0
                && runtimeFilterInputRows >= runtimeFilteredRows) {
            double rfFilterRatio = (double) (100 * runtimeFilteredRows / runtimeFilterInputRows);
            return rfFilterRatio < 100 * rfSafeThreshold;
        } else {
            throw new RuntimeException("Illegal runtime stats found");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanStatistics other = (PlanStatistics) o;
        return nodeId == other.nodeId
            && inputRows == other.inputRows
            && outputRows == other.outputRows
            && commonFilteredRows == other.commonFilteredRows
            && commonFilterInputRows == other.commonFilterInputRows
            && runtimeFilteredRows == other.runtimeFilteredRows
            && runtimeFilterInputRows == other.runtimeFilterInputRows
            && joinBuilderRows == other.joinBuilderRows
            && joinProbeRows == other.joinProbeRows
            && joinBuilderSkewRatio == other.joinBuilderSkewRatio
            && joinProbeSkewRatio == other.joinProbeSkewRatio
            && instanceNum == other.instanceNum;
    }
}
