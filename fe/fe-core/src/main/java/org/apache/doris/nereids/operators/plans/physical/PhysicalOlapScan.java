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

package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanOperatorVisitor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeafPlan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Physical olap scan plan operator.
 */
public class PhysicalOlapScan extends PhysicalScan {
    private final long selectedIndexId;
    private final List<Long> selectedTabletId;
    private final List<Long> selectedPartitionId;

    private final OlapTable olapTable;

    /**
     * Constructor for PhysicalOlapScan.
     *
     * @param olapTable OlapTable in Doris
     * @param qualifier table's name
     */
    public PhysicalOlapScan(OlapTable olapTable, List<String> qualifier) {
        super(OperatorType.PHYSICAL_OLAP_SCAN, qualifier);
        this.olapTable = olapTable;
        this.selectedIndexId = olapTable.getBaseIndexId();
        this.selectedTabletId = Lists.newArrayList();
        this.selectedPartitionId = olapTable.getPartitionIds();
        for (Partition partition : olapTable.getAllPartitions()) {
            selectedTabletId.addAll(partition.getBaseIndex().getTabletIdsInOrder());
        }
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public OlapTable getTable() {
        return olapTable;
    }

    @Override
    public String toString() {
        return "Scan Olap Table " + StringUtils.join(qualifier, ".") + "." + olapTable.getName()
                + " (selected index id: " + selectedTabletId + ", selected partition ids: " + selectedPartitionId
                + ", selected tablet ids: " + selectedTabletId + ")";
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalOlapScan((PhysicalLeafPlan<PhysicalOlapScan>) plan, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.of();
    }
}
