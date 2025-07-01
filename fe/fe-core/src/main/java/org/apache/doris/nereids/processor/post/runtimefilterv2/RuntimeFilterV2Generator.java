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

package org.apache.doris.nereids.processor.post.runtimefilterv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * RuntimeFilterV2Generator
 */
public class RuntimeFilterV2Generator extends PlanPostProcessor {

    public RuntimeFilterV2Generator() {
    }

    @Override
    public Plan visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        computeRuntimeFilterForIntersectAndExcept(intersect, context);
        return visitPhysicalSetOperation(intersect, context);
    }

    @Override
    public Plan visitPhysicalExcept(PhysicalExcept except, CascadesContext context) {
        computeRuntimeFilterForIntersectAndExcept(except, context);
        return visitPhysicalSetOperation(except, context);
    }

    private void computeRuntimeFilterForIntersectAndExcept(PhysicalSetOperation setOp, CascadesContext context) {
        AbstractPlan child0 = (AbstractPlan) setOp.child(0);
        if (child0.getStats() != null
                && ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable() != null
                && child0.getStats().getRowCount()
                < ConnectContext.get().getSessionVariable().runtimeFilterMaxBuildRowCount) {
            for (int slotIdx : chooseSourceSlots(setOp)) {
                Expression sourceExpression = setOp.getRegularChildrenOutputs().get(0).get(slotIdx);
                for (int childId = 1; childId < setOp.children().size(); childId++) {
                    Plan child = setOp.children().get(childId);
                    Expression targetExpression = setOp.getRegularChildrenOutputs().get(childId).get(slotIdx);
                    Statistics stats = child0.getStats();
                    long buildNdvOrRowCount = -1;
                    if (stats != null) {
                        buildNdvOrRowCount = (long) stats.getRowCount();
                        ColumnStatistic colStats = stats.findColumnStatistics(
                                sourceExpression);
                        if (colStats != null && !colStats.isUnKnown) {
                            buildNdvOrRowCount = Math.max(1, (long) colStats.ndv);
                        }
                    }
                    PushDownContext pushDownContext = new PushDownContext(
                            context.getRuntimeFilterV2Context(),
                            setOp,
                            sourceExpression,
                            buildNdvOrRowCount,
                            slotIdx,
                            targetExpression);
                    child.accept(PushDownVisitor.INSTANCE, pushDownContext);
                }
            }
        }
    }

    /**
     *
     * do not use metric data type column (such as array, map, struct, json, bitmap, ...) as runtime filter source
     *
     */
    private List<Integer> chooseSourceSlots(PhysicalSetOperation setOp) {
        List<Slot> output = setOp.getOutput();
        for (int i = 0; i < output.size(); i++) {
            if (!output.get(i).getDataType().isOnlyMetricType()
                    && !setOp.getLogicalProperties().getTrait().getUniformValue(output.get(i)).isPresent()) {
                return ImmutableList.of(i);
            }
        }
        return ImmutableList.of();
    }
}
