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

package org.apache.doris.nereids.cost;

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

/**
 * Calculate the cost of a plan.
 */
@Developing
//TODO: memory cost and network cost should be estimated by byte size.
public class CostCalculator {
    protected static double BROADCAST_MEM_LIMIT = ConnectContext.get().getSessionVariable()
            .getBroadcastHashtableMemLimitPercentage();
    protected static double DEFAULT_CPU_WEIGHT = ConnectContext.get().getSessionVariable().getCboCpuWeight();
    protected static double DEFAULT_MEM_WEIGHT = ConnectContext.get().getSessionVariable().getCboMemWeight();
    protected static double DEFAULT_NET_WEIGHT = ConnectContext.get().getSessionVariable().getCboNetWeight();

    protected static boolean ENABLE_COST_V2 = false;
    protected static double MEM_LIMIT = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
    protected static double ROWS_LIMIT = ConnectContext.get().getSessionVariable().getBroadcastRowCountLimit();
    protected static double PENAL_FACTOR = ConnectContext.get().getSessionVariable().getNereidsCboPenaltyFactor();

    /**
     * Init session variable before each query to avoid getSessionVariable in each job
     */
    public static void init() {
        ENABLE_COST_V2 = ConnectContext.get().getSessionVariable().getEnableNewCostModel();
        DEFAULT_CPU_WEIGHT = ConnectContext.get().getSessionVariable().getCboCpuWeight();
        DEFAULT_MEM_WEIGHT = ConnectContext.get().getSessionVariable().getCboMemWeight();
        DEFAULT_NET_WEIGHT = ConnectContext.get().getSessionVariable().getCboNetWeight();
        MEM_LIMIT = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
        ROWS_LIMIT = ConnectContext.get().getSessionVariable().getBroadcastRowCountLimit();
        BROADCAST_MEM_LIMIT = ConnectContext.get().getSessionVariable().getBroadcastHashtableMemLimitPercentage();
        PENAL_FACTOR = ConnectContext.get().getSessionVariable().getNereidsCboPenaltyFactor();
    }

    /**
     * Calculate cost for groupExpression
     */
    public static Cost calculateCost(GroupExpression groupExpression, List<PhysicalProperties> childrenProperties) {
        PlanContext planContext = new PlanContext(groupExpression);
        if (childrenProperties.size() >= 2 && childrenProperties.get(1)
                .getDistributionSpec() instanceof DistributionSpecReplicated) {
            planContext.setBroadcastJoin();
        }
        if (ENABLE_COST_V2) {
            CostModelV2 costModelV2 = new CostModelV2();
            return groupExpression.getPlan().accept(costModelV2, planContext);
        } else {
            CostModelV1 costModelV1 = new CostModelV1();
            return groupExpression.getPlan().accept(costModelV1, planContext);
        }
    }

    /**
     * Calculate cost without groupExpression
     */
    public static Cost calculateCost(Plan plan, PlanContext planContext) {
        if (ENABLE_COST_V2) {
            CostModelV2 costModel = new CostModelV2();
            return plan.accept(costModel, planContext);
        } else {
            CostModelV1 costModel = new CostModelV1();
            return plan.accept(costModel, planContext);
        }
    }

    public static Cost addChildCost(Plan plan, Cost planCost, Cost childCost, int index) {
        if (ENABLE_COST_V2) {
            return CostModelV2.addChildCost(plan, planCost, childCost, index);
        }
        return CostModelV1.addChildCost(plan, planCost, childCost, index);
    }
}
