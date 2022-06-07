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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOperator;

/**
 * Calculate the cost of a plan.
 */
public class CostCalculator {
    /**
     * Constructor.
     */
    public static double calculateCost(GroupExpression expression) {
        assert expression.getOperator() instanceof PhysicalOperator;
        CostEstimate costEstimate = ((PhysicalOperator<?>) expression.getOperator()).calculateCost();
        return costFormula(costEstimate);
    }

    private static double costFormula(CostEstimate costEstimate) {
        double cpuCostWeight = 1;
        double memoryCostWeight = 1;
        double networkCostWeight = 1;
        return costEstimate.getCpuCost() * cpuCostWeight + costEstimate.getMemoryCost() * memoryCostWeight
                + costEstimate.getNetworkCost() * networkCostWeight;
    }
}