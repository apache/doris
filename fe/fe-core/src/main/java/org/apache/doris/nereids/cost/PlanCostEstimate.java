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

import com.google.common.base.Preconditions;

/**
 * Use for estimating the cost of plan.
 */
public final class PlanCostEstimate {
    private static final PlanCostEstimate INFINITE =
            new PlanCostEstimate(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    private static final PlanCostEstimate ZERO = new PlanCostEstimate(0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    /**
     * Constructor of PlanCostEstimate.
     */
    public PlanCostEstimate(double cpuCost, double memoryCost, double networkCost) {
        Preconditions.checkArgument(!(cpuCost < 0), "cpuCost cannot be negative: %s", cpuCost);
        Preconditions.checkArgument(!(memoryCost < 0), "memoryCost cannot be negative: %s", memoryCost);
        Preconditions.checkArgument(!(networkCost < 0), "networkCost cannot be negative: %s", networkCost);
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    public static PlanCostEstimate infinite() {
        return INFINITE;
    }

    public static PlanCostEstimate zero() {
        return ZERO;
    }

    public double getCpuCost() {
        return cpuCost;
    }

    public double getMemoryCost() {
        return memoryCost;
    }

    public double getNetworkCost() {
        return networkCost;
    }

}
