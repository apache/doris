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
public final class CostEstimate {
    private static final CostEstimate INFINITE =
            new CostEstimate(Double.POSITIVE_INFINITY,
                    Double.POSITIVE_INFINITY,
                    Double.POSITIVE_INFINITY,
                    Double.POSITIVE_INFINITY);
    private static final CostEstimate ZERO = new CostEstimate(0, 0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;
    //penalty for
    // 1. right deep tree
    // 2. right XXX join
    private final double penalty;

    /**
     * Constructor of CostEstimate.
     */
    public CostEstimate(double cpuCost, double memoryCost, double networkCost, double penaltiy) {
        // TODO: fix stats
        if (cpuCost < 0) {
            cpuCost = 0;
        }
        if (memoryCost < 0) {
            memoryCost = 0;
        }
        if (networkCost < 0) {
            networkCost = 0;
        }
        Preconditions.checkArgument(!(cpuCost < 0), "cpuCost cannot be negative: %s", cpuCost);
        Preconditions.checkArgument(!(memoryCost < 0), "memoryCost cannot be negative: %s", memoryCost);
        Preconditions.checkArgument(!(networkCost < 0), "networkCost cannot be negative: %s", networkCost);
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
        this.penalty = penaltiy;
    }

    public static CostEstimate infinite() {
        return INFINITE;
    }

    public static CostEstimate zero() {
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

    public double getPenalty() {
        return penalty;
    }

    public static CostEstimate of(double cpuCost, double maxMemory, double networkCost, double rightDeepPenaltiy) {
        return new CostEstimate(cpuCost, maxMemory, networkCost, rightDeepPenaltiy);
    }

    public static CostEstimate of(double cpuCost, double maxMemory, double networkCost) {
        return new CostEstimate(cpuCost, maxMemory, networkCost, 0);
    }

    public static CostEstimate ofCpu(double cpuCost) {
        return new CostEstimate(cpuCost, 0, 0, 0);
    }

    public static CostEstimate ofMemory(double memoryCost) {
        return new CostEstimate(0, memoryCost, 0, 0);
    }

    /**
     * sum of cost estimate
     */
    public static CostEstimate sum(CostEstimate one, CostEstimate two, CostEstimate... more) {
        double cpuCostSum = one.cpuCost + two.cpuCost;
        double memoryCostSum = one.memoryCost + two.memoryCost;
        double networkCostSum = one.networkCost + one.networkCost;
        for (CostEstimate costEstimate : more) {
            cpuCostSum += costEstimate.cpuCost;
            memoryCostSum += costEstimate.memoryCost;
            networkCostSum += costEstimate.networkCost;
        }
        return CostEstimate.of(cpuCostSum, memoryCostSum, networkCostSum);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append((long) cpuCost)
                .append("/").append((long) memoryCost)
                .append("/").append((long) networkCost)
                .append("/").append((long) penalty);
        return sb.toString();
    }
}
