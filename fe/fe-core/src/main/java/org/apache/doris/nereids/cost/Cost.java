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

/**
 * CostV1.
 */
public class Cost {
    private static final Cost INFINITE = new Cost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY);
    private static final Cost ZERO = new Cost(0, 0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    private final double cost;

    /**
     * Constructor of CostV1.
     */
    public Cost(CostWeight costWeight, double cpuCost, double memoryCost, double networkCost) {
        // TODO: fix stats
        cpuCost = Double.max(0, cpuCost);
        memoryCost = Double.max(0, memoryCost);
        networkCost = Double.max(0, networkCost);
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;

        this.cost = costWeight.cpuWeight * cpuCost + costWeight.memoryWeight * memoryCost
                + costWeight.networkWeight * networkCost;
    }

    private Cost(double cost, double cpuCost, double memoryCost, double networkCost) {
        this.cost = cost;
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    public static Cost infinite() {
        return INFINITE;
    }

    public static Cost zero() {
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

    public double getValue() {
        return cost;
    }

    public static Cost of(CostWeight costWeight, double cpuCost, double maxMemory, double networkCost) {
        return new Cost(costWeight, cpuCost, maxMemory, networkCost);
    }

    public static Cost ofCpu(CostWeight costWeight, double cpuCost) {
        return new Cost(costWeight, cpuCost, 0, 0);
    }

    /** Add another cost and compute the weighted value from the summed components. */
    public Cost add(Cost other, CostWeight costWeight) {
        return new Cost(costWeight,
                cpuCost + other.cpuCost,
                memoryCost + other.memoryCost,
                networkCost + other.networkCost);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append((long) cpuCost).append("/")
                .append((long) memoryCost).append("/").append((long) networkCost)
                .append("/").append("]");
        return sb.toString();
    }
}
