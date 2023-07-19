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

class CostV1 implements Cost {
    private static final CostV1 INFINITE = new CostV1(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    private static final CostV1 ZERO = new CostV1(0, 0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;
    //penalty for
    // 1. right deep tree
    // 2. right XXX join
    private final double penalty;

    private final double cost;

    /**
     * Constructor of CostEstimate.
     */
    public CostV1(double cpuCost, double memoryCost, double networkCost, double penaltiy) {
        // TODO: fix stats
        cpuCost = Double.max(0, cpuCost);
        memoryCost = Double.max(0, memoryCost);
        networkCost = Double.max(0, networkCost);
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
        this.penalty = penaltiy;

        CostWeight costWeight = CostWeight.get();
        this.cost = costWeight.cpuWeight * cpuCost + costWeight.memoryWeight * memoryCost
                + costWeight.networkWeight * networkCost + costWeight.penaltyWeight * penalty;
    }

    public CostV1(double cost) {
        this.cost = cost;
        this.cpuCost = 0;
        this.networkCost = 0;
        this.memoryCost = 0;
        this.penalty = 0;
    }

    public static CostV1 infinite() {
        return INFINITE;
    }

    public static CostV1 zero() {
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

    public double getValue() {
        return cost;
    }

    public static CostV1 of(double cpuCost, double maxMemory, double networkCost, double rightDeepPenaltiy) {
        return new CostV1(cpuCost, maxMemory, networkCost, rightDeepPenaltiy);
    }

    public static CostV1 of(double cpuCost, double maxMemory, double networkCost) {
        return new CostV1(cpuCost, maxMemory, networkCost, 0);
    }

    public static CostV1 ofCpu(double cpuCost) {
        return new CostV1(cpuCost, 0, 0, 0);
    }

    public static CostV1 ofMemory(double memoryCost) {
        return new CostV1(0, memoryCost, 0, 0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append((long) cpuCost).append("/")
                .append((long) memoryCost).append("/").append((long) networkCost)
                .append("/").append((long) penalty).append("]");
        return sb.toString();
    }
}

