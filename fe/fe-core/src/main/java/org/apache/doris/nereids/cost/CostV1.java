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

import org.apache.doris.qe.SessionVariable;

class CostV1 implements Cost {
    private static final CostV1 INFINITE = new CostV1(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY);
    private static final CostV1 ZERO = new CostV1(0, 0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    private final double cost;

    /**
     * Constructor of CostV1.
     */
    public CostV1(SessionVariable sessionVariable, double cpuCost, double memoryCost, double networkCost) {
        // TODO: fix stats
        cpuCost = Double.max(0, cpuCost);
        memoryCost = Double.max(0, memoryCost);
        networkCost = Double.max(0, networkCost);
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;

        CostWeight costWeight = CostWeight.get(sessionVariable);
        this.cost = costWeight.cpuWeight * cpuCost + costWeight.memoryWeight * memoryCost
                + costWeight.networkWeight * networkCost;
    }

    private CostV1(double cost, double cpuCost, double memoryCost, double networkCost) {
        this.cost = cost;
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
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

    public double getValue() {
        return cost;
    }

    public static CostV1 of(SessionVariable sessionVariable, double cpuCost, double maxMemory, double networkCost) {
        return new CostV1(sessionVariable, cpuCost, maxMemory, networkCost);
    }

    public static CostV1 ofCpu(SessionVariable sessionVariable, double cpuCost) {
        return new CostV1(sessionVariable, cpuCost, 0, 0);
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
