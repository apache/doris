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
 * cost weight.
 */
public class CostWeight {
    private final double cpuWeight;
    private final double memoryWeight;
    private final double networkWeight;
    private final double penaltyWeight;

    /**
     * Constructor
     */
    public CostWeight(double cpuWeight, double memoryWeight, double networkWeight, double penaltyWeight) {
        Preconditions.checkArgument(cpuWeight >= 0, "cpuWeight cannot be negative");
        Preconditions.checkArgument(memoryWeight >= 0, "memoryWeight cannot be negative");
        Preconditions.checkArgument(networkWeight >= 0, "networkWeight cannot be negative");

        this.cpuWeight = cpuWeight;
        this.memoryWeight = memoryWeight;
        this.networkWeight = networkWeight;
        this.penaltyWeight = penaltyWeight;
    }

    public double calculate(CostEstimate costEstimate) {
        return costEstimate.getCpuCost() * cpuWeight + costEstimate.getMemoryCost() * memoryWeight
                + costEstimate.getNetworkCost() * networkWeight
                + costEstimate.getPenalty() * penaltyWeight;
    }
}
