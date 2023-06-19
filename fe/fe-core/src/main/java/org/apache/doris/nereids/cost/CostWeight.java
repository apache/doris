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

import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

/**
 * cost weight.
 * The intuition behind `HEAVY_OPERATOR_PUNISH_FACTOR` is we need to avoid this form of join patterns:
 * Plan1: L join ( AGG1(A) join AGG2(B))
 * But
 * Plan2: L join AGG1(A) join AGG2(B) is welcomed.
 * AGG is time-consuming operator. From the perspective of rowCount, nereids may choose Plan1,
 * because `Agg1 join Agg2` generates few tuples. But in Plan1, Agg1 and Agg2 are done in serial, in Plan2, Agg1 and
 * Agg2 are done in parallel. And hence, Plan1 should be punished.
 * <p>
 * An example is tpch q15.
 */
public class CostWeight {
    static final double CPU_WEIGHT = 1;
    static final double MEMORY_WEIGHT = 1;
    static final double NETWORK_WEIGHT = 1.5;
    static final double DELAY = 0.5;

    final double cpuWeight;
    final double memoryWeight;
    final double networkWeight;
    final double ioWeight;
    /*
     * About PENALTY:
     * Except stats information, there are some special criteria in doris.
     * For example, in hash join cluster, BE could build hash tables
     * in parallel for left deep tree. And hence, we need to punish right deep tree.
     * penaltyWeight is the factor of punishment.
     * The punishment is denoted by stats.penalty.
     */
    final double penaltyWeight;

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
        this.ioWeight = 1;
    }

    public static CostWeight get() {
        return new CostWeight(CPU_WEIGHT, MEMORY_WEIGHT, NETWORK_WEIGHT,
                ConnectContext.get().getSessionVariable().getNereidsCboPenaltyFactor());
    }

    //TODO: add it in session variable
    public static double getDelay() {
        return DELAY;
    }

    public double weightSum(double cpuCost, double ioCost, double netCost) {
        return cpuCost * cpuWeight + ioCost * ioWeight + netCost * networkWeight;
    }
}
