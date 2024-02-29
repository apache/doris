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

/**
 * This cost model calculates the distributed cost by dividing it into two components: startCost and runCost.
 * The startCost represents the cost of an operator starting to emit the first tuple, while the runCost represents
 * the cost of the operator emitting all tuples.
 * <p>
 * If all operators run in parallel, the child and parent operators can be represented as follows:
 * childStart ---> childRun
 * |---> operatorStart ---> operatorRun
 * <p>
 * If all operators run serially, the order would be:
 * childStart ---> childRun ---> operatorStart ---> operatorRun
 * <p>
 * The degree of parallelism is controlled by the decay parameter, with a value of 1 indicating perfect serial execution
 * and a value of 0 indicating perfect parallel execution.
 */
class CostV2 implements Cost {
    double memory;
    double runCost;
    double startCost;
    double childStartCost;
    double childRunCost;
    double cost;

    double leftStartCost = 0;
    double limitRatio = 1;

    /**
     * Constructor of CostV2.
     */
    CostV2(double startCost, double runCost, double memory) {
        this.memory = memory;
        this.runCost = makeValidDouble(runCost);
        this.startCost = makeValidDouble(startCost);
        this.childRunCost = 0;
        this.childStartCost = 0;
        this.cost = this.startCost + this.runCost;
    }

    public void setLimitRation(double ratio) {
        this.limitRatio = Double.min(1, ratio);
    }

    public double getLimitRation() {
        return limitRatio;
    }

    public void updateChildCost(double childStartCost, double childRunCost, double memory) {
        childStartCost = makeValidDouble(childStartCost);
        childRunCost = makeValidDouble(childRunCost);
        this.childStartCost = Double.max(childStartCost, this.childStartCost);
        this.childRunCost = Double.max(childRunCost, this.childRunCost);
        this.cost = startCost + this.childStartCost + Double.max(
                this.childRunCost + this.runCost * CostWeight.getDelay(), this.runCost);
        this.memory += memory;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(runCost)
                .append("/").append(startCost)
                .append("/").append(cost);
        return sb.toString();
    }

    private double makeValidDouble(Double value) {
        if (Double.isNaN(value)) {
            return 0;
        }
        if (Double.isInfinite(value)) {
            return Double.MAX_VALUE / 1000;
        }
        return value;
    }

    @Override
    public double getValue() {
        double maxExecMemByte = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
        if (memory > maxExecMemByte) {
            cost *= Math.ceil(memory / maxExecMemByte);
        }
        return cost;
    }

    public void finish() {
        startCost = startCost + childStartCost;
        runCost = cost - startCost;
    }

    public double getRunCost() {
        return runCost;
    }

    public double getStartCost() {
        return startCost;
    }

    public double getCost() {
        return cost;
    }

    public double getMemory() {
        return memory;
    }

    public static Cost zero() {
        return new CostV2(0, 0, 0);
    }

    public static Cost infinite() {
        return new CostV2(0, Double.MAX_VALUE, Double.MAX_VALUE);
    }
}
