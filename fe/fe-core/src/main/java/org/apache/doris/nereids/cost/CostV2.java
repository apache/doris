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

class CostV2 implements Cost {
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
    CostV2(double startCost, double runCost) {
        this.runCost = runCost;
        this.startCost = startCost;
        this.childRunCost = 0;
        this.childStartCost = 0;
        this.cost = startCost + runCost;
    }

    public void setLimitRation(double ratio) {
        this.limitRatio = Double.min(1, ratio);
    }

    public double getLimitRation() {
        return limitRatio;
    }

    public void updateChildCost(double childStartCost, double childRunCost) {
        this.childStartCost = Double.max(childStartCost, this.childStartCost);
        this.childRunCost = Double.max(childRunCost, this.childRunCost);
        this.cost = startCost + this.childStartCost + Double.max(
                this.childRunCost + this.runCost * CostWeight.getDelay(), this.runCost);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(runCost)
                .append("/").append(startCost)
                .append("/").append(cost);
        return sb.toString();
    }

    @Override
    public double getValue() {
        return Double.min(cost, Double.MAX_VALUE);
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

    public static Cost zero() {
        return new CostV2(0, 0);
    }

    public static Cost infinite() {
        return new CostV2(0, Double.MAX_VALUE);
    }
}

