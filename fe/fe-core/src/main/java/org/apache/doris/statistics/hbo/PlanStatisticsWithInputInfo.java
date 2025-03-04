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

package org.apache.doris.statistics.hbo;

import java.util.Objects;


public class PlanStatisticsWithInputInfo {
    private final int nodeId;
    private final PlanStatistics planStatistics;

    private final InputTableStatisticsInfo inputTableInfo;

    public PlanStatisticsWithInputInfo(int id, PlanStatistics planStatistics, InputTableStatisticsInfo inputTableInfo) {
        this.nodeId = id;
        this.planStatistics = Objects.requireNonNull(planStatistics, "planStatistics is null");
        this.inputTableInfo = inputTableInfo;
    }


    public int getId() {
        return nodeId;
    }

    public PlanStatistics getPlanStatistics() {
        return planStatistics;
    }

    public InputTableStatisticsInfo getInputTableInfo() {
        return inputTableInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanStatisticsWithInputInfo that = (PlanStatisticsWithInputInfo) o;
        return nodeId == that.nodeId && planStatistics.equals(that.planStatistics)
                && inputTableInfo.equals(that.inputTableInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, planStatistics, inputTableInfo);
    }

}
