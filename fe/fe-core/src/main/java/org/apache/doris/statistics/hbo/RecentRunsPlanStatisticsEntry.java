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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RecentRunsPlanStatisticsEntry {
    private final PlanStatistics planStatistics;
    private final List<PlanStatistics> inputTableStatistics;

    public RecentRunsPlanStatisticsEntry(PlanStatistics planStatistics, List<PlanStatistics> inputTableStatistics) {
        this.planStatistics = planStatistics == null ? PlanStatistics.EMPTY : planStatistics;
        this.inputTableStatistics = inputTableStatistics == null ? new ArrayList<>() : inputTableStatistics;
    }

    public PlanStatistics getPlanStatistics() {
        return planStatistics;
    }

    public List<PlanStatistics> getInputTableStatistics() {
        return inputTableStatistics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecentRunsPlanStatisticsEntry that = (RecentRunsPlanStatisticsEntry) o;
        return Objects.equals(planStatistics, that.planStatistics)
                && Objects.equals(inputTableStatistics, that.inputTableStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planStatistics, inputTableStatistics);
    }

    @Override
    public String toString() {
        return String.format("RecentRunsPlanStatisticsEntry{planStatistics=%s, inputTableStatistics=%s}",
                planStatistics, inputTableStatistics);
    }
}
