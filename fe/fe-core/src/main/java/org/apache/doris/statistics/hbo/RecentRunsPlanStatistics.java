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

public class RecentRunsPlanStatistics {
    private static final RecentRunsPlanStatistics EMPTY = new RecentRunsPlanStatistics(null);
    private final List<RecentRunsPlanStatisticsEntry> recentRunsStatistics;

    public RecentRunsPlanStatistics(List<RecentRunsPlanStatisticsEntry> recentRunsStatistics) {
        this.recentRunsStatistics = recentRunsStatistics == null ? new ArrayList<>() : recentRunsStatistics;
    }

    public List<RecentRunsPlanStatisticsEntry> getRecentRunsStatistics() {
        return recentRunsStatistics;
    }

    @Override
    public String toString() {
        return String.format("RecentRunsPlanStatistics{recentRunsStatistics=%s}", recentRunsStatistics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecentRunsPlanStatistics other = (RecentRunsPlanStatistics) o;

        return Objects.equals(recentRunsStatistics, other.recentRunsStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recentRunsStatistics);
    }

    public static RecentRunsPlanStatistics empty() {
        return EMPTY;
    }
}
