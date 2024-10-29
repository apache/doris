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

package org.apache.doris.statistics;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Objects;

public class HistoricalPlanStatistics
{
    private static final HistoricalPlanStatistics EMPTY = new HistoricalPlanStatistics(emptyList());

    // Output plan statistics from previous runs
    private final List<HistoricalPlanStatisticsEntry> lastRunsStatistics;

    public HistoricalPlanStatistics(List<HistoricalPlanStatisticsEntry> lastRunsStatistics)
    {
        // Check for nulls, to make it thrift backwards compatible
        this.lastRunsStatistics = unmodifiableList(lastRunsStatistics == null ? emptyList() : lastRunsStatistics);
    }

    public List<HistoricalPlanStatisticsEntry> getLastRunsStatistics()
    {
        return lastRunsStatistics;
    }

    @Override
    public String toString()
    {
        return format("HistoricalPlanStatistics{lastRunsStatistics=%s}", lastRunsStatistics);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HistoricalPlanStatistics other = (HistoricalPlanStatistics) o;

        return Objects.equals(lastRunsStatistics, other.lastRunsStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastRunsStatistics);
    }

    public static HistoricalPlanStatistics empty()
    {
        return EMPTY;
    }
}