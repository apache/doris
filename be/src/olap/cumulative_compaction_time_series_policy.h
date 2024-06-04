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

#pragma once

#include "olap/cumulative_compaction_policy.h"

namespace doris {

inline constexpr std::string_view CUMULATIVE_TIME_SERIES_POLICY = "time_series";

/// TimeSeries cumulative compaction policy implementation.
/// The following three conditions will be considered when calculating compaction scores and selecting input rowsets in this policy:
/// Condition 1: the size of input files for compaction meets the requirement of time_series_compaction_goal_size_mbytes
/// Condition 2: the number of input files reaches the threshold specified by time_series_compaction_file_count_threshold
/// Condition 3: the time interval between compactions exceeds the value specified by  time_series_compaction_time_threshold_seconds
/// The conditions are evaluated sequentially, starting with Condition 1.
/// If any condition is met, the compaction score calculation or selection of input rowsets will be successful.
class TimeSeriesCumulativeCompactionPolicy final : public CumulativeCompactionPolicy {
public:
    TimeSeriesCumulativeCompactionPolicy() = default;
    ~TimeSeriesCumulativeCompactionPolicy() {}

    // Its main policy is calculating the accumulative compaction score after current cumulative_point in tablet.
    uint32_t calc_cumulative_compaction_score(Tablet* tablet) override;

    /// TimeSeris cumulative compaction policy implements calculate cumulative point function.
    /// When the first time the tablet does compact, this calculation is executed. Its main policy is to find first rowset
    /// which does not satisfied the _compaction_goal_size * 0.8.
    /// The result of compaction may be slightly smaller than the _compaction_goal_size.
    void calculate_cumulative_point(Tablet* tablet,
                                    const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                    int64_t current_cumulative_point,
                                    int64_t* cumulative_point) override;

    /// Its main policy is picking rowsets from candidate rowsets by Condition 1, 2, 3.
    int pick_input_rowsets(Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
                           const int64_t max_compaction_score, const int64_t min_compaction_score,
                           std::vector<RowsetSharedPtr>* input_rowsets,
                           Version* last_delete_version, size_t* compaction_score,
                           bool allow_delete = false) override;

    /// The point must be updated after each cumulative compaction is completed.
    /// We want each rowset to do cumulative compaction once.
    void update_cumulative_point(Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr _output_rowset,
                                 Version& last_delete_version) override;

    void update_compaction_level(Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr output_rowset) override;

    std::string_view name() override { return CUMULATIVE_TIME_SERIES_POLICY; }
};

} // namespace doris