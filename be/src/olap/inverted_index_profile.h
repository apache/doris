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

#include <string>
#include <vector>

#include "olap/inverted_index_stats.h"
#include "util/runtime_profile.h"

namespace doris {

class InvertedIndexProfileReporter {
public:
    InvertedIndexProfileReporter() = default;
    ~InvertedIndexProfileReporter() = default;

    void update(RuntimeProfile* profile, const InvertedIndexStatistics* statistics) {
        // Determine the iteration limit: the smaller of 20 or the size of statistics->stats
        size_t iteration_limit = std::min<size_t>(20, statistics->stats.size());

        for (size_t i = 0; i < iteration_limit; ++i) {
            const auto& stats = statistics->stats[i];

            ADD_TIMER_WITH_LEVEL(profile, hit_rows_name, 1);
            auto* hit_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "HitRows_" + stats.column_name,
                                                          TUnit::UNIT, hit_rows_name, 1);
            COUNTER_UPDATE(hit_rows, stats.hit_rows);

            ADD_TIMER_WITH_LEVEL(profile, exec_time_name, 1);
            auto* exec_time = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ExecTime_" + stats.column_name,
                                                           TUnit::TIME_NS, exec_time_name, 1);
            COUNTER_UPDATE(exec_time, stats.exec_time);
        }
    }

private:
    static constexpr const char* hit_rows_name = "HitRows";
    static constexpr const char* exec_time_name = "ExecTime";
};

} // namespace doris
