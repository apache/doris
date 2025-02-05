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
        for (auto stats : statistics->stats) {
            ADD_TIMER_WITH_LEVEL(profile, filter_rows_name, 1);
            auto* filter_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "fr_" + stats.column_name,
                                                             TUnit::UNIT, filter_rows_name, 1);
            COUNTER_UPDATE(filter_rows, stats.filter_rows);
        }

        for (auto stats : statistics->stats) {
            ADD_TIMER_WITH_LEVEL(profile, filter_time_name, 1);
            auto* filter_time = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "ft_" + stats.column_name,
                                                             TUnit::TIME_NS, filter_time_name, 1);
            COUNTER_UPDATE(filter_time, stats.filter_time);
        }
    }

private:
    static constexpr const char* filter_rows_name = "FilteredRows";
    static constexpr const char* filter_time_name = "FilteredTime";
};

} // namespace doris
