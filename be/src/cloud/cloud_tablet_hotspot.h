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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>

#include "gen_cpp/BackendService.h"
#include "olap/tablet.h"

namespace doris {

// This counter is used to count the tablet query frequency
struct HotspotCounter {
    HotspotCounter(int64_t table_id, int64_t index_id, int64_t partition_id)
            : table_id(table_id), index_id(index_id), partition_id(partition_id) {}
    // One break point every hour
    void make_dot_point();
    uint64_t qpd();
    uint64_t qpw();
    int64_t table_id;
    int64_t index_id;
    int64_t partition_id;
    std::chrono::system_clock::time_point last_access_time;
    std::atomic_uint64_t cur_counter {0};
    std::deque<uint64_t> history_counters;
    std::atomic_uint64_t week_history_counter {0};
    std::atomic_uint64_t day_history_counter {0};
    static inline int64_t time_interval = 1 * 60 * 60;
    static inline int64_t week_counters_size = ((7 * 24 * 60 * 60) / time_interval) - 1;
    static inline int64_t day_counters_size = ((24 * 60 * 60) / time_interval) - 1;
};

using HotspotCounterPtr = std::shared_ptr<HotspotCounter>;

class TabletHotspot {
public:
    TabletHotspot();
    ~TabletHotspot();
    // When query the tablet, count it
    void count(const BaseTablet& tablet);
    void get_top_n_hot_partition(std::vector<THotTableMessage>* hot_tables);

private:
    void make_dot_point();

    struct HotspotMap {
        std::mutex mtx;
        std::unordered_map<int64_t, HotspotCounterPtr> map;
    };
    static constexpr size_t s_slot_size = 1024;
    std::array<HotspotMap, s_slot_size> _tablets_hotspot;
    std::thread _counter_thread;
    bool _closed {false};
    std::mutex _mtx;
    std::condition_variable _cond;
};

} // namespace doris
