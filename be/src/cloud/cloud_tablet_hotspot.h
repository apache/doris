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

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "gen_cpp/BackendService.h"
#include "storage/tablet/tablet_fwd.h"

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
using TabletHotspotMapKey = std::pair<int64_t, int64_t>;

struct TabletHotspotMapValue {
    uint64_t qpd = 0; // query per day
    uint64_t qpw = 0; // query per week
    int64_t last_access_time;
};

struct MapKeyHash {
    int64_t operator()(const std::pair<int64_t, int64_t>& key) const {
        return std::hash<int64_t> {}(key.first) + std::hash<int64_t> {}(key.second);
    }
};

class TabletHotspot {
public:
    struct MaintenanceStats {
        uint64_t total_counters_before_gc = 0;
        uint64_t total_counters_after_gc = 0;
        uint64_t non_empty_slots_before_gc = 0;
        uint64_t non_empty_slots_after_gc = 0;
        uint64_t max_slot_size_before_gc = 0;
        uint64_t max_slot_size_after_gc = 0;
        uint64_t sum_bucket_count_before_gc = 0;
        uint64_t sum_bucket_count_after_gc = 0;
        uint64_t copied_counters = 0;
        uint64_t evicted_counters = 0;
        uint64_t compacted_slots = 0;
        uint64_t gc_elapsed_ms = 0;
    };

    explicit TabletHotspot(bool start_counter_thread = true);
    ~TabletHotspot();
    // When query the tablet, count it
    void count(const BaseTablet& tablet);
    void get_top_n_hot_partition(std::vector<THotTableMessage>* hot_tables);

private:
    void count(int64_t tablet_id, int64_t table_id, int64_t index_id, int64_t partition_id);
    void make_dot_point();
    MaintenanceStats run_maintenance_once(std::chrono::system_clock::time_point now);
    static bool is_gc_eligible(const HotspotCounter& counter,
                               std::chrono::system_clock::time_point now);

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

    std::mutex _last_partitions_mtx;
    std::unordered_map<TabletHotspotMapKey, std::unordered_map<int64_t, TabletHotspotMapValue>,
                       MapKeyHash>
            _last_day_hot_partitions;
    std::unordered_map<TabletHotspotMapKey, std::unordered_map<int64_t, TabletHotspotMapValue>,
                       MapKeyHash>
            _last_week_hot_partitions;
    std::atomic_uint64_t _count_calls_total {0};
    std::atomic_uint64_t _existing_hit_total {0};
    std::atomic_uint64_t _new_counter_total {0};
    std::atomic_uint64_t _last_round_new_counter_total {0};
    std::atomic_uint64_t _get_top_n_hot_partition_call_total {0};
    std::atomic_uint64_t _make_dot_point_round_total {0};
};

} // namespace doris
