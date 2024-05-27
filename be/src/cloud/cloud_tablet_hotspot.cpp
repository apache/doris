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

#include "cloud/cloud_tablet_hotspot.h"

#include <chrono>
#include <mutex>

#include "cloud/config.h"
#include "olap/tablet_fwd.h"
#include "runtime/exec_env.h"

namespace doris {

void TabletHotspot::count(const BaseTablet& tablet) {
    size_t slot_idx = tablet.tablet_id() % s_slot_size;
    auto& slot = _tablets_hotspot[slot_idx];
    std::lock_guard lock(slot.mtx);
    HotspotCounterPtr counter;
    if (auto iter = slot.map.find(tablet.tablet_id()); iter == slot.map.end()) {
        counter = std::make_shared<HotspotCounter>(tablet.table_id(), tablet.index_id(),
                                                   tablet.partition_id());
        slot.map.insert(std::make_pair(tablet.tablet_id(), counter));
    } else {
        counter = iter->second;
    }
    counter->last_access_time = std::chrono::system_clock::now();
    counter->cur_counter++;
}

TabletHotspot::TabletHotspot() {
    _counter_thread = std::thread(&TabletHotspot::make_dot_point, this);
}

TabletHotspot::~TabletHotspot() {
    {
        std::lock_guard lock(_mtx);
        _closed = true;
    }
    _cond.notify_all();
    if (_counter_thread.joinable()) {
        _counter_thread.join();
    }
}

struct MapKeyHash {
    int64_t operator()(const std::pair<int64_t, int64_t>& key) const {
        return std::hash<int64_t> {}(key.first) + std::hash<int64_t> {}(key.second);
    }
};
struct TabletHotspotMapValue {
    uint64_t qpd = 0; // query per day
    uint64_t qpw = 0; // query per week
    int64_t last_access_time;
};

using TabletHotspotMapKey = std::pair<int64_t, int64_t>;

void TabletHotspot::get_top_n_hot_partition(std::vector<THotTableMessage>* hot_tables) {
    // map<pair<table_id, index_id>, map<partition_id, value>> for day
    std::unordered_map<TabletHotspotMapKey, std::unordered_map<int64_t, TabletHotspotMapValue>,
                       MapKeyHash>
            day_hot_partitions;
    // map<pair<table_id, index_id>, map<partition_id, value>> for week
    std::unordered_map<TabletHotspotMapKey, std::unordered_map<int64_t, TabletHotspotMapValue>,
                       MapKeyHash>
            week_hot_partitions;

    std::for_each(_tablets_hotspot.begin(), _tablets_hotspot.end(), [&](HotspotMap& map) {
        std::lock_guard lock(map.mtx);
        for (auto& [_, counter] : map.map) {
            if (counter->qpd() != 0) {
                auto& hot_partition = day_hot_partitions[std::make_pair(
                        counter->table_id, counter->index_id)][counter->partition_id];
                hot_partition.qpd = std::max(hot_partition.qpd, counter->qpd());
                hot_partition.qpw = std::max(hot_partition.qpw, counter->qpw());
                hot_partition.last_access_time =
                        std::max(hot_partition.last_access_time,
                                 std::chrono::duration_cast<std::chrono::seconds>(
                                         counter->last_access_time.time_since_epoch())
                                         .count());
            } else if (counter->qpw() != 0) {
                auto& hot_partition = week_hot_partitions[std::make_pair(
                        counter->table_id, counter->index_id)][counter->partition_id];
                hot_partition.qpd = 0;
                hot_partition.qpw = std::max(hot_partition.qpw, counter->qpw());
                hot_partition.last_access_time =
                        std::max(hot_partition.last_access_time,
                                 std::chrono::duration_cast<std::chrono::seconds>(
                                         counter->last_access_time.time_since_epoch())
                                         .count());
            }
        }
    });
    constexpr int N = 50;
    int return_partitions = 0;
    auto get_return_partitions =
            [=, &return_partitions](
                    const std::unordered_map<TabletHotspotMapKey,
                                             std::unordered_map<int64_t, TabletHotspotMapValue>,
                                             MapKeyHash>& hot_partition) {
                for (const auto& [key, partition_to_value] : hot_partition) {
                    THotTableMessage msg;
                    msg.table_id = key.first;
                    msg.index_id = key.second;
                    for (const auto& [partition_id, value] : partition_to_value) {
                        if (return_partitions > N) {
                            return;
                        }
                        THotPartition hot_partition;
                        hot_partition.__set_partition_id(partition_id);
                        hot_partition.__set_query_per_day(value.qpd);
                        hot_partition.__set_query_per_week(value.qpw);
                        hot_partition.__set_last_access_time(value.last_access_time);
                        msg.hot_partitions.push_back(hot_partition);
                        return_partitions++;
                    }
                    msg.__isset.hot_partitions = !msg.hot_partitions.empty();
                    hot_tables->push_back(std::move(msg));
                }
            };
    get_return_partitions(day_hot_partitions);
    get_return_partitions(week_hot_partitions);
}

void HotspotCounter::make_dot_point() {
    uint64_t value = cur_counter.load();
    cur_counter = 0;
    if (history_counters.size() == week_counters_size) {
        uint64_t week_counter_remove = history_counters.back();
        uint64_t day_counter_remove = history_counters[day_counters_size - 1];
        week_history_counter = week_history_counter - week_counter_remove + value;
        day_history_counter = day_history_counter - day_counter_remove + value;
        history_counters.pop_back();
    } else if (history_counters.size() < day_counters_size) {
        week_history_counter += value;
        day_history_counter += value;
    } else {
        week_history_counter += value;
        uint64_t day_counter_remove = history_counters[day_counters_size - 1];
        day_history_counter = day_history_counter - day_counter_remove + value;
    }
    history_counters.push_front(value);
}

uint64_t HotspotCounter::qpd() {
    return day_history_counter + cur_counter.load();
}

uint64_t HotspotCounter::qpw() {
    return week_history_counter + cur_counter.load();
}

void TabletHotspot::make_dot_point() {
    while (true) {
        {
            std::unique_lock lock(_mtx);
            _cond.wait_for(lock, std::chrono::seconds(HotspotCounter::time_interval),
                           [this]() { return _closed; });
            if (_closed) {
                break;
            }
        }
        std::for_each(_tablets_hotspot.begin(), _tablets_hotspot.end(), [](HotspotMap& map) {
            std::vector<HotspotCounterPtr> counters;
            {
                std::lock_guard lock(map.mtx);
                for (auto& [_, counter] : map.map) {
                    counters.push_back(counter);
                }
            }
            std::for_each(counters.begin(), counters.end(),
                          [](HotspotCounterPtr& counter) { counter->make_dot_point(); });
        });
    }
}

} // namespace doris
