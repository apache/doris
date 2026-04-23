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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <vector>

#include "runtime/memory/global_memory_arbitrator.h"
#include "storage/tablet/base_tablet.h"

namespace doris {

namespace {

using HotPartitionSnapshot =
        std::unordered_map<TabletHotspotMapKey, std::unordered_map<int64_t, TabletHotspotMapValue>,
                           MapKeyHash>;

constexpr uint64_t kCountHeartbeatInterval = 500000;
constexpr uint64_t kNewCounterLogInterval = 4096;
constexpr size_t kCompactBucketMultiplier = 4;
constexpr size_t kCompactEraseRatioDivisor = 4;

using SystemTimePoint = std::chrono::system_clock::time_point;
using SteadyClock = std::chrono::steady_clock;

double bytes_to_mb(int64_t bytes) {
    return static_cast<double>(bytes) / (1024.0 * 1024.0);
}

double ratio_or_zero(uint64_t numerator, uint64_t denominator) {
    if (denominator == 0) {
        return 0.0;
    }
    return static_cast<double>(numerator) / static_cast<double>(denominator);
}

int64_t process_memory_usage_for_diag() {
#ifdef BE_TEST
    return 0;
#else
    return GlobalMemoryArbitrator::process_memory_usage();
#endif
}

uint64_t count_hot_partition_entries(const HotPartitionSnapshot& snapshot) {
    uint64_t entries = 0;
    for (const auto& [_, partition_to_value] : snapshot) {
        entries += partition_to_value.size();
    }
    return entries;
}

bool should_compact_slot(size_t slot_size_before, size_t slot_size_after, size_t bucket_count_after,
                         size_t erased_count) {
    if (erased_count == 0) {
        return false;
    }
    if (slot_size_after == 0) {
        return true;
    }
    return bucket_count_after > (kCompactBucketMultiplier * slot_size_after) ||
           (slot_size_before > 0 && erased_count * kCompactEraseRatioDivisor >= slot_size_before);
}

} // namespace

void TabletHotspot::count(const BaseTablet& tablet) {
    count(tablet.tablet_id(), tablet.table_id(), tablet.index_id(), tablet.partition_id());
}

void TabletHotspot::count(int64_t tablet_id, int64_t table_id, int64_t index_id,
                          int64_t partition_id) {
    const uint64_t count_calls_total =
            _count_calls_total.fetch_add(1, std::memory_order_relaxed) + 1;

    size_t slot_idx = tablet_id % s_slot_size;
    auto& slot = _tablets_hotspot[slot_idx];
    bool should_log_new_counter = false;
    uint64_t new_counter_total = 0;
    {
        std::lock_guard lock(slot.mtx);
        HotspotCounterPtr counter;
        if (auto iter = slot.map.find(tablet_id); iter == slot.map.end()) {
            counter = std::make_shared<HotspotCounter>(table_id, index_id, partition_id);
            slot.map.insert(std::make_pair(tablet_id, counter));
            new_counter_total = _new_counter_total.fetch_add(1, std::memory_order_relaxed) + 1;
            should_log_new_counter = (new_counter_total % kNewCounterLogInterval == 0);
        } else {
            counter = iter->second;
            _existing_hit_total.fetch_add(1, std::memory_order_relaxed);
        }
        counter->last_access_time = std::chrono::system_clock::now();
        counter->cur_counter.fetch_add(1, std::memory_order_relaxed);
    }

    if (should_log_new_counter) {
        LOG(INFO) << "tablet_hotspot_diag new_counter_total=" << new_counter_total
                  << " count_calls_total=" << count_calls_total
                  << " existing_hit_total=" << _existing_hit_total.load(std::memory_order_relaxed);
    }
    if (count_calls_total % kCountHeartbeatInterval == 0) {
        LOG(INFO) << "tablet_hotspot_diag count_heartbeat count_calls_total=" << count_calls_total
                  << " existing_hit_total=" << _existing_hit_total.load(std::memory_order_relaxed)
                  << " new_counter_total=" << _new_counter_total.load(std::memory_order_relaxed);
    }
}

TabletHotspot::TabletHotspot(bool start_counter_thread) {
    if (start_counter_thread) {
        _counter_thread = std::thread(&TabletHotspot::make_dot_point, this);
    }
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

void get_return_partitions(
        const std::unordered_map<TabletHotspotMapKey,
                                 std::unordered_map<int64_t, TabletHotspotMapValue>, MapKeyHash>&
                hot_partition,
        const std::unordered_map<TabletHotspotMapKey,
                                 std::unordered_map<int64_t, TabletHotspotMapValue>, MapKeyHash>&
                last_hot_partition,
        std::vector<THotTableMessage>* hot_tables, int& return_partitions, int N) {
    for (const auto& [key, partition_to_value] : hot_partition) {
        THotTableMessage msg;
        msg.table_id = key.first;
        msg.index_id = key.second;
        for (const auto& [partition_id, value] : partition_to_value) {
            if (return_partitions > N) {
                return;
            }
            auto last_value_iter = last_hot_partition.find(key);
            if (last_value_iter != last_hot_partition.end()) {
                auto last_partition_iter = last_value_iter->second.find(partition_id);
                if (last_partition_iter != last_value_iter->second.end()) {
                    const auto& last_value = last_partition_iter->second;
                    if (std::abs(static_cast<int64_t>(value.qpd) -
                                 static_cast<int64_t>(last_value.qpd)) < 5 &&
                        std::abs(static_cast<int64_t>(value.qpw) -
                                 static_cast<int64_t>(last_value.qpw)) < 10 &&
                        std::abs(static_cast<int64_t>(value.last_access_time) -
                                 static_cast<int64_t>(last_value.last_access_time)) < 60) {
                        LOG(INFO) << "skip partition_id=" << partition_id << " qpd=" << value.qpd
                                  << " qpw=" << value.qpw
                                  << " last_access_time=" << value.last_access_time
                                  << " last_qpd=" << last_value.qpd
                                  << " last_qpw=" << last_value.qpw
                                  << " last_access_time=" << last_value.last_access_time;
                        continue;
                    }
                }
            }
            THotPartition new_hot_partition;
            new_hot_partition.__set_partition_id(partition_id);
            new_hot_partition.__set_query_per_day(value.qpd);
            new_hot_partition.__set_query_per_week(value.qpw);
            new_hot_partition.__set_last_access_time(value.last_access_time);
            msg.hot_partitions.push_back(new_hot_partition);
            return_partitions++;
        }
        msg.__isset.hot_partitions = !msg.hot_partitions.empty();
        hot_tables->push_back(std::move(msg));
    }
}

void TabletHotspot::get_top_n_hot_partition(std::vector<THotTableMessage>* hot_tables) {
    const uint64_t call_id =
            _get_top_n_hot_partition_call_total.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t last_day_tables_before = 0;
    uint64_t last_day_entries_before = 0;
    uint64_t last_week_tables_before = 0;
    uint64_t last_week_entries_before = 0;
    {
        std::lock_guard lock(_last_partitions_mtx);
        last_day_tables_before = _last_day_hot_partitions.size();
        last_day_entries_before = count_hot_partition_entries(_last_day_hot_partitions);
        last_week_tables_before = _last_week_hot_partitions.size();
        last_week_entries_before = count_hot_partition_entries(_last_week_hot_partitions);
    }
    const int64_t process_mem_before = process_memory_usage_for_diag();

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
                        std::max<int64_t>(hot_partition.last_access_time,
                                          std::chrono::duration_cast<std::chrono::seconds>(
                                                  counter->last_access_time.time_since_epoch())
                                                  .count());
            } else if (counter->qpw() != 0) {
                auto& hot_partition = week_hot_partitions[std::make_pair(
                        counter->table_id, counter->index_id)][counter->partition_id];
                hot_partition.qpd = 0;
                hot_partition.qpw = std::max(hot_partition.qpw, counter->qpw());
                hot_partition.last_access_time =
                        std::max<int64_t>(hot_partition.last_access_time,
                                          std::chrono::duration_cast<std::chrono::seconds>(
                                                  counter->last_access_time.time_since_epoch())
                                                  .count());
            }
        }
    });
    constexpr int N = 50;
    int return_partitions = 0;
    const uint64_t day_tables_built = day_hot_partitions.size();
    const uint64_t day_entries_built = count_hot_partition_entries(day_hot_partitions);
    const uint64_t week_tables_built = week_hot_partitions.size();
    const uint64_t week_entries_built = count_hot_partition_entries(week_hot_partitions);
    uint64_t last_day_tables_after = 0;
    uint64_t last_day_entries_after = 0;
    uint64_t last_week_tables_after = 0;
    uint64_t last_week_entries_after = 0;

    {
        std::unique_lock lock(_last_partitions_mtx);
        get_return_partitions(day_hot_partitions, _last_day_hot_partitions, hot_tables,
                              return_partitions, N);
        get_return_partitions(week_hot_partitions, _last_week_hot_partitions, hot_tables,
                              return_partitions, N);

        _last_day_hot_partitions = std::move(day_hot_partitions);
        _last_week_hot_partitions = std::move(week_hot_partitions);
        last_day_tables_after = _last_day_hot_partitions.size();
        last_day_entries_after = count_hot_partition_entries(_last_day_hot_partitions);
        last_week_tables_after = _last_week_hot_partitions.size();
        last_week_entries_after = count_hot_partition_entries(_last_week_hot_partitions);
    }

    const int64_t process_mem_after = process_memory_usage_for_diag();

    LOG(INFO) << "tablet_hotspot_diag get_top_n_hot_partition call_id=" << call_id
              << " day_tables_built=" << day_tables_built
              << " day_entries_built=" << day_entries_built
              << " week_tables_built=" << week_tables_built
              << " week_entries_built=" << week_entries_built
              << " returned_partitions=" << return_partitions
              << " last_day_tables_before=" << last_day_tables_before
              << " last_day_entries_before=" << last_day_entries_before
              << " last_day_tables_after=" << last_day_tables_after
              << " last_day_entries_after=" << last_day_entries_after
              << " last_week_tables_before=" << last_week_tables_before
              << " last_week_entries_before=" << last_week_entries_before
              << " last_week_tables_after=" << last_week_tables_after
              << " last_week_entries_after=" << last_week_entries_after
              << " process_mem_before_mb=" << bytes_to_mb(process_mem_before)
              << " process_mem_after_mb=" << bytes_to_mb(process_mem_after);
}

void HotspotCounter::make_dot_point() {
    uint64_t value = cur_counter.exchange(0, std::memory_order_acq_rel);
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

bool TabletHotspot::is_gc_eligible(const HotspotCounter& counter, SystemTimePoint now) {
    const auto week_window = std::chrono::seconds((HotspotCounter::week_counters_size + 1) *
                                                  HotspotCounter::time_interval);
    return counter.last_access_time < now - week_window &&
           counter.cur_counter.load(std::memory_order_relaxed) == 0 &&
           counter.day_history_counter.load(std::memory_order_relaxed) == 0 &&
           counter.week_history_counter.load(std::memory_order_relaxed) == 0;
}

TabletHotspot::MaintenanceStats TabletHotspot::run_maintenance_once(SystemTimePoint now) {
    MaintenanceStats stats;
    auto gc_elapsed = SteadyClock::duration::zero();

    for (size_t slot_idx = 0; slot_idx < s_slot_size; ++slot_idx) {
        auto& slot = _tablets_hotspot[slot_idx];
        std::vector<HotspotCounterPtr> counters;
        size_t slot_size_before = 0;
        size_t slot_bucket_count_before = 0;

        {
            std::lock_guard lock(slot.mtx);
            slot_size_before = slot.map.size();
            slot_bucket_count_before = slot.map.bucket_count();
            stats.total_counters_before_gc += slot_size_before;
            if (slot_size_before > 0) {
                ++stats.non_empty_slots_before_gc;
            }
            stats.max_slot_size_before_gc =
                    std::max<uint64_t>(stats.max_slot_size_before_gc, slot_size_before);
            stats.sum_bucket_count_before_gc += slot_bucket_count_before;
            counters.reserve(slot_size_before);
            for (auto& [_, counter] : slot.map) {
                counters.push_back(counter);
            }
        }
        stats.copied_counters += counters.size();
        std::for_each(counters.begin(), counters.end(),
                      [](HotspotCounterPtr& counter) { counter->make_dot_point(); });

        size_t erased_count = 0;
        bool compacted = false;
        size_t slot_size_after = 0;
        size_t slot_bucket_count_after = 0;
        auto gc_start = SteadyClock::now();
        {
            std::lock_guard lock(slot.mtx);
            for (auto iter = slot.map.begin(); iter != slot.map.end();) {
                if (is_gc_eligible(*iter->second, now)) {
                    iter = slot.map.erase(iter);
                    ++erased_count;
                } else {
                    ++iter;
                }
            }

            if (should_compact_slot(slot_size_before, slot.map.size(), slot.map.bucket_count(),
                                    erased_count)) {
                decltype(slot.map) compacted_map;
                compacted_map.max_load_factor(slot.map.max_load_factor());
                compacted_map.reserve(slot.map.size());
                for (const auto& [tablet_id, counter] : slot.map) {
                    compacted_map.emplace(tablet_id, counter);
                }
                slot.map.swap(compacted_map);
                compacted = true;
            }

            slot_size_after = slot.map.size();
            slot_bucket_count_after = slot.map.bucket_count();
            stats.total_counters_after_gc += slot_size_after;
            if (slot_size_after > 0) {
                ++stats.non_empty_slots_after_gc;
            }
            stats.max_slot_size_after_gc =
                    std::max<uint64_t>(stats.max_slot_size_after_gc, slot_size_after);
            stats.sum_bucket_count_after_gc += slot_bucket_count_after;
        }
        gc_elapsed += SteadyClock::now() - gc_start;

        stats.evicted_counters += erased_count;
        if (compacted) {
            ++stats.compacted_slots;
        }
        if (erased_count > 0 || compacted) {
            LOG(INFO) << "tablet_hotspot_diag gc_slot"
                      << " slot_idx=" << slot_idx << " slot_size_before=" << slot_size_before
                      << " slot_size_after=" << slot_size_after
                      << " bucket_count_before=" << slot_bucket_count_before
                      << " bucket_count_after=" << slot_bucket_count_after
                      << " erased_count=" << erased_count << " compacted=" << compacted;
        }
    }

    stats.gc_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(gc_elapsed).count();
    return stats;
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

        const uint64_t round =
                _make_dot_point_round_total.fetch_add(1, std::memory_order_relaxed) + 1;
        const uint64_t count_calls_total = _count_calls_total.load(std::memory_order_relaxed);
        const uint64_t existing_hit_total = _existing_hit_total.load(std::memory_order_relaxed);
        const uint64_t new_counter_total_before =
                _new_counter_total.load(std::memory_order_relaxed);
        const int64_t process_mem_before = process_memory_usage_for_diag();

        const auto now = std::chrono::system_clock::now();
        const MaintenanceStats stats = run_maintenance_once(now);

        const int64_t process_mem_after = process_memory_usage_for_diag();
        const uint64_t new_counter_total_after = _new_counter_total.load(std::memory_order_relaxed);
        const uint64_t prev_round_new_counter_total = _last_round_new_counter_total.exchange(
                new_counter_total_after, std::memory_order_relaxed);
        const uint64_t new_counter_delta =
                new_counter_total_after >= prev_round_new_counter_total
                        ? (new_counter_total_after - prev_round_new_counter_total)
                        : 0;

        LOG(INFO) << "tablet_hotspot_diag make_dot_point round=" << round
                  << " total_counters=" << stats.total_counters_before_gc
                  << " total_counters_before_gc=" << stats.total_counters_before_gc
                  << " total_counters_after_gc=" << stats.total_counters_after_gc
                  << " non_empty_slots=" << stats.non_empty_slots_before_gc
                  << " non_empty_slots_before_gc=" << stats.non_empty_slots_before_gc
                  << " non_empty_slots_after_gc=" << stats.non_empty_slots_after_gc
                  << " max_slot_size=" << stats.max_slot_size_before_gc
                  << " max_slot_size_before_gc=" << stats.max_slot_size_before_gc
                  << " max_slot_size_after_gc=" << stats.max_slot_size_after_gc
                  << " sum_bucket_count=" << stats.sum_bucket_count_before_gc
                  << " sum_bucket_count_before_gc=" << stats.sum_bucket_count_before_gc
                  << " sum_bucket_count_after_gc=" << stats.sum_bucket_count_after_gc
                  << " copied_counters=" << stats.copied_counters
                  << " evicted_counters=" << stats.evicted_counters << " evicted_ratio="
                  << ratio_or_zero(stats.evicted_counters, stats.total_counters_before_gc)
                  << " compacted_slots=" << stats.compacted_slots << " bucket_reclaim_ratio="
                  << ratio_or_zero(
                             stats.sum_bucket_count_before_gc >= stats.sum_bucket_count_after_gc
                                     ? (stats.sum_bucket_count_before_gc -
                                        stats.sum_bucket_count_after_gc)
                                     : 0,
                             stats.sum_bucket_count_before_gc)
                  << " gc_elapsed_ms=" << stats.gc_elapsed_ms
                  << " count_calls_total=" << count_calls_total
                  << " existing_hit_total=" << existing_hit_total
                  << " new_counter_total_before=" << new_counter_total_before
                  << " new_counter_total_after=" << new_counter_total_after
                  << " new_counter_delta=" << new_counter_delta
                  << " process_mem_before_mb=" << bytes_to_mb(process_mem_before)
                  << " process_mem_after_mb=" << bytes_to_mb(process_mem_after);
    }
}

} // namespace doris
