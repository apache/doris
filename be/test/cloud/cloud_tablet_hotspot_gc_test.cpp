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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <tuple>
#include <vector>

#include "cloud/cloud_tablet_hotspot.h"

namespace doris {
namespace {

using SystemTimePoint = std::chrono::system_clock::time_point;
using PartitionSnapshot =
        std::map<std::tuple<int64_t, int64_t, int64_t>, std::pair<uint64_t, uint64_t>>;

size_t slot_idx(int64_t tablet_id) {
    return tablet_id % TabletHotspot::s_slot_size;
}

HotspotCounterPtr insert_counter(TabletHotspot* hotspot, int64_t tablet_id, int64_t table_id,
                                 int64_t index_id, int64_t partition_id,
                                 SystemTimePoint last_access_time, uint64_t cur_counter,
                                 uint64_t day_history_counter, uint64_t week_history_counter) {
    auto counter = std::make_shared<HotspotCounter>(table_id, index_id, partition_id);
    counter->last_access_time = last_access_time;
    counter->cur_counter.store(cur_counter, std::memory_order_relaxed);
    counter->day_history_counter.store(day_history_counter, std::memory_order_relaxed);
    counter->week_history_counter.store(week_history_counter, std::memory_order_relaxed);

    auto& slot = hotspot->_tablets_hotspot[slot_idx(tablet_id)];
    std::lock_guard lock(slot.mtx);
    slot.map[tablet_id] = counter;
    return counter;
}

PartitionSnapshot export_snapshot(TabletHotspot* hotspot) {
    std::vector<THotTableMessage> hot_tables;
    hotspot->get_top_n_hot_partition(&hot_tables);

    PartitionSnapshot snapshot;
    for (const auto& table_msg : hot_tables) {
        for (const auto& partition : table_msg.hot_partitions) {
            snapshot[std::make_tuple(table_msg.table_id, table_msg.index_id,
                                     partition.partition_id)] =
                    std::make_pair(partition.query_per_day, partition.query_per_week);
        }
    }
    return snapshot;
}

} // namespace

TEST(TabletHotspotGcTest, GcEligibilityRequiresExpiredAndZeroContribution) {
    const auto now = std::chrono::system_clock::now();

    HotspotCounter expired_zero_counter(1, 2, 3);
    expired_zero_counter.last_access_time = now - std::chrono::hours(24 * 8);

    HotspotCounter recent_zero_counter(1, 2, 3);
    recent_zero_counter.last_access_time = now - std::chrono::hours(24 * 6);

    HotspotCounter expired_week_counter(1, 2, 3);
    expired_week_counter.last_access_time = now - std::chrono::hours(24 * 8);
    expired_week_counter.week_history_counter.store(1, std::memory_order_relaxed);

    EXPECT_TRUE(TabletHotspot::is_gc_eligible(expired_zero_counter, now));
    EXPECT_FALSE(TabletHotspot::is_gc_eligible(recent_zero_counter, now));
    EXPECT_FALSE(TabletHotspot::is_gc_eligible(expired_week_counter, now));
}

TEST(TabletHotspotGcTest, RunMaintenanceEvictsExpiredZeroCounter) {
    TabletHotspot hotspot(false);
    const auto now = std::chrono::system_clock::now();
    const int64_t tablet_id = 1001;

    insert_counter(&hotspot, tablet_id, 11, 12, 13, now - std::chrono::hours(24 * 8), 0, 0, 0);

    const auto stats = hotspot.run_maintenance_once(now);
    auto& slot = hotspot._tablets_hotspot[slot_idx(tablet_id)];

    std::lock_guard lock(slot.mtx);
    EXPECT_EQ(1u, stats.total_counters_before_gc);
    EXPECT_EQ(0u, stats.total_counters_after_gc);
    EXPECT_EQ(1u, stats.evicted_counters);
    EXPECT_EQ(slot.map.end(), slot.map.find(tablet_id));
}

TEST(TabletHotspotGcTest, RunMaintenanceKeepsRecentZeroCounter) {
    TabletHotspot hotspot(false);
    const auto now = std::chrono::system_clock::now();
    const int64_t tablet_id = 1002;

    insert_counter(&hotspot, tablet_id, 21, 22, 23, now - std::chrono::hours(24 * 6), 0, 0, 0);

    const auto stats = hotspot.run_maintenance_once(now);
    auto& slot = hotspot._tablets_hotspot[slot_idx(tablet_id)];

    std::lock_guard lock(slot.mtx);
    EXPECT_EQ(1u, stats.total_counters_before_gc);
    EXPECT_EQ(1u, stats.total_counters_after_gc);
    EXPECT_EQ(0u, stats.evicted_counters);
    ASSERT_NE(slot.map.end(), slot.map.find(tablet_id));
}

TEST(TabletHotspotGcTest, RunMaintenanceRemovesColdCounterAndCountRecreatesIt) {
    TabletHotspot hotspot(false);
    const auto now = std::chrono::system_clock::now();
    const int64_t tablet_id = 1003;

    insert_counter(&hotspot, tablet_id, 31, 32, 33, now - std::chrono::hours(24 * 8), 0, 0, 0);

    const auto stats = hotspot.run_maintenance_once(now);
    EXPECT_EQ(1u, stats.evicted_counters);

    hotspot.count(tablet_id, 31, 32, 33);

    auto& slot = hotspot._tablets_hotspot[slot_idx(tablet_id)];
    std::lock_guard lock(slot.mtx);
    auto iter = slot.map.find(tablet_id);
    ASSERT_NE(slot.map.end(), iter);
    EXPECT_EQ(31, iter->second->table_id);
    EXPECT_EQ(32, iter->second->index_id);
    EXPECT_EQ(33, iter->second->partition_id);
    EXPECT_EQ(1u, iter->second->cur_counter.load(std::memory_order_relaxed));
}

TEST(TabletHotspotGcTest, RunMaintenanceCompactsSparseShard) {
    TabletHotspot hotspot(false);
    const auto now = std::chrono::system_clock::now();
    const int64_t slot_seed = 17;

    for (int i = 0; i < 256; ++i) {
        const int64_t tablet_id = slot_seed + static_cast<int64_t>(i) * TabletHotspot::s_slot_size;
        insert_counter(&hotspot, tablet_id, 41, 42, 43, now - std::chrono::hours(24 * 8), 0, 0, 0);
    }

    auto& slot = hotspot._tablets_hotspot[slot_idx(slot_seed)];
    size_t bucket_count_before = 0;
    {
        std::lock_guard lock(slot.mtx);
        bucket_count_before = slot.map.bucket_count();
        ASSERT_GE(slot.map.size(), 256u);
    }

    const auto stats = hotspot.run_maintenance_once(now);

    std::lock_guard lock(slot.mtx);
    EXPECT_EQ(256u, stats.evicted_counters);
    EXPECT_EQ(1u, stats.compacted_slots);
    EXPECT_TRUE(slot.map.empty());
    EXPECT_LT(slot.map.bucket_count(), bucket_count_before);
}

TEST(TabletHotspotGcTest, GcKeepsHotspotExportStable) {
    const auto now = std::chrono::system_clock::now();

    TabletHotspot baseline(false);
    insert_counter(&baseline, 2001, 51, 52, 53, now - std::chrono::hours(1), 0, 7, 11);
    insert_counter(&baseline, 2002, 51, 52, 54, now - std::chrono::hours(24 * 8), 0, 0, 0);

    TabletHotspot with_gc(false);
    insert_counter(&with_gc, 2001, 51, 52, 53, now - std::chrono::hours(1), 0, 7, 11);
    insert_counter(&with_gc, 2002, 51, 52, 54, now - std::chrono::hours(24 * 8), 0, 0, 0);

    const PartitionSnapshot before_gc = export_snapshot(&baseline);
    const auto stats = with_gc.run_maintenance_once(now);
    EXPECT_EQ(1u, stats.evicted_counters);
    const PartitionSnapshot after_gc = export_snapshot(&with_gc);

    EXPECT_EQ(before_gc, after_gc);
    ASSERT_EQ(1u, after_gc.size());
    const auto iter = after_gc.find(std::make_tuple(51, 52, 53));
    ASSERT_NE(after_gc.end(), iter);
    EXPECT_EQ(7u, iter->second.first);
    EXPECT_EQ(11u, iter->second.second);
}

} // namespace doris
