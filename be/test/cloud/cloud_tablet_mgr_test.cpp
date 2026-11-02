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

#include "cloud/cloud_tablet_mgr.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "storage/tablet/tablet_meta.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {

class CloudTabletMgrTest : public testing::Test {
public:
    CloudTabletMgrTest() : _engine(CloudStorageEngine(EngineOptions())) {}

    void SetUp() override {
        _tablet_meta.reset(new TabletMeta(1, 2, 99999, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
                                          UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                          TCompressionType::LZ4F));
    }

    void TearDown() override {}

protected:
    TabletMetaSharedPtr _tablet_meta;
    CloudStorageEngine _engine;
};

TEST_F(CloudTabletMgrTest, VacuumReclaimsWarmupStatesWithoutStaleOrUnusedRowsets) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    Defer cleanup([sp] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    });
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [this](auto&& args) {
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = _tablet_meta;
        try_any_cast_ret<Status>(args)->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });
    sp->enable_processing();

    CloudTabletMgr mgr(_engine);
    auto result = mgr.get_tablet(_tablet_meta->tablet_id());
    ASSERT_TRUE(result.has_value()) << result.error();
    auto tablet = result.value();
    RowsetMeta rowset;
    rowset.set_rowset_id(_engine.next_rowset_id());
    rowset.set_num_segments(0);
    ASSERT_TRUE(tablet->add_rowset_warmup_state(
            rowset, WarmUpTriggerSource::EVENT_DRIVEN,
            std::chrono::steady_clock::now() -
                    std::chrono::seconds(config::warm_up_rowset_state_gc_grace_period_seconds +
                                         1)));
    ASSERT_FALSE(tablet->has_stale_rowsets());
    ASSERT_FALSE(tablet->need_remove_unused_rowsets());

    CountDownLatch stop_latch(1);
    mgr.vacuum_stale_rowsets(stop_latch);
    EXPECT_EQ(tablet->get_rowset_warmup_state(rowset.rowset_id()).progress, WarmUpProgress::NONE);
}

TEST_F(CloudTabletMgrTest, DeleteBitmapCheckDoesNotHoldTabletMapLock) {
    const bool old_check = config::enable_check_agg_and_remove_pre_rowsets_delete_bitmap;
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    Defer cleanup([&] {
        sp->disable_processing();
        sp->clear_all_call_backs();
        config::enable_check_agg_and_remove_pre_rowsets_delete_bitmap = old_check;
    });
    config::enable_check_agg_and_remove_pre_rowsets_delete_bitmap = true;

    CloudTabletMgr mgr(_engine);
    auto tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
    mgr.put_tablet_for_UT(tablet);
    TabletMetaSharedPtr other_meta(new TabletMeta(
            1, 2, _tablet_meta->tablet_id() + 1, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}},
            UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
    auto other_tablet = std::make_shared<CloudTablet>(_engine, other_meta);

    CountDownLatch check_entered(1);
    CountDownLatch resume_check(1);
    sp->set_call_back(
            "BaseTablet::check_agg_delete_bitmap_for_stale_rowsets:before_traverse_delete_bitmap",
            [&](auto&&) {
                check_entered.count_down();
                resume_check.wait();
            });
    sp->enable_processing();

    CountDownLatch stop_latch(1);
    auto vacuum = std::async(std::launch::async, [&] { mgr.vacuum_stale_rowsets(stop_latch); });
    Defer unblock_check([&] { resume_check.count_down(); });
    ASSERT_TRUE(check_entered.wait_for(std::chrono::seconds(10)));

    auto map_access = std::async(std::launch::async, [&] {
        mgr.put_tablet_for_UT(other_tablet);
        return mgr.get_weak_tablets().size();
    });
    const auto map_status = map_access.wait_for(std::chrono::seconds(10));
    // Release the check before joining, so the old locking behavior fails without hanging UT.
    resume_check.count_down();
    vacuum.get();
    EXPECT_EQ(map_status, std::future_status::ready);
    EXPECT_EQ(map_access.get(), 2);
}

// Test concurrent get_tablet calls for the same tablet_id.
// Reproduces bug where tablet ends up in cache but not in _tablet_map.
//
// Bug scenario (old code before fix):
// 1. Thread A and Thread B call get_tablet(tablet_id) concurrently
// 2. SingleFlight ensures only one loads, both get same shared_ptr<CloudTablet>
// 3. Both threads continue (synchronized via sync point):
//    - Thread A: _cache->insert(key, ValueA) -> HA, _tablet_map->put(tablet)
//    - Thread B: _cache->insert(key, ValueB) -> HB (evicts ValueA), _tablet_map->put(tablet)
// 4. After both complete: cache has ValueB (refs=2: HB + cache), tablet_map has tablet
// 5. Thread A's retA goes out of scope -> release(HA) -> ValueA refs 1->0 (was evicted)
//    -> ValueA::~Value() -> _tablet_map.erase(tablet.get()) -> tablet removed from tablet_map
// 6. Thread B's retB still valid, cache still has ValueB (refs=2: HB + cache)
// 7. Thread B's retB goes out of scope -> release(HB) -> ValueB refs 2->1 (cache still holds it)
//    -> ValueB still alive, but tablet_map entry was erased by ValueA destructor
// 8. Current state: cache has ValueB, tablet_map is EMPTY
//
// At this point:
// - New get_tablet() calls hit cache (ValueB), taking cache hit path -> never touch tablet_map
// - Compaction scheduler uses get_weak_tablets() which iterates tablet_map -> sees nothing
// - Tablet is "alive" in cache but invisible to compaction scheduler
//
// Fix: Move cache insert and tablet_map put inside SingleFlight lambda.
// Only the leader creates one cache entry and puts into tablet_map once.
TEST_F(CloudTabletMgrTest, TestConcurrentGetTabletTabletMapConsistency) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // Mock get_tablet_meta to return our test tablet meta
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [this](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = _tablet_meta;
        try_any_cast_ret<Status>(args)->second = true;
    });

    // Mock sync_tablet_rowsets to return OK
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });

    // Use callback with barrier to ensure both threads reach sync point before continuing
    std::atomic<int> count {0};
    std::mutex mtx;
    std::condition_variable cv;
    const int kNumThreads = 2;

    sp->set_call_back("CloudTabletMgr::get_tablet.not_found_in_cache", [&](auto&& args) {
        int arrived = ++count;
        if (arrived < kNumThreads) {
            // First thread waits for second thread
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return count.load() >= kNumThreads; });
        } else {
            // Second thread notifies first thread
            cv.notify_all();
        }
    });

    CloudTabletMgr mgr(_engine);
    const int64_t tablet_id = 99999;

    std::shared_ptr<CloudTablet> tablet1;
    std::shared_ptr<CloudTablet> tablet2;

    // Thread 1: calls get_tablet
    std::thread t1([&]() {
        auto res = mgr.get_tablet(tablet_id);
        ASSERT_TRUE(res.has_value()) << res.error();
        tablet1 = res.value();
    });

    // Thread 2: also calls get_tablet for the same tablet_id
    std::thread t2([&]() {
        auto res = mgr.get_tablet(tablet_id);
        ASSERT_TRUE(res.has_value()) << res.error();
        tablet2 = res.value();
    });

    t1.join();
    t2.join();

    // Both should have gotten the same tablet (same raw pointer) due to SingleFlight
    EXPECT_EQ(tablet1.get(), tablet2.get())
            << "SingleFlight should ensure both threads get the same tablet instance";

    // Release tablet1, tablet2 to trigger ValueA destructor (ValueA was evicted by Thread B's insert)
    // With the bug: ValueA::~Value() calls _tablet_map.erase(), removing tablet from tablet_map
    //              tablet2 still holds a reference, cache entry ValueB is still valid
    // After fix: tablet remains in tablet_map (only one entry was created by leader)
    tablet1.reset();
    tablet2.reset();

    // First check: verify tablet is still in cache using force_use_only_cached=true
    // With the bug: cache entry still exists (refs=1 from cache only), get_tablet should succeed
    // After fix: cache entry still exists, get_tablet should also succeed
    auto cache_hit_result = mgr.get_tablet(tablet_id, false, false, nullptr, true, true);
    bool found_in_cache = cache_hit_result.has_value();

    // Second check: verify tablet is in tablet_map
    // With the bug: tablet was erased from tablet_map by Value destructors
    // After fix: tablet should still be in tablet_map
    auto all_tablets = mgr.get_all_tablet();

    // Find our tablet in the returned list
    bool found_in_tablet_map = false;
    for (const auto& t : all_tablets) {
        if (t->tablet_id() == tablet_id) {
            found_in_tablet_map = true;
            break;
        }
    }

    // Verify the bug scenario: tablet in cache but not in tablet_map
    EXPECT_TRUE(found_in_cache) << "Tablet " << tablet_id << " should be in cache";

    // After the fix, tablet should be in tablet_map
    // Before the fix, tablet would be missing from tablet_map
    EXPECT_TRUE(found_in_tablet_map)
            << "Tablet " << tablet_id
            << " should be in tablet_map. "
               "If this fails, it means the bug is present: tablet is in cache (found_in_cache="
            << found_in_cache << ") but was erased from tablet_map by Value destructors.";

    sp->disable_processing();
    sp->clear_all_call_backs();
}

TEST_F(CloudTabletMgrTest, TestGetTabletIfCachedOnlyReturnsCachedTablet) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [this](auto&& args) {
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = _tablet_meta;
        try_any_cast_ret<Status>(args)->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });

    CloudTabletMgr mgr(_engine);
    const int64_t cached_tablet_id = 99999;
    const int64_t uncached_tablet_id = 100000;

    EXPECT_EQ(nullptr, mgr.get_tablet_if_cached(cached_tablet_id));

    auto res = mgr.get_tablet(cached_tablet_id);
    ASSERT_TRUE(res.has_value()) << res.error();

    auto cached_tablet = mgr.get_tablet_if_cached(cached_tablet_id);
    ASSERT_NE(nullptr, cached_tablet);
    EXPECT_EQ(cached_tablet_id, cached_tablet->tablet_id());

    EXPECT_EQ(nullptr, mgr.get_tablet_if_cached(uncached_tablet_id));

    sp->disable_processing();
    sp->clear_all_call_backs();
}

// A tablet under continuous ingest keeps last_sync_rowsets_time_s permanently fresh, because every
// rowset sync advances it. Selecting meta work by that same clock meant such a tablet never had
// sync_meta() called on it at all, so it kept serving the tablet properties -- the file cache
// TTL among them -- that it happened to be built with.
TEST_F(CloudTabletMgrTest, SyncTabletsRefreshesMetaOfContinuouslyIngestedTablet) {
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    std::mutex mutex;
    std::unordered_map<int64_t, TabletMetaSharedPtr> metas;
    std::unordered_map<int64_t, int> meta_syncs;
    std::unordered_map<int64_t, int> rowset_syncs;

    auto meta_for = [&](int64_t tablet_id) {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = metas.find(tablet_id);
        if (it == metas.end()) {
            it = metas.emplace(tablet_id, std::make_shared<TabletMeta>(
                                                  1, 2, tablet_id, 15674, 4, 5, TTabletSchema(), 6,
                                                  std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                                  UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                                  TCompressionType::LZ4F))
                         .first;
        }
        return it->second;
    };

    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        *tablet_meta_ptr = meta_for(tablet_id);
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++meta_syncs[tablet_id];
        }
        try_any_cast_ret<Status>(args)->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [&](auto&& args) {
        auto* tablet = try_any_cast<CloudTablet*>(args[0]);
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++rowset_syncs[tablet->tablet_id()];
        }
        try_any_cast_ret<Status>(args)->second = true;
    });

    CloudTabletMgr mgr(_engine);
    constexpr int64_t kBothStale = 70001;
    constexpr int64_t kIngesting = 70002;
    constexpr int64_t kFresh = 70003;

    std::vector<std::shared_ptr<CloudTablet>> tablets;
    for (int64_t tablet_id : {kBothStale, kIngesting, kFresh}) {
        auto res = mgr.get_tablet(tablet_id);
        ASSERT_TRUE(res.has_value()) << res.error();
        tablets.push_back(res.value());
    }

    const int64_t now = ::time(nullptr);
    const int64_t stale = now - config::tablet_sync_interval_s - 10;

    tablets[0]->last_sync_rowsets_time_s = stale;
    tablets[0]->last_sync_tablet_meta_time_s = stale;
    // Rowsets pulled a moment ago, meta left behind.
    tablets[1]->last_sync_rowsets_time_s = now;
    tablets[1]->last_sync_tablet_meta_time_s = stale;
    tablets[2]->last_sync_rowsets_time_s = now;
    tablets[2]->last_sync_tablet_meta_time_s = now;

    {
        std::lock_guard<std::mutex> lock(mutex);
        meta_syncs.clear();
        rowset_syncs.clear();
    }

    CountDownLatch latch(1);
    mgr.sync_tablets(latch);

    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(1, meta_syncs[kBothStale]);
    EXPECT_EQ(1, rowset_syncs[kBothStale]);

    // The regression: meta has to be refreshed even though the rowset clock never goes stale,
    // and it costs one RPC rather than the two a full sync would.
    EXPECT_EQ(1, meta_syncs[kIngesting]);
    EXPECT_EQ(0, rowset_syncs[kIngesting]);
    EXPECT_GE(tablets[1]->last_sync_tablet_meta_time_s, now);

    EXPECT_EQ(0, meta_syncs[kFresh]);
    EXPECT_EQ(0, rowset_syncs[kFresh]);

    sp->disable_processing();
    sp->clear_all_call_backs();
}

} // namespace doris
