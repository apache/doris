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

#include "io/cache/tablet_cache_policy_registry.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <map>
#include <thread>
#include <vector>

#include "common/config.h"
#include "util/time.h"

namespace doris::io {

class TabletCachePolicyRegistryTest : public testing::Test {
protected:
    void SetUp() override {
        _registry.set_fetcher([this](int64_t tablet_id, TabletCachePolicy* policy) {
            _fetched.push_back(tablet_id);
            auto it = _remote.find(tablet_id);
            if (it == _remote.end()) {
                return Status::NotFound("tablet {} not found", tablet_id);
            }
            if (!it->second.has_value()) {
                return Status::InternalError("meta service unavailable");
            }
            *policy = *it->second;
            return Status::OK();
        });
    }

    TabletCachePolicyRegistry _registry;
    // The policy the meta service reports per tablet; nullopt makes the fetch fail.
    std::map<int64_t, std::optional<TabletCachePolicy>> _remote;
    std::vector<int64_t> _fetched;
};

TEST_F(TabletCachePolicyRegistryTest, deadline) {
    EXPECT_EQ((TabletCachePolicy {0, 100}).deadline(), 0);
    EXPECT_EQ((TabletCachePolicy {10, 0}).deadline(), 0);
    EXPECT_EQ((TabletCachePolicy {10, 100}).deadline(), 110);
    EXPECT_EQ((TabletCachePolicy {std::numeric_limits<int64_t>::max(), 100}).deadline(),
              std::numeric_limits<int64_t>::max());
}

TEST_F(TabletCachePolicyRegistryTest, unknown_tablet_falls_back_to_block_deadline) {
    const int64_t now = 1000;
    EXPECT_FALSE(_registry.deadline(1).has_value());
    EXPECT_TRUE(_registry.is_protected(1, now + 1, now));
    EXPECT_FALSE(_registry.is_protected(1, now, now));
    // Data outside any tablet only ever has the deadline it was written with.
    _registry.upsert(2, {100, now});
    EXPECT_TRUE(_registry.is_protected(0, now + 1, now));
}

TEST_F(TabletCachePolicyRegistryTest, known_policy_overrides_block_deadline) {
    const int64_t now = UnixSeconds();
    // TTL removed by ALTER: the block was written while it had one.
    _registry.upsert(1, {0, now - 10});
    EXPECT_EQ(_registry.deadline(1), 0);
    EXPECT_FALSE(_registry.is_protected(1, now + 3600, now));
    // TTL added by ALTER: the block was written without one.
    _registry.upsert(2, {3600, now - 10});
    EXPECT_TRUE(_registry.is_protected(2, 0, now));
    // Expired.
    _registry.upsert(3, {5, now - 10});
    EXPECT_FALSE(_registry.is_protected(3, now + 3600, now));
}

TEST_F(TabletCachePolicyRegistryTest, versions_track_direction_of_change) {
    const int64_t now = UnixSeconds();
    auto gained = _registry.protection_gained_version();
    auto lost = _registry.protection_lost_version();

    _registry.upsert(1, {100, now});
    // Learning a tablet can move its blocks either way.
    EXPECT_GT(_registry.protection_gained_version(), gained);
    EXPECT_GT(_registry.protection_lost_version(), lost);

    gained = _registry.protection_gained_version();
    lost = _registry.protection_lost_version();
    _registry.upsert(1, {100, now});
    EXPECT_EQ(_registry.protection_gained_version(), gained);
    EXPECT_EQ(_registry.protection_lost_version(), lost);

    _registry.upsert(1, {200, now});
    EXPECT_GT(_registry.protection_gained_version(), gained);
    EXPECT_EQ(_registry.protection_lost_version(), lost);

    gained = _registry.protection_gained_version();
    _registry.upsert(1, {0, now});
    EXPECT_EQ(_registry.protection_gained_version(), gained);
    EXPECT_GT(_registry.protection_lost_version(), lost);

    lost = _registry.protection_lost_version();
    _registry.erase(1);
    EXPECT_FALSE(_registry.deadline(1).has_value());
    EXPECT_GT(_registry.protection_gained_version(), gained);
    EXPECT_GT(_registry.protection_lost_version(), lost);
}

TEST_F(TabletCachePolicyRegistryTest, restore_does_not_override_live_policy) {
    _registry.upsert(1, {100, 1000});
    _registry.restore(1, {200, 1000});
    EXPECT_EQ(_registry.deadline(1), 1100);
    _registry.restore(2, {200, 1000});
    EXPECT_EQ(_registry.deadline(2), 1200);
}

TEST_F(TabletCachePolicyRegistryTest, has_deadline_in) {
    _registry.upsert(1, {100, 1000});
    _registry.upsert(2, {0, 1000});
    EXPECT_TRUE(_registry.has_deadline_in(1099, 1100));
    EXPECT_FALSE(_registry.has_deadline_in(1100, 2000));
    EXPECT_FALSE(_registry.has_deadline_in(0, 1099));
    _registry.upsert(1, {300, 1000});
    EXPECT_FALSE(_registry.has_deadline_in(1099, 1100));
    EXPECT_TRUE(_registry.has_deadline_in(1299, 1300));
}

TEST_F(TabletCachePolicyRegistryTest, refresh_fetches_noted_unknown_tablets) {
    const int64_t now = UnixSeconds();
    _remote[1] = TabletCachePolicy {3600, now};

    // Looking a tablet up notes it; tablet 2 no longer exists.
    EXPECT_FALSE(_registry.is_protected(1, 0, now));
    EXPECT_FALSE(_registry.is_protected(2, 0, now));
    _registry.refresh(100);

    EXPECT_EQ(_registry.deadline(1), now + 3600);
    EXPECT_TRUE(_registry.is_protected(1, 0, now));
    EXPECT_FALSE(_registry.deadline(2).has_value());

    // A tablet that does not exist is not fetched again.
    _fetched.clear();
    _registry.refresh(100);
    EXPECT_TRUE(_fetched.empty());
}

// A failed fetch most likely means the meta service is unavailable: the round ends there instead
// of failing one call after another, and that tablet is left alone for a while.
TEST_F(TabletCachePolicyRegistryTest, failed_fetch_ends_round_and_backs_off) {
    const int64_t now = UnixSeconds();
    _remote[1] = std::nullopt;
    for (int64_t id = 1; id <= 5; ++id) {
        if (id > 1) {
            _remote[id] = TabletCachePolicy {0, now};
        }
        _registry.note_tablet(id);
    }
    _registry.refresh(100);
    auto failed = std::find(_fetched.begin(), _fetched.end(), 1);
    ASSERT_NE(failed, _fetched.end());
    EXPECT_EQ(failed + 1, _fetched.end());
    EXPECT_FALSE(_registry.deadline(1).has_value());

    // The rest is fetched by the next round; tablet 1 is not, even when noted again.
    _registry.note_tablet(1);
    _registry.refresh(100);
    EXPECT_EQ(std::count(_fetched.begin(), _fetched.end(), 1), 1);
    for (int64_t id = 2; id <= 5; ++id) {
        EXPECT_EQ(_registry.deadline(id), 0) << id;
    }
}

// Every cache path reports its tablets, and a fetch is a remote call that may take long when the
// meta service does: a report must never wait for one.
TEST_F(TabletCachePolicyRegistryTest, report_does_not_wait_for_fetch) {
    std::atomic<bool> fetching {false};
    std::atomic<bool> released {false};
    _registry.set_fetcher([&](int64_t, TabletCachePolicy* policy) {
        fetching = true;
        while (!released) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        *policy = {0, 1000};
        return Status::OK();
    });
    _registry.note_tablet(1);
    std::thread refresher([&] { _registry.refresh(1); });
    while (!fetching) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto reported = std::async(std::launch::async, [&] { _registry.report_cached_tablets({2}); });
    EXPECT_EQ(reported.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    released = true;
    refresher.join();
    reported.wait();

    _registry.refresh(100);
    EXPECT_EQ(_registry.deadline(1), 0);
    EXPECT_EQ(_registry.deadline(2), 0);
}

// The background refresh runs while a cache is open, and stops with the last one.
TEST_F(TabletCachePolicyRegistryTest, background_refresh_follows_open_caches) {
    _remote[1] = TabletCachePolicy {0, 1000};
    _registry.register_cache();
    _registry.register_cache();
    _registry.unregister_cache();
    _registry.note_tablet(1);
    for (int i = 0; i < 100 && !_registry.deadline(1).has_value(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(_registry.deadline(1), 0);
    _registry.unregister_cache();
    EXPECT_FALSE(_registry._refresh_thread.joinable());
}

TEST_F(TabletCachePolicyRegistryTest, refresh_respects_budget) {
    for (int64_t id = 1; id <= 10; ++id) {
        _remote[id] = TabletCachePolicy {0, 1000};
        _registry.note_tablet(id);
    }
    _registry.refresh(4);
    EXPECT_EQ(_fetched.size(), 4);
    EXPECT_EQ(_registry.size(), 4);
    _registry.refresh(100);
    EXPECT_EQ(_fetched.size(), 10);
    EXPECT_EQ(_registry.size(), 10);
}

TEST_F(TabletCachePolicyRegistryTest, refresh_fetches_reported_due_policies) {
    const int64_t now = UnixSeconds();
    _remote[1] = TabletCachePolicy {100, now};
    _remote[3] = TabletCachePolicy {200, now};
    // Restored from a persisted copy: known, but never refreshed by this process.
    _registry.restore(1, {0, now});
    _registry.restore(4, {0, now});
    _registry.upsert(2, {0, now});

    // Nothing is fetched for a tablet no cache path holds.
    _registry.refresh(100);
    EXPECT_TRUE(_fetched.empty());

    // Reported: tablet 1 is due, tablet 2 was just confirmed, tablet 3 is unknown.
    _registry.report_cached_tablets({1, 2, 3});
    _registry.refresh(100);
    std::sort(_fetched.begin(), _fetched.end());
    EXPECT_EQ(_fetched, (std::vector<int64_t> {1, 3}));
    EXPECT_EQ(_registry.deadline(1), now + 100);
    EXPECT_EQ(_registry.deadline(3), now + 200);
}

TEST_F(TabletCachePolicyRegistryTest, refresh_forgets_unused_policies) {
    const auto original_interval = config::file_cache_tablet_policy_refresh_interval_s;
    // Twice the interval has passed for anything confirmed or reported before now.
    config::file_cache_tablet_policy_refresh_interval_s = -1;
    _registry.upsert(1, {100, 1000});
    _registry.restore(2, {100, 1000});
    _registry.refresh(100);
    config::file_cache_tablet_policy_refresh_interval_s = original_interval;
    EXPECT_FALSE(_registry.deadline(1).has_value());
    EXPECT_FALSE(_registry.deadline(2).has_value());
}

} // namespace doris::io
