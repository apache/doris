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

// Unit tests for CloudWarmUpManager peer candidate management.
// Tests cover record_balanced_tablet, get_peer_candidates,
// update_peer_candidate_on_success, update_peer_candidate_on_rpc_failure,
// and remove_balanced_tablet.

#include <gtest/gtest.h>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "util/defer_op.h"

namespace doris {

// ============================================================
// Test fixture
// ============================================================

class CloudWarmUpManagerPeerTest : public testing::Test {
public:
    CloudWarmUpManagerPeerTest()
            : _engine(CloudStorageEngine(EngineOptions {})),
              _mgr(std::make_unique<CloudWarmUpManager>(_engine)) {}

protected:
    CloudStorageEngine _engine;
    std::unique_ptr<CloudWarmUpManager> _mgr;
};

// ============================================================
// record_balanced_tablet / get_peer_candidates
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, get_peer_candidates_unknown_tablet_returns_empty) {
    auto candidates = _mgr->get_peer_candidates(99999);
    EXPECT_TRUE(candidates.empty());
}

TEST_F(CloudWarmUpManagerPeerTest, record_single_candidate_and_retrieve) {
    constexpr int64_t tablet_id = 100;
    _mgr->record_balanced_tablet(tablet_id, "192.168.0.1", 9060, "cg_a");

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].host, "192.168.0.1");
    EXPECT_EQ(candidates[0].brpc_port, 9060);
    EXPECT_EQ(candidates[0].compute_group_id, "cg_a");
    EXPECT_EQ(candidates[0].consecutive_rpc_failures, 0);
}

TEST_F(CloudWarmUpManagerPeerTest, record_warmup_candidate_upserts_same_cg) {
    // Warm-up rebalance: a tablet has at most one warm-up peer (the current rebalance source).
    // Repeated calls with the same compute_group_id should upsert (update) not accumulate.
    constexpr int64_t tablet_id = 200;
    _mgr->record_balanced_tablet(tablet_id, "host_first", 9060, "cg_self");
    _mgr->record_balanced_tablet(tablet_id, "host_second", 9061, "cg_self");
    _mgr->record_balanced_tablet(tablet_id, "host_third", 9062, "cg_self");

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    // Still only 1 candidate — latest rebalance source wins.
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].host, "host_third");
    EXPECT_EQ(candidates[0].brpc_port, 9062);
    EXPECT_EQ(candidates[0].compute_group_id, "cg_self");
    EXPECT_EQ(candidates[0].consecutive_rpc_failures, 0);
}

TEST_F(CloudWarmUpManagerPeerTest, record_with_empty_compute_group_id) {
    constexpr int64_t tablet_id = 300;
    _mgr->record_balanced_tablet(tablet_id, "10.0.0.1", 9060);

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_TRUE(candidates[0].compute_group_id.empty());
}

// ============================================================
// update_peer_candidate_on_success
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, update_on_success_sets_last_successful_cg) {
    constexpr int64_t tablet_id = 400;
    _mgr->record_balanced_tablet(tablet_id, "10.0.0.2", 9060, "cg_peer");

    // Verify candidates are present.
    auto candidates_before = _mgr->get_peer_candidates(tablet_id);
    ASSERT_FALSE(candidates_before.empty());

    _mgr->update_peer_candidate_on_success(tablet_id, "cg_peer");

    // get_peer_candidates still returns the same candidates.
    auto candidates_after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates_after.size(), 1u);
    EXPECT_EQ(candidates_after[0].compute_group_id, "cg_peer");
}

TEST_F(CloudWarmUpManagerPeerTest, update_on_success_noop_for_unknown_tablet) {
    // Should not crash or throw.
    _mgr->update_peer_candidate_on_success(99998, "cg_x");
}

// ============================================================
// update_peer_candidate_on_rpc_failure
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, rpc_failure_increments_counter) {
    constexpr int64_t tablet_id = 500;
    _mgr->record_balanced_tablet(tablet_id, "10.0.0.3", 9060, "cg_b");

    _mgr->update_peer_candidate_on_rpc_failure(tablet_id, "10.0.0.3", 9060);

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].consecutive_rpc_failures, 1);
}

TEST_F(CloudWarmUpManagerPeerTest, rpc_failure_evicts_at_threshold) {
    constexpr int64_t tablet_id = 600;
    const int threshold = config::peer_rpc_failure_eviction_threshold;
    ASSERT_GT(threshold, 0) << "threshold must be positive";

    _mgr->record_balanced_tablet(tablet_id, "10.0.0.4", 9060, "cg_c");

    // Apply (threshold - 1) failures: candidate should still be present.
    for (int i = 0; i < threshold - 1; ++i) {
        _mgr->update_peer_candidate_on_rpc_failure(tablet_id, "10.0.0.4", 9060);
        auto candidates = _mgr->get_peer_candidates(tablet_id);
        ASSERT_EQ(candidates.size(), 1u) << "candidate evicted too early on iteration " << i + 1;
        EXPECT_EQ(candidates[0].consecutive_rpc_failures, i + 1);
    }

    // One more failure should trigger eviction.
    _mgr->update_peer_candidate_on_rpc_failure(tablet_id, "10.0.0.4", 9060);
    auto candidates_after = _mgr->get_peer_candidates(tablet_id);
    EXPECT_TRUE(candidates_after.empty()) << "candidate should have been evicted";
}

TEST_F(CloudWarmUpManagerPeerTest, rpc_failure_evicts_matching_candidate_only) {
    constexpr int64_t tablet_id = 700;
    const int threshold = config::peer_rpc_failure_eviction_threshold;

    _mgr->record_balanced_tablet(tablet_id, "host_keep", 9060, "cg_keep");
    _mgr->record_balanced_tablet(tablet_id, "host_evict", 9061, "cg_evict");

    // Exhaust failures only for host_evict.
    for (int i = 0; i < threshold; ++i) {
        _mgr->update_peer_candidate_on_rpc_failure(tablet_id, "host_evict", 9061);
    }

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].host, "host_keep");
}

TEST_F(CloudWarmUpManagerPeerTest, rpc_failure_noop_for_unknown_tablet) {
    // Should not crash.
    _mgr->update_peer_candidate_on_rpc_failure(99997, "1.2.3.4", 9060);
}

TEST_F(CloudWarmUpManagerPeerTest, rpc_failure_noop_for_unmatched_host_port) {
    constexpr int64_t tablet_id = 750;
    _mgr->record_balanced_tablet(tablet_id, "10.0.0.5", 9060, "cg_d");

    // Different host/port — should not modify the existing candidate.
    _mgr->update_peer_candidate_on_rpc_failure(tablet_id, "10.0.0.5", 9999);

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].consecutive_rpc_failures, 0);
}

// ============================================================
// remove_balanced_tablet
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, remove_balanced_tablet_clears_entry) {
    constexpr int64_t tablet_id = 800;
    _mgr->record_balanced_tablet(tablet_id, "10.0.0.6", 9060, "cg_e");

    auto before = _mgr->get_peer_candidates(tablet_id);
    ASSERT_FALSE(before.empty());

    _mgr->remove_balanced_tablet(tablet_id);

    auto after = _mgr->get_peer_candidates(tablet_id);
    EXPECT_TRUE(after.empty());
}

TEST_F(CloudWarmUpManagerPeerTest, remove_balanced_tablet_noop_for_unknown) {
    // Should not crash.
    _mgr->remove_balanced_tablet(99996);
}

TEST_F(CloudWarmUpManagerPeerTest, remove_does_not_affect_other_tablets) {
    _mgr->record_balanced_tablet(1001, "h1", 9060, "cg_1");
    _mgr->record_balanced_tablet(1002, "h2", 9060, "cg_2");

    _mgr->remove_balanced_tablet(1001);

    EXPECT_TRUE(_mgr->get_peer_candidates(1001).empty());
    auto other = _mgr->get_peer_candidates(1002);
    ASSERT_EQ(other.size(), 1u);
    EXPECT_EQ(other[0].host, "h2");
}

// ============================================================
// affinity: get_peer_candidates reorders by last_successful_cg (Issue 3)
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, affinity_puts_preferred_cg_first) {
    constexpr int64_t tablet_id = 1100;
    // Register two candidates from different CGs.
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");

    // Initially no preference: verify both are present.
    auto before = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(before.size(), 2u);

    // Simulate a successful read from cg_b.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_b");

    // After affinity update, cg_b candidate should be at the front.
    auto after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(after.size(), 2u);
    EXPECT_EQ(after[0].compute_group_id, "cg_b")
            << "preferred CG must be moved to front by stable_partition";
    EXPECT_EQ(after[1].compute_group_id, "cg_a");
}

TEST_F(CloudWarmUpManagerPeerTest, affinity_noop_when_already_first) {
    constexpr int64_t tablet_id = 1200;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");

    // host_b was inserted last → it is at front.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_b");
    auto result = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].compute_group_id, "cg_b");
}

TEST_F(CloudWarmUpManagerPeerTest, affinity_unchanged_by_get_only) {
    constexpr int64_t tablet_id = 1300;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");

    // Reading candidates must NOT persist the reordering into the stored list.
    // get_peer_candidates returns a copy; calling it again should yield the same order.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_a");
    auto first_call = _mgr->get_peer_candidates(tablet_id);
    auto second_call = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(first_call.size(), second_call.size());
    for (size_t i = 0; i < first_call.size(); ++i) {
        EXPECT_EQ(first_call[i].compute_group_id, second_call[i].compute_group_id)
                << "candidate order must be stable across repeated get calls";
    }
}

// ============================================================
// rotate_peer_candidate_on_cache_miss (Issue 3)
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, rotate_on_cache_miss_moves_candidate_to_end) {
    constexpr int64_t tablet_id = 1400;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");
    _mgr->record_balanced_tablet(tablet_id, "host_c", 9062, "cg_c");

    // Candidates are [host_c, host_b, host_a] (newest inserted first).
    // Rotate the front candidate (host_c).
    auto before = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(before.size(), 3u);
    const std::string front_host = before[0].host;
    _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, front_host, before[0].brpc_port);

    auto after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(after.size(), 3u);
    // front_host must have moved to the end.
    EXPECT_EQ(after[2].host, front_host) << "cache-miss candidate must be rotated to end of list";
    EXPECT_NE(after[0].host, front_host) << "front of list must have changed after rotate";
}

TEST_F(CloudWarmUpManagerPeerTest, rotate_on_cache_miss_noop_for_last_element) {
    constexpr int64_t tablet_id = 1500;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");

    auto candidates = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(candidates.size(), 2u);
    // Rotate the last candidate → should be a noop.
    const auto& last = candidates.back();
    _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, last.host, last.brpc_port);

    auto after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(after.size(), 2u);
    EXPECT_EQ(after[0].host, candidates[0].host);
    EXPECT_EQ(after[1].host, candidates[1].host);
}

TEST_F(CloudWarmUpManagerPeerTest, rotate_on_cache_miss_noop_for_unknown_tablet) {
    // Should not crash.
    _mgr->rotate_peer_candidate_on_cache_miss(99993, "1.2.3.4", 9060);
}

TEST_F(CloudWarmUpManagerPeerTest, rotate_on_cache_miss_noop_for_unmatched_host) {
    constexpr int64_t tablet_id = 1600;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");

    auto before = _mgr->get_peer_candidates(tablet_id);
    // Rotate with a host/port that doesn't match → noop.
    _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, "nonexistent", 9999);

    auto after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(after.size(), before.size());
    for (size_t i = 0; i < before.size(); ++i) {
        EXPECT_EQ(after[i].host, before[i].host);
    }
}

TEST_F(CloudWarmUpManagerPeerTest, rotate_distributes_over_all_candidates) {
    // Verifies that repeatedly rotating the front candidate cycles through all CGs.
    constexpr int64_t tablet_id = 1700;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9061, "cg_b");
    _mgr->record_balanced_tablet(tablet_id, "host_c", 9062, "cg_c");

    std::set<std::string> seen_front;
    // After N rotations of the front element every candidate should have been at front.
    for (int round = 0; round < 3; ++round) {
        auto cands = _mgr->get_peer_candidates(tablet_id);
        ASSERT_EQ(cands.size(), 3u);
        seen_front.insert(cands[0].host);
        _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, cands[0].host, cands[0].brpc_port);
    }
    EXPECT_EQ(seen_front.size(), 3u) << "each of the 3 candidates must appear at front once";
}

// Regression test for the rotate+affinity deadlock (Issue 3).
//
// Before fix: rotate_peer_candidate_on_cache_miss() moved the preferred CG to the end of
// the storage list, but get_peer_candidates() applies stable_partition every time using the
// unchanged last_successful_compute_group_id. On the very next call, stable_partition would
// promote the preferred CG back to the front — completely undoing the rotate.
//
// Fix: rotate also clears last_successful_compute_group_id when the rotated candidate belongs
// to the currently preferred CG.
TEST_F(CloudWarmUpManagerPeerTest, rotate_on_cache_miss_clears_affinity_for_preferred_cg) {
    constexpr int64_t tablet_id = 1800;
    // storage after inserts (last inserted goes to front):
    //   [cg_b@host_b, cg_a@host_a]
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9070, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9071, "cg_b");

    // Simulate a prior successful read from cg_b: cg_b is now the preferred CG.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_b");

    // get_peer_candidates() with affinity: cg_b moves to front → [cg_b, cg_a].
    auto cands_before = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(cands_before.size(), 2u);
    EXPECT_EQ(cands_before[0].compute_group_id, "cg_b") << "affinity must promote cg_b to front";

    // cg_b now misses its cache: rotate it away.
    _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, "host_b", 9071);

    // After rotate: cg_b should no longer be at the front AND the affinity for cg_b must have
    // been cleared.  Without the fix, get_peer_candidates() would stable_partition cg_b back
    // to front and the rotate would be a no-op from the caller's perspective.
    auto cands_after = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(cands_after.size(), 2u);
    EXPECT_EQ(cands_after[0].compute_group_id, "cg_a")
            << "after rotating preferred CG on miss, the other CG must appear first";
    EXPECT_NE(cands_after[0].compute_group_id, "cg_b")
            << "cg_b must not reappear at front after rotate cleared its affinity";
}

// Verify that rotate does NOT clear affinity when a non-preferred CG is rotated.
TEST_F(CloudWarmUpManagerPeerTest, rotate_non_preferred_cg_preserves_affinity) {
    constexpr int64_t tablet_id = 1900;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9080, "cg_a");
    _mgr->record_balanced_tablet(tablet_id, "host_b", 9081, "cg_b");

    // cg_a is preferred.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_a");

    // Rotate cg_b (non-preferred) on cache miss — affinity for cg_a must be preserved.
    _mgr->rotate_peer_candidate_on_cache_miss(tablet_id, "host_b", 9081);

    // cg_a is still preferred; get_peer_candidates() promotes it to front.
    auto cands = _mgr->get_peer_candidates(tablet_id);
    ASSERT_EQ(cands.size(), 2u);
    EXPECT_EQ(cands[0].compute_group_id, "cg_a")
            << "affinity for cg_a must survive a rotate of a different CG";
}

// ============================================================
// Tablet-level cooldown (consecutive all-miss)
// ============================================================

TEST_F(CloudWarmUpManagerPeerTest, cooldown_not_triggered_below_threshold) {
    constexpr int64_t tablet_id = 2000;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");

    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    config::peer_all_miss_cooldown_threshold = 3;
    Defer restore {[old_threshold]() { config::peer_all_miss_cooldown_threshold = old_threshold; }};

    // Record 2 all-miss (below threshold of 3) — should NOT enter cooldown.
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);

    EXPECT_FALSE(_mgr->is_peer_cooldown(tablet_id));
    auto cands = _mgr->get_peer_candidates(tablet_id);
    EXPECT_EQ(cands.size(), 1u) << "below threshold: candidates must still be returned";
}

TEST_F(CloudWarmUpManagerPeerTest, cooldown_triggered_at_threshold) {
    constexpr int64_t tablet_id = 2001;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");

    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    const int64_t old_duration = config::peer_all_miss_cooldown_duration_s;
    config::peer_all_miss_cooldown_threshold = 3;
    config::peer_all_miss_cooldown_duration_s = 600; // 10 minutes
    Defer restore {[old_threshold, old_duration]() {
        config::peer_all_miss_cooldown_threshold = old_threshold;
        config::peer_all_miss_cooldown_duration_s = old_duration;
    }};

    // Hit threshold exactly.
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);

    EXPECT_TRUE(_mgr->is_peer_cooldown(tablet_id));
    auto cands = _mgr->get_peer_candidates(tablet_id);
    EXPECT_TRUE(cands.empty()) << "cooldown active: get_peer_candidates must return empty";
}

TEST_F(CloudWarmUpManagerPeerTest, cooldown_reset_by_success) {
    constexpr int64_t tablet_id = 2002;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");

    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    config::peer_all_miss_cooldown_threshold = 3;
    Defer restore {[old_threshold]() { config::peer_all_miss_cooldown_threshold = old_threshold; }};

    // Accumulate 2 misses, then a success resets the counter.
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_a");

    // 2 more misses — still below threshold (counter was reset).
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);

    EXPECT_FALSE(_mgr->is_peer_cooldown(tablet_id));
    auto cands = _mgr->get_peer_candidates(tablet_id);
    EXPECT_EQ(cands.size(), 1u);
}

TEST_F(CloudWarmUpManagerPeerTest, cooldown_success_clears_active_cooldown) {
    constexpr int64_t tablet_id = 2003;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");

    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    config::peer_all_miss_cooldown_threshold = 2;
    Defer restore {[old_threshold]() { config::peer_all_miss_cooldown_threshold = old_threshold; }};

    // Enter cooldown.
    _mgr->record_peer_all_miss(tablet_id);
    _mgr->record_peer_all_miss(tablet_id);
    ASSERT_TRUE(_mgr->is_peer_cooldown(tablet_id));

    // A success while in cooldown clears it immediately.
    _mgr->update_peer_candidate_on_success(tablet_id, "cg_a");
    EXPECT_FALSE(_mgr->is_peer_cooldown(tablet_id));
    auto cands = _mgr->get_peer_candidates(tablet_id);
    EXPECT_EQ(cands.size(), 1u);
}

TEST_F(CloudWarmUpManagerPeerTest, cooldown_expires_after_duration) {
    constexpr int64_t tablet_id = 2004;
    _mgr->record_balanced_tablet(tablet_id, "host_a", 9060, "cg_a");

    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    const int64_t old_duration = config::peer_all_miss_cooldown_duration_s;
    config::peer_all_miss_cooldown_threshold = 1;
    config::peer_all_miss_cooldown_duration_s = 0; // expire immediately
    Defer restore {[old_threshold, old_duration]() {
        config::peer_all_miss_cooldown_threshold = old_threshold;
        config::peer_all_miss_cooldown_duration_s = old_duration;
    }};

    _mgr->record_peer_all_miss(tablet_id);
    // cooldown_duration_s = 0 → cooldown_until_ms = now + 0 → already expired.
    // is_peer_cooldown checks now < cooldown_until_ms, which is false.
    EXPECT_FALSE(_mgr->is_peer_cooldown(tablet_id));
    auto cands = _mgr->get_peer_candidates(tablet_id);
    EXPECT_EQ(cands.size(), 1u) << "expired cooldown: candidates must be returned";
}

TEST_F(CloudWarmUpManagerPeerTest, cooldown_noop_for_unknown_tablet) {
    // Should not crash or create phantom entries.
    _mgr->record_peer_all_miss(99999);
    EXPECT_FALSE(_mgr->is_peer_cooldown(99999));
}

} // namespace doris
